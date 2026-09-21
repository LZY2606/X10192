package restore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hdt3213/rdb/model"
)

func bytesSortString(value string) string {
	return string([]byte(value))
}

func cloneByteSlices(values [][]byte) [][]byte {
	out := make([][]byte, len(values))
	copy(out, values)
	return out
}

func sortByteSlices(values [][]byte) {
	sort.Slice(values, func(i, j int) bool { return bytesSortString(string(values[i])) < bytesSortString(string(values[j])) })
}

func shortDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:12])
}

func shortDigestObject(object model.RedisObject) string {
	return fmt.Sprintf("%d|%s|%s|%s", object.GetDBIndex(), object.GetType(), object.GetEncoding(), object.GetKey())
}

func sanitizeID(value string) string {
	var b strings.Builder
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func itemID(kind string, db int, key, typ string) string {
	ident := typ
	if key != "" {
		ident += ":" + key
	}
	if kind == "key" || kind == "module-key" {
		return fmt.Sprintf("db%010d:%s:%s", db, sanitizeID(ident), shortDigestString(strconv.Itoa(db) + "|" + typ + "|" + key)[:12])
	}
	return fmt.Sprintf("%s:db%010d:%s:%s", kind, db, sanitizeID(ident), shortDigestString(strconv.Itoa(db) + "|" + kind + "|" + typ + "|" + key)[:12])
}

func shortDigestString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func selectID(db int) string {
	return fmt.Sprintf("db%010d:select", db)
}

func sourceSignature(source Source) string {
	return strings.Join([]string{source.Kind, strconv.Itoa(source.DB), source.ObjectType, source.Encoding, source.Key}, "\x00")
}

func (p *Planner) command(item *PlanItem, phase, role string, args [][]byte) PlannedCommand {
	return PlannedCommand{
		ID:     fmt.Sprintf("%s:%s:%03d", item.ID, phase, len(item.CreateCommands)+len(item.FollowUpCommands)+1),
		ItemID: item.ID,
		DB:     item.TargetDB,
		Phase:  phase,
		Role:   role,
		Args:   argsToArg(args),
	}
}

func argsToArg(args [][]byte) []Arg {
	out := make([]Arg, 0, len(args))
	for _, arg := range args {
		out = append(out, newArg(arg))
	}
	return out
}

func (p *Planner) block(item *PlanItem, code, message string, evidence []string) {
	item.Status = ItemStatusBlocked
	item.Blockers = append(item.Blockers, Finding{Code: code, Message: message, Source: item.Source, Evidence: evidence})
}

func (p *Planner) warn(item *PlanItem, code, message string, evidence []string) {
	item.Warnings = append(item.Warnings, Warning{Code: code, Message: message, Source: item.Source, Evidence: evidence})
}

func dedupWarnings(warnings []Warning) []Warning {
	type key struct {
		code, source, message string
	}
	seen := make(map[key]struct{})
	out := make([]Warning, 0, len(warnings))
	for _, warning := range warnings {
		k := key{warning.Code, sourceSignature(warning.Source), warning.Message}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, warning)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Code != out[j].Code {
			return out[i].Code < out[j].Code
		}
		return sourceSignature(out[i].Source) < sourceSignature(out[j].Source)
	})
	return out
}

func (p *Planner) addEvictionMetadata(item *PlanItem, object model.RedisObject) {
	var evidence []string
	if info, ok := object.(model.EvictionInfo); ok {
		if info.GetIdleTime() >= 0 {
			evidence = append(evidence, "lru-idle-seconds="+strconv.FormatInt(info.GetIdleTime(), 10))
		}
		if info.GetFreq() >= 0 {
			evidence = append(evidence, "lfu-frequency="+strconv.FormatInt(info.GetFreq(), 10))
		}
	}
	if len(evidence) > 0 {
		p.warn(item, "eviction-metadata-not-restorable", "LRU/LFU eviction metadata cannot be set by generic key commands", evidence)
	}
}

func expirationUnit(object model.RedisObject) string {
	if base := baseObjectOf(object); base != nil && base.ExpireUnit != "" {
		return base.ExpireUnit
	}
	return model.MillisecondExpiration
}

func baseObjectOf(object model.RedisObject) *model.BaseObject {
	switch obj := object.(type) {
	case *model.StringObject:
		return obj.BaseObject
	case *model.ListObject:
		return obj.BaseObject
	case *model.SetObject:
		return obj.BaseObject
	case *model.ZSetObject:
		return obj.BaseObject
	case *model.HashObject:
		return obj.BaseObject
	case *model.StreamObject:
		return obj.BaseObject
	case *model.ModuleTypeObject:
		return obj.BaseObject
	case *model.FunctionsObject:
		return obj.BaseObject
	}
	return nil
}

func (p *Planner) addKeyExpiration(item *PlanItem, object model.RedisObject) {
	expireAt := object.GetExpiration()
	if expireAt == nil {
		return
	}
	expireAtUTC := expireAt.UTC()
	ttl := expireAtUTC.Sub(p.opts.ReferenceTime).Milliseconds()
	plan := ExpirationPlan{
		SourceUnit:         expirationUnit(object),
		ExpireAt:           expireAtUTC,
		ReferenceTime:      p.opts.ReferenceTime,
		RelativeTTLMillis:  ttl,
		ExpiredAtReference: ttl <= 0,
	}
	if plan.ExpiredAtReference {
		switch p.opts.ExpiredKeys {
		case ExpiredKeySkip:
			item.Status = ItemStatusSkipped
			item.CreateCommands = nil
			item.FollowUpCommands = nil
			item.FieldExpirations = nil
			plan.Action = "skip"
			p.warn(item, "expired-key-skipped", "key was already expired at the restore reference time", []string{"expireAt=" + formatMillis(expireAtUTC), "referenceTime=" + formatMillis(p.opts.ReferenceTime)})
		case ExpiredKeyKeep:
			plan.Action = "keep-without-ttl"
			p.warn(item, "expired-key-kept-without-ttl", "expired key is restored without expiration as requested by policy", []string{"expireAt=" + formatMillis(expireAtUTC)})
		case ExpiredKeyError:
			plan.Action = "error"
			p.block(item, "expired-key", "key was already expired at the restore reference time", []string{"expireAt=" + formatMillis(expireAtUTC)})
		}
		item.Expiration = &plan
		return
	}

	var args [][]byte
	if p.opts.ExpirationMode == ExpirationAbsolute {
		args = [][]byte{[]byte("PEXPIREAT"), []byte(item.Key), []byte(strconv.FormatInt(expireAtUTC.UnixNano()/int64(time.Millisecond), 10))}
	} else if plan.SourceUnit == model.SecondExpiration {
		relativeSeconds := (ttl + 999) / 1000
		args = [][]byte{[]byte("EXPIRE"), []byte(item.Key), []byte(strconv.FormatInt(relativeSeconds, 10))}
	} else {
		args = [][]byte{[]byte("PEXPIRE"), []byte(item.Key), []byte(strconv.FormatInt(ttl, 10))}
	}
	cmd := p.command(item, PhaseFollowUp, RoleExpiration, args)
	if len(item.CreateCommands) > 0 {
		cmd.DependsOn = append(cmd.DependsOn, item.CreateCommands[0].ID)
	}
	item.FollowUpCommands = append(item.FollowUpCommands, cmd)
	plan.CommandID = cmd.ID
	plan.Action = "set"
	item.Expiration = &plan
}

func formatMillis(t time.Time) string {
	return strconv.FormatInt(t.UnixNano()/int64(time.Millisecond), 10)
}

func parseFunctionHeader(payload string) (engine, name string) {
	engine = "lua"
	if strings.HasPrefix(payload, "#!") {
		line := payload
		if idx := strings.IndexByte(line, '\n'); idx >= 0 {
			line = line[:idx]
		}
		for _, part := range strings.Fields(line[2:]) {
			if strings.HasPrefix(part, "name=") {
				name = strings.TrimPrefix(part, "name=")
			} else if !strings.Contains(part, "=") {
				engine = part
			}
		}
	}
	if name == "" {
		name = "library-" + shortDigestString(payload)[:12]
	}
	return engine, name
}

func streamID(id *model.StreamId) string {
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Sequence, 10)
}

func compareStreamID(a, b *model.StreamId) bool {
	if a.Ms != b.Ms {
		return a.Ms < b.Ms
	}
	return a.Sequence < b.Sequence
}

func (p *Planner) addHashFieldExpirations(item *PlanItem, obj *model.HashObject, fields []string) {
	if len(obj.FieldExpirations) == 0 {
		return
	}
	if !p.opts.Profile.HashFieldExpiration {
		p.block(item, "hash-field-expiration-unsupported", "target profile does not support hash field expiration commands", []string{"HPEXPIREAT", "HPERSIST"})
		return
	}

	now := p.opts.ReferenceTime
	grouped := make(map[int64][]string)
	noTTL := make([]string, 0)
	expired := make([]string, 0)
	for _, field := range fields {
		expireMs, exists := obj.FieldExpirations[field]
		if !exists {
			continue
		}
		if expireMs == 0 {
			noTTL = append(noTTL, field)
			continue
		}
		expireAt := time.Unix(0, expireMs*int64(time.Millisecond)).UTC()
		ttl := expireAt.Sub(now).Milliseconds()
		fieldPlan := FieldExpirationPlan{
			Field:              newStringArg(field),
			ExpireAt:           expireAt,
			ReferenceTime:      now,
			RelativeTTLMillis:  ttl,
			SourceUnit:         model.MillisecondExpiration,
			ExpiredAtReference: ttl <= 0,
		}
		if fieldPlan.ExpiredAtReference {
			fieldPlan.Action = "delete-field"
			expired = append(expired, field)
			p.warn(item, "expired-hash-field-skipped", "hash field was already expired at the restore reference time", []string{"field=" + field, "expireAt=" + formatMillis(expireAt)})
		} else {
			fieldPlan.Action = "set"
			grouped[expireMs] = append(grouped[expireMs], field)
		}
		item.FieldExpirations = append(item.FieldExpirations, fieldPlan)
	}
	for _, field := range fields {
		if _, exists := obj.FieldExpirations[field]; exists && obj.FieldExpirations[field] == 0 {
			item.FieldExpirations = append(item.FieldExpirations, FieldExpirationPlan{
				Field:         newStringArg(field),
				ReferenceTime: now,
				SourceUnit:    model.MillisecondExpiration,
				Action:        "persistent",
			})
		}
	}
	sort.Slice(item.FieldExpirations, func(i, j int) bool {
		return item.FieldExpirations[i].Field.Text < item.FieldExpirations[j].Field.Text
	})

	expireTimes := make([]int64, 0, len(grouped))
	for expireMs := range grouped {
		expireTimes = append(expireTimes, expireMs)
	}
	sort.Slice(expireTimes, func(i, j int) bool { return expireTimes[i] < expireTimes[j] })
	for _, expireMs := range expireTimes {
		expireFields := grouped[expireMs]
		sort.Slice(expireFields, func(i, j int) bool { return bytesSortString(expireFields[i]) < bytesSortString(expireFields[j]) })
		args := [][]byte{[]byte("HPEXPIREAT"), []byte(obj.Key), []byte(strconv.FormatInt(expireMs, 10)), []byte("FIELDS"), []byte(strconv.Itoa(len(expireFields)))}
		for _, field := range expireFields {
			args = append(args, []byte(field))
		}
		cmd := p.command(item, PhaseFollowUp, RoleHashFieldExpiration, args)
		cmd.DependsOn = append(cmd.DependsOn, item.CreateCommands[0].ID)
		item.FollowUpCommands = append(item.FollowUpCommands, cmd)
		for i := range item.FieldExpirations {
			if item.FieldExpirations[i].Action == "set" && item.FieldExpirations[i].ExpireAt.UnixNano()/int64(time.Millisecond) == expireMs {
				item.FieldExpirations[i].CommandID = cmd.ID
			}
		}
	}
	if len(noTTL) > 0 {
		sort.Slice(noTTL, func(i, j int) bool { return bytesSortString(noTTL[i]) < bytesSortString(noTTL[j]) })
		args := [][]byte{[]byte("HPERSIST"), []byte(obj.Key), []byte("FIELDS"), []byte(strconv.Itoa(len(noTTL)))}
		for _, field := range noTTL {
			args = append(args, []byte(field))
		}
		item.FollowUpCommands = append(item.FollowUpCommands, p.command(item, PhaseFollowUp, RoleHashFieldExpiration, args))
	}
	if len(expired) > 0 {
		sort.Slice(expired, func(i, j int) bool { return bytesSortString(expired[i]) < bytesSortString(expired[j]) })
		args := [][]byte{[]byte("HDEL"), []byte(obj.Key)}
		for _, field := range expired {
			args = append(args, []byte(field))
		}
		cmd := p.command(item, PhaseFollowUp, RoleHashFieldExpiration, args)
		cmd.DependsOn = append(cmd.DependsOn, item.CreateCommands[0].ID)
		item.FollowUpCommands = append(item.FollowUpCommands, cmd)
		for i := range item.FieldExpirations {
			if item.FieldExpirations[i].Action == "delete-field" {
				item.FieldExpirations[i].CommandID = cmd.ID
			}
		}
	}
}
