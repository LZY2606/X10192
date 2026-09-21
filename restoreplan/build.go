package restoreplan

import (
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// FromRDB decodes r from an RDB stream and builds a RestorePlan from the
// decoded objects. It only reads bytes; it never connects to a Redis server.
func FromRDB(r io.Reader, opts Options) (*RestorePlan, error) {
	dec := core.NewDecoder(r).WithSpecialOpCode()
	var objects []model.RedisObject
	err := dec.Parse(func(object model.RedisObject) bool {
		objects = append(objects, object)
		return true
	})
	if err != nil {
		return nil, err
	}
	return Build(objects, opts), nil
}

// Build produces a deterministic RestorePlan from decoded RDB objects.
// The result does not depend on the order of objects nor on Go map
// iteration order: items are sorted by (db, key, kind, part) and every
// collection derived from a map is sorted before use.
func Build(objects []model.RedisObject, opts Options) *RestorePlan {
	opts.normalize()
	b := &builder{opts: opts}
	for _, obj := range objects {
		b.planObject(obj)
	}
	return b.finish(len(objects))
}

type builder struct {
	opts     Options
	items    []*PlanItem
	warnings []Warning
	blockers []Blocker
	skipped  int
}

func (b *builder) finish(objectCount int) *RestorePlan {
	// deterministic order: db, key, kind rank, source part
	sort.SliceStable(b.items, func(i, j int) bool {
		a, c := b.items[i], b.items[j]
		if a.DB != c.DB {
			return a.DB < c.DB
		}
		if a.Key != c.Key {
			return a.Key < c.Key
		}
		if kindRank[a.Kind] != kindRank[c.Kind] {
			return kindRank[a.Kind] < kindRank[c.Kind]
		}
		return a.Source.Part < c.Source.Part
	})
	// insert explicit select-db items whenever the target db changes
	withSelect := make([]*PlanItem, 0, len(b.items)+4)
	currentDB := -1
	for _, item := range b.items {
		if item.DB != currentDB {
			currentDB = item.DB
			withSelect = append(withSelect, &PlanItem{
				Kind: KindSelectDB,
				DB:   currentDB,
				CreateCommands: []Command{{
					Name: "SELECT",
					Args: []string{fmt.Sprintf("%d", currentDB)},
					Note: "explicit database switch",
				}},
				Source: Source{DB: currentDB, Type: "meta", Part: "select-db"},
			})
		}
		withSelect = append(withSelect, item)
	}
	// assign stable ids and resolve dependencies
	idByDepKey := make(map[string]string, len(withSelect))
	for i, item := range withSelect {
		item.ID = fmt.Sprintf("pi-%06d", i+1)
		if item.depKey != "" {
			idByDepKey[item.depKey] = item.ID
		}
	}
	for _, item := range withSelect {
		for _, dep := range item.depends {
			if id, ok := idByDepKey[dep]; ok {
				item.DependsOn = append(item.DependsOn, id)
			}
		}
	}
	// attach item ids to plan-level warnings/blockers raised during planning
	plan := &RestorePlan{
		Version:       1,
		RefTime:       b.opts.RefTime.UTC().Format(time.RFC3339Nano),
		ExpiredPolicy: b.opts.ExpiredPolicy,
		Profile:       b.opts.Profile,
		Items:         withSelect,
		Warnings:      b.warnings,
		Blockers:      b.blockers,
	}
	plan.Stats = Stats{
		Objects:  objectCount,
		Items:    len(withSelect),
		Skipped:  b.skipped,
		Warnings: len(b.warnings),
		Blockers: len(b.blockers),
	}
	return plan
}

func (b *builder) planObject(obj model.RedisObject) {
	switch o := obj.(type) {
	case *model.StringObject:
		b.planString(o)
	case *model.ListObject:
		b.planList(o)
	case *model.SetObject:
		b.planSet(o)
	case *model.HashObject:
		b.planHash(o)
	case *model.ZSetObject:
		b.planZSet(o)
	case *model.StreamObject:
		b.planStream(o)
	case *model.FunctionsObject:
		b.planFunction(o)
	case *model.ModuleTypeObject:
		b.blockers = append(b.blockers, Blocker{
			Reason:   "module value cannot be losslessly restored as plain commands",
			Evidence: fmt.Sprintf("db=%d key=%q moduleType=%q encoding=%q", o.GetDBIndex(), o.GetKey(), o.ModuleType, o.GetEncoding()),
		})
	case *model.AuxObject, *model.DBSizeObject:
		// rdb metadata, not user data; intentionally not planned
	default:
		b.blockers = append(b.blockers, Blocker{
			Reason:   "unsupported object type or encoding; not silently skipped",
			Evidence: fmt.Sprintf("db=%d key=%q type=%q encoding=%q", obj.GetDBIndex(), obj.GetKey(), obj.GetType(), obj.GetEncoding()),
		})
	}
}

// expiry computes the expiration semantics of an object and applies the
// expired-key policy. It returns the expiry descriptor, the follow-up
// PEXPIREAT command, and whether the object must be skipped.
func (b *builder) expiry(base *model.BaseObject) (*Expiry, *Command, bool) {
	exp := base.GetExpiration()
	if exp == nil {
		return nil, nil, false
	}
	absMs := exp.UnixMilli()
	ttlMs := absMs - b.opts.RefTime.UnixMilli()
	desc := &Expiry{
		AbsUnixMs: absMs,
		TTLMs:     ttlMs,
		Expired:   ttlMs <= 0,
		Command:   "PEXPIREAT",
	}
	cmd := &Command{
		Name: "PEXPIREAT",
		Args: []string{base.GetKey(), fmt.Sprintf("%d", absMs)},
		Note: "absolute expiration; replays identically regardless of restore delay",
	}
	if !desc.Expired {
		return desc, cmd, false
	}
	where := fmt.Sprintf("db=%d key=%q", base.GetDBIndex(), base.GetKey())
	switch b.opts.ExpiredPolicy {
	case ExpiredKeep:
		b.warnings = append(b.warnings, Warning{
			Message: fmt.Sprintf("%s expired at %s (before ref time); restored anyway and may expire immediately",
				where, exp.UTC().Format(time.RFC3339Nano)),
		})
		return desc, cmd, false
	case ExpiredError:
		b.blockers = append(b.blockers, Blocker{
			Reason: "key is already expired at restore reference time",
			Evidence: fmt.Sprintf("%s expiration=%s refTime=%s",
				where, exp.UTC().Format(time.RFC3339Nano), b.opts.RefTime.UTC().Format(time.RFC3339Nano)),
		})
		return nil, nil, true
	default: // ExpiredSkip
		b.skipped++
		b.warnings = append(b.warnings, Warning{
			Message: fmt.Sprintf("%s expired at %s (before ref time); skipped by policy",
				where, exp.UTC().Format(time.RFC3339Nano)),
		})
		return nil, nil, true
	}
}

func groupID(db int, key string) string {
	return fmt.Sprintf("obj:%d:%s", db, key)
}

func chunkCount(total, chunk int) int {
	if total == 0 {
		return 0
	}
	return (total + chunk - 1) / chunk
}

func (b *builder) newItem(kind string, base *model.BaseObject, part string) *PlanItem {
	return &PlanItem{
		Kind: kind,
		DB:   base.GetDBIndex(),
		Key:  base.GetKey(),
		Source: Source{
			DB:       base.GetDBIndex(),
			Key:      base.GetKey(),
			Type:     base.Type,
			Encoding: base.GetEncoding(),
			Part:     part,
		},
	}
}

func (b *builder) planString(o *model.StringObject) {
	expiry, expireCmd, skip := b.expiry(o.BaseObject)
	if skip {
		return
	}
	item := b.newItem(KindString, o.BaseObject, "")
	item.Group = groupID(o.GetDBIndex(), o.GetKey())
	item.depKey = item.Group
	item.CreateCommands = []Command{{
		Name: "SET",
		Args: []string{o.GetKey(), string(o.Value)},
	}}
	item.Expiry = expiry
	if expireCmd != nil {
		item.FollowUpCommands = []Command{*expireCmd}
		item.Atomicity = atomicityNote
	}
	b.items = append(b.items, item)
}

func (b *builder) planList(o *model.ListObject) {
	expiry, expireCmd, skip := b.expiry(o.BaseObject)
	if skip {
		return
	}
	item := b.newItem(KindList, o.BaseObject, "")
	item.Group = groupID(o.GetDBIndex(), o.GetKey())
	item.depKey = item.Group
	chunk := b.opts.ChunkSize
	for start := 0; start < len(o.Values); start += chunk {
		end := start + chunk
		if end > len(o.Values) {
			end = len(o.Values)
		}
		args := make([]string, 0, end-start+1)
		args = append(args, o.GetKey())
		for _, v := range o.Values[start:end] {
			args = append(args, string(v))
		}
		item.CreateCommands = append(item.CreateCommands, Command{Name: "RPUSH", Args: args})
	}
	item.Expiry = expiry
	if expireCmd != nil {
		item.FollowUpCommands = []Command{*expireCmd}
	}
	if len(item.CreateCommands)+len(item.FollowUpCommands) > 1 {
		item.Atomicity = atomicityNote
	}
	b.items = append(b.items, item)
}

func (b *builder) planSet(o *model.SetObject) {
	expiry, expireCmd, skip := b.expiry(o.BaseObject)
	if skip {
		return
	}
	item := b.newItem(KindSet, o.BaseObject, "")
	item.Group = groupID(o.GetDBIndex(), o.GetKey())
	item.depKey = item.Group
	members := make([]string, 0, len(o.Members))
	for _, m := range o.Members {
		members = append(members, string(m))
	}
	// sets are unordered; sorting keeps the plan byte-stable
	sort.Strings(members)
	chunk := b.opts.ChunkSize
	for start := 0; start < len(members); start += chunk {
		end := start + chunk
		if end > len(members) {
			end = len(members)
		}
		args := append([]string{o.GetKey()}, members[start:end]...)
		item.CreateCommands = append(item.CreateCommands, Command{Name: "SADD", Args: args})
	}
	item.Expiry = expiry
	if expireCmd != nil {
		item.FollowUpCommands = []Command{*expireCmd}
	}
	if len(item.CreateCommands)+len(item.FollowUpCommands) > 1 {
		item.Atomicity = atomicityNote
	}
	b.items = append(b.items, item)
}

func (b *builder) planHash(o *model.HashObject) {
	expiry, expireCmd, skip := b.expiry(o.BaseObject)
	if skip {
		return
	}
	item := b.newItem(KindHash, o.BaseObject, "")
	item.Group = groupID(o.GetDBIndex(), o.GetKey())
	item.depKey = item.Group
	// hash fields come from a Go map; sort them for determinism
	fields := make([]string, 0, len(o.Hash))
	for f := range o.Hash {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	chunk := b.opts.ChunkSize
	for start := 0; start < len(fields); start += chunk {
		end := start + chunk
		if end > len(fields) {
			end = len(fields)
		}
		args := []string{o.GetKey()}
		for _, f := range fields[start:end] {
			args = append(args, f, string(o.Hash[f]))
		}
		item.CreateCommands = append(item.CreateCommands, Command{Name: "HSET", Args: args})
	}
	item.Expiry = expiry
	if expireCmd != nil {
		item.FollowUpCommands = append(item.FollowUpCommands, *expireCmd)
	}
	// hash field expirations (Redis 7.4+ HFE)
	if len(o.FieldExpirations) > 0 {
		hfeFields := make([]string, 0, len(o.FieldExpirations))
		for f := range o.FieldExpirations {
			hfeFields = append(hfeFields, f)
		}
		sort.Strings(hfeFields)
		if !b.opts.Profile.HashFieldExpire {
			b.blockers = append(b.blockers, Blocker{
				Reason: "hash field expiration requires HPEXPIREAT (Redis >= 7.4), not supported by target profile",
				Evidence: fmt.Sprintf("db=%d key=%q fieldsWithTTL=%d target=%s@%s",
					o.GetDBIndex(), o.GetKey(), len(hfeFields), b.opts.Profile.Name, b.opts.Profile.Version),
			})
		} else {
			for _, f := range hfeFields {
				item.FollowUpCommands = append(item.FollowUpCommands, Command{
					Name: "HPEXPIREAT",
					Args: []string{o.GetKey(), fmt.Sprintf("%d", o.FieldExpirations[f]), "FIELDS", "1", f},
					Note: "absolute field expiration in milliseconds",
				})
			}
		}
	}
	if len(item.CreateCommands)+len(item.FollowUpCommands) > 1 {
		item.Atomicity = atomicityNote
	}
	b.items = append(b.items, item)
}

func (b *builder) planZSet(o *model.ZSetObject) {
	expiry, expireCmd, skip := b.expiry(o.BaseObject)
	if skip {
		return
	}
	item := b.newItem(KindZSet, o.BaseObject, "")
	item.Group = groupID(o.GetDBIndex(), o.GetKey())
	item.depKey = item.Group
	entries := make([]*model.ZSetEntry, len(o.Entries))
	copy(entries, o.Entries)
	// zset members are unique; sorting by member keeps the plan byte-stable
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Member < entries[j].Member
	})
	chunk := b.opts.ChunkSize
	for start := 0; start < len(entries); start += chunk {
		end := start + chunk
		if end > len(entries) {
			end = len(entries)
		}
		args := []string{o.GetKey()}
		for _, e := range entries[start:end] {
			args = append(args, formatScore(e.Score), e.Member)
		}
		item.CreateCommands = append(item.CreateCommands, Command{Name: "ZADD", Args: args})
	}
	item.Expiry = expiry
	if expireCmd != nil {
		item.FollowUpCommands = []Command{*expireCmd}
	}
	if len(item.CreateCommands)+len(item.FollowUpCommands) > 1 {
		item.Atomicity = atomicityNote
	}
	b.items = append(b.items, item)
}
