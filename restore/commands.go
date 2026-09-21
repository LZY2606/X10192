package restore

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/hdt3213/rdb/model"
)

func cmd(args ...string) *Command {
	return &Command{Args: args}
}

func singleGroup(purpose string, c *Command) *CommandGroup {
	return &CommandGroup{Commands: []*Command{c}, Atomicity: AtomicitySingle, Purpose: purpose}
}

func formatFloat(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
}

func formatID(id *model.StreamId) string {
	if id == nil {
		return "0-0"
	}
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Sequence, 10)
}

// planKeyContent fills create nodes for the five plain types, streams,
// modules and unknown encodings.
func (b *builder) planKeyContent(item *PlanItem, obj model.RedisObject) {
	switch o := obj.(type) {
	case *model.StringObject:
		b.addCreateNode(item, cmd("SET", o.Key, string(o.Value)))
	case *model.ListObject:
		args := append([]string{"RPUSH", o.Key}, bytesToStrings(o.Values)...)
		b.addCreateNode(item, cmd(args...))
	case *model.SetObject:
		members := bytesToStrings(o.Members)
		sort.Strings(members)
		args := append([]string{"SADD", o.Key}, members...)
		b.addCreateNode(item, cmd(args...))
	case *model.HashObject:
		b.planHash(item, o)
	case *model.ZSetObject:
		b.planZSet(item, o)
	case *model.StreamObject:
		b.planStream(item, o)
	case *model.ModuleTypeObject:
		b.block(item, WarnModuleUnsupported,
			fmt.Sprintf("module key %q (module %q) cannot be restored: module value type %q is not registered and its binary payload cannot be reproduced with built-in commands",
				o.Key, o.ModuleType, o.GetType()))
	default:
		b.block(item, WarnUnknownObject,
			fmt.Sprintf("key %q has unsupported object type %q (encoding %q)",
				obj.GetKey(), obj.GetType(), obj.GetEncoding()))
	}
}

func bytesToStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func (b *builder) addCreateNode(item *PlanItem, c *Command) {
	item.Nodes = append(item.Nodes, &PlanNode{
		ID:     nodeID(item, nodeKindCreate),
		Kind:   nodeKindCreate,
		Label:  "create " + item.Type,
		Groups: []*CommandGroup{singleGroup("create", c)},
	})
}

func (b *builder) planHash(item *PlanItem, o *model.HashObject) {
	fields := sortedByteStrings(o.Hash)
	args := make([]string, 0, 2+2*len(fields))
	args = append(args, "HSET", o.Key)
	for _, f := range fields {
		args = append(args, f, string(o.Hash[f]))
	}
	b.addCreateNode(item, cmd(args...))

	// Field-level expirations (RDB hash HFE). 0 means "no TTL".
	withTTL := make([]string, 0)
	for f, at := range o.FieldExpirations {
		if at != 0 {
			withTTL = append(withTTL, f)
		}
	}
	if len(withTTL) == 0 {
		return
	}
	sort.Strings(withTTL)
	if !b.opts.Target.Supports(FeatureHPEXPIREAT) {
		b.warnItem(item, nil, WarnHashFieldExpiryUnsupported,
			fmt.Sprintf("hash %q has %d field-level TTL(s); HPEXPIREAT requires Redis 7.4 and target %s does not provide it, field TTLs are dropped",
				o.Key, len(withTTL), b.opts.Target.Name),
			item.ID)
		return
	}
	// Group fields sharing the same deadline into one HPEXPIREAT command so
	// multi-field hashes do not emit a command per field. Groups are emitted
	// in ascending deadline order; fields inside are sorted.
	byDeadline := make(map[int64][]string)
	for _, f := range withTTL {
		at := o.FieldExpirations[f]
		byDeadline[at] = append(byDeadline[at], f)
	}
	deadlines := make([]int64, 0, len(byDeadline))
	for d := range byDeadline {
		deadlines = append(deadlines, d)
	}
	sort.Slice(deadlines, func(i, j int) bool { return deadlines[i] < deadlines[j] })
	create := nodeByKind(item, nodeKindCreate)
	groups := make([]*CommandGroup, 0, len(deadlines))
	for _, d := range deadlines {
		fs := byDeadline[d]
		sort.Strings(fs)
		args := make([]string, 0, 5+len(fs))
		args = append(args, "HPEXPIREAT", o.Key, strconv.FormatInt(d, 10),
			"FIELDS", strconv.Itoa(len(fs)))
		args = append(args, fs...)
		groups = append(groups, &CommandGroup{
			Commands:  []*Command{cmd(args...)},
			Atomicity: AtomicitySingle,
			Purpose:   "field-expire",
		})
	}
	node := &PlanNode{
		ID:        nodeID(item, "field-expire"),
		Kind:      nodeKindExpire,
		Label:     "hash field expirations",
		DependsOn: []string{create.ID},
		Groups:    groups,
		Notes: []string{
			"HPEXPIREAT arguments are absolute millisecond unix deadlines copied from the RDB HFE block",
			"field-expire groups run after create and are not atomic with HSET",
		},
	}
	item.Nodes = append(item.Nodes, node)
}

func (b *builder) planZSet(item *PlanItem, o *model.ZSetObject) {
	entries := make([]*model.ZSetEntry, len(o.Entries))
	copy(entries, o.Entries)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Member < entries[j].Member })
	args := make([]string, 0, 2+2*len(entries))
	args = append(args, "ZADD", o.Key)
	for _, e := range entries {
		args = append(args, formatFloat(e.Score), e.Member)
	}
	b.addCreateNode(item, cmd(args...))
}

func nodeByKind(item *PlanItem, kind string) *PlanNode {
	for _, n := range item.Nodes {
		if n.Kind == kind {
			return n
		}
	}
	return nil
}

// planKeyExpiration attaches absolute + relative TTL semantics and enforces
// the configured ExpiredPolicy.
func (b *builder) planKeyExpiration(item *PlanItem, obj model.RedisObject, precision string) {
	exp := obj.GetExpiration()
	if exp == nil {
		return
	}
	if precision == "" {
		precision = "millisecond"
	}
	absMS := exp.UnixNano() / int64(time.Millisecond)
	refMS := b.opts.ReferenceTime.UnixNano() / int64(time.Millisecond)
	relative := absMS - refMS
	if relative < 0 {
		relative = 0
	}
	spec := &ExpirationSpec{
		AbsoluteAtMS:  absMS,
		RelativeTTLMS: relative,
		Precision:     precision,
		Expired:       !exp.After(b.opts.ReferenceTime),
	}
	item.Expiration = spec

	switch {
	case spec.Expired && b.opts.ExpiredPolicy == ExpiredPolicySkip:
		item.Status = ItemStatusSkipped
		// Skipped items carry no commands: the key must not exist after replay.
		item.Nodes = nil
		id := b.addWarning(WarnKeyExpiredSkipped, SeverityWarning,
			fmt.Sprintf("key %q expired at %d ms (before reference time) and is skipped by policy",
				obj.GetKey(), absMS),
			item.ID)
		item.WarningIDs = append(item.WarningIDs, id)
		return
	case spec.Expired && b.opts.ExpiredPolicy == ExpiredPolicyKeep:
		id := b.addWarning(WarnKeyExpiredKept, SeverityWarning,
			fmt.Sprintf("key %q was already expired; restored without TTL by policy (deadline %d ms dropped)",
				obj.GetKey(), absMS),
			item.ID)
		item.WarningIDs = append(item.WarningIDs, id)
		spec.AbsoluteCommand = nil
		spec.RelativeCommand = nil
		return
	case spec.Expired && b.opts.ExpiredPolicy == ExpiredPolicyError:
		b.block(item, WarnKeyExpiredBlocked,
			fmt.Sprintf("key %q expired at %d ms (reference time %s)",
				obj.GetKey(), absMS, b.opts.ReferenceTime.UTC().Format(time.RFC3339Nano)))
		return
	}

	if !b.opts.Target.Supports(FeaturePEXPIREAT) {
		b.block(item, WarnPEXPIREATUnsupported,
			fmt.Sprintf("key %q carries a TTL but target %s predates PEXPIREAT (Redis 2.6)",
				obj.GetKey(), b.opts.Target.Name))
		return
	}

	absCmd := cmd("PEXPIREAT", obj.GetKey(), strconv.FormatInt(absMS, 10))
	relCmd := cmd("PEXPIRE", obj.GetKey(), strconv.FormatInt(relative, 10))
	spec.AbsoluteCommand = absCmd
	spec.RelativeCommand = relCmd

	depends := []string{}
	if c := nodeByKind(item, nodeKindCreate); c != nil {
		depends = append(depends, c.ID)
	}
	node := &PlanNode{
		ID:        nodeID(item, nodeKindExpire),
		Kind:      nodeKindExpire,
		Label:     "set key ttl",
		DependsOn: depends,
		Groups: []*CommandGroup{{
			// Absolute replay is the default; RelativeCommand is available
			// for reference-time replay. They are alternatives, never both.
			Commands:  []*Command{absCmd},
			Atomicity: AtomicitySingle,
			Purpose:   "key-expire",
		}},
		Notes: []string{
			"absolute command reproduces the RDB deadline exactly",
			"relative command (PEXPIRE) applies the remaining TTL measured at reference time",
			"the expire step is not atomic with create",
		},
	}
	item.Nodes = append(item.Nodes, node)
}
