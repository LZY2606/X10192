package restoreplan

import (
	"bytes"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

const hexDigits = "0123456789abcdef"

func hexBytes(b []byte) string {
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hexDigits[c>>4]
		out[i*2+1] = hexDigits[c&15]
	}
	return string(out)
}

func newCmd(role CommandRole, name string, args ...[]byte) *Command {
	c := &Command{Name: name, Role: role, Args: make([]Arg, len(args))}
	for i, a := range args {
		c.Args[i] = Arg(a)
	}
	return c
}

func textCmd(role CommandRole, name string, args ...string) *Command {
	c := &Command{Name: name, Role: role, Args: make([]Arg, len(args))}
	for i, a := range args {
		c.Args[i] = Arg(a)
	}
	return c
}

// chunkKV splits sorted field/value pairs into deterministic chunks while
// keeping the create phase a single logical (non-atomic) group.
func (p *planner) chunkCreateKV(item *PlanItem, name string, key []byte, pairs [][2][]byte, boundaryNote string) {
	max := p.opts.MaxArgsPerCommand
	if max <= 0 || 2+len(pairs)*2 <= max {
		args := make([][]byte, 0, 2+len(pairs)*2)
		args = append(args, key)
		for _, kv := range pairs {
			args = append(args, kv[0], kv[1])
		}
		item.Create = append(item.Create, newCmd(RoleCreate, name, args...))
		return
	}
	chunk := (max - 2) / 2
	if chunk < 1 {
		chunk = 1
	}
	for start := 0; start < len(pairs); start += chunk {
		end := start + chunk
		if end > len(pairs) {
			end = len(pairs)
		}
		args := make([][]byte, 0, 2+(end-start)*2)
		args = append(args, key)
		for _, kv := range pairs[start:end] {
			args = append(args, kv[0], kv[1])
		}
		item.Create = append(item.Create, newCmd(RoleCreate, name, args...))
	}
	item.Warnings = append(item.Warnings, Warning{
		Code:     "create-command-chunked",
		ItemID:   item.ID,
		Message:  "create command split into chunks due to MaxArgsPerCommand",
		Evidence: []string{boundaryNote},
	})
}

func (p *planner) chunkCreateElems(item *PlanItem, name string, key []byte, elems [][]byte, boundaryNote string) {
	max := p.opts.MaxArgsPerCommand
	if max <= 0 || 1+len(elems) <= max {
		args := make([][]byte, 0, 1+len(elems))
		args = append(args, key)
		args = append(args, elems...)
		item.Create = append(item.Create, newCmd(RoleCreate, name, args...))
		return
	}
	chunk := max - 1
	if chunk < 1 {
		chunk = 1
	}
	for start := 0; start < len(elems); start += chunk {
		end := start + chunk
		if end > len(elems) {
			end = len(elems)
		}
		args := make([][]byte, 0, 1+(end-start))
		args = append(args, key)
		args = append(args, elems[start:end]...)
		item.Create = append(item.Create, newCmd(RoleCreate, name, args...))
	}
	item.Warnings = append(item.Warnings, Warning{
		Code:     "create-command-chunked",
		ItemID:   item.ID,
		Message:  "create command split into chunks due to MaxArgsPerCommand",
		Evidence: []string{boundaryNote},
	})
}

func (p *planner) planString(o *model.StringObject) *PlanItem {
	item := p.baseItem(o)
	p.addEvictionWarning(item, o)
	item.Create = append(item.Create, newCmd(RoleCreate, "SET", []byte(o.Key), o.Value))
	_ = p.applyKeyExpiry(item, o) // strings cannot carry field expiry errors
	p.finalize(item)
	return item
}

func (p *planner) planList(o *model.ListObject) *PlanItem {
	item := p.baseItem(o)
	p.addEvictionWarning(item, o)
	elems := make([][]byte, len(o.Values))
	copy(elems, o.Values) // list order is positional and must be preserved
	p.chunkCreateElems(item, "RPUSH", []byte(o.Key), elems, "list")
	_ = p.applyKeyExpiry(item, o)
	p.finalize(item)
	return item
}

func (p *planner) planSet(o *model.SetObject) *PlanItem {
	item := p.baseItem(o)
	p.addEvictionWarning(item, o)
	members := make([][]byte, len(o.Members))
	copy(members, o.Members)
	// Set membership is order-independent; sorting guarantees byte-identical
	// plans regardless of decoder/map iteration order.
	sort.Slice(members, func(i, j int) bool { return bytes.Compare(members[i], members[j]) < 0 })
	p.chunkCreateElems(item, "SADD", []byte(o.Key), members, "set")
	_ = p.applyKeyExpiry(item, o)
	p.finalize(item)
	return item
}

func (p *planner) planZSet(o *model.ZSetObject) *PlanItem {
	item := p.baseItem(o)
	p.addEvictionWarning(item, o)
	args := make([][]byte, 0, 1+len(o.Entries)*2)
	args = append(args, []byte(o.Key))
	for _, e := range o.Entries {
		args = append(args, []byte(formatScore(e.Score)), []byte(e.Member))
	}
	item.Create = append(item.Create, newCmd(RoleCreate, "ZADD", args...))
	_ = p.applyKeyExpiry(item, o)
	p.finalize(item)
	return item
}

// formatScore mirrors redis' shortest-round-trip double formatting.
func formatScore(score float64) string {
	return strconv.FormatFloat(score, 'f', -1, 64)
}

func sortedHashPairs(hash map[string][]byte) [][2][]byte {
	fields := make([]string, 0, len(hash))
	for f := range hash {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	pairs := make([][2][]byte, len(fields))
	for i, f := range fields {
		pairs[i] = [2][]byte{[]byte(f), hash[f]}
	}
	return pairs
}
