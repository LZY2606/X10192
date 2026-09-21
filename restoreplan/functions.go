package restoreplan

import (
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// collectFunctions turns the RDB function library payload (the exact body
// accepted by FUNCTION LOAD) into a plan item. Libraries are never silently
// skipped: unsupported targets get a fatal blocker.
func (p *planner) collectFunctions(o *model.FunctionsObject) {
	name, engine := parseFunctionLibrary(o.FunctionsLua)
	id := "functions:db-:" + name
	item := &PlanItem{
		ID:       id,
		Kind:     KindFunctions,
		DB:       -1,
		Type:     model.FunctionsType,
		Encoding: "functions",
		Status:   StatusReady,
		Source: Source{
			Kind: "functions", Type: model.FunctionsType, Encoding: "functions",
		},
	}
	if existing, ok := p.funcs[name]; ok {
		existing.Warnings = append(existing.Warnings, Warning{
			Code:    "duplicate-function-library",
			ItemID:  existing.ID,
			Message: "function library " + strconv.Quote(name) + " appeared multiple times; payload merged deterministically",
		})
		// replace payload deterministically (smaller bytes wins)
		if len(o.FunctionsLua) < len(existing.SourceEvidencePayload()) {
			existing.replaceFunctionPayload(o.FunctionsLua, engine)
		}
		return
	}
	item.replaceFunctionPayload(o.FunctionsLua, engine)
	p.funcs[name] = item
	p.order = append(p.order, item)
}

// parseFunctionLibrary extracts "#!lua name=<lib>" metadata.
func parseFunctionLibrary(payload string) (name string, engine string) {
	engine = "lua"
	name = ""
	// first line looks like: #!lua name=mylib
	lineEnd := indexByte(payload, '\n')
	first := payload
	rest := ""
	if lineEnd >= 0 {
		first = payload[:lineEnd]
		rest = payload[lineEnd+1:]
	}
	_ = rest
	if len(first) >= 2 && first[0] == '#' && first[1] == '!' {
		header := first[2:]
		sp := indexByte(header, ' ')
		if sp >= 0 {
			engine = header[:sp]
			header = header[sp+1:]
		}
		const prefix = "name="
		if len(header) >= len(prefix) && header[:len(prefix)] == prefix {
			name = header[len(prefix):]
		}
	}
	if name == "" {
		name = "library-" + strconv.Itoa(len(payload))
	}
	return name, engine
}

func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// functionPayloadField stores payload on the item via a deterministic private
// convention: we keep it in Create command args (FUNCTION LOAD REPLACE payload)
// while the item is being assembled.
func (item *PlanItem) replaceFunctionPayload(payload, engine string) {
	item.Create = []*Command{{
		Name: "FUNCTION",
		Args: []Arg{Arg("LOAD"), Arg("REPLACE"), Arg(payload)},
		Role: RoleFunctions,
		Notes: []string{
			"payload is the exact RDB functions blob, replayable by FUNCTION LOAD",
			"engine=" + engine,
		},
	}}
	item.Atomicity = Atomicity{Atomic: true, NumCommands: 1, Boundary: "single command"}
}

// SourceEvidencePayload returns the raw functions payload held in the item.
func (item *PlanItem) SourceEvidencePayload() string {
	if len(item.Create) == 1 && item.Create[0].Name == "FUNCTION" && len(item.Create[0].Args) == 3 {
		return string(item.Create[0].Args[2])
	}
	return ""
}

// finalizeFunctions applies the capability gate after all events are read.
func (p *planner) finalizeFunctions(items []*PlanItem) {
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	for _, item := range items {
		if item.Kind != KindFunctions {
			continue
		}
		if !p.target.Functions {
			item.Status = StatusBlocked
			item.Blocker = &Blocker{
				Code:               "functions-unsupported",
				ItemID:             item.ID,
				Reason:             "FUNCTION LOAD requires Redis 7.0 / Valkey with functions support",
				RequiredCapability: "functions",
				Fatal:              true,
				Evidence:           []string{"target=" + p.target.Name},
			}
			item.Create = nil
			item.Atomicity = Atomicity{Atomic: false, NumCommands: 0, Boundary: "blocked: target lacks functions support"}
		}
	}
}
