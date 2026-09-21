package restore

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/hdt3213/rdb/model"
)

// libHeader matches the RDB function library payload start "#!lua name=..."
// (or engine variants). Libraries are concatenated in one RDB opcode value,
// so the payload is split at each header line.
var libHeader = regexp.MustCompile(`(?m)^#![^\n]*name=[^\s\n]+[^\n]*`)

func (b *builder) planFunctions(ev Event) *PlanItem {
	obj, ok := ev.Object.(*model.FunctionsObject)
	item := &PlanItem{
		ID:       functionsID,
		Kind:     kindFunctions,
		DB:       -1,
		Type:     model.FunctionsType,
		Encoding: "functions",
		Source: Source{
			DB:       -1,
			Key:      "functions",
			Offset:   ev.Offset,
			Sequence: ev.Sequence,
		},
		Status:    ItemStatusActive,
		DependsOn: nil,
	}
	b.items = append(b.items, item)
	b.stats.ItemCount++

	payload := ""
	if ok {
		payload = obj.FunctionsLua
	}
	libs := splitFunctionLibraries(payload)
	if len(libs) == 0 {
		b.block(item, WarnFunctionsPayloadEmpty,
			"RDB function opcode carried no parseable '#!lua name=...' library payload")
		b.stats.BlockedItems++
		return item
	}

	if !b.opts.Target.Supports(FeatureFunctions) {
		b.block(item, WarnFunctionsUnsupported,
			fmt.Sprintf("function libraries (%d) require FUNCTION LOAD (Redis 7.0); target %s cannot load them",
				len(libs), b.opts.Target.Name))
		b.stats.BlockedItems++
		return item
	}

	// REPLACE makes re-running the plan idempotent. Libraries are emitted in
	// content order after sorting to survive payload reordering.
	sort.Strings(libs)
	commands := make([]*Command, 0, len(libs))
	for _, lib := range libs {
		commands = append(commands, cmd("FUNCTION", "LOAD", "REPLACE", lib))
	}
	item.Nodes = append(item.Nodes, &PlanNode{
		ID:     nodeID(item, nodeKindFunctions),
		Kind:   nodeKindFunctions,
		Label:  "load function libraries",
		Groups: []*CommandGroup{{Commands: commands, Atomicity: AtomicityNonAtomic, Purpose: "function-load"}},
		Notes: []string{
			"FUNCTION LOAD REPLACE is idempotent and one command per RDB library",
			"libraries load before every key item (item dependency)",
		},
	})
	b.stats.ActiveItems++
	return item
}

// splitFunctionLibraries splits the concatenated RDB functions payload into
// individual "#!..." library scripts in byte-deterministic fashion.
func splitFunctionLibraries(payload string) []string {
	if payload == "" {
		return nil
	}
	locs := libHeader.FindAllStringIndex(payload, -1)
	if len(locs) == 0 {
		return nil
	}
	libs := make([]string, 0, len(locs))
	for i, loc := range locs {
		start := loc[0]
		end := len(payload)
		if i+1 < len(locs) {
			end = locs[i+1][0]
		}
		lib := payload[start:end]
		// Trim a single trailing newline boundary between concatenated libs.
		for len(lib) > 0 && (lib[len(lib)-1] == '\n' || lib[len(lib)-1] == '\r') {
			lib = lib[:len(lib)-1]
		}
		if lib != "" {
			libs = append(libs, lib)
		}
	}
	return libs
}
