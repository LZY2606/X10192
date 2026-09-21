package restoreplan

import (
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// blockedModuleItem emits a fatal blocker instead of silently dropping an
// unknown module value, as the decoder would otherwise do.
func (p *planner) blockedModuleItem(o *model.ModuleTypeObject) *PlanItem {
	item := p.baseItem(o)
	item.Type = o.ModuleType
	item.Source.Type = o.ModuleType
	item.Status = StatusBlocked
	evidence := []string{
		"moduleType=" + o.ModuleType,
		"encoding=" + o.GetEncoding(),
		"target=" + p.target.Name,
	}
	if o.Value == nil {
		evidence = append(evidence, "raw-value-skipped-by-decoder")
	}
	item.Blocker = &Blocker{
		Code:               "module-type-unsupported",
		ItemID:             item.ID,
		Reason:             "module data type " + strconv.Quote(o.ModuleType) + " has no portable restore command; RDB module value cannot be replayed safely",
		RequiredCapability: "module:" + o.ModuleType,
		Fatal:              true,
		Evidence:           evidence,
	}
	item.Atomicity = Atomicity{Atomic: false, NumCommands: 0, Boundary: "blocked: module type cannot be restored"}
	return item
}

// blockedUnknownItem covers objects/encodings this planner does not know how
// to emit; they must never be silently skipped.
func (p *planner) blockedUnknownItem(o model.RedisObject) *PlanItem {
	item := p.baseItem(o)
	item.Status = StatusBlocked
	item.Blocker = &Blocker{
		Code:   "unknown-object-type",
		ItemID: item.ID,
		Reason: "decoded object type " + strconv.Quote(o.GetType()) + " is not supported by the restore planner",
		Fatal:  true,
		Evidence: []string{
			"type=" + o.GetType(),
			"encoding=" + o.GetEncoding(),
		},
	}
	item.Atomicity = Atomicity{Atomic: false, NumCommands: 0, Boundary: "blocked: unsupported object type"}
	return item
}
