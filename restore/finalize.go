package restore

import "sort"

func (p *Planner) finalizeLogicalGroups(plan *RestorePlan) {
	for _, item := range p.items {
		group := item.LogicalGroup
		group.CommandIDs = make([]string, 0, len(item.CreateCommands)+len(item.FollowUpCommands))
		for _, cmd := range item.CreateCommands {
			group.CommandIDs = append(group.CommandIDs, cmd.ID)
		}
		for _, cmd := range item.FollowUpCommands {
			group.CommandIDs = append(group.CommandIDs, cmd.ID)
		}
		sort.Strings(group.CommandIDs)
		if len(group.CommandIDs) > 1 {
			group.NonAtomicBecause = "commands are ordered with dependencies but are not wrapped in MULTI/EXEC; a failure between them can leave a partially restored logical object"
		}
		item.LogicalGroup = group
	}
	for i := range plan.GlobalItems {
		for _, item := range p.items {
			if item.ID == plan.GlobalItems[i].ID {
				plan.GlobalItems[i] = *item
			}
		}
	}
}

func (p *Planner) setDependencies(plan *RestorePlan) {
	for i := range plan.Databases {
		db := &plan.Databases[i]
		var previous string
		for _, itemID := range db.ItemIDs {
			first := p.firstNodeID(itemID)
			if first != "" {
				if previous != "" {
					if cmd := findCommand(plan.Nodes, first); cmd != nil {
						cmd.DependsOn = appendUnique(cmd.DependsOn, previous)
					}
				}
				previous = p.lastNodeID(itemID)
			}
		}
	}
	sortCommands(plan.Nodes)
}

func appendUnique(values []string, value string) []string {
	for _, existing := range values {
		if existing == value {
			return values
		}
	}
	return append(values, value)
}

func findCommand(nodes []PlannedCommand, id string) *PlannedCommand {
	for i := range nodes {
		if nodes[i].ID == id {
			return &nodes[i]
		}
	}
	return nil
}

func (p *Planner) firstNodeID(itemID string) string {
	for _, item := range p.items {
		if item.ID != itemID {
			continue
		}
		if len(item.CreateCommands) > 0 {
			return item.CreateCommands[0].ID
		}
		if len(item.FollowUpCommands) > 0 {
			return item.FollowUpCommands[0].ID
		}
		return ""
	}
	return ""
}

func (p *Planner) lastNodeID(itemID string) string {
	for _, item := range p.items {
		if item.ID == itemID {
			if len(item.FollowUpCommands) > 0 {
				return item.FollowUpCommands[len(item.FollowUpCommands)-1].ID
			}
			if len(item.CreateCommands) > 0 {
				return item.CreateCommands[len(item.CreateCommands)-1].ID
			}
		}
	}
	return ""
}

func (p *Planner) sortPlan(plan *RestorePlan) {
	for i := range plan.GlobalItems {
		sortItemCommands(&plan.GlobalItems[i])
	}
	for _, item := range p.items {
		sortItemCommands(item)
	}
	sortCommands(plan.Nodes)
	var warnings []Warning
	var blockers []Blocker
	for _, item := range p.items {
		warnings = append(warnings, item.Warnings...)
		blockers = append(blockers, item.Blockers...)
	}
	plan.Warnings = dedupWarnings(warnings)
	plan.Blockers = dedupBlockers(blockers)
}

func sortItemCommands(item *PlanItem) {
	sortCommands(item.CreateCommands)
	sortCommands(item.FollowUpCommands)
}

func sortCommands(commands []PlannedCommand) {
	sort.SliceStable(commands, func(i, j int) bool { return commands[i].ID < commands[j].ID })
}

func dedupBlockers(blockers []Blocker) []Blocker {
	type key struct {
		code, source, message string
	}
	seen := make(map[key]struct{})
	out := make([]Blocker, 0, len(blockers))
	for _, blocker := range blockers {
		k := key{blocker.Code, sourceSignature(blocker.Source), blocker.Message}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, blocker)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Code != out[j].Code {
			return out[i].Code < out[j].Code
		}
		return sourceSignature(out[i].Source) < sourceSignature(out[j].Source)
	})
	return out
}
