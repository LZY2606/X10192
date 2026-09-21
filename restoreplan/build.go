package restoreplan

import (
	"bytes"
	"sort"
	"strconv"
)

// build sorts all content, assigns step ids/dependencies, aggregates plan
// level warnings/blockers and produces the final deterministic script.
func (p *planner) build() (*RestorePlan, error) {
	p.finalizeFunctions(p.order)
	for _, item := range p.order {
		if item.Kind == KindKey {
			// key items were finalized when planned; only re-sort after
			// dedup merges that may have appended warnings.
			sortWarnings(item.Warnings)
			sortBlockers(item.PartialBlockers)
		} else {
			sortWarnings(item.Warnings)
			sortBlockers(item.PartialBlockers)
		}
	}

	items := append([]*PlanItem(nil), p.order...)
	sort.Slice(items, func(i, j int) bool { return itemOrderLess(items[i], items[j]) })

	plan := &RestorePlan{
		ReferenceTimeMs: p.refMs,
		Target:          p.target.Name,
		ExpiryMode:      p.mode,
		ExpiredPolicy:   p.policy,
		Items:           items,
	}

	dbSeen := make(map[int]bool)
	var dbs []int
	for _, item := range items {
		if item.Kind == KindKey && !dbSeen[item.DB] {
			dbSeen[item.DB] = true
			dbs = append(dbs, item.DB)
		}
	}
	sort.Ints(dbs)
	plan.DBS = dbs

	// Flat, numbered execution script. Function libraries come first (they
	// are db-global), then per-db SELECT + items ordered by (db, key).
	stepNo := 0
	prevID := ""
	addStep := func(db int, itemID string, phase ItemPhase, c *Command) string {
		stepNo++
		id := "step-" + padStep(stepNo)
		// Every command runs after the previous one, so the script has one
		// total order; item-internal ordering adds explicit dependencies.
		if prevID != "" {
			c.DependsOn = appendUnique(c.DependsOn, prevID)
		}
		plan.Steps = append(plan.Steps, &Step{
			ID: id, DB: db, ItemID: itemID, Phase: phase, Cmd: c,
		})
		prevID = id
		return id
	}

	for _, item := range items {
		if item.Kind == KindFunctions {
			for _, c := range item.Create {
				addStep(-1, item.ID, PhaseCreate, c)
			}
			continue
		}
	}

	currentDB := -2
	dbFirstStep := make(map[int]string)
	dbItemCount := make(map[int]int)
	for _, item := range items {
		if item.Kind != KindKey {
			continue
		}
		dbItemCount[item.DB]++
	}

	// Track each item's first create step so follow-ups depend on it.
	itemCreateStep := make(map[string]string)
	itemFirstStep := make(map[string]string)

	emitItem := func(item *PlanItem) {
		if len(item.Create) == 0 && len(item.FollowUps) == 0 {
			return // blocked/skipped item: no SELECT, no steps
		}
		if item.DB != currentDB {
			sel := textCmd(RoleSelectDB, "SELECT", strconv.Itoa(item.DB))
			id := addStep(item.DB, "", "", sel)
			dbFirstStep[item.DB] = id
			currentDB = item.DB
		}
		var firstID string
		for _, c := range item.Create {
			id := addStep(item.DB, item.ID, PhaseCreate, c)
			if firstID == "" {
				firstID = id
			}
		}
		createID := firstID
		for _, c := range item.FollowUps {
			if createID != "" {
				c.DependsOn = appendUnique(c.DependsOn, createID)
			}
			addStep(item.DB, item.ID, PhaseFollowUp, c)
		}
		itemCreateStep[item.ID] = createID
		itemFirstStep[item.ID] = firstID
	}

	// Emit items grouped by ascending db, each group ordered by key bytes.
	for _, db := range dbs {
		for _, item := range items {
			if item.Kind == KindKey && item.DB == db {
				emitItem(item)
			}
		}
	}
	_ = dbItemCount
	_ = itemCreateStep
	_ = itemFirstStep

	// Aggregate skipped keys, warnings and blockers.
	for _, item := range items {
		if item.Status == StatusSkippedExpired {
			plan.SkippedExpired = append(plan.SkippedExpired, SkippedKey{
				ItemID: item.ID, DB: item.DB, Key: item.Key,
				Reason: "expired at reference time",
			})
		}
		for _, w := range item.Warnings {
			plan.Warnings = append(plan.Warnings, w)
		}
		for _, b := range item.PartialBlockers {
			b.ItemID = item.ID
			plan.Blockers = append(plan.Blockers, b)
		}
		if item.Blocker != nil {
			b := *item.Blocker
			b.ItemID = item.ID
			plan.Blockers = append(plan.Blockers, b)
		}
	}
	for _, w := range p.globalWarnings {
		plan.Warnings = append(plan.Warnings, w)
	}
	sort.Slice(plan.Warnings, func(i, j int) bool {
		if plan.Warnings[i].Code != plan.Warnings[j].Code {
			return plan.Warnings[i].Code < plan.Warnings[j].Code
		}
		if plan.Warnings[i].ItemID != plan.Warnings[j].ItemID {
			return plan.Warnings[i].ItemID < plan.Warnings[j].ItemID
		}
		return stringsJoin(plan.Warnings[i].Evidence) < stringsJoin(plan.Warnings[j].Evidence)
	})
	sort.Slice(plan.Blockers, func(i, j int) bool {
		if plan.Blockers[i].Code != plan.Blockers[j].Code {
			return plan.Blockers[i].Code < plan.Blockers[j].Code
		}
		if plan.Blockers[i].ItemID != plan.Blockers[j].ItemID {
			return plan.Blockers[i].ItemID < plan.Blockers[j].ItemID
		}
		return stringsJoin(plan.Blockers[i].Evidence) < stringsJoin(plan.Blockers[j].Evidence)
	})
	sort.Slice(plan.SkippedExpired, func(i, j int) bool {
		if plan.SkippedExpired[i].DB != plan.SkippedExpired[j].DB {
			return plan.SkippedExpired[i].DB < plan.SkippedExpired[j].DB
		}
		return plan.SkippedExpired[i].Key < plan.SkippedExpired[j].Key
	})

	return plan, nil
}

func itemOrderLess(a, b *PlanItem) bool {
	if a.Kind != b.Kind {
		return a.Kind == KindFunctions // functions before keys
	}
	if a.Kind == KindFunctions {
		return a.ID < b.ID
	}
	if a.DB != b.DB {
		return a.DB < b.DB
	}
	return bytes.Compare([]byte(a.Key), []byte(b.Key)) < 0
}

func appendUnique(ss []string, s string) []string {
	for _, v := range ss {
		if v == s {
			return ss
		}
	}
	return append(ss, s)
}

func padStep(n int) string {
	s := strconv.Itoa(n)
	for len(s) < 4 {
		s = "0" + s
	}
	return s
}
