package restore

import (
	"strings"
	"testing"
)

func TestFunctionLibraryPlan(t *testing.T) {
	events := collectFile(t, "function.rdb")
	var sawFunc bool
	for _, e := range events {
		if e.Object.GetType() == "functions" {
			sawFunc = true
		}
	}
	if !sawFunc {
		t.Fatal("function.rdb fixture exposed no functions object; use WithSpecialOpCode")
	}

	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}
	plan, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	fn := mustFindItem(t, plan, functionsID)
	if fn.Kind != kindFunctions || fn.DB != -1 {
		t.Fatalf("functions item wrong: %+v", fn)
	}
	if !hasCmdPrefix(fn, "FUNCTION", "LOAD", "REPLACE") {
		t.Fatalf("functions commands: %v", allCommands(fn))
	}
	// The registered library payload must be carried verbatim.
	var payload string
	for _, n := range fn.Nodes {
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				if c.Args[0] == "FUNCTION" {
					payload = c.Args[3]
				}
			}
		}
	}
	if !strings.HasPrefix(payload, "#!lua name=mylib") {
		t.Fatalf("payload missing header: %q", payload)
	}

	// Functions must be applied first and keys depend on them.
	if plan.Items[0].ID != functionsID {
		t.Fatalf("functions not first: %s", plan.Items[0].ID)
	}
	for _, it := range plan.Items[1:] {
		found := false
		for _, d := range it.DependsOn {
			if d == functionsID {
				found = true
			}
		}
		if !found {
			t.Fatalf("key item %s must depend on functions", it.ID)
		}
	}

	// On Redis 6.0 functions are an evidence-backed blocker, never skipped.
	plan60, err := BuildPlanFromEvents(events, Options{
		ReferenceTime: refTime, Target: ProfileRedis60})
	if err != nil {
		t.Fatal(err)
	}
	fn60 := mustFindItem(t, plan60, functionsID)
	if fn60.Status != ItemStatusBlocked {
		t.Fatalf("functions status on 6.0 = %s", fn60.Status)
	}
	if !itemHasCode(fn60, plan60, WarnFunctionsUnsupported) {
		t.Fatal("missing functions-unsupported blocker")
	}
	if plan60.Executable {
		t.Fatal("plan with blocked functions must be non-executable")
	}

	assertDeterministic(t, events, opts)
}

func TestUnknownModuleIsBlocker(t *testing.T) {
	rdb := buildModuleRDB(t)
	events := collect(t, bytesReader(rdb))
	var sawModule bool
	for _, e := range events {
		if _, ok := e.Object.(interface{ GetType() string }); ok && e.Object.GetType() != "string" {
			sawModule = true
		}
	}
	if !sawModule {
		t.Fatal("module fixture decoded no module object")
	}
	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}
	plan, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatal(err)
	}
	var moduleItem *PlanItem
	for _, it := range plan.Items {
		if strings.Contains(it.ID, "mod:key") {
			moduleItem = it
		}
	}
	if moduleItem == nil {
		t.Fatalf("module item missing: %v", itemIDs(plan))
	}
	if moduleItem.Status != ItemStatusBlocked {
		t.Fatalf("module status=%s want blocked", moduleItem.Status)
	}
	if !itemHasCode(moduleItem, plan, WarnModuleUnsupported) {
		t.Fatalf("missing module blocker: %v", plan.Warnings)
	}
	if plan.Executable {
		t.Fatal("plan containing unknown module must be non-executable")
	}
	assertDeterministic(t, events, opts)
}

func TestHashFieldExpiration(t *testing.T) {
	// Redis 7.4 HFE fixture ships with the project.
	events := collectFile(t, "hash_with_hfe.rdb")
	plan74, err := BuildPlanFromEvents(events, Options{
		ReferenceTime: refTime, Target: ProfileRedis74})
	if err != nil {
		t.Fatal(err)
	}
	var hash *PlanItem
	for _, it := range plan74.Items {
		if it.Encoding == "listpackex" || it.Encoding == "hashex" {
			hash = it
		}
	}
	if hash == nil {
		t.Fatalf("no HFE hash found: %v", itemIDs(plan74))
	}
	if !hasCmdPrefix(hash, "HSET") {
		t.Fatalf("hash create: %v", allCommands(hash))
	}
	if !hasCmdPrefix(hash, "HPEXPIREAT") {
		t.Fatalf("field expiry command missing: %v", allCommands(hash))
	}
	// Field-expire node must run after create.
	exp := nodeByKind(hash, nodeKindExpire)
	if exp == nil {
		t.Fatal("field expire node missing")
	}
	create := nodeByKind(hash, nodeKindCreate)
	if create == nil || len(exp.DependsOn) == 0 || exp.DependsOn[0] != create.ID {
		t.Fatalf("field expire deps=%v", exp.DependsOn)
	}

	// On 7.0 field TTLs cannot be set: explicit warning + values preserved.
	plan70, err := BuildPlanFromEvents(events, Options{
		ReferenceTime: refTime, Target: ProfileRedis70})
	if err != nil {
		t.Fatal(err)
	}
	var hash70 *PlanItem
	for _, it := range plan70.Items {
		if it.ID == hash.ID {
			hash70 = it
		}
	}
	if !itemHasCode(hash70, plan70, WarnHashFieldExpiryUnsupported) {
		t.Fatalf("expected hash field expiry warning on 7.0: %v", plan70.Warnings)
	}
	if !hasCmdPrefix(hash70, "HSET") || hasCmdPrefix(hash70, "HPEXPIREAT") {
		t.Fatal("field values must be kept but HPEXPIREAT omitted on 7.0")
	}

	assertDeterministic(t, events, Options{ReferenceTime: refTime, Target: ProfileRedis74})
}
