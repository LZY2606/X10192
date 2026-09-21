package restore

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }

func TestPlanPlainTypesAndMultiDB(t *testing.T) {
	rdb := buildMultiTypeRDB(t)
	events := collect(t, bytesReader(rdb))
	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}

	plan, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if !plan.Executable {
		t.Fatalf("plan should be executable, blockers=%v", plan.Warnings)
	}
	if plan.Version != RestorePlanVersion {
		t.Fatalf("version=%d", plan.Version)
	}

	// Two databases, explicit SELECT switches, sorted by index.
	if len(plan.Databases) != 2 {
		t.Fatalf("databases=%d", len(plan.Databases))
	}
	if plan.Databases[0].DB != 0 || plan.Databases[1].DB != 1 {
		t.Fatalf("db order: %+v", plan.Databases)
	}
	if got := plan.Databases[0].SwitchCommand.Args; got[0] != "SELECT" || got[1] != "0" {
		t.Fatalf("select0=%v", got)
	}

	// Item execution order: db then key ascending.
	wantOrder := []string{
		"db0/key/hash:h",
		"db0/key/list:l",
		"db0/key/set:s",
		"db0/key/str:string",
		"db0/key/zset:z",
		"db1/key/db1:only",
	}
	if len(plan.Items) != len(wantOrder) {
		t.Fatalf("items=%d want=%d", len(plan.Items), len(wantOrder))
	}
	for i, want := range wantOrder {
		if plan.Items[i].ID != want {
			t.Fatalf("item[%d]=%q want %q", i, plan.Items[i].ID, want)
		}
	}

	str := mustFindItem(t, plan, "db0/key/str:string")
	if !hasCmdPrefix(str, "SET", "str:string", "v1") {
		t.Fatalf("string cmd: %v", allCommands(str))
	}

	list := mustFindItem(t, plan, "db0/key/list:l")
	if !hasCmdPrefix(list, "RPUSH", "list:l", "x", "y", "z") {
		t.Fatalf("list cmd: %v", allCommands(list))
	}

	// Set members must be sorted for determinism.
	set := mustFindItem(t, plan, "db0/key/set:s")
	var sadd []string
	for _, n := range set.Nodes {
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				if c.Args[0] == "SADD" {
					sadd = c.Args
				}
			}
		}
	}
	assertSortedStrings(t, "sadd members", sadd[2:])

	// Hash fields must be sorted (a before b).
	hash := mustFindItem(t, plan, "db0/key/hash:h")
	var hset []string
	for _, n := range hash.Nodes {
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				if c.Args[0] == "HSET" {
					hset = c.Args
				}
			}
		}
	}
	assertDeepEqual(t, "hset args", hset,
		[]string{"HSET", "hash:h", "a", "1", "b", "2"})

	// Zset members sorted, scores float-formatted.
	zset := mustFindItem(t, plan, "db0/key/zset:z")
	var zadd []string
	for _, n := range zset.Nodes {
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				if c.Args[0] == "ZADD" {
					zadd = c.Args
				}
			}
		}
	}
	assertDeepEqual(t, "zadd args", zadd,
		[]string{"ZADD", "zset:z", "1", "alpha", "2.5", "beta"})

	// Source position populated.
	if str.Source.DB != 0 || str.Source.Key != "str:string" {
		t.Fatalf("source=%+v", str.Source)
	}

	assertDeterministic(t, events, opts)
}

func TestPlanMillisecondExpiry(t *testing.T) {
	rdb := buildMultiTypeRDB(t)
	events := collect(t, bytesReader(rdb))
	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}
	plan, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatal(err)
	}
	zset := mustFindItem(t, plan, "db0/key/zset:z")
	if zset.Expiration == nil {
		t.Fatal("expected expiration spec")
	}
	if zset.Expiration.Precision != "millisecond" {
		t.Fatalf("precision=%s", zset.Expiration.Precision)
	}
	wantAbs := time.Date(2030, 1, 2, 3, 4, 5, 6_000_000, time.UTC).UnixNano() / int64(time.Millisecond)
	if zset.Expiration.AbsoluteAtMS != wantAbs {
		t.Fatalf("abs=%d want=%d", zset.Expiration.AbsoluteAtMS, wantAbs)
	}
	wantTTL := wantAbs - refTime.UnixNano()/int64(time.Millisecond)
	if zset.Expiration.RelativeTTLMS != wantTTL {
		t.Fatalf("ttl=%d want=%d", zset.Expiration.RelativeTTLMS, wantTTL)
	}
	if zset.Expiration.AbsoluteCommand.Args[0] != "PEXPIREAT" {
		t.Fatalf("abs cmd=%v", zset.Expiration.AbsoluteCommand.Args)
	}
	if zset.Expiration.RelativeCommand.Args[0] != "PEXPIRE" {
		t.Fatalf("rel cmd=%v", zset.Expiration.RelativeCommand.Args)
	}
	// Expire node depends on create and is documented non-atomic.
	expNode := nodeByID(zset, "db0/key/zset:z/expire")
	if expNode == nil || len(expNode.DependsOn) == 0 {
		t.Fatalf("expire node missing: %+v", expNode)
	}
	if expNode.Groups[0].Atomicity != AtomicitySingle {
		t.Fatalf("expire atomicity=%s", expNode.Groups[0].Atomicity)
	}
}

func TestPlanSecondExpiry(t *testing.T) {
	rdb := secondExpiryRDB(t)
	events := collect(t, bytesReader(rdb))
	for i := range events {
		events[i].Precision = "second"
	}
	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}
	plan, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatal(err)
	}
	item := mustFindItem(t, plan, "db0/key/sec:ttl")
	if item.Expiration == nil || item.Expiration.Precision != "second" {
		t.Fatalf("expiration=%+v", item.Expiration)
	}
	if item.Expiration.AbsoluteAtMS != 2000000000*1000 {
		t.Fatalf("abs=%d", item.Expiration.AbsoluteAtMS)
	}
	assertDeterministic(t, events, opts)
}

func TestExpiredPolicies(t *testing.T) {
	// keys_with_expiry.rdb deadline is 2022-12-25, long before refTime 2025.
	events := collectFile(t, "keys_with_expiry.rdb")
	if len(events) != 1 {
		t.Fatalf("events=%d", len(events))
	}

	t.Run("skip", func(t *testing.T) {
		plan, err := BuildPlanFromEvents(events, Options{
			ReferenceTime: refTime, ExpiredPolicy: ExpiredPolicySkip, Target: ProfileRedis74})
		if err != nil {
			t.Fatal(err)
		}
		item := mustFindItem(t, plan, "db0/key/expires_ms_precision")
		if item.Status != ItemStatusSkipped || len(item.Nodes) != 0 {
			t.Fatalf("status=%s nodes=%d", item.Status, len(item.Nodes))
		}
		if !itemHasCode(item, plan, WarnKeyExpiredSkipped) {
			t.Fatalf("missing skip warning: %v", plan.Warnings)
		}
	})

	t.Run("keep", func(t *testing.T) {
		plan, err := BuildPlanFromEvents(events, Options{
			ReferenceTime: refTime, ExpiredPolicy: ExpiredPolicyKeep, Target: ProfileRedis74})
		if err != nil {
			t.Fatal(err)
		}
		item := mustFindItem(t, plan, "db0/key/expires_ms_precision")
		if item.Status != ItemStatusActive || item.Expiration == nil || item.Expiration.AbsoluteCommand != nil {
			t.Fatalf("status=%s exp=%+v", item.Status, item.Expiration)
		}
		if !itemHasCode(item, plan, WarnKeyExpiredKept) {
			t.Fatalf("missing kept warning")
		}
	})

	t.Run("error", func(t *testing.T) {
		plan, err := BuildPlanFromEvents(events, Options{
			ReferenceTime: refTime, ExpiredPolicy: ExpiredPolicyError, Target: ProfileRedis74})
		if err == nil {
			t.Fatal("expected error under policy=error")
		}
		item := mustFindItem(t, plan, "db0/key/expires_ms_precision")
		if item.Status != ItemStatusBlocked {
			t.Fatalf("status=%s", item.Status)
		}
		if plan.Executable {
			t.Fatal("plan must be non-executable")
		}
	})
}

func TestPlanIsJSONSerializable(t *testing.T) {
	events := collect(t, bytesReader(buildMultiTypeRDB(t)))
	plan, err := BuildPlanFromEvents(events, Options{ReferenceTime: refTime, Target: ProfileRedis74})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(plan)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back RestorePlan
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(back.Items) != len(plan.Items) {
		t.Fatalf("round trip items=%d", len(back.Items))
	}
	if back.Target.Name != ProfileRedis74.Name {
		t.Fatalf("target=%+v", back.Target)
	}
}
