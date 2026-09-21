package restore

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestNoMapOrderInOutput scans the serialized plan for every existing
// fixture and proves that repeated builds (which re-run Go map iteration) and
// shuffled input event order produce byte-identical JSON.
func TestDeterminismAcrossFixtures(t *testing.T) {
	fixtures := []string{
		"memory.rdb",
		"multiple_databases.rdb",
		"keys_with_expiry.rdb",
		"stream_listpacks_1.rdb",
		"stream_listoacks_3.rdb",
		"stream_listpacks_2.rdb",
		"function.rdb",
		"hash_with_hfe.rdb",
		"hash_as_listpack_with_hfe.rdb",
		"regular_set.rdb",
		"regular_sorted_set.rdb",
		"hash.rdb",
		"quicklist.rdb",
	}
	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}
	for _, f := range fixtures {
		f := f
		t.Run(f, func(t *testing.T) {
			events := collectFile(t, f)
			assertDeterministic(t, events, opts)
		})
	}
}

// TestMapIterationStress builds plans repeatedly over a large hash/set/zset so
// randomized Go map iteration cannot leak into command argument order.
func TestMapIterationStress(t *testing.T) {
	// Construct big hash/set/zset objects directly (no RDB needed).
	const n = 200
	hashMap := make(map[string][]byte, n)
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("field-%04d", i)
		hashMap[k] = []byte(fmt.Sprintf("value-%06d", i*7))
	}
	events := []Event{
		{Object: makeString("k:string", "v")},
		{Object: makeHash("k:hash", hashMap)},
	}
	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}

	var first []byte
	for iter := 0; iter < 50; iter++ {
		plan, err := BuildPlanFromEvents(events, opts)
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = data
		} else if string(first) != string(data) {
			t.Fatalf("hash plan changed at iter %d", iter)
		}
	}
	// Hash HSET args must be sorted, regardless of map iteration.
	plan, _ := BuildPlanFromEvents(events, opts)
	hash := mustFindItem(t, plan, "db0/key/k:hash")
	var hset []string
	for _, nn := range hash.Nodes {
		for _, g := range nn.Groups {
			for _, c := range g.Commands {
				if c.Args[0] == "HSET" {
					hset = c.Args
				}
			}
		}
	}
	fields := make([]string, 0, (len(hset)-2)/2)
	for i := 2; i+1 < len(hset); i += 2 {
		fields = append(fields, hset[i])
		if hset[i] != fmt.Sprintf("field-%04d", (i-2)/2) {
			t.Fatalf("hset field order wrong at %d: %s", i, hset[i])
		}
	}
	assertSortedStrings(t, "hset fields", fields)
}

func TestZeroReferenceTimeRejected(t *testing.T) {
	_, err := BuildPlan(nil, Options{})
	if err == nil || !strings.Contains(err.Error(), "ReferenceTime") {
		t.Fatalf("expected ReferenceTime error, got %v", err)
	}
}

func TestWarningsSortedAndAggregated(t *testing.T) {
	events := collectFile(t, "stream_listpacks_1.rdb")
	plan, err := BuildPlanFromEvents(events, Options{
		ReferenceTime: refTime, Target: ProfileRedis74})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(plan.Warnings))
	for _, w := range plan.Warnings {
		ids = append(ids, w.ID)
	}
	assertSortedStrings(t, "warning ids", ids)
	// Consumer-time findings across consumers must aggregate into one warning.
	count := 0
	for _, w := range plan.Warnings {
		if w.Code == WarnConsumerTimeLoss {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 aggregated consumer-time warning, got %d", count)
	}
}

func TestStatsConsistency(t *testing.T) {
	events := collectFile(t, "multiple_databases.rdb")
	plan, err := BuildPlanFromEvents(events, Options{
		ReferenceTime: refTime, Target: ProfileRedis74})
	if err != nil {
		t.Fatal(err)
	}
	if plan.Stats.ItemCount != len(plan.Items) {
		t.Fatalf("item count mismatch")
	}
	if plan.Stats.TouchedDBCount != 2 {
		t.Fatalf("db count=%d", plan.Stats.TouchedDBCount)
	}
	nodes, commands := 0, 0
	for _, it := range plan.Items {
		nodes += len(it.Nodes)
		for _, n := range it.Nodes {
			for _, g := range n.Groups {
				commands += len(g.Commands)
			}
		}
	}
	if plan.Stats.NodeCount != nodes {
		t.Fatalf("node stats %d != %d", plan.Stats.NodeCount, nodes)
	}
	if plan.Stats.CommandCount != commands {
		t.Fatalf("cmd stats %d != %d", plan.Stats.CommandCount, commands)
	}
}

func TestAuxAndDBSizeIgnored(t *testing.T) {
	events := []Event{
		{Object: makeAux("redis-ver", "7.2.0")},
		{Object: makeString("real", "x")},
	}
	plan, err := BuildPlanFromEvents(events, Options{
		ReferenceTime: time.Now().UTC(), Target: ProfileRedis74})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Items) != 1 || plan.Items[0].Key != "real" {
		t.Fatalf("aux must not produce items: %v", itemIDs(plan))
	}
}
