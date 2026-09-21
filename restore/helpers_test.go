package restore

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/hdt3213/rdb/model"
)

var refTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func findItem(plan *RestorePlan, id string) *PlanItem {
	for _, it := range plan.Items {
		if it.ID == id {
			return it
		}
	}
	return nil
}

func mustFindItem(t *testing.T, plan *RestorePlan, id string) *PlanItem {
	t.Helper()
	it := findItem(plan, id)
	if it == nil {
		t.Fatalf("item %q not found, have: %v", id, itemIDs(plan))
	}
	return it
}

func itemIDs(plan *RestorePlan) []string {
	out := make([]string, 0, len(plan.Items))
	for _, it := range plan.Items {
		out = append(out, it.ID)
	}
	return out
}

func nodeKinds(item *PlanItem) []string {
	out := make([]string, 0, len(item.Nodes))
	for _, n := range item.Nodes {
		out = append(out, n.Kind)
	}
	return out
}

func nodeByID(item *PlanItem, id string) *PlanNode {
	for _, n := range item.Nodes {
		if n.ID == id {
			return n
		}
	}
	return nil
}

func allCommands(item *PlanItem) []string {
	var out []string
	for _, n := range item.Nodes {
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				if len(c.Args) > 0 {
					out = append(out, c.Args[0])
				}
			}
		}
	}
	return out
}

func hasCmdPrefix(item *PlanItem, prefix ...string) bool {
	for _, n := range item.Nodes {
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				if argsHavePrefix(c.Args, prefix) {
					return true
				}
			}
		}
	}
	return false
}

func nodeHasCmdPrefix(node *PlanNode, prefix ...string) bool {
	for _, g := range node.Groups {
		for _, c := range g.Commands {
			if argsHavePrefix(c.Args, prefix) {
				return true
			}
		}
	}
	return false
}

func argsHavePrefix(args, prefix []string) bool {
	if len(args) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if args[i] != p {
			return false
		}
	}
	return true
}

func warningCodes(plan *RestorePlan) map[string]*Warning {
	out := make(map[string]*Warning)
	for _, w := range plan.Warnings {
		out[w.Code] = w
	}
	return out
}

func itemHasCode(item *PlanItem, plan *RestorePlan, code string) bool {
	for _, id := range item.WarningIDs {
		for _, w := range plan.Warnings {
			if w.ID == id && w.Code == code {
				return true
			}
		}
	}
	return false
}

// shuffleEvents returns a reordered copy of events using the given rng.
func shuffleEvents(rng *rand.Rand, events []Event) []Event {
	out := make([]Event, len(events))
	copy(out, events)
	rng.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	return out
}

func assertDeterministic(t *testing.T, events []Event, opts Options) {
	t.Helper()
	p1, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatalf("first build: %v", err)
	}
	b1, err := json.Marshal(p1)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	p2, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatalf("second build: %v", err)
	}
	b2, err := json.Marshal(p2)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatal("plan is not deterministic across repeated builds")
	}

	// Reordering the input event container must not change the plan bytes.
	rng := rand.New(rand.NewSource(42))
	for i := 0; i < 10; i++ {
		shuffled := shuffleEvents(rng, events)
		ps, err := BuildPlanFromEvents(shuffled, opts)
		if err != nil {
			t.Fatalf("shuffled build: %v", err)
		}
		bs, err := json.Marshal(ps)
		if err != nil {
			t.Fatalf("marshal shuffled: %v", err)
		}
		if string(bs) != string(b1) {
			t.Fatalf("plan changed after event shuffle (iter %d)", i)
		}
	}
}

func assertSortedStrings(t *testing.T, label string, s []string) {
	t.Helper()
	if !sort.StringsAreSorted(s) {
		t.Fatalf("%s not sorted: %v", label, s)
	}
}

func assertDeepEqual(t *testing.T, label string, got, want interface{}) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s mismatch:\n got=%v\nwant=%v", label, got, want)
	}
}

func makeString(key, value string) model.RedisObject {
	return &model.StringObject{
		BaseObject: &model.BaseObject{DB: 0, Key: key, Type: model.StringType, Encoding: model.StringEncoding},
		Value:      []byte(value),
	}
}

func makeHash(key string, hash map[string][]byte) model.RedisObject {
	return &model.HashObject{
		BaseObject: &model.BaseObject{DB: 0, Key: key, Type: model.HashType, Encoding: model.HashEncoding},
		Hash:       hash,
	}
}

func makeAux(key, value string) model.RedisObject {
	return &model.AuxObject{
		BaseObject: &model.BaseObject{Key: key, Type: model.AuxType},
		Value:      value,
	}
}
