package restoreplan

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/hdt3213/rdb/model"
)

func names(cmds []*Command) []string {
	out := make([]string, len(cmds))
	for i, c := range cmds {
		out[i] = c.Name
	}
	return out
}

func phaseNames(item *PlanItem) []string {
	var out []string
	out = append(out, names(item.Create)...)
	out = append(out, names(item.FollowUps)...)
	return out
}

// TestStreamGroupAndPEL uses the real RDB v1 fixture with four groups,
// consumers and a populated pending entries list.
func TestStreamGroupAndPEL(t *testing.T) {
	objects := decodeFixture(t, "../cases/stream_listpacks_1.json"[:0]+
		"../cases/stream_listpacks_1.rdb")
	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis74})
	if err != nil {
		t.Fatal(err)
	}
	item := findItem(plan.Items, 0, "listpack")
	if item == nil {
		t.Fatalf("stream item missing")
	}

	all := phaseNames(item)
	// ordering: XADD entries -> XSETID -> groups (XGROUP CREATE,
	// CREATECONSUMER, XCLAIM)
	xaddEnd, xsetIdx, createIdx, claimIdx := -1, -1, -1, -1
	for i, n := range all {
		switch n {
		case "XADD":
			xaddEnd = i
		case "XSETID":
			if xsetIdx == -1 {
				xsetIdx = i
			}
		case "XGROUP":
			for _, a := range item.FollowUps {
				if len(a.Args) > 0 && string(a.Args[0]) == "CREATE" {
					if createIdx == -1 {
						createIdx = i
					}
				}
			}
		case "XCLAIM":
			if claimIdx == -1 {
				claimIdx = i
			}
		}
	}
	if !(xaddEnd >= 0 && xsetIdx > xaddEnd && createIdx > xsetIdx && claimIdx > createIdx) {
		t.Fatalf("stream command ordering wrong: xadd=%d xsetid=%d create=%d claim=%d all=%v",
			xaddEnd, xsetIdx, createIdx, claimIdx, all)
	}

	// last-delivered-id pinned for g4 (group without consumers/PEL)
	foundG4 := false
	for _, c := range item.FollowUps {
		if c.Name == "XGROUP" && len(c.Args) >= 3 && string(c.Args[1]) == "g4" {
			if string(c.Args[2]) != "1528507831415-0" {
				t.Fatalf("g4 lastId = %v", c.TextArgs())
			}
			foundG4 = true
		}
	}
	if !foundG4 {
		t.Fatalf("g4 create missing")
	}

	// PEL claim pins exact delivery time and retry count, sorted by id
	var claim152 []*Command
	for _, c := range item.FollowUps {
		if c.Name == "XCLAIM" {
			claim152 = append(claim152, c)
		}
	}
	if len(claim152) != 7 { // 4 PEL in g1 + 1 in g2 + 2 in g3
		t.Fatalf("XCLAIM count = %d want 7", len(claim152))
	}
	first := claim152[0].TextArgs()
	if first[4] != "1528507816450-0" {
		t.Fatalf("PEL not id-sorted: %v", first)
	}
	if !reflect.DeepEqual(first[5:9], []string{"TIME", "1528516636879", "RETRYCOUNT", "1"}) {
		t.Fatalf("PEL time/retry not pinned: %v", first[5:])
	}
	if first[3] != "c1" {
		t.Fatalf("PEL owner c1 expected, got %q", first[3])
	}

	// consumer seen/active times are always flagged as unreplayable
	wc := warningCodes(item)
	if !wc["consumer-time-lost"] {
		t.Fatalf("missing consumer-time-lost warning, got %v", wc)
	}

	// atomicity boundary must be explicit for multi-command stream
	if item.Atomicity.Atomic || item.Atomicity.NumCommands != len(all) {
		t.Fatalf("stream atomicity wrong: %+v", item.Atomicity)
	}
}

// TestStreamEntriesRead covers v3 ENTRIESREAD + PEL + metadata.
func TestStreamEntriesRead(t *testing.T) {
	objects := decodeFixture(t, "../cases/stream_listoacks_3.rdb")
	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis74})
	if err != nil {
		t.Fatal(err)
	}
	item := findItem(plan.Items, 0, "mystream")
	if item == nil {
		t.Fatal("mystream missing")
	}
	foundEntriesRead, foundMeta := false, false
	for _, c := range item.FollowUps {
		if c.Name == "XGROUP" {
			for i, a := range c.Args {
				if string(a) == "ENTRIESREAD" && i+1 < len(c.Args) && string(c.Args[i+1]) == "1" {
					foundEntriesRead = true
				}
			}
		}
		if c.Name == "XSETID" {
			for i, a := range c.Args {
				if string(a) == "ENTRIESADDED" && i+1 < len(c.Args) && string(c.Args[i+1]) == "1" {
					foundMeta = true
				}
			}
		}
	}
	if !foundEntriesRead || !foundMeta {
		t.Fatalf("entriesRead=%v xsetidMeta=%v cmds=%v", foundEntriesRead, foundMeta, cmdTexts(item.FollowUps))
	}

	// older target loses entries-read / stream counters explicitly
	old, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis62})
	if err != nil {
		t.Fatal(err)
	}
	oldItem := findItem(old.Items, 0, "mystream")
	codes := map[string]bool{}
	for _, b := range oldItem.PartialBlockers {
		codes[b.Code] = true
	}
	if !codes["xsetid-metadata-unsupported"] {
		t.Fatalf("expected metadata blocker, got %+v", oldItem.PartialBlockers)
	}
}

// TestFunctions covers function library restore plus target gating.
func TestFunctions(t *testing.T) {
	objects := decodeFixture(t, "../cases/function.rdb")
	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis74})
	if err != nil {
		t.Fatal(err)
	}
	lib := findFunction(plan.Items, "mylib")
	if lib == nil {
		t.Fatalf("function item missing")
	}
	if !reflect.DeepEqual(lib.Create[0].TextArgs()[:3], []string{"FUNCTION", "LOAD", "REPLACE"}) {
		t.Fatalf("function cmd = %v", lib.Create[0].TextArgs())
	}

	old, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis62})
	if err != nil {
		t.Fatal(err)
	}
	libOld := findFunction(old.Items, "mylib")
	if libOld == nil || libOld.Status != StatusBlocked ||
		libOld.Blocker == nil || libOld.Blocker.Code != "functions-unsupported" {
		t.Fatalf("expected functions blocker: %+v", libOld)
	}
}

// TestUnknownModule asserts a fatal blocker with evidence (never a silent skip).
func TestUnknownModule(t *testing.T) {
	objects := decodeBytes(t, buildUnknownModuleRDB(t))
	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis74})
	if err != nil {
		t.Fatal(err)
	}
	item := findItem(plan.Items, 0, "modkey")
	if item == nil {
		t.Fatal("module item missing")
	}
	if item.Status != StatusBlocked || item.Blocker == nil ||
		item.Blocker.Code != "module-type-unsupported" || !item.Blocker.Fatal {
		t.Fatalf("module not blocked: %+v", item)
	}
	if item.Blocker.RequiredCapability != "module:test-type" {
		t.Fatalf("cap = %q", item.Blocker.RequiredCapability)
	}
	// no executable command must be emitted for a fatal blocker
	if len(item.Create)+len(item.FollowUps) != 0 {
		t.Fatalf("blocked module has commands")
	}
	// blocker must surface at plan level
	found := false
	for _, b := range plan.Blockers {
		if b.Code == "module-type-unsupported" && b.ItemID == item.ID {
			found = true
		}
	}
	if !found {
		t.Fatalf("blocker not aggregated: %+v", plan.Blockers)
	}
}

// TestDeterminism checks byte-identical plans across repeated generation and
// after reordering the input event container.
func TestDeterminism(t *testing.T) {
	var objects []model.RedisObject
	objects = append(objects, decodeFixture(t, "../cases/multiple_databases.rdb")...)
	objects = append(objects, decodeFixture(t, "../cases/stream_listoacks_3.rdb")...)
	objects = append(objects, decodeFixture(t, "../cases/function.rdb")...)
	objects = append(objects, decodeFixture(t, "../cases/hash_with_hfe.rdb")...)

	makeJSON := func(objs []model.RedisObject) []byte {
		plan, err := Generate(objs, Options{
			ReferenceTime: fixedRef, Target: Redis74, ExpiryMode: ExpiryRelative,
		})
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(plan)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	first := makeJSON(objects)
	for i := 0; i < 5; i++ {
		if got := makeJSON(objects); !reflect.DeepEqual(got, first) {
			t.Fatalf("generation %d differs", i)
		}
	}

	// reverse the container order
	reversed := append([]model.RedisObject(nil), objects...)
	for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
		reversed[i], reversed[j] = reversed[j], reversed[i]
	}
	if got := makeJSON(reversed); !reflect.DeepEqual(got, first) {
		t.Fatalf("plan differs after event reordering")
	}

	// JSON itself must serialize identically again (no map nondeterminism)
	if got := makeJSON(objects); !reflect.DeepEqual(got, first) {
		t.Fatalf("re-marshal differs")
	}
}

// TestHashFieldExpiry exercises HPEXPIREAT follow-ups and old-target gating.
func TestHashFieldExpiry(t *testing.T) {
	objects := decodeFixture(t, "../cases/hash_with_hfe.rdb")
	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis74})
	if err != nil {
		t.Fatal(err)
	}
	item := findItem(plan.Items, 0, "hash-hfe")
	if item == nil {
		t.Fatal("hfe item missing")
	}
	// fields F1..F3 have future absolute expirations (year 2057+), F4..F8 persistent
	count := 0
	for _, c := range item.FollowUps {
		if c.Name == "HPEXPIREAT" {
			count++
		}
	}
	if count != 3 {
		t.Fatalf("HPEXPIREAT count = %d want 3: %v", count, cmdTexts(item.FollowUps))
	}

	old, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis72})
	if err != nil {
		t.Fatal(err)
	}
	oldItem := findItem(old.Items, 0, "hash-hfe")
	if oldItem.Status != StatusLossy {
		t.Fatalf("status = %s want lossy", oldItem.Status)
	}
	found := false
	for _, b := range oldItem.PartialBlockers {
		if b.Code == "hash-field-expiry-unsupported" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected hfe blocker: %+v", oldItem.PartialBlockers)
	}
	if len(oldItem.Create) == 0 {
		t.Fatalf("hash content must still be restorable")
	}
}

// TestStreamsUnsupportedProfile gates streams on an old profile.
func TestStreamsUnsupportedProfile(t *testing.T) {
	objects := decodeFixture(t, "../cases/stream_listoacks_3.rdb")
	old := &CapabilityProfile{Name: "redis-4.0.0", Flavor: "redis", Major: 4}
	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: old})
	if err != nil {
		t.Fatal(err)
	}
	item := findItem(plan.Items, 0, "mystream")
	if item.Status != StatusBlocked || item.Blocker.Code != "streams-unsupported" {
		t.Fatalf("stream not gated: %+v", item)
	}
}

var _ = time.UnixMilli
