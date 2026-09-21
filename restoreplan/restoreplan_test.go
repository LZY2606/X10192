package restoreplan

import (
	"bytes"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// decodeFixture parses an RDB fixture with special opcodes (functions/aux).
func decodeFixture(t *testing.T, path string) []model.RedisObject {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	dec := core.NewDecoder(f).WithSpecialOpCode()
	var out []model.RedisObject
	if err := dec.Parse(func(o model.RedisObject) bool {
		out = append(out, o)
		return true
	}); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return out
}

var fixedRef = time.UnixMilli(1700000000000) // 2023-11-14T22:13:20Z

func findItem(items []*PlanItem, db int, key string) *PlanItem {
	for _, it := range items {
		if it.Kind == KindKey && it.DB == db && it.Key == key {
			return it
		}
	}
	return nil
}

func findFunction(items []*PlanItem, lib string) *PlanItem {
	for _, it := range items {
		if it.Kind == KindFunctions && it.Key == "" &&
			(len(it.Create) == 1) && bytes.Contains([]byte(it.Create[0].Args[2]), []byte("name="+lib)) {
			return it
		}
	}
	return nil
}

func cmdTexts(cs []*Command) [][]string {
	out := make([][]string, len(cs))
	for i, c := range cs {
		out[i] = c.TextArgs()
	}
	return out
}

func containsCmd(cmds [][]string, want []string) bool {
	for _, c := range cmds {
		if reflect.DeepEqual(c, want) {
			return true
		}
	}
	return false
}

func warningCodes(item *PlanItem) map[string]bool {
	m := map[string]bool{}
	for _, w := range item.Warnings {
		m[w.Code] = true
	}
	return m
}

// TestBaselineTypes asserts structured plans for string/hash/list/set/zset.
func TestBaselineTypes(t *testing.T) {
	var objects []model.RedisObject
	objects = append(objects, decodeFixture(t, "../cases/easily_compressible_string_key.rdb")...)
	objects = append(objects, decodeFixture(t, "../cases/hash_as_ziplist.rdb")...)
	objects = append(objects, decodeFixture(t, "../cases/ziplist_that_compresses_easily.rdb")...)
	objects = append(objects, decodeFixture(t, "../cases/regular_set.rdb")...)
	objects = append(objects, decodeFixture(t, "../cases/sorted_set_as_ziplist.rdb")...)

	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis74})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Blockers) != 0 {
		t.Fatalf("unexpected blockers: %+v", plan.Blockers)
	}

	// string
	str := findItem(plan.Items, 0, "force_compressible_string_key")
	if str == nil {
		t.Fatalf("string item missing")
	}
	if !reflect.DeepEqual(str.Create[0].TextArgs(), []string{"SET", "force_compressible_string_key", "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"}) {
		t.Fatalf("bad SET: %v", str.Create[0].TextArgs())
	}

	// hash: fields sorted deterministically, HSET (not HMSET)
	hash := findItem(plan.Items, 0, "zipmap_compresses_easily")
	if hash == nil {
		t.Fatalf("hash item missing")
	}
	got := hash.Create[0].TextArgs()
	want := []string{"HSET", "zipmap_compresses_easily", "a", "aa", "aa", "aaaa", "aaaaa", "aaaaaaaaaaaaaa"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hash HSET = %v want %v", got, want)
	}

	// list: order preserved
	list := findItem(plan.Items, 0, "ziplist_compresses_easily")
	if list == nil || list.Create[0].Name != "RPUSH" {
		t.Fatalf("list restore wrong: %+v", list)
	}

	// set: members byte-sorted
	set := findItem(plan.Items, 0, "regular_set")
	if set == nil {
		t.Fatalf("set item missing")
	}
	setArgs := set.Create[0].TextArgs()
	wantSet := []string{"SADD", "regular_set", "alpha", "beta", "delta", "gamma", "kappa", "phi"}
	if !reflect.DeepEqual(setArgs, wantSet) {
		t.Fatalf("set SADD = %v want %v", setArgs, wantSet)
	}

	// zset: score/member pairs survive
	zset := findItem(plan.Items, 0, "sorted_set_as_ziplist")
	if zset == nil || zset.Create[0].Name != "ZADD" {
		t.Fatalf("zset restore wrong: %+v", zset)
	}
	if !containsCmd(cmdTexts(zset.Create), []string{
		"ZADD", "sorted_set_as_ziplist", "1", "8b6ba6718a786daefa69438148361901",
		"2.37", "cb7a24bb7528f934b841b34c3a73e0c7", "3.423", "523af537946b79c4f8369ed39ba78605",
	}) {
		t.Fatalf("zset command mismatch: %v", cmdTexts(zset.Create))
	}

	// every normal item is a single atomic command here
	for _, it := range plan.Items {
		if it.Status == StatusReady && !it.Atomicity.Atomic {
			t.Fatalf("single-command item %s not atomic", it.ID)
		}
	}
}

// TestMultiDB verifies explicit SELECT steps and ordering.
func TestMultiDB(t *testing.T) {
	objects := decodeFixture(t, "../cases/multiple_databases.rdb")
	plan, err := Generate(objects, Options{ReferenceTime: fixedRef, Target: Redis74})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.DBS, []int{0, 2}) {
		t.Fatalf("dbs = %v", plan.DBS)
	}
	var selects []int
	for _, s := range plan.Steps {
		if s.Cmd != nil && s.Cmd.Name == "SELECT" {
			selects = append(selects, s.DB)
		}
	}
	if !reflect.DeepEqual(selects, []int{0, 2}) {
		t.Fatalf("select steps = %v", selects)
	}
	// item for db 2 must come after item for db 0
	idxZero, idxSecond := -1, -1
	for i, it := range plan.Items {
		if it.Key == "key_in_zeroth_database" {
			idxZero = i
		}
		if it.Key == "key_in_second_database" {
			idxSecond = i
		}
	}
	if !(idxZero >= 0 && idxSecond > idxZero) {
		t.Fatalf("items not db-ordered: %d %d", idxZero, idxSecond)
	}
}

// TestExpirationAbsoluteAndRelative covers millisecond fixture plus a
// hand-crafted second-precision RDB.
func TestExpirationAbsoluteAndRelative(t *testing.T) {
	// millisecond fixture (expires 2022, well before fixedRef 2023).
	msObjs := decodeFixture(t, "../cases/keys_with_expiry.rdb")

	t.Run("skip expired by default", func(t *testing.T) {
		plan, err := Generate(msObjs, Options{ReferenceTime: fixedRef, Target: Redis74})
		if err != nil {
			t.Fatal(err)
		}
		if len(plan.SkippedExpired) != 1 || plan.SkippedExpired[0].Key != "expires_ms_precision" {
			t.Fatalf("skipped = %+v", plan.SkippedExpired)
		}
	})

	t.Run("keep expired with absolute PEXPIREAT", func(t *testing.T) {
		plan, err := Generate(msObjs, Options{
			ReferenceTime: fixedRef, Target: Redis74, ExpiredKeys: ExpiredKeep,
		})
		if err != nil {
			t.Fatal(err)
		}
		it := findItem(plan.Items, 0, "expires_ms_precision")
		if it == nil || it.Expiry == nil {
			t.Fatalf("item/expiry missing")
		}
		if it.Expiry.AbsoluteAtMs != 1671963072573 {
			t.Fatalf("abs ms = %d", it.Expiry.AbsoluteAtMs)
		}
		if !containsCmd(cmdTexts(it.FollowUps), []string{"PEXPIREAT", "expires_ms_precision", "1671963072573"}) {
			t.Fatalf("no PEXPIREAT: %v", cmdTexts(it.FollowUps))
		}
	})

	t.Run("error policy", func(t *testing.T) {
		_, err := Generate(msObjs, Options{
			ReferenceTime: fixedRef, Target: Redis74, ExpiredKeys: ExpiredError,
		})
		if _, ok := err.(*ExpiredKeyError); !ok {
			t.Fatalf("want *ExpiredKeyError, got %v", err)
		}
	})

	// future-dated second precision RDB built in-memory (minimal new fixture):
	// string "sec" with opCodeExpireTime at 2100-01-01T00:00:00Z.
	secondObjs := decodeBytes(t, buildSecondsExpiryRDB(t))
	t.Run("relative TTL replay", func(t *testing.T) {
		abs, err := Generate(secondObjs, Options{
			ReferenceTime: fixedRef, Target: Redis74, ExpiryMode: ExpiryAbsolute,
		})
		if err != nil {
			t.Fatal(err)
		}
		it := findItem(abs.Items, 0, "sec")
		if it == nil || it.Expiry == nil {
			t.Fatalf("sec item missing")
		}
		wantAbs := time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano() / int64(time.Millisecond)
		if it.Expiry.AbsoluteAtMs != wantAbs {
			t.Fatalf("abs = %d want %d", it.Expiry.AbsoluteAtMs, wantAbs)
		}
		if it.Expiry.Expired {
			t.Fatalf("future key marked expired")
		}

		rel, err := Generate(secondObjs, Options{
			ReferenceTime: fixedRef, Target: Redis74, ExpiryMode: ExpiryRelative,
		})
		if err != nil {
			t.Fatal(err)
		}
		rit := findItem(rel.Items, 0, "sec")
		wantTTL := wantAbs - fixedRef.UnixNano()/int64(time.Millisecond)
		if rit.Expiry.RelativeTTLMs != wantTTL {
			t.Fatalf("ttl = %d want %d", rit.Expiry.RelativeTTLMs, wantTTL)
		}
		if !containsCmd(cmdTexts(rit.FollowUps), []string{"PEXPIRE", "sec", strconv.FormatInt(wantTTL, 10)}) {
			t.Fatalf("no PEXPIRE: %v", cmdTexts(rit.FollowUps))
		}
	})
}
