package restore

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

func decodeObjects(t *testing.T, path string) []model.RedisObject {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var objects []model.RedisObject
	decoder := core.NewDecoder(file).WithSpecialOpCode()
	if err := decoder.Parse(func(object model.RedisObject) bool {
		objects = append(objects, object)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return objects
}

func keyObjects(t *testing.T, paths ...string) []model.RedisObject {
	t.Helper()
	var objects []model.RedisObject
	for _, path := range paths {
		objects = append(objects, decodeObjects(t, path)...)
	}
	return objects
}

func referenceAt(t time.Time) Options {
	return Options{ReferenceTime: t, Profile: Redis74Profile(), ExpirationMode: ExpirationAbsolute}
}

func findItem(plan *RestorePlan, db int, key string) *PlanItem {
	for i := range plan.Items {
		item := &plan.Items[i]
		if item.TargetDB == db && item.Key == key {
			return item
		}
	}
	return nil
}

func commandArgs(item *PlanItem, role string) []string {
	commands := append(append([]PlannedCommand{}, item.CreateCommands...), item.FollowUpCommands...)
	for _, cmd := range commands {
		if cmd.Role == role {
			return argStrings(cmd.Args)
		}
	}
	return nil
}

func allCommandArgs(item *PlanItem) [][]string {
	commands := append(append([]PlannedCommand{}, item.CreateCommands...), item.FollowUpCommands...)
	out := make([][]string, 0, len(commands))
	for _, cmd := range commands {
		out = append(out, argStrings(cmd.Args))
	}
	return out
}

func argStrings(args []Arg) []string {
	out := make([]string, 0, len(args))
	for _, arg := range args {
		out = append(out, arg.Text)
	}
	return out
}

func containsCommand(item *PlanItem, want []string) bool {
	for _, got := range allCommandArgs(item) {
		if reflect.DeepEqual(got, want) {
			return true
		}
	}
	return false
}

func hasWarning(item *PlanItem, code string) bool {
	for _, warning := range item.Warnings {
		if warning.Code == code {
			return true
		}
	}
	return false
}

func TestPlanBasicTypesAndMultipleDBsDeterministic(t *testing.T) {
	objects := keyObjects(t,
		"../cases/hash_as_ziplist.rdb",
		"../cases/listpack.rdb",
		"../cases/intset_16.rdb",
		"../cases/sorted_set_as_ziplist.rdb",
		"../cases/multiple_databases.rdb",
	)
	opts := referenceAt(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	first, err := PlanFromObjects(objects, opts)
	if err != nil {
		t.Fatal(err)
	}

	if item := findItem(first, 0, "key_in_zeroth_database"); item == nil || item.ObjectType != model.StringType || !containsCommand(item, []string{"SET", "key_in_zeroth_database", "zero"}) {
		t.Fatalf("db0 string plan mismatch: %#v", item)
	}
	if item := findItem(first, 2, "key_in_second_database"); item == nil || item.TargetDB != 2 || !containsCommand(item, []string{"SET", "key_in_second_database", "second"}) {
		t.Fatalf("db2 string plan mismatch: %#v", item)
	}
	if len(first.Databases) < 2 || first.Databases[0].DB != 0 || first.Databases[1].DB != 2 {
		t.Fatalf("databases not explicit/sorted: %#v", first.Databases)
	}
	if first.Databases[1].SelectNodeID != "db0000000002:select" {
		t.Fatalf("unexpected select id: %s", first.Databases[1].SelectNodeID)
	}

	list := findItem(first, 0, "l")
	if list == nil {
		t.Fatal("list item not found")
	}
	if got := commandArgs(list, RoleCreate); len(got) < 3 || got[0] != "RPUSH" || got[1] != "l" || !reflect.DeepEqual(got[2:], []string{"1", "20000", "aaaa", "4", "16380", "-16380", "1048576", "268435456", "8589934592"}) {
		t.Fatalf("unexpected list command: %#v", got)
	}

	set := findItem(first, 0, "intset_16")
	if set == nil {
		t.Fatal("set item not found")
	}
	if got := commandArgs(set, RoleCreate); !reflect.DeepEqual(got[2:], []string{"32764", "32765", "32766"}) {
		t.Fatalf("set members not sorted: %#v", got)
	}

	zset := findItem(first, 0, "sorted_set_as_ziplist")
	if zset == nil {
		t.Fatal("zset item not found")
	}
	if got := commandArgs(zset, RoleCreate); !reflect.DeepEqual(got[2:], []string{"1", "8b6ba6718a786daefa69438148361901", "2.37", "cb7a24bb7528f934b841b34c3a73e0c7", "3.423", "523af537946b79c4f8369ed39ba78605"}) {
		t.Fatalf("zset entries not deterministic: %#v", got)
	}

	hash := findItem(first, 0, "zipmap_compresses_easily")
	if hash == nil {
		t.Fatal("hash item not found")
	}
	if got := commandArgs(hash, RoleCreate); len(got) < 4 || got[0] != "HSET" || !reflect.DeepEqual(got[2:], []string{"a", "aa", "aa", "aaaa", "aaaaa", "aaaaaaaaaaaaaa"}) {
		t.Fatalf("hash fields not sorted: %#v", got)
	}

	shuffled := append([]model.RedisObject(nil), objects...)
	sort.SliceStable(shuffled, func(i, j int) bool { return i > j })
	second, err := PlanFromObjects(shuffled, opts)
	if err != nil {
		t.Fatal(err)
	}
	firstJSON, _ := json.Marshal(first)
	secondJSON, _ := json.Marshal(second)
	if !bytes.Equal(firstJSON, secondJSON) {
		t.Fatalf("plan changed after event reordering\nfirst=%s\nsecond=%s", firstJSON, secondJSON)
	}
}

func TestPlanExpirationSemanticsAndPolicies(t *testing.T) {
	objects := keyObjects(t, "../cases/restore_expirations.rdb")
	reference := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	plan, err := PlanFromObjects(objects, Options{ReferenceTime: reference, Profile: Redis74Profile(), ExpirationMode: ExpirationAbsolute})
	if err != nil {
		t.Fatal(err)
	}
	seconds := findItem(plan, 0, "seconds_ttl")
	if seconds == nil || seconds.Source.ByteOffset != 19 || seconds.Expiration == nil || seconds.Expiration.SourceUnit != model.SecondExpiration {
		t.Fatalf("seconds expiration missing: %#v", seconds)
	}
	if got := commandArgs(seconds, RoleExpiration); !reflect.DeepEqual(got, []string{"PEXPIREAT", "seconds_ttl", "1893553445000"}) {
		t.Fatalf("absolute seconds replay should preserve millisecond timestamp: %#v", got)
	}
	milliseconds := findItem(plan, 0, "milliseconds_ttl")
	if milliseconds == nil || milliseconds.Source.ByteOffset != 48 || milliseconds.Expiration == nil || milliseconds.Expiration.SourceUnit != model.MillisecondExpiration || milliseconds.Expiration.RelativeTTLMillis != 97445678 {
		t.Fatalf("millisecond expiration mismatch: %#v", milliseconds)
	}
	if got := commandArgs(milliseconds, RoleExpiration); !reflect.DeepEqual(got, []string{"PEXPIREAT", "milliseconds_ttl", "1893553445678"}) {
		t.Fatalf("millisecond replay mismatch: %#v", got)
	}
	relativeSeconds, err := PlanFromObjects(objects, Options{ReferenceTime: reference, Profile: Redis74Profile(), ExpirationMode: ExpirationRelative})
	if err != nil {
		t.Fatal(err)
	}
	seconds = findItem(relativeSeconds, 0, "seconds_ttl")
	if got := commandArgs(seconds, RoleExpiration); !reflect.DeepEqual(got, []string{"EXPIRE", "seconds_ttl", "97445"}) {
		t.Fatalf("relative seconds TTL mismatch: %#v", got)
	}
	milliseconds = findItem(relativeSeconds, 0, "milliseconds_ttl")
	if got := commandArgs(milliseconds, RoleExpiration); !reflect.DeepEqual(got, []string{"PEXPIRE", "milliseconds_ttl", "97445678"}) {
		t.Fatalf("relative millisecond TTL mismatch: %#v", got)
	}
	expiredObjects := keyObjects(t, "../cases/expiration.rdb")
	skipped, err := PlanFromObjects(expiredObjects, Options{ReferenceTime: reference, Profile: Redis74Profile(), ExpiredKeys: ExpiredKeySkip})
	if err != nil {
		t.Fatal(err)
	}
	expired := findItem(skipped, 0, "expired")
	if expired == nil || expired.Status != ItemStatusSkipped || len(expired.CreateCommands) != 0 || len(expired.FollowUpCommands) != 0 || !hasWarning(expired, "expired-key-skipped") {
		t.Fatalf("skip policy mismatch: %#v", expired)
	}
	kept, err := PlanFromObjects(expiredObjects, Options{ReferenceTime: reference, Profile: Redis74Profile(), ExpiredKeys: ExpiredKeyKeep})
	if err != nil {
		t.Fatal(err)
	}
	expired = findItem(kept, 0, "expired")
	if expired == nil || expired.Status != ItemStatusActive || len(expired.CreateCommands) != 1 || expired.Expiration == nil || expired.Expiration.Action != "keep-without-ttl" {
		t.Fatalf("keep policy mismatch: %#v", expired)
	}
	errored, err := PlanFromObjects(expiredObjects, Options{ReferenceTime: reference, Profile: Redis74Profile(), ExpiredKeys: ExpiredKeyError})
	if err != nil {
		t.Fatal(err)
	}
	expired = findItem(errored, 0, "expired")
	if expired == nil || expired.Status != ItemStatusBlocked || len(expired.Blockers) != 1 || expired.Blockers[0].Code != "expired-key" {
		t.Fatalf("error policy mismatch: %#v", expired)
	}
}

func TestPlanStreamGroupsConsumersAndPEL(t *testing.T) {
	objects := keyObjects(t, "../cases/stream_listoacks_3.rdb")
	opts := referenceAt(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	plan, err := PlanFromObjects(objects, opts)
	if err != nil {
		t.Fatal(err)
	}
	stream := findItem(plan, 0, "mystream")
	if stream == nil {
		t.Fatal("stream item missing")
	}
	if got := commandArgs(stream, RoleCreate); !reflect.DeepEqual(got, []string{"XADD", "mystream", "1704557973866-0", "name", "Sara", "surname", "OConnor"}) {
		t.Fatalf("stream entry command mismatch: %#v", got)
	}
	var group *PlannedCommand
	var pel *PlannedCommand
	for i := range stream.FollowUpCommands {
		switch stream.FollowUpCommands[i].Role {
		case RoleStreamGroup:
			group = &stream.FollowUpCommands[i]
		case RoleStreamPEL:
			pel = &stream.FollowUpCommands[i]
		}
	}
	if group == nil {
		t.Fatal("group command missing")
	}
	if got := argStrings(group.Args); !reflect.DeepEqual(got, []string{"XGROUP", "CREATE", "mystream", "consumer-group-name", "1704557973866-0", "ENTRIESREAD", "1"}) {
		t.Fatalf("group command mismatch: %#v", got)
	}
	if pel == nil {
		t.Fatal("PEL command missing")
	}
	want := []string{"XCLAIM", "mystream", "consumer-group-name", "consumer-name", "0", "1704557973866-0", "TIME", "1704557998397", "RETRYCOUNT", "1", "FORCE", "JUSTID", "LASTID", "1704557973866-0"}
	if got := argStrings(pel.Args); !reflect.DeepEqual(got, want) {
		t.Fatalf("PEL command mismatch: %#v", got)
	}
	if len(pel.DependsOn) != 1 || pel.DependsOn[0] != group.ID {
		t.Fatalf("PEL must depend on group: %#v", pel.DependsOn)
	}
	if len(group.DependsOn) != 1 || group.DependsOn[0] != stream.CreateCommands[0].ID {
		t.Fatalf("group must depend on entries: %#v", group.DependsOn)
	}
	if !hasWarning(stream, "stream-consumer-time-not-restorable") {
		t.Fatalf("consumer active/seen time loss was not noted: %#v", stream.Warnings)
	}
	if !hasWarning(stream, "stream-metadata-not-restorable") {
		t.Fatalf("entries-read/internal metadata warning missing: %#v", stream.Warnings)
	}
	if stream.LogicalGroup.GroupID == "" || len(stream.LogicalGroup.CommandIDs) < 3 || stream.LogicalGroup.NonAtomicBecause == "" {
		t.Fatalf("logical atomicity boundary missing: %#v", stream.LogicalGroup)
	}
}

func TestPlanFunctionsAndUnknownModule(t *testing.T) {
	functionObjects := keyObjects(t, "../cases/function.rdb")
	opts := referenceAt(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	plan, err := PlanFromObjects(functionObjects, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.GlobalItems) != 1 {
		t.Fatalf("expected one function item, got %#v", plan.GlobalItems)
	}
	function := plan.GlobalItems[0]
	if got := argStrings(function.CreateCommands[0].Args); len(got) != 4 || got[0] != "FUNCTION" || got[1] != "LOAD" || got[2] != "REPLACE" || got[3] != "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)" {
		t.Fatalf("function restore command mismatch: %#v", got)
	}
	old, err := PlanFromObjects(functionObjects, Options{ReferenceTime: opts.ReferenceTime, Profile: Redis6Profile()})
	if err != nil {
		t.Fatal(err)
	}
	if len(old.GlobalItems) != 1 || old.GlobalItems[0].Status != ItemStatusBlocked || old.GlobalItems[0].Blockers[0].Code != "functions-unsupported" {
		t.Fatalf("functions must be an evidence-backed blocker on Redis 6: %#v", old.GlobalItems)
	}
	moduleObjects := keyObjects(t, "../cases/restore_unknown_module.rdb")
	modulePlan, err := PlanFromObjects(moduleObjects, opts)
	if err != nil {
		t.Fatal(err)
	}
	module := findItem(modulePlan, 0, "unknown_module_key")
	if module == nil || module.Status != ItemStatusBlocked || len(module.Blockers) != 1 || module.Blockers[0].Code != "module-value-unsupported" {
		t.Fatalf("unknown module must not be skipped: %#v", module)
	}
	if len(modulePlan.Blockers) != 1 || len(modulePlan.Nodes) != 1 {
		t.Fatalf("blocked module must not generate replay nodes: nodes=%#v blockers=%#v", modulePlan.Nodes, modulePlan.Blockers)
	}
}

func TestPlanHashFieldExpirationCapability(t *testing.T) {
	objects := keyObjects(t, "../cases/hash_as_listpack_with_hfe.rdb")
	opts := referenceAt(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	plan, err := PlanFromObjects(objects, opts)
	if err != nil {
		t.Fatal(err)
	}
	hash := findItem(plan, 0, "listpack-hfe")
	if hash == nil {
		t.Fatal("hash item missing")
	}
	var hpe []string
	for _, cmd := range hash.FollowUpCommands {
		if cmd.Role == RoleHashFieldExpiration {
			args := argStrings(cmd.Args)
			if args[0] == "HPEXPIREAT" {
				hpe = args
			}
		}
	}
	if len(hpe) == 0 || hpe[3] != "FIELDS" || hpe[4] != "1" {
		t.Fatalf("expected deterministic HPEXPIREAT command, got %#v", hash.FollowUpCommands)
	}
	old, err := PlanFromObjects(objects, Options{ReferenceTime: opts.ReferenceTime, Profile: Redis72Profile()})
	if err != nil {
		t.Fatal(err)
	}
	hash = findItem(old, 0, "listpack-hfe")
	if hash == nil || hash.Status != ItemStatusBlocked || hash.Blockers[0].Code != "hash-field-expiration-unsupported" {
		t.Fatalf("HFE must be blocker on Redis 7.2: %#v", hash)
	}
}

func TestPlanFromEventsPreservesSourceAndOrdersEmptyStreamWithMKSTREAM(t *testing.T) {
	reference := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	objects := keyObjects(t, "../cases/restore_expirations.rdb")
	var keyObject model.RedisObject
	for _, object := range objects {
		if object.GetKey() == "seconds_ttl" {
			keyObject = object
		}
	}
	events := []Event{{Object: keyObject, Source: &Source{Kind: "key", DB: 3, ByteOffset: 12345, Key: "moved:seconds"}}}
	plan, err := PlanFromEvents(events, Options{ReferenceTime: reference, Profile: Redis74Profile()})
	if err != nil {
		t.Fatal(err)
	}
	item := findItem(plan, 3, "moved:seconds")
	if item == nil || item.Source.ByteOffset != 12345 || item.TargetDB != 3 {
		t.Fatalf("event source was not preserved: %#v", item)
	}

	stream := &model.StreamObject{
		BaseObject: &model.BaseObject{DB: 0, Key: "empty-grouped", Type: model.StreamType},
		Version:    2,
		Length:     0,
		LastId:     &model.StreamId{},
		Groups: []*model.StreamGroup{{
			Name:        "g",
			LastId:      &model.StreamId{},
			EntriesRead: 0,
		}},
	}
	streamPlan, err := PlanFromObjects([]model.RedisObject{stream}, Options{ReferenceTime: reference, Profile: Redis74Profile()})
	if err != nil {
		t.Fatal(err)
	}
	item = findItem(streamPlan, 0, "empty-grouped")
	if item == nil || len(item.CreateCommands) != 0 || len(item.FollowUpCommands) != 1 {
		t.Fatalf("empty stream should create via group MKSTREAM: %#v", item)
	}
	if got := argStrings(item.FollowUpCommands[0].Args); !reflect.DeepEqual(got, []string{"XGROUP", "CREATE", "empty-grouped", "g", "0-0", "MKSTREAM"}) {
		t.Fatalf("empty stream group command mismatch: %#v", got)
	}
}

func TestPlanRespectsCustomCapabilityProfile(t *testing.T) {
	functionObjects := keyObjects(t, "../cases/function.rdb")
	opts := Options{
		ReferenceTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Profile:       CapabilityProfile{Name: "custom", Version: "internal"},
	}
	plan, err := PlanFromObjects(functionObjects, opts)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Profile.Name != "custom" || len(plan.GlobalItems) != 1 || plan.GlobalItems[0].Status != ItemStatusBlocked {
		t.Fatalf("custom empty-capability profile was not respected: %#v", plan.Profile)
	}
}
