package restore

import "testing"

func TestPEXPIREATBlockerOnOldProfile(t *testing.T) {
	rdb := buildMultiTypeRDB(t)
	events := collect(t, bytesReader(rdb))
	profile24, err := NewProfile("redis-2.4", "2.4.0")
	if err != nil {
		t.Fatal(err)
	}
	plan, err := BuildPlanFromEvents(events, Options{
		ReferenceTime: refTime, Target: profile24})
	if err != nil {
		t.Fatal(err)
	}
	zset := mustFindItem(t, plan, "db0/key/zset:z")
	if zset.Status != ItemStatusBlocked {
		t.Fatalf("zset status=%s", zset.Status)
	}
	if !itemHasCode(zset, plan, WarnPEXPIREATUnsupported) {
		t.Fatalf("expected pexpireat blocker: %v", plan.Warnings)
	}
	if plan.Executable {
		t.Fatal("plan must be non-executable")
	}
}

func TestFeatureOverride(t *testing.T) {
	profile := ProfileRedis60.WithFeatureOverride(FeatureFunctions, true)
	if !profile.Supports(FeatureFunctions) {
		t.Fatal("override should enable functions on 6.0 profile")
	}
	if profile.Supports(FeatureHPEXPIREAT) {
		t.Fatal("unrelated feature should still be gated")
	}
}
