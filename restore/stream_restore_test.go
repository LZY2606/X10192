package restore

import (
	"testing"
)

// TestStreamGroupPEL uses the project fixture stream_listpacks_1.rdb whose
// "listpack" stream carries groups g1..g4, consumers and PEL entries.
func TestStreamGroupPEL(t *testing.T) {
	events := collectFile(t, "stream_listpacks_1.rdb")
	opts := Options{ReferenceTime: refTime, Target: ProfileRedis74}
	plan, err := BuildPlanFromEvents(events, opts)
	if err != nil {
		t.Fatalf("build: %v", err)
	}

	stream := mustFindItem(t, plan, "db0/key/listpack")
	if stream.Type != "stream" {
		t.Fatalf("type=%s", stream.Type)
	}

	// Required node order: create -> group -> consumer -> pel.
	kinds := nodeKinds(stream)
	index := map[string]int{}
	for i, k := range kinds {
		if _, exists := index[k]; !exists {
			index[k] = i
		}
	}
	createIdx, okCreate := index[nodeKindCreate]
	if !okCreate {
		t.Fatalf("missing create node, kinds=%v", kinds)
	}
	groupIdx := index[nodeKindGroup]
	consumerIdx := index[nodeKindConsumer]
	pelIdx := index[nodeKindPEL]
	if !(createIdx < groupIdx && groupIdx < consumerIdx && consumerIdx < pelIdx) {
		t.Fatalf("node ordering wrong create=%d group=%d consumer=%d pel=%d (%v)",
			createIdx, groupIdx, consumerIdx, pelIdx, kinds)
	}

	// Every XADD precedes every XGROUP and XCLAIM only appears in pel nodes.
	var createEnds, groupStarts int
	for _, n := range stream.Nodes {
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				switch c.Args[0] {
				case "XADD":
					if n.Kind != nodeKindCreate {
						t.Fatalf("XADD in non-create node %s", n.Kind)
					}
				case "XGROUP":
					if c.Args[1] != "CREATE" && c.Args[1] != "CREATECONSUMER" {
						t.Fatalf("unexpected XGROUP form %v", c.Args[:2])
					}
				case "XCLAIM":
					if n.Kind != nodeKindPEL {
						t.Fatalf("XCLAIM in non-pel node %s", n.Kind)
					}
				}
			}
		}
	}
	_ = createEnds
	_ = groupStarts

	// g1 carries consumers c1/c2 with PEL; group node must exist with the
	// recorded last-delivered-id 1528507816954-0.
	g1 := nodeByID(stream, "db0/key/listpack/group/g1")
	if g1 == nil {
		t.Fatalf("g1 group node missing")
	}
	if !nodeHasCmdPrefix(g1, "XGROUP", "CREATE", "listpack", "g1", "1528507816954-0") {
		t.Fatalf("g1 create cmd wrong: %v", g1.Groups[0].Commands[0].Args)
	}

	// Consumer nodes must depend on their group node.
	c1 := nodeByID(stream, "db0/key/listpack/group/g1/consumer/c1")
	if c1 == nil {
		t.Fatalf("consumer c1 missing")
	}
	if len(c1.DependsOn) != 1 || c1.DependsOn[0] != g1.ID {
		t.Fatalf("consumer deps=%v", c1.DependsOn)
	}

	// PEL node must depend on group and consumer nodes, and use XCLAIM with
	// TIME (7.4 supports it) preserving delivery instants.
	pel := nodeByID(stream, "db0/key/listpack/group/g1/pel")
	if pel == nil {
		t.Fatalf("g1 pel node missing")
	}
	for _, dep := range []string{g1.ID, c1.ID} {
		found := false
		for _, d := range pel.DependsOn {
			if d == dep {
				found = true
			}
		}
		if !found {
			t.Fatalf("pel missing dependency %s, have %v", dep, pel.DependsOn)
		}
	}
	var timeClaim, forceClaim bool
	for _, g := range pel.Groups {
		for _, c := range g.Commands {
			for _, a := range c.Args {
				if a == "TIME" {
					timeClaim = true
				}
				if a == "FORCE" {
					forceClaim = true
				}
			}
		}
	}
	if !timeClaim || !forceClaim {
		t.Fatalf("pel XCLAIM missing TIME=%v FORCE=%v", timeClaim, forceClaim)
	}

	// Deleted messages in this fixture must produce the deleted-entries warning.
	if _, ok := warningCodes(plan)[WarnStreamDeletedEntries]; !ok {
		t.Fatalf("expected %s warning, have %v", WarnStreamDeletedEntries, warningCodes(plan))
	}

	assertDeterministic(t, events, opts)
}

func TestStreamProfileDowngradeLoss(t *testing.T) {
	events := collectFile(t, "stream_listpacks_1.rdb")
	// Redis 6.0: no CREATECONSUMER, no ENTRIESREAD (v2 fixture n/a), no
	// XCLAIM TIME. Expect explicit warnings, never silent skipping.
	plan, err := BuildPlanFromEvents(events, Options{ReferenceTime: refTime, Target: ProfileRedis60})
	if err != nil {
		t.Fatal(err)
	}
	codes := warningCodes(plan)
	for _, code := range []string{WarnConsumerNotCreated, WarnXClaimTimeUnsupported} {
		if _, ok := codes[code]; !ok {
			t.Fatalf("expected warning %s on redis 6.0, have %v", code, codes)
		}
	}
	// No consumer nodes on 6.0 (implicitly created through XCLAIM).
	stream := mustFindItem(t, plan, "db0/key/listpack")
	for _, n := range stream.Nodes {
		if n.Kind == nodeKindConsumer {
			t.Fatalf("unexpected consumer node on 6.0: %s", n.ID)
		}
	}
	// XCLAIM must use IDLE, not TIME.
	for _, n := range stream.Nodes {
		if n.Kind != nodeKindPEL {
			continue
		}
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				for _, a := range c.Args {
					if a == "TIME" {
						t.Fatalf("TIME must not be used on 6.0: %v", c.Args)
					}
				}
			}
		}
	}
}

func TestEmptyStreamAndMKStream(t *testing.T) {
	// Synthesize an empty stream with a group via the encoder fixture.
	events := emptyStreamWithGroupEvents(t)
	plan74, err := BuildPlanFromEvents(events, Options{ReferenceTime: refTime, Target: ProfileRedis74})
	if err != nil {
		t.Fatal(err)
	}
	item := mustFindItem(t, plan74, "db0/key/empty:stream")
	// With a group and MKSTREAM, no workaround node; group uses MKSTREAM.
	var sawMK bool
	for _, n := range item.Nodes {
		if n.Kind == nodeKindEmptyWorkaround {
			t.Fatalf("workaround node should not exist with MKSTREAM: %s", n.ID)
		}
		for _, g := range n.Groups {
			for _, c := range g.Commands {
				for _, a := range c.Args {
					if a == "MKSTREAM" {
						sawMK = true
					}
				}
			}
		}
	}
	if !sawMK {
		t.Fatal("expected MKSTREAM on 7.4 group create")
	}

	// On a profile without streams MKSTREAM is a streams baseline feature, so
	// a no-group empty stream exercises the workaround branch instead.
	eventsNoGroup := emptyStreamNoGroupEvents(t)
	planNoGroup, err := BuildPlanFromEvents(eventsNoGroup, Options{
		ReferenceTime: refTime, Target: ProfileRedis74})
	if err != nil {
		t.Fatal(err)
	}
	ng := mustFindItem(t, planNoGroup, "db0/key/lonely:stream")
	var sawWorkaround bool
	for _, n := range ng.Nodes {
		if n.Kind == nodeKindEmptyWorkaround {
			sawWorkaround = true
			found := false
			for _, g := range n.Groups {
				if g.Atomicity == AtomicityMultiExec && len(g.Commands) == 4 {
					found = true
				}
			}
			if !found {
				t.Fatalf("workaround must be a 4-command MULTI/EXEC group")
			}
		}
	}
	if !sawWorkaround {
		t.Fatal("expected empty-stream workaround node")
	}
	if _, ok := warningCodes(planNoGroup)[WarnStreamEmptyWorkaround]; !ok {
		t.Fatal("expected empty stream workaround warning")
	}
}
