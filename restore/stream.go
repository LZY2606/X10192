package restore

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// stream message flattened for deterministic sorting.
type flatMsg struct {
	id     string
	idObj  *model.StreamId
	fields [][2]string
}

// planStream enforces the required order:
// entries -> groups -> consumers -> pending state.
func (b *builder) planStream(item *PlanItem, o *model.StreamObject) {
	msgs := flattenMessages(o)
	live := msgs

	// Groups (sorted by name), then consumers, then PEL.
	groups := make([]*model.StreamGroup, len(o.Groups))
	copy(groups, o.Groups)
	sort.Slice(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })

	// An empty stream is materialized by XGROUP CREATE MKSTREAM when at least
	// one group exists and the target supports it; otherwise the XADD/XDEL
	// workaround node is required before group creation.
	emptyMaterializedByGroup := len(live) == 0 && len(groups) > 0 &&
		b.opts.Target.Supports(FeatureXGroupMKStream)
	var createNode *PlanNode
	if len(live) > 0 || !emptyMaterializedByGroup {
		createNode = b.streamCreateNode(item, o, live)
	} else {
		// Placeholder dependency target: first group materializes the key;
		// create metadata warning still references the item.
		createNode = nil
	}

	// Structural counters are only flagged when XADD cannot reproduce them.
	b.warnStreamMeta(item, o, live)

	liveIDs := make(map[string]bool, len(live))
	for _, m := range live {
		liveIDs[m.id] = true
	}

	for _, g := range groups {
		groupNode := b.planGroup(item, o, g, createNode, len(live) == 0 && !emptyMaterializedByGroup)
		b.planConsumersAndPEL(item, o, g, groupNode, liveIDs)
	}
}

func flattenMessages(o *model.StreamObject) []flatMsg {
	out := make([]flatMsg, 0)
	for _, entry := range o.Entries {
		for _, msg := range entry.Msgs {
			if msg.Deleted {
				continue
			}
			fieldNames := make([]string, 0, len(msg.Fields))
			for k := range msg.Fields {
				fieldNames = append(fieldNames, k)
			}
			sort.Strings(fieldNames)
			fields := make([][2]string, 0, len(fieldNames))
			for _, k := range fieldNames {
				fields = append(fields, [2]string{k, msg.Fields[k]})
			}
			out = append(out, flatMsg{id: formatID(msg.Id), idObj: msg.Id, fields: fields})
		}
	}
	sort.Slice(out, func(i, j int) bool { return streamIDLess(out[i].idObj, out[j].idObj) })
	return out
}

func streamIDLess(a, c *model.StreamId) bool {
	if a == nil || c == nil {
		return false
	}
	if a.Ms != c.Ms {
		return a.Ms < c.Ms
	}
	return a.Sequence < c.Sequence
}

// streamCreateNode emits XADD per message (non-atomic batch) or the empty
// stream workaround.
func (b *builder) streamCreateNode(item *PlanItem, o *model.StreamObject, live []flatMsg) *PlanNode {
	if len(live) > 0 {
		commands := make([]*Command, 0, len(live))
		for _, m := range live {
			args := make([]string, 0, 3+2*len(m.fields))
			args = append(args, "XADD", o.Key, m.id)
			for _, fv := range m.fields {
				args = append(args, fv[0], fv[1])
			}
			commands = append(commands, cmd(args...))
		}
		node := &PlanNode{
			ID:     nodeID(item, nodeKindCreate),
			Kind:   nodeKindCreate,
			Label:  "xadd stream entries",
			Groups: []*CommandGroup{{Commands: commands, Atomicity: AtomicityNonAtomic, Purpose: "create"}},
			Notes: []string{
				"each XADD is an independent command: the batch is not atomic",
				"messages are emitted in ascending id order; fields are sorted for deterministic replay",
			},
		}
		item.Nodes = append(item.Nodes, node)
		return node
	}

	// No live messages. If there are groups we can use MKSTREAM (handled in
	// planGroup), but with zero groups the key would not exist. Emit the
	// documented workaround guarded by an explicit MULTI/EXEC boundary.
	workaround := []*Command{
		cmd("MULTI"),
		cmd("XADD", o.Key, "0-1", "restore", "placeholder"),
		cmd("XDEL", o.Key, "0-1"),
		cmd("EXEC"),
	}
	node := &PlanNode{
		ID:     nodeID(item, nodeKindEmptyWorkaround),
		Kind:   nodeKindEmptyWorkaround,
		Label:  "materialize empty stream",
		Groups: []*CommandGroup{{Commands: workaround, Atomicity: AtomicityMultiExec, Purpose: "create-empty-stream"}},
		Notes: []string{
			"empty streams cannot be created directly; XADD+XDEL inside MULTI/EXEC leaves an empty stream key",
		},
	}
	b.warnItem(item, node, WarnStreamEmptyWorkaround,
		fmt.Sprintf("stream %q contained no live entries; an XADD 0-1/XDEL workaround is required to materialize it", o.Key))
	item.Nodes = append(item.Nodes, node)
	return node
}

func (b *builder) warnStreamMeta(item *PlanItem, o *model.StreamObject, live []flatMsg) {
	notes := []string{}
	if o.LastId != nil {
		notes = append(notes, "last-id="+formatID(o.LastId))
	}
	if o.FirstId != nil {
		notes = append(notes, "first-id="+formatID(o.FirstId))
	}
	if o.MaxDeletedId != nil {
		notes = append(notes, "max-deleted-id="+formatID(o.MaxDeletedId))
	}
	if o.AddedEntriesCount != 0 {
		notes = append(notes, "added-entries-count="+strconv.FormatUint(o.AddedEntriesCount, 10))
	}

	// Count deleted messages for evidence.
	deletedIDs := make([]string, 0)
	for _, entry := range o.Entries {
		for _, msg := range entry.Msgs {
			if msg.Deleted {
				deletedIDs = append(deletedIDs, formatID(msg.Id))
			}
		}
	}
	sort.Strings(deletedIDs)
	if len(deletedIDs) > 0 {
		b.warnItem(item, nil, WarnStreamDeletedEntries,
			fmt.Sprintf("stream %q has %d deleted message(s) not representable by XADD; tombstones are lost",
				o.Key, len(deletedIDs)),
			append([]string{item.ID}, deletedIDs...)...)
	}

	// XADD sets last-generated-id to the maximum inserted id. Only flag loss
	// when the replayed ids cannot cover the structural counters (empty
	// stream, deleted tombstones, or gaps relative to added-entries-count).
	metaLoss := len(live) == 0
	if !metaLoss {
		maxLive := live[len(live)-1].idObj
		if o.LastId != nil && streamIDLess(maxLive, o.LastId) {
			metaLoss = true
		}
		if o.AddedEntriesCount != 0 && o.AddedEntriesCount != uint64(len(live)) &&
			o.FirstId != nil && streamIDLess(maxLive, o.LastId) {
			metaLoss = true
		}
	}
	if !metaLoss {
		return
	}
	create := nodeByKind(item, nodeKindCreate)
	if create == nil {
		create = nodeByKind(item, nodeKindEmptyWorkaround)
	}
	b.warnItem(item, create, WarnStreamMetaLoss,
		fmt.Sprintf("stream %q metadata counters (%v) cannot be fully set by public commands",
			o.Key, notes),
		item.ID)
}

func (b *builder) planGroup(item *PlanItem, o *model.StreamObject, g *model.StreamGroup, createNode *PlanNode, needsWorkaround bool) *PlanNode {
	args := []string{"XGROUP", "CREATE", o.Key, g.Name, formatID(g.LastId)}
	notes := []string{
		"last-delivered-id is set to " + formatID(g.LastId),
	}
	mkstream := createNode == nil && b.opts.Target.Supports(FeatureXGroupMKStream)
	if mkstream {
		args = append(args, "MKSTREAM")
		notes = append(notes, "MKSTREAM materializes the empty stream key")
	}
	if g.EntriesRead != 0 {
		if b.opts.Target.Supports(FeatureXGroupEntriesRead) {
			args = append(args, "ENTRIESREAD", strconv.FormatUint(g.EntriesRead, 10))
			notes = append(notes, "entries-read="+strconv.FormatUint(g.EntriesRead, 10))
		} else {
			notes = append(notes,
				fmt.Sprintf("entries-read=%d is lost: ENTRIESREAD requires Redis 7.0 (target %s)",
					g.EntriesRead, b.opts.Target.Name))
		}
	}
	depends := []string{}
	if createNode != nil {
		depends = append(depends, createNode.ID)
	}
	node := &PlanNode{
		ID:        nodeID(item, nodeKindGroup, escapeSegment(g.Name)),
		Kind:      nodeKindGroup,
		Label:     "create consumer group " + g.Name,
		DependsOn: depends,
		Groups:    []*CommandGroup{singleGroup("create-group", cmd(args...))},
		Notes:     notes,
	}
	if needsWorkaround && !mkstream {
		b.warnItem(item, node, WarnStreamEmptyWorkaround,
			fmt.Sprintf("stream %q / group %q: target %s lacks XGROUP MKSTREAM, group creation depends on the empty-stream workaround",
				o.Key, g.Name, b.opts.Target.Name))
	}
	if g.EntriesRead != 0 && !b.opts.Target.Supports(FeatureXGroupEntriesRead) {
		b.warnItem(item, node, WarnEntriesReadUnsupported,
			fmt.Sprintf("group %q entries-read=%d is dropped on target %s (ENTRIESREAD requires Redis 7.0)",
				g.Name, g.EntriesRead, b.opts.Target.Name))
	}
	item.Nodes = append(item.Nodes, node)
	return node
}

// planConsumersAndPEL emits consumer creation (7.0) and XCLAIM-based PEL
// reconstruction. Consumers precede PEL; PEL owner and delivery semantics are
// recorded explicitly.
func (b *builder) planConsumersAndPEL(item *PlanItem, o *model.StreamObject, g *model.StreamGroup, groupNode *PlanNode, liveIDs map[string]bool) {
	// Map pending id -> owning consumer, derived from consumer.Pending lists.
	owner := make(map[string]string)
	consumers := make([]*model.StreamConsumer, len(g.Consumers))
	copy(consumers, g.Consumers)
	sort.Slice(consumers, func(i, j int) bool { return consumers[i].Name < consumers[j].Name })
	for _, c := range consumers {
		for _, id := range c.Pending {
			owner[formatID(id)] = c.Name
		}
	}

	canCreateConsumer := b.opts.Target.Supports(FeatureXGroupCreateConsumer)
	var consumerNodeIDs []string
	for _, c := range consumers {
		if !canCreateConsumer {
			continue
		}
		node := &PlanNode{
			ID: nodeID(item, nodeKindGroup, escapeSegment(g.Name),
				nodeKindConsumer, escapeSegment(c.Name)),
			Kind:      nodeKindConsumer,
			Label:     "create consumer " + g.Name + "/" + c.Name,
			DependsOn: []string{groupNode.ID},
			Groups: []*CommandGroup{singleGroup("create-consumer",
				cmd("XGROUP", "CREATECONSUMER", o.Key, g.Name, c.Name))},
			Notes: []string{
				fmt.Sprintf("consumer seen-time=%d active-time=%d cannot be restored", c.SeenTime, c.ActiveTime),
				"CREATECONSUMER creates the consumer with zero deliveries; PEL is attached in the pel node",
			},
		}
		b.warnItem(item, node, WarnConsumerTimeLoss,
			fmt.Sprintf("consumer %q seen/active timestamps (seen=%d active=%d) are not replayable",
				c.Name, c.SeenTime, c.ActiveTime))
		item.Nodes = append(item.Nodes, node)
		consumerNodeIDs = append(consumerNodeIDs, node.ID)
	}
	if !canCreateConsumer && len(consumers) > 0 {
		names := make([]string, 0, len(consumers))
		for _, c := range consumers {
			names = append(names, c.Name)
		}
		sort.Strings(names)
		b.warnItem(item, groupNode, WarnConsumerNotCreated,
			fmt.Sprintf("group %q has %d consumer(s) (%v); XGROUP CREATECONSUMER requires Redis 7.0, consumers are implicitly created by XCLAIM where possible",
				g.Name, len(consumers), names))
	}

	// PEL via XCLAIM, one command per pending entry sorted by id.
	pending := make([]*model.StreamNAck, len(g.Pending))
	copy(pending, g.Pending)
	sort.Slice(pending, func(i, j int) bool { return streamIDLess(pending[i].Id, pending[j].Id) })
	if len(pending) == 0 {
		return
	}

	refMS := uint64(b.opts.ReferenceTime.UnixNano() / 1e6)
	xclaimTime := b.opts.Target.Supports(FeatureXClaimTime)
	commands := make([]*CommandGroup, 0, len(pending))
	missingOwners := make([]string, 0)
	missingEntries := make([]string, 0)
	for _, nack := range pending {
		id := formatID(nack.Id)
		consumerName, ok := owner[id]
		if !ok {
			missingOwners = append(missingOwners, id)
			continue
		}
		if !liveIDs[id] {
			missingEntries = append(missingEntries, id)
		}
		args := []string{
			"XCLAIM", o.Key, g.Name, consumerName, "0", id,
			"RETRY", strconv.FormatUint(nack.DeliveryCount, 10),
			"FORCE", "JUSTID",
		}
		if xclaimTime {
			args = append(args, "TIME", strconv.FormatUint(nack.DeliveryTime, 10))
		} else {
			idle := int64(0)
			if refMS > nack.DeliveryTime {
				idle = int64(refMS - nack.DeliveryTime)
			}
			args = append(args, "IDLE", strconv.FormatInt(idle, 10))
		}
		commands = append(commands, &CommandGroup{
			Commands:  []*Command{cmd(args...)},
			Atomicity: AtomicitySingle,
			Purpose:   "restore-pel",
		})
	}

	depends := append([]string{groupNode.ID}, consumerNodeIDs...)
	sort.Strings(depends)
	node := &PlanNode{
		ID:        nodeID(item, nodeKindGroup, escapeSegment(g.Name), nodeKindPEL),
		Kind:      nodeKindPEL,
		Label:     "restore pending entries for " + g.Name,
		DependsOn: depends,
		Groups:    commands,
		Notes: []string{
			"each XCLAIM re-attaches one pending entry to its RDB owner with RETRY=delivery-count",
			"FORCE claims entries even though the fresh group has no deliveries",
			"XCLAIM commands are independent and the PEL batch is not atomic",
			"idle-time semantics: TIME (Redis 6.2+) is exact; IDLE is relative and lossy",
		},
	}
	sort.Strings(missingOwners)
	sort.Strings(missingEntries)
	if len(missingOwners) > 0 {
		b.block(item, WarnPELOwnerMissing,
			fmt.Sprintf("group %q: %d pending entry/entries %v are not referenced by any consumer in the RDB, owner cannot be reconstructed",
				g.Name, len(missingOwners), missingOwners))
	}
	if len(missingEntries) > 0 {
		b.warnItem(item, node, WarnPELEntryMissing,
			fmt.Sprintf("group %q: pending id(s) %v have no corresponding stream message; XCLAIM FORCE will fail for them",
				g.Name, missingEntries))
	}
	if !xclaimTime {
		b.warnItem(item, node, WarnXClaimTimeUnsupported,
			fmt.Sprintf("target %s predates XCLAIM TIME (Redis 6.2); IDLE is computed from reference time and loses exact delivery instants",
				b.opts.Target.Name))
	}
	item.Nodes = append(item.Nodes, node)
}
