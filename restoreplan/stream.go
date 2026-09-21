package restoreplan

import (
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// planStream restores entries first, then stream identity metadata, then
// consumer groups (create -> consumers -> pending entries). Ordering and
// losses are explicit because no single command can recreate a stream.
func (p *planner) planStream(o *model.StreamObject) *PlanItem {
	item := p.baseItem(o)
	p.addEvictionWarning(item, o)

	if !p.target.Streams {
		item.Status = StatusBlocked
		item.Blocker = &Blocker{
			Code: "streams-unsupported", ItemID: item.ID,
			Reason:             "XADD/XGROUP/XCLAIM require Redis 5.0 or newer",
			RequiredCapability: "streams",
			Fatal:              true,
			Evidence:           []string{"target=" + p.target.Name},
		}
		item.Atomicity = Atomicity{Atomic: false, NumCommands: 0, Boundary: "blocked: target lacks streams support"}
		return item
	}

	messages := p.collectStreamMessages(item, o)
	groups := sortedGroups(o.Groups)

	p.planStreamCreate(item, o, messages, groups)
	p.planStreamIdentity(item, o, messages)
	for _, g := range groups {
		p.planStreamGroup(item, o, g, len(messages) == 0)
	}

	_ = p.applyKeyExpiry(item, o)
	p.finalize(item)
	return item
}

type streamMsg struct {
	id     model.StreamId
	fields [][2]string
}

// collectStreamMessages flattens listpack nodes into globally sorted
// messages, skipping RDB tombstone markers (deleted entries are unreachable
// history and are reported instead of being recreated).
func (p *planner) collectStreamMessages(item *PlanItem, o *model.StreamObject) []streamMsg {
	msgs := make([]streamMsg, 0)
	deleted := make([]string, 0)
	for _, entry := range o.Entries {
		for _, m := range entry.Msgs {
			if m.Deleted {
				deleted = append(deleted, formatID(m.Id))
				continue
			}
			sm := streamMsg{id: *m.Id}
			keys := make([]string, 0, len(m.Fields))
			for k := range m.Fields {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				sm.fields = append(sm.fields, [2]string{k, m.Fields[k]})
			}
			msgs = append(msgs, sm)
		}
	}
	sort.Slice(msgs, func(i, j int) bool { return lessID(&msgs[i].id, &msgs[j].id) })
	sort.Strings(deleted)
	if len(deleted) > 0 {
		ev := deleted
		if len(ev) > 10 {
			ev = ev[:10]
		}
		item.Warnings = append(item.Warnings, Warning{
			Code:     "stream-deleted-entry-not-replayable",
			ItemID:   item.ID,
			Message:  "tombstoned/trimmed stream entries cannot be recreated; " + strconv.Itoa(len(deleted)) + " deleted entries omitted",
			Evidence: ev,
		})
	}
	return msgs
}

func (p *planner) planStreamCreate(item *PlanItem, o *model.StreamObject, msgs []streamMsg, groups []*model.StreamGroup) {
	key := o.Key
	if len(msgs) == 0 {
		// No data to XADD. An empty stream can only be materialized together
		// with a group via MKSTREAM.
		if len(groups) == 0 {
			item.Status = StatusBlocked
			item.Blocker = &Blocker{
				Code: "empty-stream-not-replayable", ItemID: item.ID,
				Reason:   "an empty stream without groups cannot be created without appending a synthetic message",
				Fatal:    true,
				Evidence: []string{"lastId=" + formatID(o.LastId)},
			}
			item.Atomicity = Atomicity{Atomic: false, NumCommands: 0, Boundary: "blocked: empty stream"}
			item.Warnings = append(item.Warnings, Warning{
				Code: "stream-metadata-lost", ItemID: item.ID,
				Message:  "empty stream identity (lastId/addedEntriesCount) cannot be replayed",
				Evidence: []string{"lastId=" + formatID(o.LastId)},
			})
			return
		}
		if !p.target.XGroupMKStream {
			item.Status = StatusBlocked
			item.Blocker = &Blocker{
				Code: "xgroup-mkstream-unsupported", ItemID: item.ID,
				Reason:             "creating an empty stream requires XGROUP CREATE MKSTREAM (Redis 6.2+)",
				RequiredCapability: "xgroupMkStream",
				Fatal:              true,
				Evidence:           []string{"target=" + p.target.Name},
			}
			item.Atomicity = Atomicity{Atomic: false, NumCommands: 0, Boundary: "blocked: empty stream needs MKSTREAM"}
			return
		}
		item.Warnings = append(item.Warnings, Warning{
			Code: "stream-metadata-lost", ItemID: item.ID,
			Message:  "empty stream materialized via MKSTREAM; lastId/addedEntriesCount/maxDeletedId metadata cannot be replayed",
			Evidence: []string{"lastId=" + formatID(o.LastId)},
		})
		return
	}
	for _, m := range msgs {
		args := make([]string, 0, 3+len(m.fields)*2)
		args = append(args, key, formatID(&m.id))
		for _, kv := range m.fields {
			args = append(args, kv[0], kv[1])
		}
		c := textCmd(RoleCreate, "XADD", args...)
		c.Notes = append(c.Notes, "entries must be restored before any group state")
		item.Create = append(item.Create, c)
	}
}

// planStreamIdentity pins lastId and, when the RDB version carries them and
// the target supports it, entries-added / max-deleted metadata.
func (p *planner) planStreamIdentity(item *PlanItem, o *model.StreamObject, msgs []streamMsg) {
	if item.Status == StatusBlocked || len(msgs) == 0 {
		return
	}
	hasMeta := o.Version >= 2
	lastID := formatID(o.LastId)
	if !p.target.XSetIdMetadata || !hasMeta {
		if hasMeta && (o.AddedEntriesCount > uint64(len(msgs)) || !isZeroID(o.MaxDeletedId)) {
			item.PartialBlockers = append(item.PartialBlockers, p.capBlocker(
				"xsetid-metadata-unsupported",
				"XSETID ENTRIESADDED/MAXDELETEDID requires Redis 7.0+",
				"lastId="+lastID,
			))
			item.Warnings = append(item.Warnings, Warning{
				Code: "stream-identity-metadata-lost", ItemID: item.ID,
				Message: "addedEntriesCount/maxDeletedId counters cannot be replayed on target profile",
				Evidence: []string{
					"addedEntriesCount=" + strconv.FormatUint(o.AddedEntriesCount, 10),
					"maxDeletedId=" + formatID(o.MaxDeletedId),
				},
			})
		}
		item.FollowUps = append(item.FollowUps, textCmd(RoleFollowUp, "XSETID", o.Key, lastID))
		return
	}
	args := []string{o.Key, lastID, "ENTRIESADDED", strconv.FormatUint(o.AddedEntriesCount, 10),
		"MAXDELETEDID", formatID(o.MaxDeletedId)}
	c := textCmd(RoleFollowUp, "XSETID", args...)
	c.Notes = append(c.Notes, "pins last-delivered window before groups are created")
	item.FollowUps = append(item.FollowUps, c)
}

// planStreamGroup restores one group: create -> consumers -> PEL.
func (p *planner) planStreamGroup(item *PlanItem, o *model.StreamObject, g *model.StreamGroup, emptyStream bool) {
	if item.Status == StatusBlocked {
		return
	}
	createArgs := []string{o.Key, g.Name, formatID(g.LastId)}
	if emptyStream && p.target.XGroupMKStream {
		createArgs = append(createArgs, "MKSTREAM")
	}
	if g.EntriesRead > 0 {
		if p.target.XSetIdMetadata {
			createArgs = append(createArgs, "ENTRIESREAD", strconv.FormatUint(g.EntriesRead, 10))
		} else {
			item.PartialBlockers = append(item.PartialBlockers, p.capBlocker(
				"xsetid-metadata-unsupported",
				"XGROUP CREATE ENTRIESREAD requires Redis 7.0+",
				"group="+g.Name,
			))
			item.Warnings = append(item.Warnings, Warning{
				Code: "group-entries-read-lost", ItemID: item.ID,
				Message: "entries-read counter of group " + strconv.Quote(g.Name) + " cannot be replayed",
				Evidence: []string{
					"group=" + g.Name,
					"entriesRead=" + strconv.FormatUint(g.EntriesRead, 10),
				},
			})
		}
	}
	create := textCmd(RoleFollowUp, "XGROUP", append([]string{"CREATE"}, createArgs...)...)
	create.Notes = append(create.Notes, "last-delivered-id pinned to "+formatID(g.LastId))
	item.FollowUps = append(item.FollowUps, create)

	// Consumers (sorted by name), before PEL ownership claims.
	consumers := append([]*model.StreamConsumer(nil), g.Consumers...)
	sort.Slice(consumers, func(i, j int) bool { return consumers[i].Name < consumers[j].Name })

	owner := make(map[string]*model.StreamConsumer)
	for _, c := range consumers {
		owner[c.Name] = c
		item.Warnings = append(item.Warnings, Warning{
			Code: "consumer-time-lost", ItemID: item.ID,
			Message: "consumer seen/active time of " + strconv.Quote(c.Name) + " cannot be set through public commands",
			Evidence: []string{
				"group=" + g.Name, "consumer=" + c.Name,
				"seenTime=" + strconv.FormatUint(c.SeenTime, 10),
				"activeTime=" + strconv.FormatUint(c.ActiveTime, 10),
			},
		})
		hasPending := len(c.Pending) > 0
		if p.target.XGroupCreateConsumer {
			item.FollowUps = append(item.FollowUps, textCmd(RoleFollowUp,
				"XGROUP", "CREATECONSUMER", o.Key, g.Name, c.Name))
		} else {
			item.PartialBlockers = append(item.PartialBlockers, p.capBlocker(
				"xgroup-createconsumer-unsupported",
				"XGROUP CREATECONSUMER requires Redis 6.2+",
				"group="+g.Name, "consumer="+c.Name,
			))
			if !hasPending {
				item.Warnings = append(item.Warnings, Warning{
					Code: "consumer-create-lost", ItemID: item.ID,
					Message:  "idle consumer " + strconv.Quote(c.Name) + " with no pending entries will not visibly exist",
					Evidence: []string{"group=" + g.Name, "consumer=" + c.Name},
				})
			}
		}
	}

	// PEL: each pending entry is claimed by its original owner with the exact
	// delivery timestamp and retry count.
	pending := append([]*model.StreamNAck(nil), g.Pending...)
	sort.Slice(pending, func(i, j int) bool { return lessID(pending[i].Id, pending[j].Id) })
	idSet := make(map[string]bool)
	for _, e := range o.Entries {
		for _, m := range e.Msgs {
			if !m.Deleted {
				idSet[formatID(m.Id)] = true
			}
		}
	}
	pelOwners := p.buildPELOwnerMap(g)
	for _, nack := range pending {
		idStr := formatID(nack.Id)
		cn, owned := pelOwners[idStr]
		if !owned {
			item.Warnings = append(item.Warnings, Warning{
				Code: "pel-owner-lost", ItemID: item.ID,
				Message:  "pending entry " + idStr + " has no resolvable consumer in RDB group metadata; ownership not recreated",
				Evidence: []string{"group=" + g.Name, "id=" + idStr},
			})
			item.PartialBlockers = append(item.PartialBlockers, Blocker{
				Code: "pel-owner-missing", ItemID: item.ID,
				Reason:   "PEL entry lacks a resolvable owner consumer",
				Evidence: []string{"group=" + g.Name, "id=" + idStr},
			})
			continue
		}
		args := []string{
			o.Key, g.Name, cn.Name, "0", idStr,
			"TIME", strconv.FormatUint(nack.DeliveryTime, 10),
			"RETRYCOUNT", strconv.FormatUint(nack.DeliveryCount, 10),
		}
		if !idSet[idStr] {
			args = append(args, "FORCE")
			item.Warnings = append(item.Warnings, Warning{
				Code: "pel-message-not-in-stream", ItemID: item.ID,
				Message:  "pending entry " + idStr + " no longer exists in the stream; recreated with FORCE as an orphan PEL record",
				Evidence: []string{"group=" + g.Name, "consumer=" + cn.Name, "id=" + idStr},
			})
		}
		c := textCmd(RoleFollowUp, "XCLAIM", args...)
		c.Notes = append(c.Notes, "TIME pins original delivery ms; idle is therefore not approximated")
		item.FollowUps = append(item.FollowUps, c)
	}
}

// buildPELOwnerMap maps pending id text -> owning consumer using each
// consumer's pending id list.
func (p *planner) buildPELOwnerMap(g *model.StreamGroup) map[string]*model.StreamConsumer {
	m := make(map[string]*model.StreamConsumer)
	for _, c := range g.Consumers {
		for _, id := range c.Pending {
			m[formatID(id)] = c
		}
	}
	return m
}

func sortedGroups(groups []*model.StreamGroup) []*model.StreamGroup {
	out := append([]*model.StreamGroup(nil), groups...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func formatID(id *model.StreamId) string {
	if id == nil {
		return "0-0"
	}
	return strconv.FormatUint(id.Ms, 10) + "-" + strconv.FormatUint(id.Sequence, 10)
}

func lessID(a, b *model.StreamId) bool {
	if a.Ms != b.Ms {
		return a.Ms < b.Ms
	}
	return a.Sequence < b.Sequence
}

func isZeroID(id *model.StreamId) bool {
	return id == nil || (id.Ms == 0 && id.Sequence == 0)
}
