package restoreplan

import (
	"fmt"
	"sort"

	"github.com/hdt3213/rdb/model"
)

// planStream expands a stream object into a logical group of items:
// entries first, then consumer groups, then consumers and PEL state.
// Dependencies between the items encode this ordering explicitly.
func (b *builder) planStream(o *model.StreamObject) {
	expiry, expireCmd, skip := b.expiry(o.BaseObject)
	if skip {
		return
	}
	group := groupID(o.GetDBIndex(), o.GetKey())
	entriesKey := group + ":entries"

	// --- entries ---
	entries := b.newItem(KindStreamEntries, o.BaseObject, "entries")
	entries.Group = group
	entries.depKey = entriesKey
	type flatMsg struct {
		ms, seq uint64
		msg     *model.StreamMessage
	}
	var msgs []flatMsg
	deleted := 0
	for _, e := range o.Entries {
		for _, m := range e.Msgs {
			if m.Deleted {
				deleted++
				continue
			}
			msgs = append(msgs, flatMsg{m.Id.Ms, m.Id.Sequence, m})
		}
	}
	sort.SliceStable(msgs, func(i, j int) bool {
		if msgs[i].ms != msgs[j].ms {
			return msgs[i].ms < msgs[j].ms
		}
		return msgs[i].seq < msgs[j].seq
	})
	for _, fm := range msgs {
		fields := make([]string, 0, len(fm.msg.Fields))
		for f := range fm.msg.Fields {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		args := []string{o.GetKey(), streamIDString(fm.ms, fm.seq)}
		for _, f := range fields {
			args = append(args, f, fm.msg.Fields[f])
		}
		entries.CreateCommands = append(entries.CreateCommands, Command{
			Name: "XADD",
			Args: args,
			Note: "explicit entry id preserves the original stream ordering",
		})
	}
	if deleted > 0 {
		entries.Warnings = append(entries.Warnings,
			fmt.Sprintf("%d deleted entries are not restored", deleted))
	}
	// last-delivered-id of the stream itself
	xsetidArgs := []string{o.GetKey()}
	if o.LastId != nil {
		xsetidArgs = append(xsetidArgs, streamIDString(o.LastId.Ms, o.LastId.Sequence))
	} else {
		xsetidArgs = append(xsetidArgs, "0-0")
	}
	if o.Version >= 2 {
		if b.opts.Profile.XSetIDMeta {
			xsetidArgs = append(xsetidArgs, "ENTRIESADDED", fmt.Sprintf("%d", o.AddedEntriesCount))
			if o.MaxDeletedId != nil {
				xsetidArgs = append(xsetidArgs, "MAXDELETEDID", streamIDString(o.MaxDeletedId.Ms, o.MaxDeletedId.Sequence))
			}
		} else if o.AddedEntriesCount > 0 || (o.MaxDeletedId != nil && (o.MaxDeletedId.Ms > 0 || o.MaxDeletedId.Sequence > 0)) {
			entries.Warnings = append(entries.Warnings,
				fmt.Sprintf("addedEntriesCount=%d and maxDeletedId require XSETID ENTRIESADDED/MAXDELETEDID (Redis >= 7.0); target %s@%s loses this metadata",
					o.AddedEntriesCount, b.opts.Profile.Name, b.opts.Profile.Version))
		}
	}
	entries.FollowUpCommands = append(entries.FollowUpCommands, Command{
		Name: "XSETID",
		Args: xsetidArgs,
		Note: "restores the stream last-delivered-id",
	})
	entries.Expiry = expiry
	if expireCmd != nil {
		entries.FollowUpCommands = append(entries.FollowUpCommands, *expireCmd)
	}
	entries.Atomicity = atomicityNote
	b.items = append(b.items, entries)

	// --- groups, consumers, PEL ---
	groups := make([]*model.StreamGroup, len(o.Groups))
	copy(groups, o.Groups)
	sort.SliceStable(groups, func(i, j int) bool { return groups[i].Name < groups[j].Name })
	for _, g := range groups {
		groupItem := b.newItem(KindStreamGroup, o.BaseObject, "group:"+g.Name)
		groupItem.Group = group
		groupItem.depKey = group + ":group:" + g.Name
		groupItem.depends = []string{entriesKey}
		lastID := "0-0"
		if g.LastId != nil {
			lastID = streamIDString(g.LastId.Ms, g.LastId.Sequence)
		}
		createArgs := []string{o.GetKey(), g.Name, lastID}
		note := "restores the group last-delivered-id"
		if g.EntriesRead > 0 {
			if b.opts.Profile.XGroupEntriesRead {
				createArgs = append(createArgs, "ENTRIESREAD", fmt.Sprintf("%d", g.EntriesRead))
				note = "restores last-delivered-id and entries-read"
			} else {
				groupItem.Warnings = append(groupItem.Warnings,
					fmt.Sprintf("entries-read=%d requires XGROUP CREATE ... ENTRIESREAD (Redis >= 7.0); target %s@%s loses entries-read",
						g.EntriesRead, b.opts.Profile.Name, b.opts.Profile.Version))
			}
		}
		groupItem.CreateCommands = []Command{{
			Name: "XGROUP",
			Args: append([]string{"CREATE"}, createArgs...),
			Note: note,
		}}
		groupItem.Atomicity = atomicityNote
		b.items = append(b.items, groupItem)

		// owner of each pending entry, resolved from consumer pending lists
		owner := make(map[string]string)
		consumers := make([]*model.StreamConsumer, len(g.Consumers))
		copy(consumers, g.Consumers)
		sort.SliceStable(consumers, func(i, j int) bool { return consumers[i].Name < consumers[j].Name })
		for _, c := range consumers {
			consumerItem := b.newItem(KindStreamConsumer, o.BaseObject, "consumer:"+g.Name+"/"+c.Name)
			consumerItem.Group = group
			consumerItem.depKey = group + ":consumer:" + g.Name + "/" + c.Name
			consumerItem.depends = []string{groupItem.depKey}
			if b.opts.Profile.XGroupCreateConsumer {
				consumerItem.CreateCommands = []Command{{
					Name: "XGROUP",
					Args: []string{"CREATECONSUMER", o.GetKey(), g.Name, c.Name},
				}}
			} else {
				consumerItem.Warnings = append(consumerItem.Warnings,
					fmt.Sprintf("XGROUP CREATECONSUMER requires Redis >= 6.2; target %s@%s creates the consumer implicitly via XCLAIM",
						b.opts.Profile.Name, b.opts.Profile.Version))
			}
			consumerItem.Warnings = append(consumerItem.Warnings,
				"consumer seen-time/active-time cannot be restored; idle/time loss is unavoidable")
			b.items = append(b.items, consumerItem)
			for _, id := range c.Pending {
				owner[streamIDString(id.Ms, id.Sequence)] = c.Name
			}
		}

		if len(g.Pending) > 0 {
			pelItem := b.newItem(KindStreamPEL, o.BaseObject, "pel:"+g.Name)
			pelItem.Group = group
			pelItem.depKey = group + ":pel:" + g.Name
			pelItem.depends = []string{groupItem.depKey}
			pending := make([]*model.StreamNAck, len(g.Pending))
			copy(pending, g.Pending)
			sort.SliceStable(pending, func(i, j int) bool {
				if pending[i].Id.Ms != pending[j].Id.Ms {
					return pending[i].Id.Ms < pending[j].Id.Ms
				}
				return pending[i].Id.Sequence < pending[j].Id.Sequence
			})
			for _, p := range pending {
				idStr := streamIDString(p.Id.Ms, p.Id.Sequence)
				consumer, ok := owner[idStr]
				if !ok {
					consumer = "restored-consumer"
					pelItem.Warnings = append(pelItem.Warnings,
						fmt.Sprintf("pending entry %s has no recorded PEL owner; assigned to placeholder consumer %q", idStr, consumer))
				}
				pelItem.FollowUpCommands = append(pelItem.FollowUpCommands, Command{
					Name: "XCLAIM",
					Args: []string{o.GetKey(), g.Name, consumer, "0", idStr,
						"TIME", fmt.Sprintf("%d", p.DeliveryTime),
						"RETRYCOUNT", fmt.Sprintf("%d", p.DeliveryCount),
						"FORCE"},
					Note: "restores PEL owner, delivery time and delivery count",
				})
			}
			pelItem.Warnings = append(pelItem.Warnings,
				"delivery times are absolute; idle time restarts from the restore moment (idle/time loss)")
			pelItem.Atomicity = atomicityNote
			b.items = append(b.items, pelItem)
		}
	}
}

// planFunction plans a function library. The library is restored from its
// source via FUNCTION LOAD; it is never silently skipped.
func (b *builder) planFunction(o *model.FunctionsObject) {
	item := b.newItem(KindFunction, o.BaseObject, "")
	item.Group = groupID(o.GetDBIndex(), o.GetKey())
	item.depKey = item.Group
	if !b.opts.Profile.Functions {
		b.blockers = append(b.blockers, Blocker{
			Reason: "function library requires FUNCTION LOAD (Redis >= 7.0), not supported by target profile",
			Evidence: fmt.Sprintf("db=%d key=%q target=%s@%s sourceBytes=%d",
				o.GetDBIndex(), o.GetKey(), b.opts.Profile.Name, b.opts.Profile.Version, len(o.FunctionsLua)),
		})
		// still emit the item so the library is visible in the plan
		item.Warnings = append(item.Warnings, "blocked by target capability profile; see plan blockers")
	}
	item.CreateCommands = []Command{{
		Name: "FUNCTION",
		Args: []string{"LOAD", "REPLACE", o.FunctionsLua},
		Note: "restores the library from source; REPLACE overwrites an existing library with the same name",
	}}
	item.Warnings = append(item.Warnings,
		"library is restored from source; engine flags not present in source are lost")
	b.items = append(b.items, item)
}
