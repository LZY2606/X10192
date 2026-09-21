package restore

import (
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

func (p *Planner) planStream(obj *model.StreamObject) *PlanItem {
	item := p.newItem(obj, "key")
	if !p.opts.Profile.Streams {
		p.block(item, "streams-unsupported", "target profile does not support Redis streams", []string{"XADD", "XGROUP"})
		return item
	}

	liveIDs := make(map[string]*model.StreamMessage)
	deletedCount := 0
	for _, entry := range obj.Entries {
		for _, msg := range entry.Msgs {
			if msg.Deleted {
				deletedCount++
				continue
			}
			liveIDs[streamID(msg.Id)] = msg
		}
	}
	messages := make([]*model.StreamMessage, 0, len(liveIDs))
	for _, msg := range liveIDs {
		messages = append(messages, msg)
	}
	sort.Slice(messages, func(i, j int) bool { return compareStreamID(messages[i].Id, messages[j].Id) })

	for _, msg := range messages {
		fields := make([]string, 0, len(msg.Fields))
		for field := range msg.Fields {
			fields = append(fields, field)
		}
		sort.Slice(fields, func(i, j int) bool { return bytesSortString(fields[i]) < bytesSortString(fields[j]) })
		args := [][]byte{[]byte("XADD"), []byte(obj.Key), []byte(streamID(msg.Id))}
		for _, field := range fields {
			args = append(args, []byte(field), []byte(msg.Fields[field]))
		}
		item.CreateCommands = append(item.CreateCommands, p.command(item, PhaseCreate, RoleCreate, args))
	}
	if len(messages) == 0 && len(obj.Groups) == 0 {
		p.block(item, "empty-stream-unsupported", "an empty stream without groups cannot be created losslessly by normal write commands", nil)
	}
	if deletedCount > 0 {
		p.warn(item, "stream-tombstones-not-restorable", "deleted stream entries are needed to reproduce deleted-id and trimming history", []string{"deletedEntries=" + strconv.Itoa(deletedCount)})
	}
	if obj.Length != uint64(len(messages)) {
		p.warn(item, "stream-length-history-not-restorable", "stream length reflects tombstone/trimming history that write commands cannot reproduce", []string{"rdbLength=" + strconv.FormatUint(obj.Length, 10), "replayedEntries=" + strconv.Itoa(len(messages))})
	}
	if obj.Version >= 2 {
		p.warn(item, "stream-metadata-not-restorable", "first-id, max-deleted-id and added-entries-count are internal stream metadata", []string{"firstId=" + optionalStreamID(obj.FirstId), "maxDeletedId=" + optionalStreamID(obj.MaxDeletedId), "addedEntriesCount=" + strconv.FormatUint(obj.AddedEntriesCount, 10)})
	}

	groupNames := make([]string, 0, len(obj.Groups))
	groupsByName := make(map[string]*model.StreamGroup)
	for _, group := range obj.Groups {
		groupNames = append(groupNames, group.Name)
		groupsByName[group.Name] = group
	}
	sort.Slice(groupNames, func(i, j int) bool { return bytesSortString(groupNames[i]) < bytesSortString(groupNames[j]) })

	for _, groupName := range groupNames {
		group := groupsByName[groupName]
		groupCmd := p.streamGroupCreate(item, obj, group, len(messages) == 0)
		item.FollowUpCommands = append(item.FollowUpCommands, groupCmd)
		p.streamConsumersAndPEL(item, obj, group, liveIDs, groupCmd)
	}
	p.addKeyExpiration(item, obj)
	p.addEvictionMetadata(item, obj)
	return item
}

func (p *Planner) streamGroupCreate(item *PlanItem, obj *model.StreamObject, group *model.StreamGroup, mkstream bool) PlannedCommand {
	args := [][]byte{[]byte("XGROUP"), []byte("CREATE"), []byte(obj.Key), []byte(group.Name), []byte(streamID(group.LastId))}
	if mkstream {
		args = append(args, []byte("MKSTREAM"))
	}
	if obj.Version >= 2 && group.EntriesRead > 0 {
		if p.opts.Profile.StreamEntriesRead {
			args = append(args, []byte("ENTRIESREAD"), []byte(strconv.FormatUint(group.EntriesRead, 10)))
		} else {
			p.warn(item, "stream-entries-read-unsupported", "target profile cannot restore consumer group entries-read exactly", []string{"group=" + group.Name, "entriesRead=" + strconv.FormatUint(group.EntriesRead, 10)})
		}
	}
	cmd := p.command(item, PhaseFollowUp, RoleStreamGroup, args)
	if len(item.CreateCommands) > 0 {
		cmd.DependsOn = append(cmd.DependsOn, item.CreateCommands[len(item.CreateCommands)-1].ID)
	}
	return cmd
}

func (p *Planner) streamConsumersAndPEL(item *PlanItem, obj *model.StreamObject, group *model.StreamGroup, liveIDs map[string]*model.StreamMessage, groupCmd PlannedCommand) {
	consumers := make([]*model.StreamConsumer, len(group.Consumers))
	copy(consumers, group.Consumers)
	sort.Slice(consumers, func(i, j int) bool { return bytesSortString(consumers[i].Name) < bytesSortString(consumers[j].Name) })
	pendingOwners := make(map[string]string)
	for _, consumer := range consumers {
		for _, id := range consumer.Pending {
			pendingOwners[streamID(id)] = consumer.Name
		}
		if len(consumer.Pending) == 0 {
			args := [][]byte{[]byte("XGROUP"), []byte("CREATECONSUMER"), []byte(obj.Key), []byte(group.Name), []byte(consumer.Name)}
			if p.opts.Profile.StreamConsumerCreate {
				cmd := p.command(item, PhaseFollowUp, RoleStreamConsumer, args)
				cmd.DependsOn = append(cmd.DependsOn, groupCmd.ID)
				item.FollowUpCommands = append(item.FollowUpCommands, cmd)
			} else {
				p.block(item, "stream-consumer-create-unsupported", "target profile cannot create a consumer without pending entries", []string{"group=" + group.Name, "consumer=" + consumer.Name})
			}
		}
		evidence := []string{"group=" + group.Name, "consumer=" + consumer.Name, "seenTime=" + strconv.FormatUint(consumer.SeenTime, 10), "activeTime=" + strconv.FormatUint(consumer.ActiveTime, 10)}
		p.warn(item, "stream-consumer-time-not-restorable", "consumer seen/active time cannot be set with generic stream commands", evidence)
	}

	pending := make([]*model.StreamNAck, len(group.Pending))
	copy(pending, group.Pending)
	sort.Slice(pending, func(i, j int) bool { return compareStreamID(pending[i].Id, pending[j].Id) })
	for _, nack := range pending {
		idText := streamID(nack.Id)
		owner := pendingOwners[idText]
		if owner == "" {
			p.block(item, "stream-pel-owner-missing", "PEL entry does not identify an owner in decoded consumer state", []string{"group=" + group.Name, "id=" + idText})
			continue
		}
		if _, exists := liveIDs[idText]; !exists {
			p.block(item, "stream-pel-entry-missing", "PEL entry points to a deleted or absent stream entry", []string{"group=" + group.Name, "consumer=" + owner, "id=" + idText})
			continue
		}
		args := [][]byte{
			[]byte("XCLAIM"), []byte(obj.Key), []byte(group.Name), []byte(owner), []byte("0"), []byte(idText),
			[]byte("TIME"), []byte(strconv.FormatUint(nack.DeliveryTime, 10)),
			[]byte("RETRYCOUNT"), []byte(strconv.FormatUint(nack.DeliveryCount, 10)),
			[]byte("FORCE"), []byte("JUSTID"),
			[]byte("LASTID"), []byte(streamID(group.LastId)),
		}
		cmd := p.command(item, PhaseFollowUp, RoleStreamPEL, args)
		cmd.DependsOn = append(cmd.DependsOn, groupCmd.ID)
		item.FollowUpCommands = append(item.FollowUpCommands, cmd)
	}
}

func optionalStreamID(id *model.StreamId) string {
	if id == nil {
		return ""
	}
	return streamID(id)
}
