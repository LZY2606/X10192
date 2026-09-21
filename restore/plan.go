package restore

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// PlanFromObjects builds a deterministic restore plan from decoded Redis objects.
func PlanFromObjects(objects []model.RedisObject, opts Options) (*RestorePlan, error) {
	return NewPlanner(opts).Plan(objects)
}

// Event is an optional wrapper carrying caller-defined source metadata.
type Event struct {
	Object model.RedisObject `json:"-"`
	Source *Source           `json:"source,omitempty"`
}

// PlanFromEvents accepts decoded events. An event without explicit source uses
// the decoded object's database, key, type and encoding.
func PlanFromEvents(events []Event, opts Options) (*RestorePlan, error) {
	objects := make([]model.RedisObject, 0, len(events))
	for _, event := range events {
		if event.Object != nil {
			objects = append(objects, event.Object)
			if event.Source != nil {
				if base := baseObjectOf(event.Object); base != nil {
					base.ByteOffset = event.Source.ByteOffset
					base.DB = event.Source.DB
					if event.Source.Key != "" {
						base.Key = event.Source.Key
					}
					if event.Source.Encoding != "" {
						base.Encoding = event.Source.Encoding
					}
				}
			}
		}
	}
	return PlanFromObjects(objects, opts)
}

// Planner builds plans without connecting to or executing anything in Redis.
type Planner struct {
	opts  Options
	items []*PlanItem
}

func NewPlanner(opts Options) *Planner {
	if opts.Profile.Name == "" {
		opts.Profile = Redis74Profile()
	}
	if opts.ExpiredKeys == "" {
		opts.ExpiredKeys = ExpiredKeySkip
	}
	if opts.ExpirationMode == "" {
		opts.ExpirationMode = ExpirationAbsolute
	}
	opts.ReferenceTime = opts.ReferenceTime.UTC()
	return &Planner{opts: opts}
}

func (p *Planner) Plan(objects []model.RedisObject) (*RestorePlan, error) {
	if p.opts.ReferenceTime.IsZero() {
		return nil, fmt.Errorf("restore: reference time is required")
	}
	if p.opts.ExpiredKeys != ExpiredKeySkip && p.opts.ExpiredKeys != ExpiredKeyKeep && p.opts.ExpiredKeys != ExpiredKeyError {
		return nil, fmt.Errorf("restore: invalid expired key policy %q", p.opts.ExpiredKeys)
	}
	if p.opts.ExpirationMode != ExpirationAbsolute && p.opts.ExpirationMode != ExpirationRelative {
		return nil, fmt.Errorf("restore: invalid expiration mode %q", p.opts.ExpirationMode)
	}

	sortable := make([]model.RedisObject, 0, len(objects))
	for _, object := range objects {
		if object != nil {
			sortable = append(sortable, object)
		}
	}
	sort.SliceStable(sortable, func(i, j int) bool {
		return p.itemSortKey(sortable[i]) < p.itemSortKey(sortable[j])
	})
	for _, object := range sortable {
		p.installItem(p.planObject(object))
	}

	byDB := make(map[int][]*PlanItem)
	var global []*PlanItem
	for _, item := range p.items {
		if item.Source.Kind == model.FunctionsType {
			global = append(global, item)
		} else if item.TargetDB < 0 {
			continue
		} else {
			byDB[item.TargetDB] = append(byDB[item.TargetDB], item)
		}
	}
	sort.Slice(global, func(i, j int) bool { return global[i].ID < global[j].ID })

	dbs := make([]int, 0, len(byDB))
	for db := range byDB {
		dbs = append(dbs, db)
	}
	sort.Ints(dbs)

	plan := &RestorePlan{
		ReferenceTime: p.opts.ReferenceTime,
		Profile:       p.opts.Profile,
		GlobalItems:   make([]PlanItem, 0, len(global)),
	}
	for _, item := range global {
		plan.GlobalItems = append(plan.GlobalItems, *item)
	}
	for _, db := range dbs {
		items := byDB[db]
		dbPlan := DatabasePlan{DB: db, SelectNodeID: selectID(db), ItemIDs: make([]string, 0, len(items))}
		for _, item := range items {
			dbPlan.ItemIDs = append(dbPlan.ItemIDs, item.ID)
			if item.Status == ItemStatusActive {
				plan.Nodes = append(plan.Nodes, item.CreateCommands...)
			}
		}
		plan.Databases = append(plan.Databases, dbPlan)
	}

	// Append one SELECT before each database. Nodes are later ordered select -> create -> follow-up.
	selectNodes := make([]PlannedCommand, 0, len(plan.Databases))
	for _, dbPlan := range plan.Databases {
		selectNodes = append(selectNodes, PlannedCommand{
			ID:     dbPlan.SelectNodeID,
			ItemID: "",
			DB:     dbPlan.DB,
			Phase:  PhaseSelect,
			Role:   RoleSelect,
			Args: []Arg{
				newStringArg("SELECT"),
				newStringArg(strconv.Itoa(dbPlan.DB)),
			},
		})
	}
	plan.Nodes = append(selectNodes, plan.Nodes...)

	for _, db := range dbs {
		for _, item := range byDB[db] {
			if item.Status == ItemStatusActive {
				plan.Nodes = append(plan.Nodes, item.FollowUpCommands...)
			}
		}
	}
	for _, item := range global {
		if item.Status == ItemStatusActive {
			plan.Nodes = append(plan.Nodes, item.CreateCommands...)
			plan.Nodes = append(plan.Nodes, item.FollowUpCommands...)
		}
	}
	p.finalizeLogicalGroups(plan)
	p.setDependencies(plan)
	p.sortPlan(plan)
	for _, item := range p.items {
		if item.TargetDB >= 0 {
			plan.Items = append(plan.Items, *item)
		}
	}
	return plan, nil
}

func (p *Planner) itemSortKey(object model.RedisObject) string {
	if object.GetType() == model.FunctionsType {
		return "\x00functions:" + strconv.Itoa(object.GetDBIndex()) + ":" + object.GetKey() + ":" + shortDigestObject(object)
	}
	return fmt.Sprintf("\x00db:%010d:key:%s:type:%s:digest:%s", object.GetDBIndex(), bytesSortString(object.GetKey()), object.GetType(), shortDigest(shortDigestObject(object)))
}

func (p *Planner) installItem(item *PlanItem) {
	p.items = append(p.items, item)
}

func (p *Planner) planObject(object model.RedisObject) *PlanItem {
	switch obj := object.(type) {
	case *model.StringObject:
		return p.planString(obj)
	case *model.ListObject:
		return p.planList(obj)
	case *model.SetObject:
		return p.planSet(obj)
	case *model.ZSetObject:
		return p.planZSet(obj)
	case *model.HashObject:
		return p.planHash(obj)
	case *model.StreamObject:
		return p.planStream(obj)
	case *model.FunctionsObject:
		return p.planFunctions(obj)
	case *model.ModuleTypeObject:
		return p.planModule(obj)
	case *model.AuxObject, *model.DBSizeObject:
		return p.planIgnored(object)
	default:
		return p.planUnsupported(object)
	}
}

func (p *Planner) newItem(object model.RedisObject, kind string) *PlanItem {
	id := itemID(kind, object.GetDBIndex(), object.GetKey(), object.GetType())
	var byteOffset int64
	if base := baseObjectOf(object); base != nil {
		byteOffset = base.ByteOffset
	}
	return &PlanItem{
		ID:         id,
		Status:     ItemStatusActive,
		TargetDB:   object.GetDBIndex(),
		Key:        object.GetKey(),
		ObjectType: object.GetType(),
		Encoding:   object.GetEncoding(),
		Source: Source{
			Kind:       kind,
			DB:         object.GetDBIndex(),
			ByteOffset: byteOffset,
			Key:        object.GetKey(),
			ObjectType: object.GetType(),
			Encoding:   object.GetEncoding(),
		},
		LogicalGroup: Atomicity{
			GroupID:  id + ":group",
			Boundary: "logical-object",
		},
	}
}

func (p *Planner) planString(obj *model.StringObject) *PlanItem {
	item := p.newItem(obj, "key")
	cmd := p.command(item, PhaseCreate, RoleCreate, [][]byte{[]byte("SET"), []byte(obj.Key), obj.Value})
	item.CreateCommands = append(item.CreateCommands, cmd)
	p.addKeyExpiration(item, obj)
	p.addEvictionMetadata(item, obj)
	return item
}

func (p *Planner) planList(obj *model.ListObject) *PlanItem {
	item := p.newItem(obj, "key")
	args := [][]byte{[]byte("RPUSH"), []byte(obj.Key)}
	for _, value := range obj.Values {
		args = append(args, value)
	}
	item.CreateCommands = append(item.CreateCommands, p.command(item, PhaseCreate, RoleCreate, args))
	p.addKeyExpiration(item, obj)
	p.addEvictionMetadata(item, obj)
	return item
}

func (p *Planner) planSet(obj *model.SetObject) *PlanItem {
	members := cloneByteSlices(obj.Members)
	sortByteSlices(members)
	args := [][]byte{[]byte("SADD"), []byte(obj.Key)}
	for _, member := range members {
		args = append(args, member)
	}
	item := p.newItem(obj, "key")
	item.CreateCommands = append(item.CreateCommands, p.command(item, PhaseCreate, RoleCreate, args))
	p.addKeyExpiration(item, obj)
	p.addEvictionMetadata(item, obj)
	return item
}

func (p *Planner) planZSet(obj *model.ZSetObject) *PlanItem {
	entries := make([]*model.ZSetEntry, len(obj.Entries))
	copy(entries, obj.Entries)
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Score != entries[j].Score {
			return entries[i].Score < entries[j].Score
		}
		return bytesSortString(entries[i].Member) < bytesSortString(entries[j].Member)
	})
	args := [][]byte{[]byte("ZADD"), []byte(obj.Key)}
	for _, entry := range entries {
		args = append(args, []byte(strconv.FormatFloat(entry.Score, 'f', -1, 64)), []byte(entry.Member))
	}
	item := p.newItem(obj, "key")
	item.CreateCommands = append(item.CreateCommands, p.command(item, PhaseCreate, RoleCreate, args))
	p.addKeyExpiration(item, obj)
	p.addEvictionMetadata(item, obj)
	return item
}

func (p *Planner) planHash(obj *model.HashObject) *PlanItem {
	item := p.newItem(obj, "key")
	fields := make([]string, 0, len(obj.Hash))
	for field := range obj.Hash {
		fields = append(fields, field)
	}
	sort.Slice(fields, func(i, j int) bool { return bytesSortString(fields[i]) < bytesSortString(fields[j]) })
	args := [][]byte{[]byte("HSET"), []byte(obj.Key)}
	for _, field := range fields {
		args = append(args, []byte(field), obj.Hash[field])
	}
	item.CreateCommands = append(item.CreateCommands, p.command(item, PhaseCreate, RoleCreate, args))
	p.addKeyExpiration(item, obj)
	p.addHashFieldExpirations(item, obj, fields)
	p.addEvictionMetadata(item, obj)
	return item
}

func (p *Planner) planFunctions(obj *model.FunctionsObject) *PlanItem {
	engine, name := parseFunctionHeader(obj.FunctionsLua)
	id := "fn:" + sanitizeID(engine+":"+name) + ":" + shortDigestString(obj.FunctionsLua)[:12]
	item := &PlanItem{
		ID:         id,
		Status:     ItemStatusActive,
		TargetDB:   -1,
		ObjectType: model.FunctionsType,
		Encoding:   obj.GetEncoding(),
		Source: Source{
			Kind:       model.FunctionsType,
			DB:         -1,
			ByteOffset: obj.ByteOffset,
			Key:        obj.Key,
			ObjectType: model.FunctionsType,
			Encoding:   obj.GetEncoding(),
		},
		LogicalGroup: Atomicity{GroupID: id + ":group", Boundary: "logical-object"},
	}
	args := [][]byte{[]byte("FUNCTION"), []byte("LOAD"), []byte("REPLACE"), []byte(obj.FunctionsLua)}
	item.CreateCommands = append(item.CreateCommands, p.command(item, PhaseCreate, RoleFunction, args))
	if !p.opts.Profile.Functions {
		p.block(item, "functions-unsupported", "target profile does not support Redis functions", []string{"FUNCTION LOAD"})
	}
	return item
}

func (p *Planner) planModule(obj *model.ModuleTypeObject) *PlanItem {
	item := p.newItem(obj, "module-key")
	p.block(item, "module-value-unsupported", "module values cannot be restored from generic Redis commands", []string{"moduleType=" + obj.ModuleType, "encoding=" + obj.GetEncoding()})
	return item
}

func (p *Planner) planIgnored(object model.RedisObject) *PlanItem {
	item := p.newItem(object, object.GetType())
	item.TargetDB = -1
	item.Key = object.GetKey()
	item.Status = ItemStatusSkipped
	if p.opts.WarnMetadata {
		p.warn(item, "metadata-not-restorable", "RDB metadata has no equivalent key restoration command", []string{"kind=" + object.GetType(), "key=" + object.GetKey()})
	}
	return item
}

func (p *Planner) planUnsupported(object model.RedisObject) *PlanItem {
	item := p.newItem(object, "unsupported")
	p.block(item, "unsupported-object", "decoder returned an object type this library cannot plan", []string{"type=" + object.GetType(), "encoding=" + object.GetEncoding()})
	return item
}
