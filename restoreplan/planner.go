package restoreplan

import (
	"errors"
	"sort"
	"strconv"
	"time"

	"github.com/hdt3213/rdb/model"
)

// Generate builds a deterministic RestorePlan from decoded RDB objects.
//
// The input may come straight from parser.NewDecoder(...).Parse callbacks
// (enable WithSpecialOpCode to include functions and aux fields). Generate
// performs no IO and never connects to Redis; the returned plan is safe to
// serialize, preview or diff.
func Generate(objects []model.RedisObject, opts Options) (*RestorePlan, error) {
	ref := opts.ReferenceTime
	if ref.IsZero() {
		if opts.ExpiryMode == ExpiryRelative {
			return nil, errors.New("restoreplan: Options.ReferenceTime is required when ExpiryMode is relative")
		}
		ref = time.Now()
	}
	policy := opts.ExpiredKeys
	if policy == "" {
		policy = ExpiredSkip
	}
	mode := opts.ExpiryMode
	if mode == "" {
		mode = ExpiryAbsolute
	}
	target := opts.Target
	if target == nil {
		target = Redis74
	}

	p := &planner{
		opts:   opts,
		refMs:  ref.UnixNano() / int64(time.Millisecond),
		policy: policy,
		mode:   mode,
		target: target,
		keys:   make(map[string]*PlanItem),
		funcs:  make(map[string]*PlanItem),
	}
	if err := p.collect(objects); err != nil {
		return nil, err
	}
	return p.build()
}

type planner struct {
	opts   Options
	refMs  int64
	policy ExpiredPolicy
	mode   ExpiryMode
	target *CapabilityProfile

	keys  map[string]*PlanItem
	funcs map[string]*PlanItem
	order []*PlanItem

	globalWarnings []Warning
}

// collect routes every event into a content-addressed bucket. The order of
// the input slice never affects the final plan: items are sorted in build().
func (p *planner) collect(objects []model.RedisObject) error {
	for _, obj := range objects {
		if obj == nil {
			continue
		}
		switch o := obj.(type) {
		case *model.FunctionsObject:
			p.collectFunctions(o)
		case *model.AuxObject:
			p.globalWarnings = append(p.globalWarnings, Warning{
				Code:     "aux-field-not-restored",
				Message:  "RDB aux field " + strconv.Quote(o.Key) + " cannot be replayed as a data command",
				Evidence: []string{o.Key},
			})
		case *model.DBSizeObject:
			// RDB_OPCODE_RESIZEDB is only a hash table sizing hint.
		case *model.ModuleTypeObject:
			p.upsertKey(p.blockedModuleItem(o))
		case *model.StringObject:
			p.upsertKey(p.planString(o))
		case *model.ListObject:
			p.upsertKey(p.planList(o))
		case *model.SetObject:
			p.upsertKey(p.planSet(o))
		case *model.HashObject:
			item, err := p.planHash(o)
			if err != nil {
				return err
			}
			p.upsertKey(item)
		case *model.ZSetObject:
			p.upsertKey(p.planZSet(o))
		case *model.StreamObject:
			p.upsertKey(p.planStream(o))
		default:
			p.upsertKey(p.blockedUnknownItem(o))
		}
	}
	return nil
}

func itemMapKey(db int, key string) string {
	return strconv.Itoa(db) + "\x00" + key
}

func (p *planner) upsertKey(item *PlanItem) {
	mk := itemMapKey(item.DB, item.Key)
	existing, ok := p.keys[mk]
	if !ok {
		p.keys[mk] = item
		p.order = append(p.order, item)
		return
	}
	// Duplicate keys cannot legally occur in one RDB. If overlapping inputs
	// are supplied, choose deterministically and warn.
	if itemStableKey(item) < itemStableKey(existing) {
		*existing = *item
	}
	existing.Warnings = append(existing.Warnings, Warning{
		Code:    "duplicate-key-event",
		ItemID:  existing.ID,
		Message: "multiple decoded events for key " + strconv.Quote(existing.Key) + " in db " + strconv.Itoa(existing.DB) + "; one was deterministically selected",
	})
}

func itemStableKey(item *PlanItem) string {
	return string(item.Kind) + "|" + item.Type + "|" + item.Encoding + "|" +
		strconv.Itoa(len(item.Create)) + "|" + strconv.Itoa(len(item.FollowUps))
}

// baseItem constructs an item whose id and source derive from content only.
func (p *planner) baseItem(obj model.RedisObject) *PlanItem {
	item := &PlanItem{
		Kind:     KindKey,
		DB:       obj.GetDBIndex(),
		Key:      obj.GetKey(),
		Type:     obj.GetType(),
		Encoding: obj.GetEncoding(),
		Status:   StatusReady,
		Source: Source{
			Kind:     "key",
			DB:       obj.GetDBIndex(),
			Key:      obj.GetKey(),
			Encoding: obj.GetEncoding(),
			Type:     obj.GetType(),
		},
	}
	item.ID = stableItemID(item.Kind, item.DB, item.Key)
	return item
}

// stableItemID derives solely from logical content, never input order.
func stableItemID(kind ItemKind, db int, key string) string {
	safe := key != ""
	if safe {
		for i := 0; i < len(key); i++ {
			c := key[i]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
				c == '-', c == '_', c == '.', c == ':':
			default:
				safe = false
			}
			if !safe {
				break
			}
		}
	}
	if safe {
		return string(kind) + ":db" + strconv.Itoa(db) + ":" + key
	}
	return string(kind) + ":db" + strconv.Itoa(db) + ":0x" + hexBytes([]byte(key))
}

// addEvictionWarning records unavoidable LRU/LFU metadata loss.
func (p *planner) addEvictionWarning(item *PlanItem, obj model.RedisObject) {
	if info, ok := obj.(model.EvictionInfo); ok {
		switch {
		case info.GetIdleTime() >= 0:
			item.Warnings = append(item.Warnings, Warning{
				Code:     "eviction-metadata-lost",
				ItemID:   item.ID,
				Message:  "LRU idle time cannot be replayed through public commands",
				Evidence: []string{"idle=" + strconv.FormatInt(info.GetIdleTime(), 10)},
			})
		case info.GetFreq() >= 0:
			item.Warnings = append(item.Warnings, Warning{
				Code:     "eviction-metadata-lost",
				ItemID:   item.ID,
				Message:  "LFU frequency cannot be replayed through public commands",
				Evidence: []string{"freq=" + strconv.FormatInt(info.GetFreq(), 10)},
			})
		}
	}
}

// expirySpec resolves key-level expiration semantics at the reference time.
func (p *planner) expirySpec(obj model.RedisObject) (*ExpirySpec, error) {
	exp := obj.GetExpiration()
	if exp == nil {
		return nil, nil
	}
	absMs := exp.UnixNano() / int64(time.Millisecond)
	spec := &ExpirySpec{
		Mode:         p.mode,
		AbsoluteAtMs: absMs,
		Precision:    "millisecond",
		Expired:      absMs <= p.refMs,
	}
	if p.mode == ExpiryRelative {
		spec.RelativeTTLMs = absMs - p.refMs
	}
	return spec, nil
}

// applyKeyExpiry handles expired-key policy and appends the expiry follow-up.
func (p *planner) applyKeyExpiry(item *PlanItem, obj model.RedisObject) error {
	spec, err := p.expirySpec(obj)
	if err != nil || spec == nil {
		return err
	}
	item.Expiry = spec
	if spec.Expired {
		switch p.policy {
		case ExpiredError:
			return &ExpiredKeyError{
				DB: item.DB, Key: item.Key,
				ExpiredAtMs: spec.AbsoluteAtMs, RefMs: p.refMs,
			}
		case ExpiredKeep:
			item.Warnings = append(item.Warnings, Warning{
				Code:     "key-already-expired",
				ItemID:   item.ID,
				Message:  "key is already expired at reference time; restored with original expiration per keep policy",
				Evidence: []string{"expiredAtMs=" + strconv.FormatInt(spec.AbsoluteAtMs, 10)},
			})
		default: // skip
			item.Status = StatusSkippedExpired
			item.Create = nil
			item.FollowUps = nil
			item.Warnings = append(item.Warnings, Warning{
				Code:     "key-skipped-expired",
				ItemID:   item.ID,
				Message:  "key is already expired at reference time and was skipped",
				Evidence: []string{"expiredAtMs=" + strconv.FormatInt(spec.AbsoluteAtMs, 10)},
			})
			item.Atomicity = Atomicity{Atomic: false, NumCommands: 0, Boundary: "not executed: key expired"}
			return nil
		}
	}
	name := "PEXPIREAT"
	val := strconv.FormatInt(spec.AbsoluteAtMs, 10)
	if p.mode == ExpiryRelative {
		name = "PEXPIRE"
		val = strconv.FormatInt(spec.RelativeTTLMs, 10)
	}
	item.FollowUps = append(item.FollowUps, &Command{
		Name:  name,
		Args:  []Arg{Arg(item.Key), Arg(val)},
		Role:  RoleFollowUp,
		Notes: []string{"replays key expiration at restore reference time"},
	})
	return nil
}

// finalize sets status/warnings and atomicity after commands are built.
func (p *planner) finalize(item *PlanItem) {
	if item.Status == StatusBlocked || item.Status == StatusSkippedExpired {
		return
	}
	n := len(item.Create) + len(item.FollowUps)
	item.Atomicity = Atomicity{
		Atomic:      n <= 1,
		NumCommands: n,
	}
	if n > 1 {
		item.Atomicity.Boundary = "multiple sequential commands; failure between commands leaves a partially restored object"
	} else if n == 1 {
		item.Atomicity.Boundary = "single command"
	} else {
		item.Atomicity.Boundary = "no command required"
	}
	if len(item.PartialBlockers) > 0 {
		item.Status = StatusLossy
	}
	if len(item.Warnings) > 0 && item.Status == StatusReady {
		item.Status = StatusLossy
	}
	sortWarnings(item.Warnings)
	sortBlockers(item.PartialBlockers)
}

// sortWarnings sorts deterministically by code then evidence.
func sortWarnings(ws []Warning) {
	sort.Slice(ws, func(i, j int) bool {
		if ws[i].Code != ws[j].Code {
			return ws[i].Code < ws[j].Code
		}
		return stringsJoin(ws[i].Evidence) < stringsJoin(ws[j].Evidence)
	})
}

func sortBlockers(bs []Blocker) {
	sort.Slice(bs, func(i, j int) bool {
		if bs[i].Code != bs[j].Code {
			return bs[i].Code < bs[j].Code
		}
		return stringsJoin(bs[i].Evidence) < stringsJoin(bs[j].Evidence)
	})
}

func stringsJoin(ss []string) string {
	out := ""
	for _, s := range ss {
		out += s + "\x00"
	}
	return out
}
