package restoreplan

import (
	"strconv"

	"github.com/hdt3213/rdb/model"
)

// planHash restores HSET pairs plus, for RDB encodings carrying field-level
// TTLs (hashex/listpackex/hash2), one deterministic follow-up command per
// field. Field expirations are absolute Unix-ms timestamps (0 == persistent).
func (p *planner) planHash(o *model.HashObject) (*PlanItem, error) {
	item := p.baseItem(o)
	p.addEvictionWarning(item, o)

	pairs := sortedHashPairs(o.Hash)
	hasFieldTTL := len(o.FieldExpirations) > 0

	if hasFieldTTL {
		// Apply expired-field policy to the field/value pairs themselves.
		kept := make([][2][]byte, 0, len(pairs))
		for _, kv := range pairs {
			field := string(kv[0])
			absMs, known := o.FieldExpirations[field]
			expired := known && absMs > 0 && absMs <= p.refMs
			if !expired {
				kept = append(kept, kv)
				continue
			}
			switch p.policy {
			case ExpiredError:
				return nil, &ExpiredKeyError{
					DB: item.DB, Key: item.Key, Field: field,
					ExpiredAtMs: absMs, RefMs: p.refMs,
				}
			case ExpiredKeep:
				kept = append(kept, kv)
				item.Warnings = append(item.Warnings, Warning{
					Code:     "field-already-expired",
					ItemID:   item.ID,
					Message:  "hash field " + strconv.Quote(field) + " is expired at reference time; kept per policy",
					Evidence: []string{"field=" + field, "expiredAtMs=" + strconv.FormatInt(absMs, 10)},
				})
			default:
				item.Warnings = append(item.Warnings, Warning{
					Code:     "field-skipped-expired",
					ItemID:   item.ID,
					Message:  "hash field " + strconv.Quote(field) + " is expired at reference time and was skipped",
					Evidence: []string{"field=" + field, "expiredAtMs=" + strconv.FormatInt(absMs, 10)},
				})
			}
		}
		pairs = kept
	}

	p.chunkCreateKV(item, "HSET", []byte(o.Key), pairs, "hash")

	if hasFieldTTL {
		fields := make([]string, 0, len(o.FieldExpirations))
		for f := range o.FieldExpirations {
			fields = append(fields, f)
		}
		sortStrings(fields)
		for _, field := range fields {
			// skip fields removed by the expired-skip policy
			if _, present := o.Hash[field]; !present {
				continue
			}
			absMs := o.FieldExpirations[field]
			if absMs > 0 && absMs <= p.refMs && p.policy == ExpiredSkip {
				continue
			}
			if !p.target.HashFieldExpiry {
				if absMs > 0 {
					item.PartialBlockers = append(item.PartialBlockers, p.capBlocker(
						"hash-field-expiry-unsupported",
						"HPEXPIREAT requires hash field expiration support (Redis 7.4 / Valkey 9)",
						"field="+field, "expiredAtMs="+strconv.FormatInt(absMs, 10),
					))
					item.Warnings = append(item.Warnings, Warning{
						Code:     "field-expiry-lost",
						ItemID:   item.ID,
						Message:  "field expiration for " + strconv.Quote(field) + " cannot be replayed on target profile",
						Evidence: []string{"field=" + field},
					})
				}
				continue
			}
			var c *Command
			switch {
			case absMs == 0:
				// Explicit persistent marker: unnecessary on a freshly HSET
				// created key, so only emitted when the value really carries
				// a persist marker alongside other TTLs.
				continue
			default:
				if p.mode == ExpiryRelative {
					ttl := absMs - p.refMs
					c = textCmd(RoleFollowUp, "HPEXPIRE", o.Key, strconv.FormatInt(ttl, 10), "FIELDS", "1", field)
				} else {
					c = textCmd(RoleFollowUp, "HPEXPIREAT", o.Key, strconv.FormatInt(absMs, 10), "FIELDS", "1", field)
				}
			}
			c.Notes = append(c.Notes, "restores per-field expiration; one command per field, ordered by field")
			item.FollowUps = append(item.FollowUps, c)
		}
	}

	if err := p.applyKeyExpiry(item, o); err != nil {
		return nil, err
	}
	p.finalize(item)
	return item, nil
}

func (p *planner) capBlocker(code, reason string, evidence ...string) Blocker {
	b := Blocker{Code: code, Reason: reason, Evidence: evidence}
	switch code {
	case "hash-field-expiry-unsupported":
		b.RequiredCapability = "hashFieldExpiry"
	case "xgroup-createconsumer-unsupported":
		b.RequiredCapability = "xgroupCreateConsumer"
	case "xsetid-metadata-unsupported":
		b.RequiredCapability = "xsetidMetadata"
	case "xgroup-mkstream-unsupported":
		b.RequiredCapability = "xgroupMkStream"
	}
	return b
}

func sortStrings(ss []string) {
	for i := 1; i < len(ss); i++ {
		for j := i; j > 0 && ss[j-1] > ss[j]; j-- {
			ss[j-1], ss[j] = ss[j], ss[j-1]
		}
	}
}
