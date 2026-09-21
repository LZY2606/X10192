package restore

import (
	"crypto/sha1"
	"encoding/hex"
	"strconv"
	"strings"
)

// Stable ids are content-derived so reordering input events or Go map
// iteration cannot change them. Only '%' and '/' (the id path separator) are
// percent-escaped inside id segments; other punctuation stays readable.

func escapeSegment(s string) string {
	if !strings.ContainsAny(s, "%/") {
		return s
	}
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '%':
			b.WriteString("%25")
		case '/':
			b.WriteString("%2F")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// keyItemID: db<N>/key/<key>
func keyItemID(db int, key string) string {
	return "db" + strconv.Itoa(db) + "/key/" + escapeSegment(key)
}

// functionsID is the single global item carrying all function libraries.
const functionsID = "functions"

func nodeID(item *PlanItem, parts ...string) string {
	return item.ID + "/" + strings.Join(parts, "/")
}

// warningID is content-addressed by code + sorted evidence.
func warningID(code string, sortedEvidence []string) string {
	h := sha1.New()
	h.Write([]byte(code))
	for _, e := range sortedEvidence {
		h.Write([]byte{0})
		h.Write([]byte(e))
	}
	return "w/" + code + "/" + hex.EncodeToString(h.Sum(nil))[:12]
}
