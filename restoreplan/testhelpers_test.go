package restoreplan

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

func decodeBytes(t *testing.T, data []byte) []model.RedisObject {
	t.Helper()
	dec := core.NewDecoder(bytes.NewReader(data)).WithSpecialOpCode()
	var out []model.RedisObject
	if err := dec.Parse(func(o model.RedisObject) bool {
		out = append(out, o)
		return true
	}); err != nil {
		t.Fatalf("parse in-memory rdb: %v", err)
	}
	return out
}

// rdbBuilder assembles a minimal valid RDB stream by hand so tests can craft
// encodings the high-level encoder does not expose (seconds TTL, modules).
type rdbBuilder struct{ buf bytes.Buffer }

func newRDBBuilder() *rdbBuilder {
	b := &rdbBuilder{}
	b.buf.WriteString("REDIS0011")
	return b
}

func (b *rdbBuilder) bytes(t *testing.T) []byte {
	t.Helper()
	b.buf.WriteByte(255)
	b.buf.Write(make([]byte, 8)) // trailing checksum slot, decoder ignores value
	return b.buf.Bytes()
}

func (b *rdbBuilder) selectDB(n int) {
	b.buf.WriteByte(254)
	b.buf.WriteByte(byte(n))
}

func (b *rdbBuilder) resizeDB(keyCount, ttlCount uint64) {
	b.buf.WriteByte(251)
	b.buf.WriteByte(byte(keyCount))
	b.buf.WriteByte(byte(ttlCount))
}

func (b *rdbBuilder) length(n int) {
	if n < 64 {
		b.buf.WriteByte(byte(n))
		return
	}
	if n < 1<<14 {
		b.buf.WriteByte(0x40 | byte(n>>8))
		b.buf.WriteByte(byte(n))
		return
	}
	b.buf.WriteByte(0x80)
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(n))
	b.buf.Write(tmp[:])
}

func (b *rdbBuilder) u64(n uint64) {
	b.buf.WriteByte(0x81)
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], n)
	b.buf.Write(tmp[:])
}

func (b *rdbBuilder) str(s string) {
	b.length(len(s))
	b.buf.WriteString(s)
}

// buildSecondsExpiryRDB creates: string key "sec" with opCodeExpireTime
// (second precision) expiring 2100-01-01T00:00:00Z.
func buildSecondsExpiryRDB(t *testing.T) []byte {
	expSec := uint32(time.Date(2100, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	b := newRDBBuilder()
	b.selectDB(0)
	b.resizeDB(1, 1)
	b.buf.WriteByte(253) // opCodeExpireTime
	var exp [4]byte
	binary.LittleEndian.PutUint32(exp[:], expSec)
	b.buf.Write(exp[:])
	b.buf.WriteByte(0) // type string
	b.str("sec")
	b.str("future")
	return b.bytes(t)
}

// buildUnknownModuleRDB creates a key "modkey" owned by module type
// "test-type" (encoding v0), with no handler registered on the decode side,
// so its raw payload is skipped by the decoder.
func buildUnknownModuleRDB(t *testing.T) []byte {
	moduleID := createTestModuleID("test-type", 0)
	b := newRDBBuilder()
	b.selectDB(0)
	b.resizeDB(1, 0)
	b.buf.WriteByte(7) // typeModule2
	b.str("modkey")
	b.u64(moduleID)
	// module aux payload: one string opcode then EOF
	b.length(int(core.ModuleOpcodeString))
	b.str("opaque-module-payload")
	b.length(int(core.ModuleOpcodeEOF))
	return b.bytes(t)
}

// createTestModuleID mirrors core.createModuleId (test-local copy).
func createTestModuleID(moduleType string, encVersion uint64) uint64 {
	var id uint64
	for i := 0; i < 9; i++ {
		id |= uint64(moduleCharCode(moduleType[i]))
		id <<= 6
	}
	id <<= 4
	id |= encVersion
	return id
}

func moduleCharCode(c uint8) uint8 {
	for i, ch := range []byte(core.ModuleTypeNameCharSet) {
		if c == ch {
			return uint8(i)
		}
	}
	panic("bad module char")
}
