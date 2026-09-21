package restore

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hdt3213/rdb/core"
	"github.com/hdt3213/rdb/model"
)

// collect decodes an RDB stream with full special opcodes and returns every
// object plus per-event provenance.
func collect(t *testing.T, r io.Reader) []Event {
	t.Helper()
	dec := core.NewDecoder(r).WithSpecialOpCode()
	var events []Event
	var seq int64
	err := dec.Parse(func(obj model.RedisObject) bool {
		seq++
		ev := Event{Object: obj, Sequence: seq}
		if exp := obj.GetExpiration(); exp != nil {
			ev.Precision = "millisecond"
		}
		events = append(events, ev)
		return true
	})
	if err != nil {
		t.Fatalf("parse rdb failed: %v", err)
	}
	return events
}

func collectFile(t *testing.T, name string) []Event {
	t.Helper()
	path := filepath.Join("..", "cases", name)
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = f.Close() }()
	return collect(t, f)
}

// buildMultiTypeRDB writes string/hash/list/set/zset into two databases with
// a millisecond TTL on one key, using the project encoder.
func buildMultiTypeRDB(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := core.NewEncoder(&buf)
	if err := enc.WriteHeader(); err != nil {
		t.Fatal(err)
	}
	ttlMS := uint64(time.Date(2030, 1, 2, 3, 4, 5, 6_000_000, time.UTC).UnixNano() / int64(time.Millisecond))
	if err := enc.WriteDBHeader(0, 5, 1); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteStringObject("str:string", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteHashMapObject("hash:h", map[string][]byte{
		"b": []byte("2"), "a": []byte("1"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteListObject("list:l", [][]byte{[]byte("x"), []byte("y"), []byte("z")}); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteSetObject("set:s", [][]byte{[]byte("three"), []byte("one"), []byte("two")}); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteZSetObject("zset:z", []*model.ZSetEntry{
		{Member: "beta", Score: 2.5},
		{Member: "alpha", Score: 1},
	}, core.WithTTL(ttlMS)); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteDBHeader(1, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteStringObject("db1:only", []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteEnd(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// buildModuleRDB writes a typeModule2 key whose module type is not registered
// on the decoding side, exercising the unknown-module blocker.
func buildModuleRDB(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString("REDIS0011")
	w := &rdbWriter{buf: &buf}
	w.byte(opSelectDB)
	w.length(0)
	w.byte(rawTypeModule2)
	w.str("mod:key")
	w.length(moduleIDForTest(t))
	w.length(uint64(core.ModuleOpcodeString))
	w.str("opaque-module-bytes")
	w.length(uint64(core.ModuleOpcodeEOF))
	w.byte(opEOF)
	buf.Write(make([]byte, 8))
	return buf.Bytes()
}

const (
	rawTypeModule2 = 7
	opSelectDB     = 254
	opResizeDB     = 251
	opEOF          = 255
	opExpireSec    = 253
)

// rdbWriter is a minimal length/string writer matching the RDB encoding used
// by core, sufficient for the small synthetic fixtures in these tests.
type rdbWriter struct{ buf *bytes.Buffer }

func (w *rdbWriter) byte(b byte) { w.buf.WriteByte(b) }

func (w *rdbWriter) length(n uint64) {
	switch {
	case n < 64:
		w.byte(byte(n))
	case n < 16384:
		w.byte(byte(0x40 | n>>8))
		w.byte(byte(n))
	default:
		w.byte(0x81) // len64Bit marker
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], n)
		w.buf.Write(b[:])
	}
}

func (w *rdbWriter) str(s string) {
	w.length(uint64(len(s)))
	w.buf.WriteString(s)
}

// secondExpiryRDB builds a minimal RDB containing a second-precision expiry
// (opcode 253) because the encoder only emits millisecond TTLs.
// Layout: REDIS0011, FE 00, FD <uint32 be? no: little endian seconds>, 00 key val, FF, 8 zero crc.
func secondExpiryRDB(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	buf.WriteString("REDIS0011")
	w := &rdbWriter{buf: &buf}
	w.byte(opSelectDB)
	w.length(0)
	w.byte(opExpireSec)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], 2000000000) // 2033-05-18
	buf.Write(b[:])
	w.byte(0) // type string
	w.str("sec:ttl")
	w.str("value")
	w.byte(opEOF)
	buf.Write(make([]byte, 8))
	return buf.Bytes()
}

// moduleIDForTest mirrors core's module id encoding for name "test-type"
// with encoding version 0.
func moduleIDForTest(t *testing.T) uint64 {
	t.Helper()
	const name = "test-type"
	const cset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	charCode := func(c byte) uint8 {
		for i, cs := range []byte(cset) {
			if cs == c {
				return uint8(i)
			}
		}
		t.Fatalf("bad module name char %q", c)
		return 0
	}
	var id uint64
	for i := 0; i < 9; i++ {
		id |= uint64(charCode(name[i]))
		id <<= 6
	}
	id <<= 4
	return id
}

func encodeRDB(t *testing.T, write func(enc *core.Encoder)) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc := core.NewEncoder(&buf)
	if err := enc.WriteHeader(); err != nil {
		t.Fatal(err)
	}
	if err := enc.WriteDBHeader(0, 1, 0); err != nil {
		t.Fatal(err)
	}
	write(enc)
	if err := enc.WriteEnd(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func emptyStreamWithGroupEvents(t *testing.T) []Event {
	t.Helper()
	rdb := encodeRDB(t, func(enc *core.Encoder) {
		stream := &model.StreamObject{
			BaseObject: &model.BaseObject{Key: "empty:stream"},
			Version:    3,
			Length:     0,
			LastId:     &model.StreamId{Ms: 0, Sequence: 0},
			FirstId:    &model.StreamId{Ms: 0, Sequence: 0},
			Entries:    []*model.StreamEntry{},
			Groups: []*model.StreamGroup{
				{
					Name:        "g",
					LastId:      &model.StreamId{Ms: 0, Sequence: 0},
					EntriesRead: 0,
					Consumers:   []*model.StreamConsumer{},
					Pending:     []*model.StreamNAck{},
				},
			},
		}
		if err := enc.WriteStreamObject("empty:stream", stream); err != nil {
			t.Fatal(err)
		}
	})
	return collect(t, bytesReader(rdb))
}

func emptyStreamNoGroupEvents(t *testing.T) []Event {
	t.Helper()
	rdb := encodeRDB(t, func(enc *core.Encoder) {
		stream := &model.StreamObject{
			BaseObject: &model.BaseObject{Key: "lonely:stream"},
			Version:    3,
			Length:     0,
			LastId:     &model.StreamId{Ms: 0, Sequence: 0},
			FirstId:    &model.StreamId{Ms: 0, Sequence: 0},
			Entries:    []*model.StreamEntry{},
			Groups:     []*model.StreamGroup{},
		}
		if err := enc.WriteStreamObject("lonely:stream", stream); err != nil {
			t.Fatal(err)
		}
	})
	return collect(t, bytesReader(rdb))
}
