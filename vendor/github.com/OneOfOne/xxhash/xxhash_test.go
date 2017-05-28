package xxhash_test

import (
	"hash/adler32"
	"hash/crc32"
	"hash/crc64"
	"hash/fnv"
	"testing"

	N "github.com/OneOfOne/xxhash"
)

const inS = `Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.
`

var (
	in = []byte(inS)
)

const (
	expected32 uint32 = 0x6101218F
	expected64 uint64 = 0xFFAE31BEBFED7652
)

func Test(t *testing.T) {
	t.Log("xxhash backend:", N.Backend)
	t.Log("Benchmark string len:", len(inS))
}

func TestHash32(t *testing.T) {
	h := N.New32()
	h.Write(in)
	r := h.Sum32()
	if r != expected32 {
		t.Errorf("expected 0x%x, got 0x%x.", expected32, r)
	}
}

func TestHash32Short(t *testing.T) {
	r := N.Checksum32(in)
	if r != expected32 {
		t.Errorf("expected 0x%x, got 0x%x.", expected32, r)
	}
}

func TestHash64(t *testing.T) {
	h := N.New64()
	h.Write(in)
	r := h.Sum64()
	if r != expected64 {
		t.Errorf("expected 0x%x, got 0x%x.", expected64, r)
	}
}

func TestHash64Short(t *testing.T) {
	r := N.Checksum64(in)
	if r != expected64 {
		t.Errorf("expected 0x%x, got 0x%x.", expected64, r)
	}
}

func TestWriteStringNil(t *testing.T) {
	h32, h64 := N.New32(), N.New64()
	for i := 0; i < 1e6; i++ {
		h32.WriteString("")
		h64.WriteString("")
	}
	_, _ = h32.Sum32(), h64.Sum64()
}

func BenchmarkXXChecksum32(b *testing.B) {
	var bv uint32
	for i := 0; i < b.N; i++ {
		bv += N.Checksum32(in)
	}
}

func BenchmarkXXChecksumString32(b *testing.B) {
	var bv uint32
	for i := 0; i < b.N; i++ {
		bv += N.ChecksumString32(inS)
	}
}

func BenchmarkXXChecksum64(b *testing.B) {
	var bv uint64
	for i := 0; i < b.N; i++ {
		bv += N.Checksum64(in)
	}
}

func BenchmarkXXChecksumString64(b *testing.B) {
	var bv uint64
	for i := 0; i < b.N; i++ {
		bv += N.ChecksumString64(inS)
	}
}

func BenchmarkFnv32(b *testing.B) {
	var bv []byte
	h := fnv.New32()
	for i := 0; i < b.N; i++ {
		h.Write(in)
		bv = h.Sum(nil)
		h.Reset()
	}
	_ = bv
}

func BenchmarkFnv64(b *testing.B) {
	var bv []byte
	h := fnv.New64()
	for i := 0; i < b.N; i++ {
		h.Write(in)
		bv = h.Sum(nil)
		h.Reset()
	}
	_ = bv
}

func BenchmarkAdler32(b *testing.B) {
	var bv uint32
	for i := 0; i < b.N; i++ {
		bv += adler32.Checksum(in)
	}
}

func BenchmarkCRC32IEEE(b *testing.B) {
	var bv uint32
	for i := 0; i < b.N; i++ {
		bv += crc32.ChecksumIEEE(in)
	}
}

func BenchmarkCRC32IEEEString(b *testing.B) {
	var bv uint32
	for i := 0; i < b.N; i++ {
		bv += crc32.ChecksumIEEE([]byte(inS))
	}
}

var crc64ISO = crc64.MakeTable(crc64.ISO)

func BenchmarkCRC64ISO(b *testing.B) {
	var bv uint64
	for i := 0; i < b.N; i++ {
		bv += crc64.Checksum(in, crc64ISO)
	}
}

func BenchmarkCRC64ISOString(b *testing.B) {
	var bv uint64
	for i := 0; i < b.N; i++ {
		bv += crc64.Checksum([]byte(inS), crc64ISO)
	}
}

func BenchmarkXXChecksum64Short(b *testing.B) {
	var bv uint64
	k := []byte("Test-key-100")
	for i := 0; i < b.N; i++ {
		bv += N.Checksum64(k)
	}
}

func BenchmarkXXChecksumString64Short(b *testing.B) {
	var bv uint64
	k := "Test-key-100"
	for i := 0; i < b.N; i++ {
		bv += N.ChecksumString64(k)
	}
}

func BenchmarkCRC32IEEEShort(b *testing.B) {
	var bv uint32
	k := []byte("Test-key-100")

	for i := 0; i < b.N; i++ {
		bv += crc32.ChecksumIEEE(k)
	}
}

func BenchmarkCRC64ISOShort(b *testing.B) {
	var bv uint64
	k := []byte("Test-key-100")
	for i := 0; i < b.N; i++ {
		bv += crc64.Checksum(k, crc64ISO)
	}
}

func BenchmarkFnv64Short(b *testing.B) {
	var bv []byte
	k := []byte("Test-key-100")
	for i := 0; i < b.N; i++ {
		h := fnv.New64()
		h.Write(k)
		bv = h.Sum(nil)
	}
	_ = bv
}

func BenchmarkXX64MultiWrites(b *testing.B) {
	var bv uint64
	h := N.New64()
	for i := 0; i < b.N; i++ {
		h.Write(in)
		bv += h.Sum64()
		h.Reset()
	}
}

func BenchmarkFnv64MultiWrites(b *testing.B) {
	var bv uint64
	h := fnv.New64()
	for i := 0; i < b.N; i++ {
		h.Write(in)
		bv += h.Sum64()
		h.Reset()
	}
}
