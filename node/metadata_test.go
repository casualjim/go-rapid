package node

import (
	"net"
	"strconv"
	"testing"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/casualjim/go-rapid/remoting"
	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

/*
var javaHashes = []int64{
	-8142702818976521972,
	-7147371770735187532,
	363079388246039928,
	7534573388476618709,
	4737150535496919548,
	4933803927329709993,
	-2015114153036544862,
	2561866043046712830,
	-4765086671522938790,
	4678032494214207661,
}


permutation 0 for 127.0.0.1:2375 -6970849018552849470
permutation 1 for 127.0.0.1:2375 -5466037957856086435
permutation 2 for 127.0.0.1:2375 4815649783673873042
permutation 3 for 127.0.0.1:2375 -2607551934136458376
permutation 4 for 127.0.0.1:2375 -1121938748156261858
permutation 5 for 127.0.0.1:2375 -161944288917418436
permutation 6 for 127.0.0.1:2375 1500223810733435098
permutation 7 for 127.0.0.1:2375 5755618641853339409
permutation 8 for 127.0.0.1:2375 5868315971009178676
permutation 9 for 127.0.0.1:2375 -7774102069163897405
*/

/*func TestNodeChecksums(t *testing.T) {
	h := "127.0.0.1"
	p := 2375
	require.Equal(t, "127.0.0.1", h)
	require.EqualValues(t, 2375, p)

	for i := 0; i < 10; i++ {
		cs := checksum(h, p, i)
		t.Logf("permutation %d for 127.0.0.1:2375 %d\n", i, cs)
	}
}

func checksum(h string, p int, seed int) int {
	var hash uint64 = 1
	hash = xxhash.ChecksumString64S(h, uint64(seed)) * 31
	prt := make([]byte, binary.Size(uint64(p)))
	binary.LittleEndian.PutUint64(prt, uint64(p))
	hash += xxhash.Checksum64S(prt, uint64(seed))
	return int(hash)
}
*/

func endpoint(t testing.TB, host string, port int) *remoting.Endpoint {
	ep, err := Endpoint(net.JoinHostPort(host, strconv.Itoa(port)))
	require.NoError(t, err)
	return ep
}

func TestMetadata_Get_ErrorMissingHost(t *testing.T) {
	md := NewMetadataRegistry()

	_, _, err := md.Get(nil)
	assert.Error(t, err)
}

func TestMetadata_Get_Success(t *testing.T) {
	addr := endpoint(t, "127.0.0.1", 9493)
	addr2 := endpoint(t, "127.0.0.1", 5555)

	md := &MetadataRegistry{
		table: map[uint64]entry{
			epchecksum.Checksum(addr, 0):  {ep: addr, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original")}}},
			epchecksum.Checksum(addr2, 0): {ep: addr2, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original2")}}},
		},
	}

	v, ok, err := md.Get(addr)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Equal(t, "original", string(v.Metadata["value"]))
	}
}

func TestMetadata_Get_Immutable(t *testing.T) {
	addr := endpoint(t, "127.0.0.1", 9493)
	addr2 := endpoint(t, "127.0.0.1", 5555)

	md := &MetadataRegistry{
		table: map[uint64]entry{
			epchecksum.Checksum(addr, 0):  {ep: addr, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original")}}},
			epchecksum.Checksum(addr2, 0): {ep: addr2, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original2")}}},
		},
	}

	v, ok, err := md.Get(addr)
	md.table[epchecksum.Checksum(addr, 0)].md.Metadata["value"] = []byte("modified")
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Equal(t, "original", string(v.Metadata["value"]))
	}
}

func TestMetadata_Add_Errors(t *testing.T) {
	md := NewMetadataRegistry()

	_, err := md.Add(nil, &remoting.Metadata{Metadata: map[string][]byte{}})
	require.Error(t, err)
	_, err = md.Add(endpoint(t, "127.0.0.1", 4949), nil)
	require.Error(t, err)
}

func TestMetadata_Add_Success(t *testing.T) {
	addr := endpoint(t, "127.0.0.1", 9493)
	addr2 := endpoint(t, "127.0.0.1", 5555)
	addr3 := endpoint(t, "127.0.0.1", 4444)

	md := &MetadataRegistry{
		table: map[uint64]entry{
			epchecksum.Checksum(addr2, 0): {ep: addr2, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original")}}},
			epchecksum.Checksum(addr3, 0): {ep: addr3, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original2")}}},
		},
	}

	updated, err := md.Add(addr, &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("updated")}})
	if assert.NoError(t, err) {
		if assert.True(t, updated) {
			vv, _, _ := md.Get(addr)
			assert.Equal(t, "updated", string(vv.Metadata["value"]))
		}
	}
}

func TestMetadata_Add_IgnoreRepeated(t *testing.T) {
	addr2 := endpoint(t, "127.0.0.1", 5555)
	addr3 := endpoint(t, "127.0.0.1", 4444)

	md := &MetadataRegistry{
		table: map[uint64]entry{
			epchecksum.Checksum(addr2, 0): {ep: addr2, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original2")}}},
			epchecksum.Checksum(addr3, 0): {ep: addr3, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original3")}}},
		},
	}

	updated, err := md.Add(addr2, &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("updated")}})
	if assert.NoError(t, err) {
		if assert.False(t, updated) {
			vv, _, _ := md.Get(addr2)
			assert.Equal(t, "original2", string(vv.Metadata["value"]))
		}
	}
}

func TestMetadata_Add_Immutable(t *testing.T) {
	addr := endpoint(t, "127.0.0.1", 9493)
	addr2 := endpoint(t, "127.0.0.1", 5555)
	addr3 := endpoint(t, "127.0.0.1", 4444)

	md := &MetadataRegistry{
		table: map[uint64]entry{
			epchecksum.Checksum(addr2, 0): {ep: addr2, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original2")}}},
			epchecksum.Checksum(addr3, 0): {ep: addr3, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original3")}}},
		},
	}

	data := map[string][]byte{"value": []byte("updated")}
	updated, err := md.Add(addr, &remoting.Metadata{Metadata: data})
	if assert.NoError(t, err) {
		if assert.True(t, updated) {
			data["value"] = []byte("ignore")
			vv, _, _ := md.Get(addr)
			assert.Equal(t, "updated", string(vv.Metadata["value"]))
		}
	}
}

func TestMetadata_Del_Errors(t *testing.T) {
	md := NewMetadataRegistry()

	err := md.Del(nil)
	assert.Error(t, err)
}

func TestMetadata_Del_Success(t *testing.T) {
	addr := endpoint(t, "127.0.0.1", 9493)
	addr2 := endpoint(t, "127.0.0.1", 5555)
	addr3 := endpoint(t, "127.0.0.1", 1394)

	md := &MetadataRegistry{
		table: map[uint64]entry{
			epchecksum.Checksum(addr, 0):  {ep: addr, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original")}}},
			epchecksum.Checksum(addr2, 0): {ep: addr2, md: &remoting.Metadata{Metadata: map[string][]byte{"value": []byte("original2")}}},
		},
	}

	assert.NoError(t, md.Del(addr3))
	if assert.NoError(t, md.Del(addr2)) {
		assert.False(t, md.Contains(addr2))
		assert.True(t, md.Contains(addr))
	}
	if assert.NoError(t, md.Del(addr)) {
		assert.False(t, md.Contains(addr))
	}
}
