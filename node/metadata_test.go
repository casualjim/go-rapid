package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMetadata_Get_ErrorMissingHost(t *testing.T) {
	md := NewMetadata()

	_, _, err := md.Get(Addr{})
	assert.Error(t, err)
}

func TestMetadata_Get_Success(t *testing.T) {
	addr := Addr{Host: "127.0.0.1", Port: 9493}
	addr2 := Addr{Host: "127.0.0.1", Port: 5555}

	md := &Metadata{
		table: map[Addr]map[string]string{
			addr:  map[string]string{"value": "original"},
			addr2: map[string]string{"value": "original2"},
		},
	}

	v, ok, err := md.Get(addr)
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Equal(t, "original", v["value"])
	}
}

func TestMetadata_Get_Immutable(t *testing.T) {
	addr := Addr{Host: "127.0.0.1", Port: 9493}
	addr2 := Addr{Host: "127.0.0.1", Port: 5555}

	md := &Metadata{
		table: map[Addr]map[string]string{
			addr:  map[string]string{"value": "original"},
			addr2: map[string]string{"value": "original2"},
		},
	}

	v, ok, err := md.Get(addr)
	md.table[addr]["value"] = "modified"
	if assert.NoError(t, err) {
		assert.True(t, ok)
		assert.Equal(t, "original", v["value"])
	}
}

func TestMetadata_Add_Errors(t *testing.T) {
	md := NewMetadata()

	_, err := md.Add(Addr{}, map[string]string{})
	require.Error(t, err)
	_, err = md.Add(Addr{Host: "127.0.0.1", Port: 4949}, nil)
	require.Error(t, err)
}

func TestMetadata_Add_Success(t *testing.T) {
	addr := Addr{Host: "127.0.0.1", Port: 9493}
	addr2 := Addr{Host: "127.0.0.1", Port: 5555}
	addr3 := Addr{Host: "127.0.0.1", Port: 4444}

	md := &Metadata{
		table: map[Addr]map[string]string{
			addr2: map[string]string{"value": "original2"},
			addr3: map[string]string{"value": "original3"},
		},
	}

	updated, err := md.Add(addr, map[string]string{"value": "updated"})
	if assert.NoError(t, err) {
		if assert.True(t, updated) {
			assert.Equal(t, "updated", md.table[addr]["value"])
		}
	}
}
func TestMetadata_Add_IgnoreRepeated(t *testing.T) {
	addr2 := Addr{Host: "127.0.0.1", Port: 5555}
	addr3 := Addr{Host: "127.0.0.1", Port: 4444}

	md := &Metadata{
		table: map[Addr]map[string]string{
			addr2: map[string]string{"value": "original2"},
			addr3: map[string]string{"value": "original3"},
		},
	}

	updated, err := md.Add(addr2, map[string]string{"value": "updated"})
	if assert.NoError(t, err) {
		if assert.False(t, updated) {
			assert.Equal(t, "original2", md.table[addr2]["value"])
		}
	}
}
func TestMetadata_Add_Immutable(t *testing.T) {
	addr := Addr{Host: "127.0.0.1", Port: 9493}
	addr2 := Addr{Host: "127.0.0.1", Port: 5555}
	addr3 := Addr{Host: "127.0.0.1", Port: 4444}

	md := &Metadata{
		table: map[Addr]map[string]string{
			addr2: map[string]string{"value": "original2"},
			addr3: map[string]string{"value": "original3"},
		},
	}

	data := map[string]string{"value": "updated"}
	updated, err := md.Add(addr, data)
	if assert.NoError(t, err) {
		if assert.True(t, updated) {
			data["value"] = "ignore"
			assert.Equal(t, "updated", md.table[addr]["value"])
		}
	}
}

func TestMetadata_Del_Errors(t *testing.T) {
	md := NewMetadata()

	err := md.Del(Addr{})
	assert.Error(t, err)
}

func TestMetadata_Del_Success(t *testing.T) {
	addr := Addr{Host: "127.0.0.1", Port: 9493}
	addr2 := Addr{Host: "127.0.0.1", Port: 5555}

	md := &Metadata{
		table: map[Addr]map[string]string{
			addr:  map[string]string{"value": "original"},
			addr2: map[string]string{"value": "original2"},
		},
	}

	if assert.NoError(t, md.Del(addr2)) {
		assert.NotContains(t, md.table, addr2)
	}
}
