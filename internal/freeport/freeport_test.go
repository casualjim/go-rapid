package freeport

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNext(t *testing.T) {
	port, err := Next()
	require.NoError(t, err)
	require.NotEqual(t, 0, port)

	listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
	require.NoError(t, err)
	require.NoError(t, listener.Close())
}

func TestNextN(t *testing.T) {
	const numPorts = 5
	ports, err := NextN(numPorts)
	require.NoError(t, err)
	require.Len(t, ports, numPorts)

	for _, port := range ports {
		listener, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(port))
		require.NoError(t, err)
		require.NoError(t, listener.Close())
	}
}
