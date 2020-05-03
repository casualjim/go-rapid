package freeport

import (
	"net"
)

// Next free port, panics on error
func MustNext() int {
	port, err := Next()
	if err != nil {
		panic(err)
	}
	return port
}

// Next free port
func Next() (port int, err error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

// Next n free ports, panics on error
func MustNextN(n int) []int {
	ports, err := NextN(n)
	if err != nil {
		panic(err)
	}
	return ports
}

// Next n free ports
func NextN(n int) ([]int, error) {
	result := make([]int, n)
	listeners := make([]net.Listener, n)
	for i := 0; i < n; i++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		listeners[i] = listener
		result[i] = listener.Addr().(*net.TCPAddr).Port
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return result, nil
}
