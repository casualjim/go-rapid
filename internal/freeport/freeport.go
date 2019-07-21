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
	listener, err := net.Listen("tcp", ":0")
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
	for i := 0; i < n; i++ {
		listener, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, err
		}
		defer listener.Close()
		result[i] = listener.Addr().(*net.TCPAddr).Port
	}
	return result, nil
}
