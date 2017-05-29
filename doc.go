package rapid

//go:generate protoc ./rapid.proto --gogofast_out=plugins=grpc:remoting
//go:generate mockgen --package mocks --destination mocks/client.go  github.com/casualjim/go-rapid Client
