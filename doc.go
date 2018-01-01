package rapid

//go:generate protoc ./rapid.proto --gogofast_out=plugins=grpc:remoting
//go:generate mockgen --package mocks --destination mocks/client.go  github.com/casualjim/go-rapid Client
// go:generate mockgen --package linkfailure --destination linkfailure/detector_mock.go  github.com/casualjim/go-rapid/linkfailure Detector
