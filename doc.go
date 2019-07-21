package rapid

//go:generate ./hack/gen-grpc
//go:generate mockery -dir api -name Client -case snake
//go:generate mockery -dir api -case snake -name Detector
// go:generate mockgen --package edgefailure --destination edgefailure/detector_mock.go  github.com/casualjim/go-rapid/edgefailure Detector
