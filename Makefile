# vim: ft=make

GO ?= go
GOVERSION ?= go1.8.3
SHELL := /bin/bash
GIT_VERSION = $(shell git describe --tags)

.DEFAULT_GOAL := help

.PHONY: check
check: goversion checkfmt ## Runs static code analysis checks
	@echo running metalint ...
	gometalinter --vendored-linters --install
	gometalinter --vendored-linters --vendor --disable=gotype --errors --fast --deadline=60s   ./...

.PHONY: test
test: ## Runs tests in all packages
	@echo running tests...
	@(go test -race $$(go list ./... | grep -v vendor))

.PHONY: update-deps
update-deps: ## Updates the dependencies with flattened vendor and without test files
	@echo updating deps...
	@dep ensure -update

.PHONY: goversion
goversion: ## Checks if installed go version is latest
	@echo Checking go version...
	@( $(GO) version | grep -q $(GOVERSION) ) || ( echo "Please install $(GOVERSION) (found: $$($(GO) version))" && exit 1 )

.PHONY: checkfmt
checkfmt: ## Checks code format
	.travis/gofmtcheck.sh

.PHONY: fmt
fmt: ## format go code
	goimports -w $$(find . -name '*.go' -not -path './vendor/*')

.PHONY: distclean
distclean: ## Clean ALL files including ignored ones
	git clean -f -d -x .

.PHONY: clean
clean: ## Clean all modified files
	git clean -f -d .

.PHONY: generate
generate: go-generate fmt

.PHONY: go-generate
go-generate: ## run go generate
	$(GO) generate .

.PHONY: devtools
devtools: ## install necessary tools for development
	@echo installing golang dep tools
	go get -u github.com/golang/dep/cmd/dep
	@echo installing grpc
	go get -u github.com/golang/protobuf/{proto,protoc-gen-go}
	go get -u google.golang.org/grpc
	go get -u github.com/gogo/protobuf/protoc-gen-gofast
	go get -u github.com/gogo/protobuf/protoc-gen-gogofast
	@echo installing mockgen
	go get github.com/golang/mock/mockgen
	@echo installing metalinter
	go get -u github.com/alecthomas/gometalinter
	gometalinter --install --update

.PHONY: help
help: ## Display make help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
