# vim: ft=make

GO ?= go
GOVERSION ?= go1.13
SHELL := /bin/bash
GIT_VERSION = $(shell git describe --tags)

.DEFAULT_GOAL := help

.PHONY: check
check: goversion checkfmt ## Runs static code analysis checks
	@echo running golangci ...
	golangci-lint run

.PHONY: test
test: ## Runs tests in all packages
	@echo running tests...
	@($(GO) test -race ./...)

.PHONY: coverage
coverage: check ## Runs coverage in all packages
	@echo running coverage...
	@gotestsum -f short-verbose -- -race -coverprofile=coverage.txt -covermode=atomic ./...

.PHONY: update-deps
update-deps: ## Updates the dependencies with flattened vendor and without test files
	@echo updating deps...
	@go get -u $$($(GO) list -m)/...

.PHONY: goversion
goversion: ## Checks if installed go version is latest
	@echo Checking go version...
	@( $(GO) version | grep -q $(GOVERSION) ) || ( echo "Please install $(GOVERSION) (found: $$($(GO) version))" && exit 1 )

.PHONY: checkfmt
checkfmt: ## Checks code format
	.circleci/gofmtcheck.sh

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
devtools: export GO111MODULE=off
devtools: ## install necessary tools for development
	@echo installing gotestsum
	go get -u gotest.tools/gotestsum
	go get -u github.com/golangci/golangci-lint/cmd/golangci-lint
	@echo installing grpc
	go get -u google.golang.org/protobuf/{proto,protoc-gen-go}
	go get -u google.golang.org/grpc
	@echo installing mockery
	go get -u github.com/vektra/mockery/cmd/mockery

.PHONY: help
help: ## Display make help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
