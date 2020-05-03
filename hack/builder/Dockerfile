FROM node:12.6-buster-slim

RUN set -e &&\
  apt-get update -yq &&\
  apt-get install -yqq curl git gnupg2 apt-transport-https ca-certificates software-properties-common &&\
  curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add - &&\
  add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" &&\
  apt-get update -y &&\
  apt-get install -y docker-ce-cli protobuf-compiler build-essential  &&\
  curl -sL https://dl.google.com/go/$(curl --silent https://golang.org/doc/devel/release.html | grep -Eo 'go[0-9]+(\.[0-9]+)+' | sort -V | uniq | tail -1).linux-amd64.tar.gz | tar -C /usr/local -xz &&\
  export PATH=/usr/local/go/bin:$PATH &&\
  go get -u golang.org/x/tools/cmd/... &&\
  GO111MODULE=on go get -u gotest.tools/gotestsum@latest &&\
	GO111MODULE=on go get -u github.com/golangci/golangci-lint/cmd/golangci-lint@latest &&\
	go get -u github.com/golang/protobuf/proto &&\
  GO111MODULE=on go get -u github.com/golang/protobuf/protoc-gen-go@latest &&\
	GO111MODULE=on go get -u google.golang.org/grpc@latest &&\
  go get -u github.com/fullstorydev/grpchan/cmd/protoc-gen-grpchan &&\
	GO111MODULE=on go get -u github.com/vektra/mockery/cmd/mockery@latest &&\
  apt-get autoremove -yqq &&\
  apt-get clean -y &&\
  apt-get autoclean -yqq &&\
  rm -rf  /tmp/* /var/tmp/* /var/lib/apt/lists/* /usr/share/doc/* /usr/share/locale/* /var/cache/debconf/*-old

ENV PATH "$PATH:/root/go/bin:/usr/local/go/bin"
