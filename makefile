# Makefile

export GOPATH=`pwd`
go get -u github.com/golang/protobuf/protoc-gen-go
go build branches/branch.go
go build controller/controller.go  
