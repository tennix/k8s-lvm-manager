GO := GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go

default: build

lvm-volume-manager:
	$(GO) build -o bin/lvm-volume-manager volume-manager/volume-manager.go

lvm-scheduler:
	$(GO) build -o bin/lvm-scheduler scheduler/scheduler.go

build: lvm-volume-manager lvm-scheduler

docker: build
	docker build --tag "127.0.0.1:5000/pingcap/lvm-manager" .

clean:
	rm -rf bin/*

.PHONY: clean
