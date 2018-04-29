GO := GO15VENDOREXPERIMENT="1" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go

default: build

lvm-manager:
	$(GO) build -o bin/lvm-volume-manager cmd/manager/main.go

lvm-scheduler:
	$(GO) build -o bin/lvm-scheduler cmd/scheduler/main.go

lvm-provisioner:
	$(GO) build -o bin/lvm-volume-provisioner cmd/provisioner/main.go

build: lvm-manager lvm-scheduler lvm-provisioner

docker: build
	docker build --tag "127.0.0.1:5000/pingcap/lvm-manager" .

clean:
	rm -rf bin/*

.PHONY: clean
