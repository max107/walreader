.PHONY: *

all: test

test:
	go test -v -p 1 ./...