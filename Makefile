.PHONY: build test run upstream loadgen demo docker-demo

build:
	go build -o bin/ratelimiter ./cmd/ratelimiter
	go build -o bin/fake-upstream ./examples/fake-upstream
	go build -o bin/loadgen ./examples/loadgen

test:
	go test ./...

# Needs a local Redis on localhost:6379 (brew services start redis).
run: build
	./bin/ratelimiter -config config.example.yaml

upstream: build
	./bin/fake-upstream -limit 100 -window 1s

loadgen: build
	./bin/loadgen -services m1=70,m2=30,m3=40 -duration 10s

docker-demo:
	docker compose up -d --build
	docker compose run --rm loadgen
