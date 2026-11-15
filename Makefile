.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/rssrewrite-worker ./cmd/rssrewrite-worker

pack:
	bash scripts/pack.sh dist
