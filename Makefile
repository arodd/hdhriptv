IMAGE_TAG ?= latest
REGISTRY_IMAGE ?=
BUILDX_BUILDER ?= hdhriptv-multiarch

.PHONY: production-build release-local docs-check

production-build:
	go build -o hdhriptv ./cmd/hdhriptv
	sudo setcap 'cap_net_bind_service=+ep' ./hdhriptv
	sudo cp hdhriptv /Datastore/bin/.

release-local:
	test -n "$(REGISTRY_IMAGE)" || (echo "REGISTRY_IMAGE is required (example: registry.example.com/org/hdhriptv)" && exit 1)
	go mod download
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o dist/hdhriptv-linux-amd64 ./cmd/hdhriptv
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags="-s -w" -o dist/hdhriptv-linux-arm64 ./cmd/hdhriptv
	test -f dist/hdhriptv-linux-amd64
	test -f dist/hdhriptv-linux-arm64
	docker version
	docker run --privileged --rm tonistiigi/binfmt --install arm64,amd64
	docker buildx inspect "$(BUILDX_BUILDER)" >/dev/null 2>&1 || docker buildx create --name "$(BUILDX_BUILDER)" --driver docker-container
	docker buildx use "$(BUILDX_BUILDER)"
	docker buildx inspect --bootstrap
	docker buildx build --platform linux/amd64,linux/arm64 --provenance=false --tag "$(REGISTRY_IMAGE):$(IMAGE_TAG)" --push .

docs-check:
	./scripts/docs/check-open-todo-links.sh
	./scripts/docs/check-api-paths.sh
