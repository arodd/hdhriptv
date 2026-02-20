IMAGE_TAG ?= latest
REGISTRY_IMAGE ?=
BUILDX_BUILDER ?= hdhriptv-multiarch
INTERNAL_REMOTE ?= origin
INTERNAL_REPO_URL ?= git@gitlab.lan:arodd/hdhriptv.git
PUBLIC_REMOTE ?= github
PUBLIC_REPO_URL ?= git@github.com:arodd/hdhriptv.git
SYNC_BRANCH ?= main
PUBLIC_SYNC_TAG ?= public-sync/latest

.PHONY: production-build release-local docs-check publish-github

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

publish-github:
	@set -eu; \
	if ! git remote get-url "$(INTERNAL_REMOTE)" >/dev/null 2>&1; then \
		echo "Adding internal remote $(INTERNAL_REMOTE) -> $(INTERNAL_REPO_URL)"; \
		git remote add "$(INTERNAL_REMOTE)" "$(INTERNAL_REPO_URL)"; \
	fi; \
	internal_url="$$(git remote get-url "$(INTERNAL_REMOTE)")"; \
	if [ "$$internal_url" != "$(INTERNAL_REPO_URL)" ]; then \
		echo "Updating internal remote $(INTERNAL_REMOTE) URL to $(INTERNAL_REPO_URL)"; \
		git remote set-url "$(INTERNAL_REMOTE)" "$(INTERNAL_REPO_URL)"; \
	fi; \
	if git remote get-url "$(PUBLIC_REMOTE)" >/dev/null 2>&1; then \
		public_url="$$(git remote get-url "$(PUBLIC_REMOTE)")"; \
		if [ "$$public_url" != "$(PUBLIC_REPO_URL)" ]; then \
			echo "Updating public remote $(PUBLIC_REMOTE) URL to $(PUBLIC_REPO_URL)"; \
			git remote set-url "$(PUBLIC_REMOTE)" "$(PUBLIC_REPO_URL)"; \
		fi; \
	else \
		echo "Adding public remote $(PUBLIC_REMOTE) -> $(PUBLIC_REPO_URL)"; \
		git remote add "$(PUBLIC_REMOTE)" "$(PUBLIC_REPO_URL)"; \
	fi; \
	git fetch "$(INTERNAL_REMOTE)" "$(SYNC_BRANCH)" --tags; \
	git fetch "$(PUBLIC_REMOTE)" --tags; \
	if ! git show-ref --verify --quiet "refs/heads/$(SYNC_BRANCH)"; then \
		echo "Local branch $(SYNC_BRANCH) does not exist."; \
		echo "Create it and sync with $(INTERNAL_REMOTE)/$(SYNC_BRANCH) before publishing."; \
		exit 1; \
	fi; \
	local_tip="$$(git rev-parse "refs/heads/$(SYNC_BRANCH)")"; \
	internal_tip="$$(git rev-parse "$(INTERNAL_REMOTE)/$(SYNC_BRANCH)")"; \
	if [ "$$local_tip" != "$$internal_tip" ]; then \
		set -- $$(git rev-list --left-right --count "refs/heads/$(SYNC_BRANCH)...$(INTERNAL_REMOTE)/$(SYNC_BRANCH)"); \
		ahead="$$1"; \
		behind="$$2"; \
		echo "Refusing to publish: local $(SYNC_BRANCH) is out of sync with $(INTERNAL_REMOTE)/$(SYNC_BRANCH)."; \
		echo "Local ahead: $$ahead, behind: $$behind"; \
		echo "Push/pull and re-run publish-github."; \
		exit 1; \
	fi; \
	tree="$$(git rev-parse "$$internal_tip^{tree}")"; \
	last_internal="$$(git rev-parse -q --verify "$(PUBLIC_SYNC_TAG)^{commit}" 2>/dev/null || true)"; \
	if [ -n "$$last_internal" ] && [ "$$last_internal" = "$$internal_tip" ]; then \
		echo "No new internal commits to publish."; \
		exit 0; \
	fi; \
	if [ -n "$$last_internal" ] && ! git merge-base --is-ancestor "$$last_internal" "$$internal_tip"; then \
		echo "Marker tag $(PUBLIC_SYNC_TAG) is not an ancestor of $(INTERNAL_REMOTE)/$(SYNC_BRANCH)."; \
		echo "Resolve marker history before publishing."; \
		exit 1; \
	fi; \
	if git rev-parse --verify -q "$(PUBLIC_REMOTE)/$(SYNC_BRANCH)" >/dev/null; then \
		public_tip="$$(git rev-parse "$(PUBLIC_REMOTE)/$(SYNC_BRANCH)")"; \
		new_commit="$$(printf 'public(main): squash sync\n\nInternal-From: %s\nInternal-To: %s\n' "$${last_internal:-<initial>}" "$$internal_tip" | git commit-tree "$$tree" -p "$$public_tip")"; \
	else \
		new_commit="$$(printf 'public(main): initial squash sync\n\nInternal-From: %s\nInternal-To: %s\n' "$${last_internal:-<initial>}" "$$internal_tip" | git commit-tree "$$tree")"; \
	fi; \
	git push "$(PUBLIC_REMOTE)" "$$new_commit:refs/heads/$(SYNC_BRANCH)"; \
	git tag -f "$(PUBLIC_SYNC_TAG)" "$$internal_tip"; \
	git push -f "$(INTERNAL_REMOTE)" "refs/tags/$(PUBLIC_SYNC_TAG)"; \
	echo "Published $$internal_tip to $(PUBLIC_REMOTE)/$(SYNC_BRANCH) as $$new_commit"
