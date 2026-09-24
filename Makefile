# ─── Project Settings ────────────────────────────────────────────────
MODULE       := durarun-operator
REGISTRY     ?= localhost:5000
TAG          ?= dev

IMG_OPERATOR := $(REGISTRY)/durarun-operator:$(TAG)
IMG_RUNNER   := $(REGISTRY)/durarun-runner:$(TAG)
IMG_GATEWAY  := $(REGISTRY)/durarun-gateway:$(TAG)

ARTIFACT_DIR ?= $(CURDIR)/_output

# Test artifact isolation
DURARUN_TEST_ARTIFACT_DIR ?= $(ARTIFACT_DIR)/test-artifacts
export DURARUN_TEST_ARTIFACT_DIR

# Tools (override on CI if needed)
CONTROLLER_GEN ?= controller-gen
GOLANGCI_LINT  ?= golangci-lint

# ─── Code Generation ────────────────────────────────────────────────
.PHONY: generate
generate:
	@echo "[generate] running controller-gen deepcopy (placeholder)"
	@# $(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./api/..."

.PHONY: manifests
manifests:
	@echo "[manifests] running controller-gen CRD manifests (placeholder)"
	@# $(CONTROLLER_GEN) crd paths="./api/..." output:crd:artifacts:config=deploy/crd

# ─── Build ───────────────────────────────────────────────────────────
.PHONY: build
build:
	go build ./...

# ─── Test & Lint ─────────────────────────────────────────────────────
.PHONY: test
test:
	@mkdir -p $(DURARUN_TEST_ARTIFACT_DIR)
	go test -race -count=1 ./...

.PHONY: benchmark
benchmark:
	@mkdir -p $(DURARUN_TEST_ARTIFACT_DIR)
	go test -race -tags=benchmark -count=1 -timeout=300s ./experiment/v3/...

.PHONY: lint
lint:
	@if command -v $(GOLANGCI_LINT) >/dev/null 2>&1; then \
		$(GOLANGCI_LINT) run ./...; \
	else \
		echo "[lint] golangci-lint not found – skipping (install: https://golangci-lint.run/usage/install/)"; \
	fi

# ─── Container Images ───────────────────────────────────────────────
.PHONY: images image-operator image-runner image-gateway
images: image-operator image-runner image-gateway

image-operator:
	docker build -t $(IMG_OPERATOR) -f Dockerfile.operator .

image-runner:
	docker build -t $(IMG_RUNNER) -f Dockerfile.runner .

image-gateway:
	docker build -t $(IMG_GATEWAY) -f Dockerfile.gateway .

# ─── Kind Cluster ────────────────────────────────────────────────────
.PHONY: kind-load
kind-load:
	kind load docker-image $(IMG_OPERATOR) $(IMG_RUNNER) $(IMG_GATEWAY)

# ─── Convenience ─────────────────────────────────────────────────────
.PHONY: verify-generated
verify-generated:
	hack/verify-generated.sh

.PHONY: clean
clean:
	rm -rf $(ARTIFACT_DIR) bin/
