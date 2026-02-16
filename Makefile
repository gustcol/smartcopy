# SmartCopy Build Automation
IMAGE_NAME ?= smartcopy
IMAGE_TAG ?= latest
REGISTRY ?= ghcr.io/smartcopy
FULL_IMAGE = $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: build build-no-cache push clean run shell dev-build test clippy release multi-arch

# Docker builds
build:
	DOCKER_BUILDKIT=1 docker build -t $(IMAGE_NAME):$(IMAGE_TAG) .

build-no-cache:
	DOCKER_BUILDKIT=1 docker build --no-cache -t $(IMAGE_NAME):$(IMAGE_TAG) .

push: build
	docker tag $(IMAGE_NAME):$(IMAGE_TAG) $(FULL_IMAGE)
	docker push $(FULL_IMAGE)

clean:
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) 2>/dev/null || true
	cargo clean

run:
	docker run --rm -it -v $(PWD):/data $(IMAGE_NAME):$(IMAGE_TAG)

shell:
	docker run --rm -it -v $(PWD):/data --entrypoint /bin/bash $(IMAGE_NAME):$(IMAGE_TAG)

# Local development
dev-build:
	cargo build --all-features

test:
	cargo test --all-features

clippy:
	cargo clippy --all-features -- -D warnings

release:
	cargo build --release --all-features

# Multi-architecture build (requires docker buildx)
multi-arch:
	docker buildx build --platform linux/amd64,linux/arm64 \
		-t $(FULL_IMAGE) --push .
