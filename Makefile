EXTENSION_NAME := quack
EXTENSION_VERSION := v0.1.0

DUCKDB_VERSION := v1.2.0
C_API_VERSION := v1.2.0

# Detect host platform; override with PLATFORM=<duckdb platform string>
HOST_OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
HOST_ARCH := $(shell uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
PLATFORM ?= $(if $(filter darwin,$(HOST_OS)),osx,$(HOST_OS))_$(HOST_ARCH)

# Go sources (multi-file layout)
GO_SRCS := $(shell find . -name '*.go' -not -path './$(BUILD_DIR)/*')

# Build tags (empty by default; set for specific platforms below)
GO_BUILD_TAGS ?=

ifeq ($(PLATFORM),osx_arm64)
export GOOS := darwin
export GOARCH := arm64
LIB_PREFIX := lib
LIB_EXT := dylib

else ifeq ($(PLATFORM),osx_amd64)
export GOOS := darwin
export GOARCH := amd64
LIB_PREFIX := lib
LIB_EXT := dylib

else ifeq ($(PLATFORM),linux_amd64)
export GOOS := linux
export GOARCH := amd64
LIB_PREFIX := lib
LIB_EXT := so
GO_BUILD_TAGS := duckdb_use_lib
DUCKDB_LIB_PATH ?=
ifneq ($(strip $(DUCKDB_LIB_PATH)),)
export CGO_LDFLAGS := -L$(DUCKDB_LIB_PATH) -Wl,-rpath,$(DUCKDB_LIB_PATH)
endif

else ifeq ($(PLATFORM),linux_arm64)
export GOOS := linux
export GOARCH := arm64
LIB_PREFIX := lib
LIB_EXT := so
GO_BUILD_TAGS := duckdb_use_lib
DUCKDB_LIB_PATH ?=
ifneq ($(strip $(DUCKDB_LIB_PATH)),)
export CGO_LDFLAGS := -L$(DUCKDB_LIB_PATH) -Wl,-rpath,$(DUCKDB_LIB_PATH)
endif

else
$(error Unsupported platform: "$(PLATFORM)")
endif

export CGO_ENABLED := 1

BUILD_DIR := build
EXTENSION_LIB_FILE := $(BUILD_DIR)/$(PLATFORM)/$(LIB_PREFIX)$(EXTENSION_NAME).$(LIB_EXT)
EXTENSION_FILE := $(BUILD_DIR)/$(PLATFORM)/$(EXTENSION_NAME).duckdb_extension

# -----------------------------
# Arrow standalone executable
# -----------------------------
ARROW_BIN_NAME := $(EXTENSION_NAME)_arrow
ARROW_BIN_FILE := $(BUILD_DIR)/$(PLATFORM)/$(ARROW_BIN_NAME)

ARROW_GO_SRCS := \
	arrow_standalone.go \
	arrow_init.go \
	shared.go \
	anthropic_batch.go \
	anthropic_request.go \
	anthropic_requests_fused.go \
	anthropic_single.go \
	dispatcher.go \
	dispatcher_fused.go \
	llm_api.go

all: $(EXTENSION_FILE)

# -------------------------------------------------
# Build shared library + package as duckdb_extension
# -------------------------------------------------
$(EXTENSION_FILE): $(GO_SRCS) go.mod go.sum append_metadata.py | $(BUILD_DIR)/$(PLATFORM)
	go build $(if $(strip $(GO_BUILD_TAGS)),-tags="$(GO_BUILD_TAGS)",) -buildmode=c-shared -o $(EXTENSION_LIB_FILE) .
	python3 append_metadata.py \
		--extension-name $(EXTENSION_NAME) \
		--extension-version $(EXTENSION_VERSION) \
		--duckdb-platform $(PLATFORM) \
		--duckdb-version $(DUCKDB_VERSION) \
		--library-file $(EXTENSION_LIB_FILE) \
		--out-file $(EXTENSION_FILE)

# -----------------------------
# Build standalone Arrow binary
# -----------------------------
arrow: $(ARROW_BIN_FILE)

$(ARROW_BIN_FILE): $(ARROW_GO_SRCS) go.mod go.sum | $(BUILD_DIR)/$(PLATFORM)
	@echo "Building Arrow standalone: $(ARROW_BIN_FILE)"
	CGO_ENABLED=0 go build \
		$(if $(strip $(GO_BUILD_TAGS)),-tags="$(GO_BUILD_TAGS)",) \
		-o $(ARROW_BIN_FILE) \
		$(ARROW_GO_SRCS)

# -----------------------------
# Directories
# -----------------------------
$(BUILD_DIR)/$(PLATFORM):
	mkdir -p $(BUILD_DIR)/$(PLATFORM)

# -----------------------------
# Cleanup
# -----------------------------
clean:
	rm -rf $(BUILD_DIR)

# -----------------------------
# Dev hygiene
# -----------------------------
fmt:
	gofmt -w $(GO_SRCS)

vet:
	go vet ./...

# -----------------------------
# Tests
# -----------------------------

test: $(EXTENSION_FILE)
	duckdb -unsigned -c " \
		PRAGMA enable_profiling='json'; \
		PRAGMA profiling_output='profile.json'; \
		LOAD '$(EXTENSION_FILE)'; \
		CREATE TABLE animals (id INTEGER, name VARCHAR); \
		INSERT INTO animals (id, name) VALUES \
			(0, 'cat'), \
			(1, 'dog'), \
			(2, 'cat'), \
			(3, 'fish'), \
			(4, 'cat'), \
			(5, 'dog'); \
		SELECT \
			id, \
			name, \
			ai_llm(name, 'What sound does this animal make?') AS sound, \
			ai_llm(name, 'Reverse the name and capitalize it') AS transformed \
		FROM animals; \
	"

.PHONY: all clean fmt vet test atest arrow
