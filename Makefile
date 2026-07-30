# kcptun-rs Makefile
#
# Workspace members:
#   kcp-rs        — KCP reliable UDP transport protocol
#   kcrypt-rs     — Shared block/AEAD cipher library (extracted from kcp-rs)
#   smux-rs       — SMUX stream multiplexer (tokio / smol dual-track)
#   qpp-rs        — Quantum Permutation Pad encryption
#   kio-rs        — Async runtime + network I/O abstraction (tokio / smol)
#   kpprof-rs     — Go-compatible pprof (net/http/pprof) server: CPU + heap/allocs + goroutine + deadlock (opt-in via --features pprof / pprof-deadlock)
#   kcptun-client — Client binary (tokio / smol)
#   kcptun-server — Server binary (tokio / smol)
#
# Runtime feature selection:
#   Native (x86_64 / Apple Silicon):  tokio (default)  — high-concurrency servers
#   ARM (armv7 / aarch64-linux):      smol  (default)  — lightweight / embedded
#
# Targets:
#   build          - Native debug build (tokio)
#   build-smol     - Native debug build (smol)
#   build-tokio    - Alias for build
#   release        - Native release build (tokio, optimized + stripped)
#   release-smol   - Native release build (smol)
#
#   build-armv7    - ARMv7 debug build (smol, minimal: no QPP for smallest size)
#   build-armv7-tokio - ARMv7 debug build (tokio)
#   build-armv7-full - ARMv7 debug build (smol + QPP)
#   release-armv7  - ARMv7 release build (smol, minimal, opt-level=s)
#   release-armv7-tokio - ARMv7 release build (tokio, opt-level=s)
#   release-armv7-full - ARMv7 release build (smol + QPP, opt-level=s)
#
#   build-arm64    - ARM64 debug build (smol, minimal)
#   build-arm64-tokio - ARM64 debug build (tokio)
#   build-arm64-full - ARM64 debug build (smol + QPP)
#   release-arm64  - ARM64 release build (smol, minimal, opt-level=s)
#   release-arm64-tokio - ARM64 release build (tokio, opt-level=s)
#   release-arm64-full - ARM64 release build (smol + QPP, opt-level=s)
#
#   linux          - Linux x86_64 release (musl, for testing from macOS)
#   linux-aarch64  - Linux aarch64 release (musl)
#   linux-full     - Linux x86_64 + QPP
#   linux-aarch64-full - Linux aarch64 + QPP
#
#   test           - Run all unit tests (tokio)
#   test-smol      - Run unit tests (smol)
#   test-both      - Run unit tests on both backends
#   stress         - Run stress tests (requires release build, tokio)
#
#   clippy         - Run clippy (tokio, warnings = errors)
#   clippy-smol    - Run clippy (smol, warnings = errors)
#   clippy-both    - Run clippy on both backends
#   fmt            - Format all Rust source code
#   check          - Fast type check (tokio)
#   check-smol     - Fast type check (smol)
#   check-both     - Fast type check on both backends
#   gate           - Run pre-commit gate: fmt --check + test + clippy
#   doc            - Generate documentation
#   size           - Show release binary sizes
#
#   bench          - Run Go vs Rust-Tokio vs Rust-Smol benchmark
#   check-deps     - Check for unused dependencies (requires cargo-udeps)
#   targets        - List all supported build targets
#   install-cross  - Install cross-compilation toolchains (rustup)
#   clean          - Remove build artifacts
#   distclean      - Remove build artifacts + vendor directory

CARGO := cargo

# Determine default target
UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Use all available CPUs for build parallelism
NUM_JOBS := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

# Packages that need runtime feature selection (tokio / smol)
# kcp-rs, kcrypt-rs, qpp-rs are runtime-agnostic — built normally with workspace.
RT_PKGS := -p kcptun-client -p kcptun-server -p kio-rs -p smux-rs -p kpprof-rs

# ---------------------------------------------------------------------------
# Cross-compilation targets
# ---------------------------------------------------------------------------
# Auto-detect C cross-compilers (Linux + macOS).
# Prefer glibc (Debian/Ubuntu packages) when present; fall back to musl
# (filosottile/musl-cross on macOS, or musl-tools elsewhere).
#
# Override any of ARMV7_*/ARM64_* on the command line if needed, e.g.:
#   make release-armv7 ARMV7_TARGET=armv7-unknown-linux-musleabihf \
#                      ARMV7_CC=arm-linux-musleabihf-gcc ...
#
# which(1) is used so this works on both GNU coreutils and BSD/macOS.
which = $(shell command -v $(1) 2>/dev/null)

# ARMv7 (e.g. Raspberry Pi 2/3, OpenWrt, embedded Linux)
ifeq ($(origin ARMV7_TARGET),undefined)
  ifneq ($(call which,arm-linux-gnueabihf-gcc),)
    ARMV7_TARGET := armv7-unknown-linux-gnueabihf
    ARMV7_PREFIX := arm-linux-gnueabihf
  else ifneq ($(call which,arm-linux-musleabihf-gcc),)
    ARMV7_TARGET := armv7-unknown-linux-musleabihf
    ARMV7_PREFIX := arm-linux-musleabihf
  else
    # Default triple for docs / install-cross when no compiler is present yet.
    ARMV7_TARGET := armv7-unknown-linux-gnueabihf
    ARMV7_PREFIX := arm-linux-gnueabihf
  endif
endif
ARMV7_PREFIX ?= $(if $(findstring musl,$(ARMV7_TARGET)),arm-linux-musleabihf,arm-linux-gnueabihf)
ARMV7_LINKER ?= $(ARMV7_PREFIX)-gcc
ARMV7_AR     ?= $(ARMV7_PREFIX)-ar
ARMV7_CC     ?= $(ARMV7_PREFIX)-gcc
ARMV7_CXX    ?= $(ARMV7_PREFIX)-g++

# ARM64 (e.g. Raspberry Pi 4/5, Apple Silicon Linux VM, AWS Graviton)
ifeq ($(origin ARM64_TARGET),undefined)
  ifneq ($(call which,aarch64-linux-gnu-gcc),)
    ARM64_TARGET := aarch64-unknown-linux-gnu
    ARM64_PREFIX := aarch64-linux-gnu
  else ifneq ($(call which,aarch64-linux-musl-gcc),)
    ARM64_TARGET := aarch64-unknown-linux-musl
    ARM64_PREFIX := aarch64-linux-musl
  else
    ARM64_TARGET := aarch64-unknown-linux-gnu
    ARM64_PREFIX := aarch64-linux-gnu
  endif
endif
ARM64_PREFIX ?= $(if $(findstring musl,$(ARM64_TARGET)),aarch64-linux-musl,aarch64-linux-gnu)
ARM64_LINKER ?= $(ARM64_PREFIX)-gcc
ARM64_AR     ?= $(ARM64_PREFIX)-ar
ARM64_CC     ?= $(ARM64_PREFIX)-gcc
ARM64_CXX    ?= $(ARM64_PREFIX)-g++

# ---------------------------------------------------------------------------
# Linux x86_64 / aarch64 (musl) cross targets — for "make linux" from macOS
# Produces static binaries suitable for testing on most Linux distros.
# ---------------------------------------------------------------------------
# These use the musl targets so the resulting binary is fully static.
# macOS prerequisites (recommended):
#   brew install filosottile/musl-cross/musl-cross
# Then: make install-cross
ifeq ($(origin LINUX_X86_TARGET),undefined)
  ifneq ($(call which,x86_64-linux-musl-gcc),)
    LINUX_X86_TARGET := x86_64-unknown-linux-musl
    LINUX_X86_PREFIX := x86_64-linux-musl
  else
    LINUX_X86_TARGET := x86_64-unknown-linux-musl
    LINUX_X86_PREFIX := x86_64-linux-musl
  endif
endif
LINUX_X86_PREFIX ?= x86_64-linux-musl
LINUX_X86_LINKER ?= $(LINUX_X86_PREFIX)-gcc
LINUX_X86_AR     ?= $(LINUX_X86_PREFIX)-ar
LINUX_X86_CC     ?= $(LINUX_X86_PREFIX)-gcc
LINUX_X86_CXX    ?= $(LINUX_X86_PREFIX)-g++

ifeq ($(origin LINUX_AARCH64_TARGET),undefined)
  ifneq ($(call which,aarch64-linux-musl-gcc),)
    LINUX_AARCH64_TARGET := aarch64-unknown-linux-musl
    LINUX_AARCH64_PREFIX := aarch64-linux-musl
  else
    LINUX_AARCH64_TARGET := aarch64-unknown-linux-musl
    LINUX_AARCH64_PREFIX := aarch64-linux-musl
  endif
endif
LINUX_AARCH64_PREFIX ?= aarch64-linux-musl
LINUX_AARCH64_LINKER ?= $(LINUX_AARCH64_PREFIX)-gcc
LINUX_AARCH64_AR     ?= $(LINUX_AARCH64_PREFIX)-ar
LINUX_AARCH64_CC     ?= $(LINUX_AARCH64_PREFIX)-gcc
LINUX_AARCH64_CXX    ?= $(LINUX_AARCH64_PREFIX)-g++

# Environment variables for cross-compilation (avoids modifying .cargo/config.toml).
# 1) Shell assignment (VAR=value cmd) only allows [A-Za-z0-9_] in VAR, so
#    CC_armv7-unknown-... is parsed as a command name → "command not found".
#    Replace hyphens with underscores.
# 2) Cargo requires CARGO_TARGET_<TRIPLE>_LINKER with the triple UPPERCASED
#    (and hyphens → underscores), or the variable is ignored.
# 3) The cc crate accepts CC_/CXX_/AR_ with lowercased triple + underscores.
define armv7-env
CARGO_TARGET_$(shell echo $(subst -,_,$(ARMV7_TARGET)) | tr '[:lower:]' '[:upper:]')_LINKER=$(ARMV7_LINKER) \
CC_$(subst -,_,$(ARMV7_TARGET))=$(ARMV7_CC) \
CXX_$(subst -,_,$(ARMV7_TARGET))=$(ARMV7_CXX) \
AR_$(subst -,_,$(ARMV7_TARGET))=$(ARMV7_AR)
endef

define arm64-env
CARGO_TARGET_$(shell echo $(subst -,_,$(ARM64_TARGET)) | tr '[:lower:]' '[:upper:]')_LINKER=$(ARM64_LINKER) \
CC_$(subst -,_,$(ARM64_TARGET))=$(ARM64_CC) \
CXX_$(subst -,_,$(ARM64_TARGET))=$(ARM64_CXX) \
AR_$(subst -,_,$(ARM64_TARGET))=$(ARM64_AR)
endef

define linux-x86-env
CARGO_TARGET_$(shell echo $(subst -,_,$(LINUX_X86_TARGET)) | tr '[:lower:]' '[:upper:]')_LINKER=$(LINUX_X86_LINKER) \
CC_$(subst -,_,$(LINUX_X86_TARGET))=$(LINUX_X86_CC) \
CXX_$(subst -,_,$(LINUX_X86_TARGET))=$(LINUX_X86_CXX) \
AR_$(subst -,_,$(LINUX_X86_TARGET))=$(LINUX_X86_AR)
endef

define linux-aarch64-env
CARGO_TARGET_$(shell echo $(subst -,_,$(LINUX_AARCH64_TARGET)) | tr '[:lower:]' '[:upper:]')_LINKER=$(LINUX_AARCH64_LINKER) \
CC_$(subst -,_,$(LINUX_AARCH64_TARGET))=$(LINUX_AARCH64_CC) \
CXX_$(subst -,_,$(LINUX_AARCH64_TARGET))=$(LINUX_AARCH64_CXX) \
AR_$(subst -,_,$(LINUX_AARCH64_TARGET))=$(LINUX_AARCH64_AR)
endef

# Fail early with actionable install hints when the selected C compiler is missing.
define require-armv7-cc
	@if ! command -v $(ARMV7_CC) >/dev/null 2>&1; then \
		echo "error: C cross-compiler '$(ARMV7_CC)' not found for $(ARMV7_TARGET)"; \
		echo "  macOS (recommended): brew install filosottile/musl-cross/musl-cross"; \
		echo "  Debian/Ubuntu:       sudo apt install gcc-arm-linux-gnueabihf"; \
		echo "  Then:                make install-cross"; \
		exit 1; \
	fi
endef

define require-arm64-cc
	@if ! command -v $(ARM64_CC) >/dev/null 2>&1; then \
		echo "error: C cross-compiler '$(ARM64_CC)' not found for $(ARM64_TARGET)"; \
		echo "  macOS (recommended): brew install filosottile/musl-cross/musl-cross"; \
		echo "  Debian/Ubuntu:       sudo apt install gcc-aarch64-linux-gnu"; \
		echo "  Then:                make install-cross"; \
		exit 1; \
	fi
endef

define require-linux-x86-cc
	@if ! command -v $(LINUX_X86_CC) >/dev/null 2>&1; then \
		echo "error: C cross-compiler '$(LINUX_X86_CC)' not found for $(LINUX_X86_TARGET)"; \
		echo "  macOS (recommended): brew install filosottile/musl-cross/musl-cross"; \
		echo "  Then:                make install-cross"; \
		exit 1; \
	fi
endef

define require-linux-aarch64-cc
	@if ! command -v $(LINUX_AARCH64_CC) >/dev/null 2>&1; then \
		echo "error: C cross-compiler '$(LINUX_AARCH64_CC)' not found for $(LINUX_AARCH64_TARGET)"; \
		echo "  macOS (recommended): brew install filosottile/musl-cross/musl-cross"; \
		echo "  Then:                make install-cross"; \
		exit 1; \
	fi
endef

.PHONY: all vendor vendor-force \
	gate \        build build-smol build-tokio release release-smol release-tokio \
        build-armv7 build-armv7-tokio build-armv7-full release-armv7 release-armv7-tokio release-armv7-full \
        build-arm64 build-arm64-tokio build-arm64-full release-arm64 release-arm64-tokio release-arm64-full \
        linux linux-aarch64 linux-full linux-aarch64-full \
        test test-smol test-both stress e2e \
        clippy clippy-smol clippy-both fmt check check-smol check-both doc size \
        bench profile profile-mem profile-go profile-rust-go \
        targets install-cross clean distclean

all: build

# ---------------------------------------------------------------------------
# vendor — re-vendor third-party dependencies from Cargo.lock
# ---------------------------------------------------------------------------
vendor:
	@echo "==> Removing old vendor directory..."
	@cp -r vendor _vendor
	@rm -rf vendor
	@echo "==> Configuring cargo to use vendored sources..."
	@mkdir -p .cargo
	@printf '%s\n' \
		'# Automatically generated by `make vendor`. Source section is rewritten each run.' \
		'[source.crates-io]' 'replace-with = "vendored-sources"' '' \
		'[source.vendored-sources]' 'directory = "vendor"' '' \
		'# ARMv8 AES for RustCrypto `aes` 0.8 (Apple Silicon / aarch64 Linux).' \
		'# Without this cfg, AES uses soft fixslice even when the CPU has FEAT_AES.' \
		'[target.aarch64-apple-darwin]' \
		'rustflags = ["--cfg", "aes_armv8"]' '' \
		'[target.aarch64-unknown-linux-gnu]' \
		'rustflags = ["--cfg", "aes_armv8"]' \
		> .cargo/config.toml
	@echo "==> Vendoring dependencies (this may take a while)..."
	@cargo vendor
	@echo "==> Done. Vendored $$(ls vendor/ | wc -l | xargs) crates."

vendor-force:
	@echo "==> Removing old vendor directory..."
	@cp -r vendor _vendor
	@rm -rf vendor
	@mkdir -p .cargo
	@printf '%s\n' \
		'# Automatically generated by `make vendor`. Source section is rewritten each run.' \
		'[source.crates-io]' 'replace-with = "vendored-sources"' '' \
		'[source.vendored-sources]' 'directory = "vendor"' '' \
		'# ARMv8 AES for RustCrypto `aes` 0.8 (Apple Silicon / aarch64 Linux).' \
		'[target.aarch64-apple-darwin]' \
		'rustflags = ["--cfg", "aes_armv8"]' '' \
		'[target.aarch64-unknown-linux-gnu]' \
		'rustflags = ["--cfg", "aes_armv8"]' \
		> .cargo/config.toml
	@echo "==> Vendoring dependencies (this may take a while)..."
	@cargo vendor
	@echo "==> Done. Vendored $$(ls vendor/ | wc -l | xargs) crates."

# ---------------------------------------------------------------------------
# build / release (native)
# ---------------------------------------------------------------------------
# Native default: tokio. Use build-smol for smol backend.
build build-tokio:
	$(CARGO) build --workspace -j $(NUM_JOBS)

build-smol:
	$(CARGO) build $(RT_PKGS) --no-default-features --features smol -j $(NUM_JOBS) --target-dir target/smol

release release-tokio:
	$(CARGO) build --workspace --release -j $(NUM_JOBS)

release-smol:
	$(CARGO) build $(RT_PKGS) --no-default-features --features smol --release -j $(NUM_JOBS) --target-dir target/smol-release

# ---------------------------------------------------------------------------
# Cross-compilation: ARMv7
# ---------------------------------------------------------------------------
# ARM default: smol (lightweight). Use *-tokio targets for tokio backend.
# Auto-selects glibc (armv7-unknown-linux-gnueabihf) or musl
# (armv7-unknown-linux-musleabihf) based on which C compiler is on PATH.
#
# Release builds use opt-level=s (size-optimized) instead of opt-level=3
# to reduce binary size (~20% smaller text section). On ARMv7/embedded
# devices, the speed difference is negligible while size matters.
#
# Prerequisites:
#   make install-cross
#   macOS:  brew install filosottile/musl-cross/musl-cross
#   Debian: sudo apt install gcc-arm-linux-gnueabihf
# ---------------------------------------------------------------------------
build-armv7:
	$(require-armv7-cc)
	@echo "==> Cross-compiling for $(ARMV7_TARGET) via $(ARMV7_CC) (smol, debug)..."
	@$(armv7-env) $(CARGO) build $(RT_PKGS) --no-default-features --features smol --target $(ARMV7_TARGET) -j $(NUM_JOBS)
	@echo "==> Binaries at target/$(ARMV7_TARGET)/debug/{kcptun-client,kcptun-server}"

build-armv7-tokio:
	$(require-armv7-cc)
	@echo "==> Cross-compiling for $(ARMV7_TARGET) via $(ARMV7_CC) (tokio, debug)..."
	@$(armv7-env) $(CARGO) build --workspace --target $(ARMV7_TARGET) -j $(NUM_JOBS)
	@echo "==> Binaries at target/$(ARMV7_TARGET)/debug/{kcptun-client,kcptun-server}"

release-armv7:
	$(require-armv7-cc)
	@echo "==> Cross-compiling for $(ARMV7_TARGET) via $(ARMV7_CC) (smol, release, opt-level=s)..."
	@$(armv7-env) CARGO_PROFILE_RELEASE_OPT_LEVEL=s $(CARGO) build $(RT_PKGS) --no-default-features --features smol --release --target $(ARMV7_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(ARMV7_TARGET)/release/kcptun-client target/$(ARMV7_TARGET)/release/kcptun-server
	@echo "==> Binaries at target/$(ARMV7_TARGET)/release/{kcptun-client,kcptun-server}"

release-armv7-tokio:
	$(require-armv7-cc)
	@echo "==> Cross-compiling for $(ARMV7_TARGET) via $(ARMV7_CC) (tokio, release, opt-level=s)..."
	@$(armv7-env) CARGO_PROFILE_RELEASE_OPT_LEVEL=s $(CARGO) build --workspace --release --target $(ARMV7_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(ARMV7_TARGET)/release/kcptun-client target/$(ARMV7_TARGET)/release/kcptun-server
	@echo "==> Binaries at target/$(ARMV7_TARGET)/release/{kcptun-client,kcptun-server}"

# Full variants (smol + QPP) — for when Quantum Permutation Pad obfuscation is needed on ARM
build-armv7-full:
	$(require-armv7-cc)
	@echo "==> Cross-compiling for $(ARMV7_TARGET) via $(ARMV7_CC) (smol + qpp, debug)..."
	@$(armv7-env) $(CARGO) build $(RT_PKGS) --no-default-features --features "smol,qpp" --target $(ARMV7_TARGET) -j $(NUM_JOBS)
	@echo "==> Binaries at target/$(ARMV7_TARGET)/debug/{kcptun-client,kcptun-server}"

release-armv7-full:
	$(require-armv7-cc)
	@echo "==> Cross-compiling for $(ARMV7_TARGET) via $(ARMV7_CC) (smol + qpp, release, opt-level=s)..."
	@$(armv7-env) CARGO_PROFILE_RELEASE_OPT_LEVEL=s $(CARGO) build $(RT_PKGS) --no-default-features --features "smol,qpp" --release --target $(ARMV7_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(ARMV7_TARGET)/release/kcptun-client target/$(ARMV7_TARGET)/release/kcptun-server
	@echo "==> Binaries at target/$(ARMV7_TARGET)/release/{kcptun-client,kcptun-server}"

# ---------------------------------------------------------------------------
# Cross-compilation: ARM64
# ---------------------------------------------------------------------------
# ARM default: smol (lightweight). Use *-tokio targets for tokio backend.
# Auto-selects glibc (aarch64-unknown-linux-gnu) or musl
# (aarch64-unknown-linux-musl) based on which C compiler is on PATH.
#
# Prerequisites:
#   make install-cross
#   macOS:  brew install filosottile/musl-cross/musl-cross
#   Debian: sudo apt install gcc-aarch64-linux-gnu
# ---------------------------------------------------------------------------
build-arm64:
	$(require-arm64-cc)
	@echo "==> Cross-compiling for $(ARM64_TARGET) via $(ARM64_CC) (smol, debug)..."
	@$(arm64-env) $(CARGO) build $(RT_PKGS) --no-default-features --features smol --target $(ARM64_TARGET) -j $(NUM_JOBS)
	@echo "==> Binaries at target/$(ARM64_TARGET)/debug/{kcptun-client,kcptun-server}"

build-arm64-tokio:
	$(require-arm64-cc)
	@echo "==> Cross-compiling for $(ARM64_TARGET) via $(ARM64_CC) (tokio, debug)..."
	@$(arm64-env) $(CARGO) build --workspace --target $(ARM64_TARGET) -j $(NUM_JOBS)
	@echo "==> Binaries at target/$(ARM64_TARGET)/debug/{kcptun-client,kcptun-server}"

release-arm64:
	$(require-arm64-cc)
	@echo "==> Cross-compiling for $(ARM64_TARGET) via $(ARM64_CC) (smol, release, opt-level=s)..."
	@$(arm64-env) CARGO_PROFILE_RELEASE_OPT_LEVEL=s $(CARGO) build $(RT_PKGS) --no-default-features --features smol --release --target $(ARM64_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(ARM64_TARGET)/release/kcptun-client target/$(ARM64_TARGET)/release/kcptun-server
	@echo "==> Binaries at target/$(ARM64_TARGET)/release/{kcptun-client,kcptun-server}"

release-arm64-tokio:
	$(require-arm64-cc)
	@echo "==> Cross-compiling for $(ARM64_TARGET) via $(ARM64_CC) (tokio, release, opt-level=s)..."
	@$(arm64-env) CARGO_PROFILE_RELEASE_OPT_LEVEL=s $(CARGO) build --workspace --release --target $(ARM64_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(ARM64_TARGET)/release/kcptun-client target/$(ARM64_TARGET)/release/kcptun-server
	@echo "==> Binaries at target/$(ARM64_TARGET)/release/{kcptun-client,kcptun-server}"

# Full variants (smol + QPP) — for when Quantum Permutation Pad obfuscation is needed on ARM
build-arm64-full:
	$(require-arm64-cc)
	@echo "==> Cross-compiling for $(ARM64_TARGET) via $(ARM64_CC) (smol + qpp, debug)..."
	@$(arm64-env) $(CARGO) build $(RT_PKGS) --no-default-features --features "smol,qpp" --target $(ARM64_TARGET) -j $(NUM_JOBS)
	@echo "==> Binaries at target/$(ARM64_TARGET)/debug/{kcptun-client,kcptun-server}"

release-arm64-full:
	$(require-arm64-cc)
	@echo "==> Cross-compiling for $(ARM64_TARGET) via $(ARM64_CC) (smol + qpp, release, opt-level=s)..."
	@$(arm64-env) CARGO_PROFILE_RELEASE_OPT_LEVEL=s $(CARGO) build $(RT_PKGS) --no-default-features --features "smol,qpp" --release --target $(ARM64_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(ARM64_TARGET)/release/kcptun-client target/$(ARM64_TARGET)/release/kcptun-server
	@echo "==> Binaries at target/$(ARM64_TARGET)/release/{kcptun-client,kcptun-server}"

# ---------------------------------------------------------------------------
# linux — build Linux x86_64 (musl) release for testing from macOS
# ---------------------------------------------------------------------------
linux:
	$(require-linux-x86-cc)
	@echo "==> Cross-compiling for $(LINUX_X86_TARGET) via $(LINUX_X86_CC) (smol, release)..."
	@$(linux-x86-env) $(CARGO) build $(RT_PKGS) --no-default-features --features smol --release --target $(LINUX_X86_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(LINUX_X86_TARGET)/release/kcptun-client target/$(LINUX_X86_TARGET)/release/kcptun-server || true
	@echo "==> Linux x86_64 binaries at target/$(LINUX_X86_TARGET)/release/{kcptun-client,kcptun-server}"

linux-aarch64:
	$(require-linux-aarch64-cc)
	@echo "==> Cross-compiling for $(LINUX_AARCH64_TARGET) via $(LINUX_AARCH64_CC) (smol, release)..."
	@$(linux-aarch64-env) $(CARGO) build $(RT_PKGS) --no-default-features --features smol --release --target $(LINUX_AARCH64_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(LINUX_AARCH64_TARGET)/release/kcptun-client target/$(LINUX_AARCH64_TARGET)/release/kcptun-server || true
	@echo "==> Linux aarch64 binaries at target/$(LINUX_AARCH64_TARGET)/release/{kcptun-client,kcptun-server}"

# Full (with QPP)
linux-full:
	$(require-linux-x86-cc)
	@echo "==> Cross-compiling for $(LINUX_X86_TARGET) via $(LINUX_X86_CC) (smol + qpp, release)..."
	@$(linux-x86-env) $(CARGO) build $(RT_PKGS) --no-default-features --features "smol,qpp" --release --target $(LINUX_X86_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(LINUX_X86_TARGET)/release/kcptun-client target/$(LINUX_X86_TARGET)/release/kcptun-server || true
	@echo "==> Linux x86_64 (+QPP) at target/$(LINUX_X86_TARGET)/release/{kcptun-client,kcptun-server}"

linux-aarch64-full:
	$(require-linux-aarch64-cc)
	@echo "==> Cross-compiling for $(LINUX_AARCH64_TARGET) via $(LINUX_AARCH64_CC) (smol + qpp, release)..."
	@$(linux-aarch64-env) $(CARGO) build $(RT_PKGS) --no-default-features --features "smol,qpp" --release --target $(LINUX_AARCH64_TARGET) -j $(NUM_JOBS)
	@ls -lh target/$(LINUX_AARCH64_TARGET)/release/kcptun-client target/$(LINUX_AARCH64_TARGET)/release/kcptun-server || true
	@echo "==> Linux aarch64 (+QPP) at target/$(LINUX_AARCH64_TARGET)/release/{kcptun-client,kcptun-server}"

# ---------------------------------------------------------------------------
# install-cross — install cross-compilation Rust toolchains via rustup
# ---------------------------------------------------------------------------
# Installs both glibc and musl triples so either auto-detected C toolchain works.
install-cross:
	@echo "==> Installing cross-compilation targets via rustup..."
	@rustup target add armv7-unknown-linux-gnueabihf armv7-unknown-linux-musleabihf
	@rustup target add aarch64-unknown-linux-gnu aarch64-unknown-linux-musl
	@rustup target add x86_64-unknown-linux-musl aarch64-unknown-linux-musl
	@echo "==> Done. Install a C cross-compiler (Makefile auto-detects which is present):"
	@echo "    macOS:  brew install filosottile/musl-cross/musl-cross"
	@echo "    Debian: sudo apt install gcc-arm-linux-gnueabihf gcc-aarch64-linux-gnu"
	@echo "  Detected now: ARMv7=$(ARMV7_TARGET) via $(ARMV7_CC); ARM64=$(ARM64_TARGET) via $(ARM64_CC)"

# ---------------------------------------------------------------------------
# targets — list all supported build targets
# ---------------------------------------------------------------------------
targets:
	@echo "kcptun-rs build targets:"
	@echo ""
	@echo "  Native (default: tokio):"
	@echo "    make build              — debug build (tokio)"
	@echo "    make build-smol         — debug build (smol)"
	@echo "    make release            — release build (tokio, LTO, stripped)"
	@echo "    make release-smol       — release build (smol, LTO, stripped)"
	@echo ""
	@echo "  ARMv7 (default: smol; auto-detects glibc or musl C toolchain):"
	@echo "    make build-armv7        — debug build (smol)"
	@echo "    make build-armv7-tokio  — debug build (tokio)"
	@echo "    make build-armv7-full   — debug build (smol + qpp)"
	@echo "    make release-armv7      — release build (smol)"
	@echo "    make release-armv7-tokio — release build (tokio)"
	@echo "    make release-armv7-full — release build (smol + qpp)"
	@echo "    currently: $(ARMV7_TARGET) via $(ARMV7_CC)"
	@echo ""
	@echo "  ARM64 (default: smol; auto-detects glibc or musl C toolchain):"
	@echo "    make build-arm64        — debug build (smol)"
	@echo "    make build-arm64-tokio  — debug build (tokio)"
	@echo "    make build-arm64-full   — debug build (smol + qpp)"
	@echo "    make release-arm64      — release build (smol)"
	@echo "    make release-arm64-tokio — release build (tokio)"
	@echo "    make release-arm64-full — release build (smol + qpp)"
	@echo "    currently: $(ARM64_TARGET) via $(ARM64_CC)"
	@echo ""
	@echo "  Linux (x86_64/aarch64 musl, for testing from macOS):"
	@echo "    make linux              — Linux x86_64 (smol, musl)"
	@echo "    make linux-aarch64      — Linux aarch64 (smol, musl)"
	@echo "    make linux-full         — Linux x86_64 (smol + qpp, musl)"
	@echo "    make linux-aarch64-full — Linux aarch64 (smol + qpp, musl)"
	@echo "    (requires musl cross; see: make install-cross)"
	@echo ""
	@echo "  Testing & linting:"
	@echo "    make test               — unit tests (tokio)"
	@echo "    make test-smol          — unit tests (smol)"
	@echo "    make test-both          — unit tests (both backends)"
	@echo "    make stress             — stress tests (tokio, release)"
	@echo "    make e2e                — Go↔Rust e2e interop (tokio + smol)"
	@echo "    make clippy             — clippy (tokio)"
	@echo "    make clippy-smol        — clippy (smol)"
	@echo "    make clippy-both        — clippy (both backends)"
	@echo "    make bench              — Go vs Rust-Tokio vs Rust-Smol"
	@echo ""
	@echo "  Profiling (Go pprof compatible):"
	@echo "    make profiling-bins     — build profiling bins + pprof (CPU+heap+allocs+...)"
	@echo "    make profile            — capture Rust CPU profile → Go protobuf (pprof)"
	@echo "    make profile-mem        — capture Rust heap + allocs only (no long CPU sample)"
	@echo "    make profile-rust-go    — alias for make profile"
	@echo "    make profile-go         — capture Go kcptun CPU profile (for comparison)"
	@echo "    After start with --pprof ADDR:"
	@echo "      curl http://ADDR/debug/pprof/heap   # inuse heap (Go pprof)"
	@echo "      curl http://ADDR/debug/pprof/allocs # cumulative allocs (Go pprof)"
	@echo "      go tool pprof -http=:0 http://ADDR/debug/pprof/profile?seconds=30"
	@echo "      go tool pprof -http=:0 http://ADDR/debug/pprof/heap"
	@echo ""
	@echo "  Prerequisites for cross-compilation:"
	@echo "    1. make install-cross   (installs rustup glibc + musl targets)"
	@echo "    2. Install a C cross-compiler (auto-detected):"
	@echo "       macOS:  brew install filosottile/musl-cross/musl-cross"
	@echo "       Debian: sudo apt install gcc-arm-linux-gnueabihf gcc-aarch64-linux-gnu"

# ---------------------------------------------------------------------------
# test / stress / clippy / fmt
# ---------------------------------------------------------------------------
test:
	ulimit -n 65536 2>/dev/null; $(CARGO) test --workspace -j $(NUM_JOBS) -- --test-threads=2

test-smol:
	ulimit -n 65536 2>/dev/null; $(CARGO) test $(RT_PKGS) --no-default-features --features smol --target-dir target/smol-test -- --test-threads=2

test-both: test test-smol

# Stress tests — data-integrity + concurrency, requires release build
stress:
	ulimit -n 65536 2>/dev/null; $(CARGO) test --release -p kcptun-server --test stress_test -- --nocapture --test-threads=1

clippy:
	$(CARGO) clippy --workspace -- -D warnings

clippy-smol:
	$(CARGO) clippy $(RT_PKGS) --no-default-features --features smol --target-dir target/smol-clippy -- -D warnings

clippy-both: clippy clippy-smol

# e2e — Go↔Rust (tokio + smol) end-to-end interoperability tests
# Requires Go kcptun binaries in tests/kcptun-go/
e2e: release release-smol
	@bash test_e2e.sh

fmt:
	$(CARGO) fmt --all

# Check for unused dependencies (requires: cargo install cargo-udeps)
check-deps:
	$(CARGO) udeps --workspace
# ---------------------------------------------------------------------------
# check — fast type check (no codegen)
# ---------------------------------------------------------------------------
check:
	$(CARGO) check --workspace

check-smol:
	$(CARGO) check $(RT_PKGS) --no-default-features --features smol

check-both: check check-smol

# ---------------------------------------------------------------------------
# gate — pre-commit quality gate (fmt --check + test + clippy)
# ---------------------------------------------------------------------------
gate:
	@echo "==> cargo fmt --all -- --check"
	@$(CARGO) fmt --all -- --check
	@echo "==> cargo test --workspace"
	@$(CARGO) test --workspace
	@echo "==> cargo clippy --workspace -- -D warnings"
	@$(CARGO) clippy --workspace -- -D warnings
	@echo "✅ All gates passed"

# ---------------------------------------------------------------------------
# doc — generate documentation
# ---------------------------------------------------------------------------
doc:
	$(CARGO) doc --workspace --no-deps

# ---------------------------------------------------------------------------
# size — show release binary sizes (human readable)
# ---------------------------------------------------------------------------
size: release release-smol
	@echo "=== Native release (tokio) ==="
	@ls -lh target/release/kcptun-{client,server} 2>/dev/null || echo "(not built)"
	@echo "=== Smol release ==="
	@ls -lh target/smol-release/kcptun-{client,server} 2>/dev/null || echo "(not built)"
	@echo "=== ARM (if built) ==="
	@find target -name 'kcptun-*' -path '*release*' -not -name '*.d' -exec ls -lh {} + 2>/dev/null | head -20 || true


# ---------------------------------------------------------------------------
# bench — Go vs Rust-Tokio vs Rust-Smol performance comparison
# ---------------------------------------------------------------------------
bench: release release-smol
	@bash bench/run_bench.sh

# Go pprof profiling — see bench/PROFILE_RUNBOOK.md
# Rust: cargo --profile profiling --features pprof (readable symbols). SKIP_PROFILE_REBUILD=1 reuses bins.
profile:
	@bash bench/profile_rust_go_pprof.sh

profile-mem:
	@bash bench/profile_rust_go_pprof.sh mem

profiling-bins:
	@extra="-C force-frame-pointers=yes"; \
	case "$$(uname -m)" in arm64|aarch64) extra="--cfg aes_armv8 $$extra" ;; esac; \
	RUSTFLAGS="$${RUSTFLAGS:+$$RUSTFLAGS }$$extra" \
		$(CARGO) build --profile profiling --features pprof -p kcptun-server -p kcptun-client -j $(NUM_JOBS)
	@echo "Binaries: target/profiling/kcptun-{client,server}  (pprof enabled: /debug/pprof/{profile,heap,allocs,...) "

# Go kcptun: net/http/pprof + go tool pprof (official Go toolchain flame graph)
profile-go:
	@bash bench/profile_go_pprof.sh

# Rust CPU profile as Go pprof protobuf (analyze with go tool pprof)
profile-rust-go:
	@bash bench/profile_rust_go_pprof.sh


# ---------------------------------------------------------------------------
# clean / distclean
# ---------------------------------------------------------------------------
clean:
	$(CARGO) clean

distclean: clean
	rm -rf vendor/
