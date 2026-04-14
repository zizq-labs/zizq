#!/usr/bin/env bash

# Build a release binary for the Zizq CLI for a single target.
#
# Produces:
#   target/release/zizq-<version>-<platform>.tar.gz        (unix)
#   target/release/zizq-<version>-<platform>.tar.gz.sha256
#   target/release/zizq-<version>-<platform>.zip           (windows)
#   target/release/zizq-<version>-<platform>.zip.sha256
#
# Usage:
#   ./release.sh --target aarch64-unknown-linux-musl
#   ./release.sh --target x86_64-unknown-linux-musl
#   ./release.sh --target x86_64-pc-windows-gnu
#   ./release.sh --target aarch64-apple-darwin
#   ./release.sh --target x86_64-apple-darwin
#   ./release.sh --check --target <target>   # run tests first
#
# Supported targets:
#   aarch64-unknown-linux-musl   (Linux ARM64, static)
#   x86_64-unknown-linux-musl    (Linux x86_64, static)
#   x86_64-pc-windows-gnu        (Windows x86_64)
#   aarch64-apple-darwin          (macOS Apple Silicon)
#   x86_64-apple-darwin           (macOS Intel)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# --- Parse args ---

CHECK=false
TARGET=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --check) CHECK=true; shift ;;
        --target) TARGET="$2"; shift 2 ;;
        *) echo "Unknown arg: $1"; exit 1 ;;
    esac
done

if [[ -z "$TARGET" ]]; then
    echo "Usage: ./release.sh --target <triple>"
    echo ""
    echo "Supported targets:"
    echo "  aarch64-unknown-linux-musl"
    echo "  x86_64-unknown-linux-musl"
    echo "  x86_64-pc-windows-gnu"
    echo "  aarch64-apple-darwin"
    echo "  x86_64-apple-darwin"
    exit 1
fi

# --- Read version ---

VERSION="$(cargo pkgid | sed 's/.*[#@]//')"
HOST_OS="$(uname -s)"

echo "==> Zizq CLI v${VERSION} (target: ${TARGET})"

# --- Validate target is buildable on this host ---

case "$TARGET" in
    *-apple-darwin)
        if [[ "$HOST_OS" != "Darwin" ]]; then
            echo "Error: macOS targets can only be built on macOS (Apple SDK requirement)."
            exit 1
        fi
        ;;
esac

# --- Determine linker, binary extension, and friendly platform name ---

LINKER=""
EXT=""
PLATFORM=""

HOST_ARCH="$(uname -m)"

case "$TARGET" in
    aarch64-unknown-linux-musl)
        PLATFORM="linux-arm64"
        if [[ "$HOST_OS" == "Linux" && "$HOST_ARCH" == "aarch64" ]]; then
            # Native target, no cross-linker needed, just musl-gcc.
            LINKER="musl-gcc"
        else
            LINKER="aarch64-linux-musl-gcc"
        fi
        ;;
    x86_64-unknown-linux-musl)
        PLATFORM="linux-x86_64"
        if [[ "$HOST_OS" == "Linux" && "$HOST_ARCH" == "x86_64" ]]; then
            # Native target, no cross-linker needed, just musl-gcc.
            LINKER="musl-gcc"
        else
            LINKER="x86_64-linux-musl-gcc"
        fi
        ;;
    x86_64-pc-windows-gnu)
        PLATFORM="windows-x86_64"
        LINKER="x86_64-w64-mingw32-gcc"
        EXT=".exe"
        ;;
    aarch64-apple-darwin)
        # Cross compilation not supported
        PLATFORM="macos-arm64"
        ;;
    x86_64-apple-darwin)
        # Cross compilation not supported
        PLATFORM="macos-x86_64"
        ;;
    *)
        echo "Error: unsupported target '${TARGET}'."
        exit 1
        ;;
esac

# --- Check that the linker is available ---

if [[ -n "$LINKER" ]]; then
    if ! command -v "$LINKER" &>/dev/null; then
        echo "Error: cross-linker '${LINKER}' not found."
        echo ""
        case "$TARGET" in
            *-linux-musl)
                echo "Install the musl cross-compilation toolchain:"
                echo ""
                if [[ "$HOST_OS" == "Darwin" ]]; then
                    # Only relevant for local development/testing. CI builds
                    # Linux releases on linux agents.
                    echo "  brew install filosottile/musl-cross/musl-cross"
                else
                    echo "  # On Debian/Ubuntu (native arch):"
                    echo "  sudo apt install musl-tools"
                    echo ""
                    echo "  # For cross-arch musl, build musl-cross-make:"
                    echo "  # https://github.com/richfelker/musl-cross-make"
                fi
                ;;
            *-windows-gnu)
                echo "Install the MinGW-w64 toolchain:"
                echo ""
                if [[ "$HOST_OS" == "Darwin" ]]; then
                    # Only relevant for local development/testing. CI builds
                    # Windows releases on linux agents.
                    echo "  brew install mingw-w64"
                else
                    echo "  sudo apt install gcc-mingw-w64-x86-64"
                fi
                ;;
        esac
        exit 1
    fi
fi

# --- Ensure Rust target is installed ---

if ! rustup target list --installed | grep -q "^${TARGET}$"; then
    echo "    Installing Rust target ${TARGET}..."
    rustup target add "$TARGET"
fi

# --- Optional pre-flight checks ---

if $CHECK; then
    echo "    Running tests..."
    cargo test
fi

# --- Configure the linker for cross-compilation ---

export CARGO_TARGET_DIR="${SCRIPT_DIR}/target"

if [[ -n "$LINKER" ]]; then
    # Convert target triple to the env var format Cargo expects:
    # aarch64-unknown-linux-musl → CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER
    TARGET_ENV="$(echo "$TARGET" | tr '[:lower:]-' '[:upper:]_')"
    export "CARGO_TARGET_${TARGET_ENV}_LINKER=${LINKER}"
fi

# --- Build ---

echo "    Compiling (release)..."
cargo build --release --target "$TARGET" --bin zizq

# --- Package ---

CARGO_BIN="target/${TARGET}/release/zizq${EXT}"
OUT_DIR="target/release"
mkdir -p "$OUT_DIR"

if [[ "$TARGET" == *-windows-* ]]; then
    # Windows: zip (preserves nothing special, but it's what Windows users expect).
    ARCHIVE="zizq-${VERSION}-${PLATFORM}.zip"
    echo "    Packaging ${ARCHIVE}..."
    zip -j "${OUT_DIR}/${ARCHIVE}" "$CARGO_BIN"
else
    # Unix: tar.gz preserves the executable permission bit.
    ARCHIVE="zizq-${VERSION}-${PLATFORM}.tar.gz"
    echo "    Packaging ${ARCHIVE}..."
    tar -czf "${OUT_DIR}/${ARCHIVE}" -C "$(dirname "$CARGO_BIN")" "$(basename "$CARGO_BIN")"
fi

# --- Checksum ---

echo "    Computing checksum..."
(cd "$OUT_DIR" && shasum -a 256 "$ARCHIVE" > "${ARCHIVE}.sha256")

echo "==> Done."
echo "    ${OUT_DIR}/${ARCHIVE}"
echo "    ${OUT_DIR}/${ARCHIVE}.sha256"
