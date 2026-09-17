#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERUS_VERSION="0.2026.09.13.671956e"
BASE_URL="https://github.com/verus-lang/verus/releases/download/release/${VERUS_VERSION}"

case "$(uname -s):$(uname -m)" in
	Linux:x86_64)
		ASSET="verus-${VERUS_VERSION}-x86-linux.zip"
		SHA256="08c85b96e0fbbbdcb1b3a9fa8943dce7598c3e7b343680820f649b4e1efda4b4"
		;;
	Darwin:x86_64)
		ASSET="verus-${VERUS_VERSION}-x86-macos.zip"
		SHA256="cbb1e7f933264c152c165e560bc27c49c821d031288a198643fc7a857baf0595"
		;;
	Darwin:arm64)
		ASSET="verus-${VERUS_VERSION}-arm64-macos.zip"
		SHA256="ab2293a469edfa8a1a28ba4a5bc9aa61205d15930f32d007444e6c848540b534"
		;;
	*)
		echo "install-verus: unsupported host $(uname -s)/$(uname -m)" >&2
		exit 1
		;;
esac

DEST="$ROOT/.cache/verus/$VERUS_VERSION"
if [ -x "$DEST/verus" ] && [ -x "$DEST/cargo-verus" ]; then
	printf '%s\n' "$DEST"
	exit 0
fi

command -v curl >/dev/null 2>&1 || { echo "install-verus: curl is required" >&2; exit 1; }
command -v unzip >/dev/null 2>&1 || { echo "install-verus: unzip is required" >&2; exit 1; }

TMP_ROOT="$(mktemp -d)"
trap 'rm -rf "$TMP_ROOT"' EXIT
ARCHIVE="$TMP_ROOT/$ASSET"
curl --fail --location --retry 3 --silent --show-error \
	-o "$ARCHIVE" "$BASE_URL/$ASSET"

if command -v sha256sum >/dev/null 2>&1; then
	printf '%s  %s\n' "$SHA256" "$ARCHIVE" | sha256sum -c - >&2
else
	printf '%s  %s\n' "$SHA256" "$ARCHIVE" | shasum -a 256 -c - >&2
fi

unzip -q "$ARCHIVE" -d "$TMP_ROOT/unpacked"
SOURCE_DIR="$(find "$TMP_ROOT/unpacked" -mindepth 1 -maxdepth 1 -type d -print -quit)"
[ -n "$SOURCE_DIR" ] || { echo "install-verus: archive had no root directory" >&2; exit 1; }

mkdir -p "$(dirname "$DEST")"
rm -rf "$DEST"
mv "$SOURCE_DIR" "$DEST"

printf '%s\n' "$DEST"
