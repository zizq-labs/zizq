#!/usr/bin/env bash

# Discover compatible client versions for integration testing.
#
# Usage:
#   discover-client-versions.sh <language> <server_version>
#
# Emits a JSON array to stdout suitable for a GitHub Actions matrix `include`.
# Each entry has:
#   label:   human label (e.g. "0.1.x", "main")
#   ref:     git ref to check out (tag name or "main")
#   tarball: release asset filename (empty when ref="main" — build from source)
#
# Compatibility rules:
#   - same major as server
#   - client minor <= server minor
#   - patch is ignored; we pick the highest patch per minor
#   - "main" is included if its package/gem version satisfies the same rules

set -euo pipefail

language="${1:?language required (node|ruby|rust|elixir)}"
server_version="${2:?server version required}"

case "$language" in
  node)
    repo="zizq-labs/zizq-node"
    version_path="package.json"
    asset_prefix="zizq-"
    asset_suffix=".tgz"
    extract_main_version() {
      gh api "repos/${repo}/contents/${version_path}?ref=main" --jq '.content' \
        | base64 -d \
        | jq -r '.version'
    }
    ;;
  ruby)
    repo="zizq-labs/zizq-ruby"
    version_path="lib/zizq/version.rb"
    asset_prefix="zizq-"
    asset_suffix=".gem"
    extract_main_version() {
      gh api "repos/${repo}/contents/${version_path}?ref=main" --jq '.content' \
        | base64 -d \
        | grep -oE 'VERSION\s*=\s*"[^"]+"' \
        | head -n1 \
        | sed -E 's/.*"([^"]+)".*/\1/'
    }
    ;;
  rust)
    repo="zizq-labs/zizq-rust"
    version_path="Cargo.toml"
    asset_prefix="zizq-"
    asset_suffix=".crate"
    # `^version` anchors on package-level keys; inline-table deps like
    # `tokio = { version = "1" }` don't match.
    extract_main_version() {
      gh api "repos/${repo}/contents/${version_path}?ref=main" --jq '.content' \
        | base64 -d \
        | grep -oE '^version\s*=\s*"[^"]+"' \
        | head -n1 \
        | sed -E 's/.*"([^"]+)".*/\1/'
    }
    ;;
  elixir)
    repo="zizq-labs/zizq-elixir"
    version_path="mix.exs"
    asset_prefix="zizq-"
    asset_suffix=".tar"
    # `@version "x.y.z"` is the single source of truth in mix.exs —
    # `release.sh` and `bump-version.sh` read the same line.
    extract_main_version() {
      gh api "repos/${repo}/contents/${version_path}?ref=main" --jq '.content' \
        | base64 -d \
        | sed -n 's/^[[:space:]]*@version "\(.*\)"/\1/p' \
        | head -n1
    }
    ;;
  *)
    echo "unknown language: $language" >&2
    exit 1
    ;;
esac

server_major="${server_version%%.*}"
rest="${server_version#*.}"
server_minor="${rest%%.*}"

# Query the client repo's releases. We filter by:
#   - tag name matches v<major>.<minor>.<patch>
#   - same major as the server
#   - minor <= server minor
#
# The tag pattern is anchored at BOTH ends deliberately: that is what
# keeps pre-releases (`v0.6.0-alpha.9`, `v1.0.0-rc.1`) out of the
# matrix. Unstable client builds must not be able to fail this repo's
# CI. Do not relax the anchors — nothing else excludes them, since the
# clients publish pre-releases as ordinary GitHub releases rather than
# marking them `--prerelease`, so `isPrerelease` would not help.
#
# Then split each version into numeric components (so v0.20.1 > v0.3.1),
# sort by those, group by [major, minor], and take the last element of
# each group — the highest patch per minor.
releases=$(gh release list --repo "$repo" --limit 200 --json tagName \
  | jq -c \
      --argjson sm "$server_major" \
      --argjson sn "$server_minor" \
      --arg asset_prefix "$asset_prefix" \
      --arg asset_suffix "$asset_suffix" \
      '
      [
        .[]
        | .tagName
        | select(test("^v[0-9]+\\.[0-9]+\\.[0-9]+$"))
        | . as $tag
        | {v: (ltrimstr("v") | split(".") | map(tonumber)), tag: $tag}
        | select(.v[0] == $sm and .v[1] <= $sn)
      ]
      | sort_by(.v)
      | group_by(.v[0:2])
      | map(.[-1])
      | map({
          label: "\(.v[0]).\(.v[1]).x",
          ref: .tag,
          tarball: "\($asset_prefix)\(.v | join("."))\($asset_suffix)"
        })
      ')

# Include main if its version is still compatible.
#
# Unlike the tag pattern above, this regex is intentionally NOT
# anchored at the end, so a pre-release version on main still matches
# and `main` is still tested. That is the point: during an alpha cycle
# the client's head carries a version like `0.6.0-alpha.9` for months,
# and it is exactly then that we most want to know the server has not
# broken it. Only the major/minor decide compatibility.
main_version=$(extract_main_version 2>/dev/null || true)
if [[ "$main_version" =~ ^([0-9]+)\.([0-9]+)\.[0-9]+ ]]; then
  main_major="${BASH_REMATCH[1]}"
  main_minor="${BASH_REMATCH[2]}"
  if [[ "$main_major" == "$server_major" ]] && (( main_minor <= server_minor )); then
    releases=$(jq -c '. + [{label:"main", ref:"main", tarball:""}]' <<< "$releases")
  fi
fi

echo "$releases"
