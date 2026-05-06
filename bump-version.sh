#!/usr/bin/env bash
set -euo pipefail

# Bump the project version.
#
# Usage:
#   ./bump-version.sh           # increment patch (e.g. 0.1.1 -> 0.1.2)
#   ./bump-version.sh 0.2.0     # set an explicit version
#
# Updates:
#   - Cargo.toml (package version)
#   - Cargo.lock (via cargo update)
#   - CHANGELOG.md (adds a new section header)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Read current version from Cargo.toml.
CURRENT=$(grep '^version' Cargo.toml | head -1 | sed 's/.*"\(.*\)"/\1/')

if [ -z "$CURRENT" ]; then
    echo "Error: could not read current version from Cargo.toml"
    exit 1
fi

if [ $# -ge 1 ]; then
    NEW="$1"
else
    # Increment patch version.
    IFS='.' read -r MAJOR MINOR PATCH <<< "$CURRENT"
    PATCH=$((PATCH + 1))
    NEW="${MAJOR}.${MINOR}.${PATCH}"
fi

if [ "$NEW" = "$CURRENT" ]; then
    echo "Already at version ${CURRENT}."
    exit 0
fi

echo "Bumping version: ${CURRENT} -> ${NEW}"

# Update Cargo.toml.
sed -i "0,/^version = \"${CURRENT}\"/s//version = \"${NEW}\"/" Cargo.toml
echo "  Updated Cargo.toml"

# Update Cargo.lock.
# If cargo update fails, do a full lockfile regeration.
cargo update -p zizq --quiet 2>/dev/null || cargo generate-lockfile --quiet 2>/dev/null || true
echo "  Updated Cargo.lock"

# Update docs.
for doc in docs/getting-started/src/quick-start.md docs/cli/src/installation.md README.md
do
    sed -i "s/Zizq ${CURRENT}/Zizq ${NEW}/" $doc
    sed -i "s/\\/v${CURRENT}\\//\\/v${NEW}\\//" $doc
    sed -i "s/zizq-${CURRENT}/zizq-${NEW}/" $doc
done

# Add new CHANGELOG section if it doesn't already exist.
if ! grep -q "^## ${NEW}" CHANGELOG.md 2>/dev/null; then
    sed -i "0,/^## /s//## ${NEW}\n\n\n## /" CHANGELOG.md
    echo "  Added CHANGELOG.md section for ${NEW}"
fi

echo "Done. Version is now ${NEW}."
echo ""
echo "Next steps:"
echo "  1. Edit CHANGELOG.md with release notes"
echo "  2. Commit: git add -A && git commit -m \"Bump version to ${NEW}\""
