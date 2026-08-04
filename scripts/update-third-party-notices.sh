#!/usr/bin/env bash
set -euo pipefail

cargo_about="${CARGO_ABOUT:-cargo-about}"
output_json="${TMPDIR:-/tmp}/minisqlite-cargo-about.json"

"$cargo_about" generate \
  --config about.toml \
  --format json \
  --locked \
  --workspace \
  --output-file "$output_json"

python3 scripts/generate-third-party-notices.py \
  "$output_json" \
  THIRD_PARTY_NOTICES.md
