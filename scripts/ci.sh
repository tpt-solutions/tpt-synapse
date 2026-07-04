#!/usr/bin/env bash
# Build + test both toolchains, then check TODO.md for drift.
# Not wired to GitHub Actions by design; call this from whatever CI runner
# (or git hook — see .githooks/pre-push) is in use.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

echo "== cargo build --workspace =="
cargo build --workspace

echo "== cargo test --workspace =="
cargo test --workspace

echo "== go build ./... =="
go build ./...

echo "== go vet ./... =="
go vet ./...

echo "== go test ./... =="
go test ./...

echo "== scripts/check_todo.sh =="
bash scripts/check_todo.sh

echo "== ci: all checks passed =="
