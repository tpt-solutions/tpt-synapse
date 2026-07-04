# Build + test both toolchains, then check TODO.md for drift.
# Not wired to GitHub Actions by design; call this from whatever CI runner
# (or git hook - see .githooks/pre-push) is in use.
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "== cargo build --workspace =="
cargo build --workspace

Write-Host "== cargo test --workspace =="
cargo test --workspace

Write-Host "== go build ./... =="
go build ./...

Write-Host "== go vet ./... =="
go vet ./...

Write-Host "== go test ./... =="
go test ./...

Write-Host "== scripts/check_todo.sh =="
bash scripts/check_todo.sh

Write-Host "== ci: all checks passed =="
