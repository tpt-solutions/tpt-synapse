#!/usr/bin/env bash
# Fails if a checked-off TODO.md item (- [x]) names a `backtick-quoted` path
# that doesn't exist in the repo, so the checklist can't silently drift ahead
# of what's actually implemented.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

todo_file="TODO.md"
status=0

# Each checked line is joined with its indented continuation lines (which
# start with 6+ spaces) so a path mentioned on a wrapped line is still seen.
current=""
flush() {
    if [[ -n "$current" ]]; then
        while IFS= read -r path; do
            [[ -z "$path" ]] && continue
            if [[ ! -e "$path" ]]; then
                echo "TODO.md: checked-off item references missing path: $path"
                echo "  in: $current"
                status=1
            fi
        done < <(grep -oE '`[A-Za-z0-9_./-]+`' <<< "$current" | tr -d '`' | grep -E '/' || true)
    fi
}

while IFS= read -r line; do
    if [[ "$line" =~ ^-\ \[x\] ]]; then
        flush
        current="$line"
    elif [[ "$line" =~ ^\ {6,} ]] && [[ -n "$current" ]]; then
        current+=" $line"
    else
        flush
        current=""
    fi
done < "$todo_file"
flush

if [[ "$status" -eq 0 ]]; then
    echo "check_todo: OK — no checked-off items reference missing paths"
fi
exit "$status"
