#!/usr/bin/env bash
set -euo pipefail

# ---- argument handling ----------------------------------------------------
MODE="${1:-repl}"          # default to "repl" if nothing supplied
case "$MODE" in
    day|hist|repl|week) ;;   # valid
    *) echo "Usage: $0 [day|hist|repl|week]" >&2; exit 1 ;;
esac

# ---- build & run ------------------------------------------------------------
./gradlew installDist          # or just “gradle installDist” if gradle is in PATH
cd app/build/install/app/bin
./app "$MODE"
cd - >/dev/null