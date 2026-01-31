#!/usr/bin/env bash
#
# Count Rust and JavaScript lines of code in this project using cloc
#

set -euo pipefail

# Get the project root (parent of scripts directory)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "Counting Rust and JavaScript code in: $PROJECT_ROOT"
echo

cloc --include-lang=Rust,JavaScript --exclude-dir=node_modules .
