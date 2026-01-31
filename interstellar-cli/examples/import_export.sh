#!/bin/bash
# Import/Export Example for Interstellar CLI
# Demonstrates GraphSON import and export functionality
#
# Run from project root: ./examples/import_export.sh

set -e

BINARY="${BINARY:-./target/debug/interstellar}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DB="/tmp/interstellar-import-export-demo-$$"
EXPORT_FILE="/tmp/interstellar-export-demo-$$.json"

# Build if needed
if [ ! -f "$BINARY" ]; then
    echo "Building debug binary..."
    cargo build
fi

cleanup() {
    rm -rf "$TEST_DB" "$EXPORT_FILE"
}
trap cleanup EXIT

echo "=== Interstellar Import/Export Demo ==="
echo "Using binary: $BINARY"
echo "Sample file: $SCRIPT_DIR/sample_graph.json"
echo

# Step 1: Import sample GraphSON file
echo "--- Step 1: Import GraphSON file ---"
echo "Command: interstellar import $TEST_DB $SCRIPT_DIR/sample_graph.json"
echo
$BINARY import "$TEST_DB" "$SCRIPT_DIR/sample_graph.json"
echo

# Step 2: Verify imported data
echo "--- Step 2: Verify imported data ---"
echo "Command: interstellar stats $TEST_DB"
echo
$BINARY stats "$TEST_DB"
echo

# Step 3: Query the imported data
echo "--- Step 3: Query imported data ---"
echo "Command: interstellar query $TEST_DB --gremlin \"g.V().valueMap().toList()\""
echo
$BINARY query "$TEST_DB" --gremlin "g.V().valueMap().toList()"
echo

echo "Command: interstellar query $TEST_DB --gremlin \"g.E().valueMap().toList()\""
echo
$BINARY query "$TEST_DB" --gremlin "g.E().valueMap().toList()"
echo

# Step 4: Add more data via Gremlin
echo "--- Step 4: Add more data via Gremlin ---"
echo "Command: interstellar query $TEST_DB --gremlin \"g.addV('person').property('name', 'Diana').property('age', 28).next()\""
echo
$BINARY query "$TEST_DB" --gremlin "g.addV('person').property('name', 'Diana').property('age', 28).next()"
echo

# Step 5: Export the database
echo "--- Step 5: Export to GraphSON ---"
echo "Command: interstellar export $TEST_DB $EXPORT_FILE --pretty"
echo
$BINARY export "$TEST_DB" "$EXPORT_FILE" --pretty
echo

# Step 6: Show exported file (first 30 lines)
echo "--- Step 6: Exported GraphSON (first 30 lines) ---"
head -30 "$EXPORT_FILE"
echo "..."
echo

# Step 7: Import into a new database (roundtrip test)
echo "--- Step 7: Roundtrip test - import exported data ---"
ROUNDTRIP_DB="/tmp/interstellar-roundtrip-$$"
echo "Command: interstellar import $ROUNDTRIP_DB $EXPORT_FILE"
echo
$BINARY import "$ROUNDTRIP_DB" "$EXPORT_FILE"
rm -rf "$ROUNDTRIP_DB"
echo

echo "=== Demo complete! ==="
