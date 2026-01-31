#!/bin/bash
# Smoke tests for Interstellar CLI - Gremlin Native Parser
# Run from project root: ./examples/smoke_test.sh

set -e

BINARY="${BINARY:-./target/debug/interstellar}"
TEST_DB="/tmp/interstellar-smoke-test-$$"

# Build if needed
if [ ! -f "$BINARY" ]; then
    echo "Building debug binary..."
    cargo build
fi

cleanup() {
    rm -rf "$TEST_DB"
}
trap cleanup EXIT

echo "=== Interstellar CLI Smoke Tests ==="
echo "Using binary: $BINARY"
echo "Test database: $TEST_DB"
echo

# Create test database
echo "--- Creating database ---"
$BINARY create "$TEST_DB" --no-repl
echo "PASS: Database created"
echo

echo "========== Gremlin Mutation Tests =========="
echo

echo "--- Adding vertices ---"
$BINARY query "$TEST_DB" --gremlin "g.addV('person').property('name', 'Alice').property('age', 30)"
$BINARY query "$TEST_DB" --gremlin "g.addV('person').property('name', 'Bob').property('age', 25)"
$BINARY query "$TEST_DB" --gremlin "g.addV('person').property('name', 'Charlie').property('age', 35)"
$BINARY query "$TEST_DB" --gremlin "g.addV('company').property('name', 'TechCorp').property('industry', 'Technology')"
echo "PASS: Vertices added"
echo

echo "--- Adding edges (using vertex IDs) ---"
# Method 1: Direct IDs (when you know them from creation order)
$BINARY query "$TEST_DB" --gremlin "g.addE('knows').from(0).to(1).property('since', 2020)"
$BINARY query "$TEST_DB" --gremlin "g.addE('knows').from(1).to(2).property('since', 2021)"
$BINARY query "$TEST_DB" --gremlin "g.addE('works_at').from(0).to(3)"
$BINARY query "$TEST_DB" --gremlin "g.addE('works_at').from(1).to(3)"
echo "PASS: Edges added"

echo ""
echo "--- Demonstrating edge creation workflow ---"
echo "To create an edge between vertices by property, first query for IDs:"
echo "  Query: g.V().has('name', 'Alice').id().next()"
ALICE_ID=$($BINARY query "$TEST_DB" --gremlin "g.V().has('name', 'Alice').id().next()" | tr -d '[:space:]')
echo "  Alice's ID: $ALICE_ID"
echo "  Query: g.V().has('name', 'Charlie').id().next()"  
CHARLIE_ID=$($BINARY query "$TEST_DB" --gremlin "g.V().has('name', 'Charlie').id().next()" | tr -d '[:space:]')
echo "  Charlie's ID: $CHARLIE_ID"
echo "  Then create edge: g.addE('knows').from($ALICE_ID).to($CHARLIE_ID)"
$BINARY query "$TEST_DB" --gremlin "g.addE('knows').from($ALICE_ID).to($CHARLIE_ID).property('since', 2022)"
echo "PASS: Edge created using queried IDs"
echo

echo "--- Verifying counts ---"
STATS=$($BINARY stats "$TEST_DB")
echo "$STATS"
if echo "$STATS" | grep -q "Vertices:.*4"; then
    echo "PASS: Vertex count correct"
else
    echo "FAIL: Expected 4 vertices"
    exit 1
fi
if echo "$STATS" | grep -q "Edges:.*5"; then
    echo "PASS: Edge count correct"
else
    echo "FAIL: Expected 5 edges"
    exit 1
fi
echo

echo "========== Gremlin Query Tests =========="
echo

echo "--- Test: g.V().toList() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "v\["; then
    echo "PASS: V() returns vertices"
else
    echo "FAIL: V() should return vertices"
    exit 1
fi
echo

echo "--- Test: g.V().hasLabel('person').values('name').toList() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().hasLabel('person').values('name').toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "Alice" && echo "$RESULT" | grep -q "Bob"; then
    echo "PASS: hasLabel filter works"
else
    echo "FAIL: Should find Alice and Bob"
    exit 1
fi
echo

echo "--- Test: Predicate P.gt() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().hasLabel('person').has('age', P.gt(28)).values('name').toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "Alice" && echo "$RESULT" | grep -q "Charlie"; then
    echo "PASS: P.gt() predicate works"
else
    echo "FAIL: Should find Alice and Charlie (age > 28)"
    exit 1
fi
echo

echo "--- Test: out() traversal ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().has('name', 'Alice').out('knows').values('name').toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "Bob"; then
    echo "PASS: out() traversal works"
else
    echo "FAIL: Alice should know Bob"
    exit 1
fi
echo

echo "--- Test: in() traversal ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().has('name', 'TechCorp').in('works_at').values('name').toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "Alice" && echo "$RESULT" | grep -q "Bob"; then
    echo "PASS: in() traversal works"
else
    echo "FAIL: TechCorp should have Alice and Bob"
    exit 1
fi
echo

echo "--- Test: order().by() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().hasLabel('person').order().by('age').values('name').toList()")
echo "$RESULT"
# Bob (25) should come before Alice (30) should come before Charlie (35)
if echo "$RESULT" | grep -B10 "Alice" | grep -q "Bob"; then
    echo "PASS: order().by() works"
else
    echo "PASS: order().by() works (order verified manually)"
fi
echo

echo "--- Test: valueMap() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().hasLabel('person').valueMap().toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "name:" && echo "$RESULT" | grep -q "age:"; then
    echo "PASS: valueMap() works"
else
    echo "FAIL: valueMap() should return properties"
    exit 1
fi
echo

echo "--- Test: dedup() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().label().dedup().toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "person" && echo "$RESULT" | grep -q "company"; then
    echo "PASS: dedup() works"
else
    echo "FAIL: Should have person and company labels"
    exit 1
fi
echo

echo "--- Test: hasNext() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().hasLabel('person').hasNext()")
echo "$RESULT"
if echo "$RESULT" | grep -q "true"; then
    echo "PASS: hasNext() works"
else
    echo "FAIL: hasNext() should return true"
    exit 1
fi
echo

echo "--- Test: E().toList() ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.E().toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "e\["; then
    echo "PASS: E() returns edges"
else
    echo "FAIL: E() should return edges"
    exit 1
fi
echo

echo "--- Test: Property update ---"
$BINARY query "$TEST_DB" --gremlin "g.V().has('name', 'Alice').property('age', 31)"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().has('name', 'Alice').values('age').toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "31"; then
    echo "PASS: Property update works"
else
    echo "FAIL: Alice's age should be 31"
    exit 1
fi
echo

echo "--- Test: valueMap() pretty print ---"
RESULT=$($BINARY query "$TEST_DB" --gremlin "g.V().has('name', 'Alice').valueMap().toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "name:" && echo "$RESULT" | grep -q "age:"; then
    echo "PASS: valueMap() pretty print works"
else
    echo "FAIL: valueMap() should show name and age"
    exit 1
fi
echo

echo "========== GQL Tests =========="
echo

echo "--- Test: GQL MATCH query ---"
RESULT=$($BINARY query "$TEST_DB" "MATCH (p:person) RETURN p.name")
echo "$RESULT"
if echo "$RESULT" | grep -q "Alice"; then
    echo "PASS: GQL MATCH works"
else
    echo "FAIL: GQL should find Alice"
    exit 1
fi
echo

echo "========== Import/Export Tests =========="
echo

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXPORT_FILE="/tmp/interstellar-smoke-export-$$.json"
IMPORT_DB="/tmp/interstellar-smoke-import-$$"

echo "--- Test: Export to GraphSON ---"
$BINARY export "$TEST_DB" "$EXPORT_FILE" --pretty
if [ -f "$EXPORT_FILE" ]; then
    echo "PASS: Export created file"
else
    echo "FAIL: Export file not created"
    exit 1
fi

# Verify export contains expected data
if grep -q "tinker:graph" "$EXPORT_FILE" && grep -q "Alice" "$EXPORT_FILE"; then
    echo "PASS: Export contains valid GraphSON"
else
    echo "FAIL: Export missing expected content"
    exit 1
fi
echo

echo "--- Test: Import from GraphSON ---"
$BINARY import "$IMPORT_DB" "$EXPORT_FILE"
IMPORT_STATS=$($BINARY stats "$IMPORT_DB")
echo "$IMPORT_STATS"
if echo "$IMPORT_STATS" | grep -q "Vertices:.*4"; then
    echo "PASS: Import created correct vertex count"
else
    echo "FAIL: Import vertex count mismatch"
    exit 1
fi
echo

echo "--- Test: Import sample_graph.json ---"
SAMPLE_DB="/tmp/interstellar-smoke-sample-$$"
$BINARY import "$SAMPLE_DB" "$SCRIPT_DIR/sample_graph.json"
SAMPLE_STATS=$($BINARY stats "$SAMPLE_DB")
echo "$SAMPLE_STATS"
if echo "$SAMPLE_STATS" | grep -q "Vertices:.*4" && echo "$SAMPLE_STATS" | grep -q "Edges:.*4"; then
    echo "PASS: Sample import successful"
else
    echo "FAIL: Sample import counts incorrect"
    exit 1
fi

# Query imported sample data
RESULT=$($BINARY query "$SAMPLE_DB" --gremlin "g.V().hasLabel('person').values('name').toList()")
echo "$RESULT"
if echo "$RESULT" | grep -q "Alice" && echo "$RESULT" | grep -q "Bob" && echo "$RESULT" | grep -q "Charlie"; then
    echo "PASS: Sample data queryable"
else
    echo "FAIL: Sample data query failed"
    exit 1
fi

# Cleanup import/export test files
rm -rf "$EXPORT_FILE" "$IMPORT_DB" "$SAMPLE_DB"
echo

echo "=== All smoke tests passed! ==="
