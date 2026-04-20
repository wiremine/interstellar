# GQL Result Row Column Order Is Non-Deterministic

## Summary

GQL query results return rows as `Value::Map(HashMap<String, Value>)`. Since `HashMap` has non-deterministic iteration order, the column ordering varies per row even within the same query result set. Columns should appear in the order specified in the `RETURN` clause.

## Reproduction

```gql
MATCH (p:Person)-[:PARTICIPATED_IN]->(e:Event)
WHERE ID(p) = 24
RETURN e.eventType, e.date, e.description
ORDER BY e.date
```

**Actual output** (column order changes per row):

```
e.date: 1820-03-15, e.eventType: birth, e.description: Born in Cork City, Ireland
e.eventType: marriage, e.description: Married at St. Mary's Church, Cork, e.date: 1844-09-10
e.eventType: immigration, e.date: 1845, e.description: Emigrated during the Great Famine
e.description: Died in Springfield, IL, e.eventType: death, e.date: 1885-11-02
```

**Expected output** (columns in RETURN order for every row):

```
e.eventType: birth, e.date: 1820-03-15, e.description: Born in Cork City, Ireland
e.eventType: marriage, e.date: 1844-09-10, e.description: Married at St. Mary's Church, Cork
e.eventType: immigration, e.date: 1845, e.description: Emigrated during the Great Famine
e.eventType: death, e.date: 1885-11-02, e.description: Died in Springfield, IL
```

## Cause

`Value::Map` wraps `HashMap<String, Value>`, which does not preserve insertion order.

## Suggested Fix

Use an order-preserving map for GQL result rows, such as `IndexMap<String, Value>` or `Vec<(String, Value)>`, so that columns appear in the order specified in the `RETURN` clause.

## Environment

- interstellar 0.1.1
- Rust nightly
- In-memory storage backend
