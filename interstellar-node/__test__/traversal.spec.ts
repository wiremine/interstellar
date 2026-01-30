import { describe, it, expect, beforeEach } from 'vitest';
import { Graph, P } from '../index.js';

describe('Traversal', () => {
  let graph: Graph;
  let alice: bigint;
  let bob: bigint;
  let charlie: bigint;
  let gremlin: bigint;

  beforeEach(() => {
    graph = new Graph();
    alice = graph.addVertex('person', { name: 'Alice', age: 30n });
    bob = graph.addVertex('person', { name: 'Bob', age: 25n });
    charlie = graph.addVertex('person', { name: 'Charlie', age: 35n });
    gremlin = graph.addVertex('software', { name: 'Gremlin', lang: 'java' });

    graph.addEdge(alice, bob, 'knows', { since: 2020n });
    graph.addEdge(alice, charlie, 'knows', { since: 2018n });
    graph.addEdge(bob, gremlin, 'uses');
    graph.addEdge(charlie, gremlin, 'uses');
  });

  describe('Terminal Steps', () => {
    it('toList() returns all results', () => {
      const results = graph.V().toList();
      expect(results).toHaveLength(4);
    });

    it('count() returns the number of elements', () => {
      expect(graph.V().count()).toBe(4);
      expect(graph.E().count()).toBe(4);
    });

    it('first() returns the first result or undefined', () => {
      const first = graph.V().hasLabel('person').first();
      expect(first).toBeDefined();
    });

    it('one() returns exactly one result', () => {
      const one = graph.V(alice).one();
      expect(one).toBeDefined();
    });

    it('hasNext() returns true if there are results', () => {
      expect(graph.V().hasNext()).toBe(true);
      expect(graph.V().hasLabel('nonexistent').hasNext()).toBe(false);
    });

    it('next() returns next result or undefined', () => {
      const next = graph.V().next();
      expect(next).toBeDefined();
    });
  });

  describe('Filter Steps', () => {
    it('hasLabel() filters by label', () => {
      const people = graph.V().hasLabel('person').toList();
      expect(people).toHaveLength(3);

      const software = graph.V().hasLabel('software').toList();
      expect(software).toHaveLength(1);
    });

    it('has() filters by property existence', () => {
      const withLang = graph.V().has('lang').toList();
      expect(withLang).toHaveLength(1);
    });

    it('hasValue() filters by exact property value', () => {
      const aliceVertex = graph.V().hasValue('name', 'Alice').toList();
      expect(aliceVertex).toHaveLength(1);
    });

    it('hasWhere() filters with predicate', () => {
      const over28 = graph.V().hasLabel('person').hasWhere('age', P.gt(28n)).toList();
      expect(over28).toHaveLength(2); // Alice (30) and Charlie (35)
    });

    it('hasNot() filters by property absence', () => {
      const withoutLang = graph.V().hasNot('lang').toList();
      expect(withoutLang).toHaveLength(3); // persons don't have 'lang'
    });

    it('hasId() filters by ID', () => {
      const result = graph.V().hasId(alice).toList();
      expect(result).toHaveLength(1);
    });

    it('limit() restricts result count', () => {
      const limited = graph.V().limit(2).toList();
      expect(limited).toHaveLength(2);
    });

    it('skip() skips first n results', () => {
      const skipped = graph.V().skip(2).toList();
      expect(skipped).toHaveLength(2);
    });

    it('range() returns a slice of results', () => {
      const ranged = graph.V().range(1, 3).toList();
      expect(ranged).toHaveLength(2);
    });

    it('dedup() removes duplicates', () => {
      // Navigate to gremlin from multiple paths
      const software = graph.V().hasLabel('person').out('uses').dedup().toList();
      expect(software).toHaveLength(1);
    });
  });

  describe('Navigation Steps', () => {
    it('out() traverses outgoing edges', () => {
      const aliceKnows = graph.V(alice).out('knows').values('name').toList();
      expect(aliceKnows).toContain('Bob');
      expect(aliceKnows).toContain('Charlie');
      expect(aliceKnows).toHaveLength(2);
    });

    it('in() traverses incoming edges', () => {
      const whoKnowsBob = graph.V(bob).in('knows').values('name').toList();
      expect(whoKnowsBob).toContain('Alice');
    });

    it('both() traverses both directions', () => {
      const bobConnections = graph.V(bob).both().toList();
      expect(bobConnections.length).toBeGreaterThanOrEqual(2); // Alice + Gremlin
    });

    it('outE() returns outgoing edges', () => {
      const edges = graph.V(alice).outE('knows').toList();
      expect(edges).toHaveLength(2);
    });

    it('inE() returns incoming edges', () => {
      const edges = graph.V(bob).inE('knows').toList();
      expect(edges).toHaveLength(1);
    });

    it('outV() returns source vertex of edge', () => {
      const sources = graph.E().outV().dedup().toList();
      expect(sources.length).toBeGreaterThan(0);
    });

    it('inV() returns target vertex of edge', () => {
      const targets = graph.E().inV().dedup().toList();
      expect(targets.length).toBeGreaterThan(0);
    });
  });

  describe('Map Steps', () => {
    it('values() extracts property values', () => {
      const names = graph.V().hasLabel('person').values('name').toList();
      expect(names).toContain('Alice');
      expect(names).toContain('Bob');
      expect(names).toContain('Charlie');
    });

    it('id() extracts element IDs', () => {
      const ids = graph.V().hasLabel('person').id().toList();
      expect(ids).toContain(alice);
      expect(ids).toContain(bob);
      expect(ids).toContain(charlie);
    });

    it('label() extracts element labels', () => {
      const labels = graph.V().label().toList();
      expect(labels).toContain('person');
      expect(labels).toContain('software');
    });

    it('valueMap() returns property maps', () => {
      const maps = graph.V(alice).valueMap().toList();
      expect(maps).toHaveLength(1);
      expect(maps[0]).toHaveProperty('name');
      expect(maps[0]).toHaveProperty('age');
    });

    it('elementMap() returns full element data', () => {
      const maps = graph.V(alice).elementMap().toList();
      expect(maps).toHaveLength(1);
      expect(maps[0]).toHaveProperty('id');
      expect(maps[0]).toHaveProperty('label');
    });

    it('constant() injects a constant value', () => {
      const constants = graph.V().limit(2).constant('fixed').toList();
      expect(constants).toEqual(['fixed', 'fixed']);
    });
  });

  describe('Aggregate Steps', () => {
    it('fold() collects all into a list', () => {
      const folded = graph.V().hasLabel('person').values('name').fold().toList();
      expect(folded).toHaveLength(1);
      expect(Array.isArray(folded[0])).toBe(true);
      expect(folded[0]).toHaveLength(3);
    });

    it('unfold() expands lists', () => {
      const unfolded = graph.V().hasLabel('person').values('name').fold().unfold().toList();
      expect(unfolded).toHaveLength(3);
    });

    it('sum() calculates sum', () => {
      const sum = graph.V().hasLabel('person').values('age').sum().one();
      expect(sum).toBe(90n); // 30 + 25 + 35
    });

    it('min() finds minimum', () => {
      const min = graph.V().hasLabel('person').values('age').min().one();
      expect(min).toBe(25n);
    });

    it('max() finds maximum', () => {
      const max = graph.V().hasLabel('person').values('age').max().one();
      expect(max).toBe(35n);
    });

    it('mean() calculates average', () => {
      const mean = graph.V().hasLabel('person').values('age').mean().one();
      expect(mean).toBe(30); // (30 + 25 + 35) / 3 = 30
    });

    it('count_() returns count as value', () => {
      const counts = graph.V().hasLabel('person').count_().toList();
      expect(counts).toEqual([3n]);
    });
  });

  describe('Order Steps', () => {
    it('orderAsc() sorts ascending', () => {
      const ages = graph.V().hasLabel('person').values('age').orderAsc().toList();
      expect(ages).toEqual([25n, 30n, 35n]);
    });

    it('orderDesc() sorts descending', () => {
      const ages = graph.V().hasLabel('person').values('age').orderDesc().toList();
      expect(ages).toEqual([35n, 30n, 25n]);
    });
  });

  describe('Side Effect Steps', () => {
    it('as() labels a step position', () => {
      // as() combined with select() for path queries
      const result = graph.V(alice).as('a').out('knows').as('b').select(['a', 'b']).toList();
      expect(result.length).toBeGreaterThan(0);
    });

    it('select() retrieves labeled steps', () => {
      const selected = graph.V(alice).as('start').out().select(['start']).toList();
      expect(selected.length).toBeGreaterThan(0);
    });
  });

  describe('Branch Steps', () => {
    it('optional() returns input if traversal fails', () => {
      // Alice has outgoing edges, so optional should return the out() result
      // For someone without outgoing edges, optional would return the input
      const result = graph.V(alice).optional(graph.V(alice).out()).toList();
      expect(result.length).toBeGreaterThan(0);
    });

    it('union() combines multiple traversals', () => {
      const combined = graph
        .V(alice)
        .union([
          graph.V(alice).out('knows'),
          graph.V(alice).values('name')
        ])
        .toList();
      expect(combined.length).toBeGreaterThan(0);
    });

    it('coalesce() returns first non-empty traversal', () => {
      const result = graph
        .V(alice)
        .coalesce([
          graph.V(alice).out('nonexistent'),
          graph.V(alice).values('name')
        ])
        .toList();
      expect(result).toContain('Alice');
    });

    it('not() filters elements that match traversal', () => {
      // People who don't use software
      const notUsers = graph.V().hasLabel('person').not(graph.V().out('uses')).toList();
      expect(notUsers).toHaveLength(1); // Only Alice doesn't use software directly
    });
  });

  describe('Path Step', () => {
    it('path() returns traversal paths', () => {
      const paths = graph.V(alice).out('knows').path().toList();
      // Note: Path tracking may return empty paths if history tracking is not enabled
      // in the underlying traversal execution. This is a known limitation.
      expect(paths).toHaveLength(2);
    });
  });
});
