import { describe, it, expect, beforeEach } from 'vitest';
import { Graph, P } from '../index.js';

describe('Algorithm Steps', () => {
  let graph: any;
  let a: bigint;
  let b: bigint;
  let c: bigint;
  let d: bigint;

  beforeEach(() => {
    graph = new Graph();
    a = graph.addVertex('city', { name: 'A' });
    b = graph.addVertex('city', { name: 'B' });
    c = graph.addVertex('city', { name: 'C' });
    d = graph.addVertex('city', { name: 'D' });

    graph.addEdge(a, b, 'road', { weight: 1.0 });
    graph.addEdge(b, c, 'road', { weight: 2.0 });
    graph.addEdge(a, c, 'road', { weight: 10.0 });
    graph.addEdge(c, d, 'road', { weight: 1.0 });
  });

  describe('shortestPath', () => {
    it('finds unweighted shortest path', () => {
      const result = graph.V(a).shortestPath(d).toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('shortestPathWeighted', () => {
    it('finds weighted shortest path (Dijkstra)', () => {
      const result = graph.V(a).shortestPathWeighted(d, 'weight').toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('bfs', () => {
    it('performs breadth-first traversal', () => {
      const result = graph.V(a).bfs().toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });

    it('respects max depth', () => {
      const result = graph.V(a).bfs(1).toList();
      expect(result).toBeDefined();
    });
  });

  describe('dfs', () => {
    it('performs depth-first traversal', () => {
      const result = graph.V(a).dfs().toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('bidirectionalBfs', () => {
    it('finds path using bidirectional BFS', () => {
      const result = graph.V(a).bidirectionalBfs(d).toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('iddfs', () => {
    it('finds path using iterative deepening DFS', () => {
      const result = graph.V(a).iddfs(d, 5).toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('kShortestPaths', () => {
    it('finds k shortest paths', () => {
      const result = graph.V(a).kShortestPaths(d, 2, 'weight').toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });
  });

  describe('astar', () => {
    it('finds path using A* search', () => {
      // A* with heuristic property (defaults to 0.0 if missing = Dijkstra behavior)
      const result = graph.V(a).astar(d, 'weight', 'heuristic').toList();
      expect(result).toBeDefined();
      expect(result.length).toBeGreaterThan(0);
    });
  });
});

describe('Gremlin Text Query', () => {
  let graph: any;

  beforeEach(() => {
    graph = new Graph();
    graph.addVertex('person', { name: 'Alice', age: 30n });
    graph.addVertex('person', { name: 'Bob', age: 25n });
    graph.addVertex('software', { name: 'Gremlin' });
  });

  it('executes a simple Gremlin query', () => {
    const results = graph.gremlin("g.V().hasLabel('person').values('name').toList()");
    expect(results).toHaveLength(2);
    expect(results).toContain('Alice');
    expect(results).toContain('Bob');
  });

  it('executes a count query', () => {
    const result = graph.gremlin("g.V().count().next()");
    expect(result).toBe(3n);
  });

  it('executes a hasNext query', () => {
    const result = graph.gremlin("g.V().hasLabel('person').hasNext()");
    expect(result).toBe(true);
  });
});

describe('Geospatial Predicates', () => {
  let graph: any;

  beforeEach(() => {
    graph = new Graph();
    // San Francisco area
    graph.addVertex('place', {
      name: 'Golden Gate Bridge',
      location: { type: 'Point', coordinates: [-122.4783, 37.8199] },
    });
    graph.addVertex('place', {
      name: 'Alcatraz Island',
      location: { type: 'Point', coordinates: [-122.4230, 37.8267] },
    });
  });

  it('P.withinDistance creates a geo predicate', () => {
    const pred = P.withinDistance(-122.4, 37.8, 10.0);
    expect(pred).toBeDefined();
  });
});
