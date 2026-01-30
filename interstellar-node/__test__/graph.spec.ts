import { describe, it, expect, beforeEach } from 'vitest';
import { Graph, P } from '../index.js';

describe('Graph', () => {
  let graph: Graph;

  beforeEach(() => {
    graph = new Graph();
  });

  describe('constructor', () => {
    it('creates an empty graph', () => {
      expect(graph.vertexCount).toBe(0);
      expect(graph.edgeCount).toBe(0);
    });

    it('starts with version 0', () => {
      expect(graph.version).toBe(0n);
    });
  });

  describe('addVertex', () => {
    it('adds a vertex with label only', () => {
      const id = graph.addVertex('person');
      expect(typeof id).toBe('bigint');
      expect(graph.vertexCount).toBe(1);
    });

    it('adds a vertex with properties', () => {
      const id = graph.addVertex('person', { name: 'Alice', age: 30n });
      expect(graph.vertexCount).toBe(1);

      const vertex = graph.getVertex(id);
      expect(vertex).toBeDefined();
      expect(vertex?.label).toBe('person');
      expect(vertex?.properties?.name).toBe('Alice');
      expect(vertex?.properties?.age).toBe(30n);
    });

    it('increments version on add', () => {
      const v0 = graph.version;
      graph.addVertex('person');
      expect(graph.version).toBeGreaterThan(v0);
    });
  });

  describe('getVertex', () => {
    it('returns null for non-existent vertex', () => {
      expect(graph.getVertex(999n)).toBeNull();
    });

    it('returns vertex with id, label, and properties', () => {
      const id = graph.addVertex('person', { name: 'Bob' });
      const vertex = graph.getVertex(id);

      expect(vertex).not.toBeNull();
      expect(vertex?.id).toBe(id);
      expect(vertex?.label).toBe('person');
      expect(vertex?.properties?.name).toBe('Bob');
    });
  });

  describe('removeVertex', () => {
    it('returns false for non-existent vertex', () => {
      expect(graph.removeVertex(999n)).toBe(false);
    });

    it('removes existing vertex', () => {
      const id = graph.addVertex('person');
      expect(graph.removeVertex(id)).toBe(true);
      expect(graph.vertexCount).toBe(0);
      expect(graph.getVertex(id)).toBeNull();
    });

    it('removes incident edges when vertex is removed', () => {
      const alice = graph.addVertex('person', { name: 'Alice' });
      const bob = graph.addVertex('person', { name: 'Bob' });
      graph.addEdge(alice, bob, 'knows');

      expect(graph.edgeCount).toBe(1);
      graph.removeVertex(alice);
      expect(graph.edgeCount).toBe(0);
    });
  });

  describe('setVertexProperty', () => {
    it('sets a new property', () => {
      const id = graph.addVertex('person');
      graph.setVertexProperty(id, 'name', 'Charlie');

      const vertex = graph.getVertex(id);
      expect(vertex?.properties?.name).toBe('Charlie');
    });

    it('updates an existing property', () => {
      const id = graph.addVertex('person', { name: 'Old Name' });
      graph.setVertexProperty(id, 'name', 'New Name');

      const vertex = graph.getVertex(id);
      expect(vertex?.properties?.name).toBe('New Name');
    });
  });

  describe('addEdge', () => {
    it('adds an edge between vertices', () => {
      const alice = graph.addVertex('person', { name: 'Alice' });
      const bob = graph.addVertex('person', { name: 'Bob' });

      const edgeId = graph.addEdge(alice, bob, 'knows');
      expect(typeof edgeId).toBe('bigint');
      expect(graph.edgeCount).toBe(1);
    });

    it('adds an edge with properties', () => {
      const alice = graph.addVertex('person');
      const bob = graph.addVertex('person');

      const edgeId = graph.addEdge(alice, bob, 'knows', { since: 2020n });
      const edge = graph.getEdge(edgeId);

      expect(edge?.label).toBe('knows');
      expect(edge?.properties?.since).toBe(2020n);
    });
  });

  describe('getEdge', () => {
    it('returns null for non-existent edge', () => {
      expect(graph.getEdge(999n)).toBeNull();
    });

    it('returns edge with id, label, from, to, and properties', () => {
      const alice = graph.addVertex('person');
      const bob = graph.addVertex('person');
      const edgeId = graph.addEdge(alice, bob, 'knows', { weight: 1n });

      const edge = graph.getEdge(edgeId);
      expect(edge).not.toBeNull();
      expect(edge?.id).toBe(edgeId);
      expect(edge?.label).toBe('knows');
      expect(edge?.from).toBe(alice);
      expect(edge?.to).toBe(bob);
      expect(edge?.properties?.weight).toBe(1n);
    });
  });

  describe('removeEdge', () => {
    it('returns false for non-existent edge', () => {
      expect(graph.removeEdge(999n)).toBe(false);
    });

    it('removes existing edge', () => {
      const alice = graph.addVertex('person');
      const bob = graph.addVertex('person');
      const edgeId = graph.addEdge(alice, bob, 'knows');

      expect(graph.removeEdge(edgeId)).toBe(true);
      expect(graph.edgeCount).toBe(0);
      expect(graph.getEdge(edgeId)).toBeNull();
    });
  });

  describe('setEdgeProperty', () => {
    it('sets a property on an edge', () => {
      const alice = graph.addVertex('person');
      const bob = graph.addVertex('person');
      const edgeId = graph.addEdge(alice, bob, 'knows');

      graph.setEdgeProperty(edgeId, 'weight', 5n);

      const edge = graph.getEdge(edgeId);
      expect(edge?.properties?.weight).toBe(5n);
    });
  });
});

describe('Graph traversal entry points', () => {
  let graph: Graph;
  let alice: bigint;
  let bob: bigint;
  let charlie: bigint;

  beforeEach(() => {
    graph = new Graph();
    alice = graph.addVertex('person', { name: 'Alice', age: 30n });
    bob = graph.addVertex('person', { name: 'Bob', age: 25n });
    charlie = graph.addVertex('software', { name: 'Gremlin', lang: 'java' });
    graph.addEdge(alice, bob, 'knows', { since: 2020n });
    graph.addEdge(bob, charlie, 'uses');
  });

  describe('V()', () => {
    it('returns a traversal starting from all vertices', () => {
      const traversal = graph.V();
      expect(traversal).toBeDefined();
    });
  });

  describe('V(ids)', () => {
    it('returns a traversal from specific vertex ID', () => {
      const traversal = graph.V(alice);
      expect(traversal).toBeDefined();
    });

    it('returns a traversal from array of vertex IDs', () => {
      const traversal = graph.V([alice, bob]);
      expect(traversal).toBeDefined();
    });
  });

  describe('E()', () => {
    it('returns a traversal starting from all edges', () => {
      const traversal = graph.E();
      expect(traversal).toBeDefined();
    });
  });
});
