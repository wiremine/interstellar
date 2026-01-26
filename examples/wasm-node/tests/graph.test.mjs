/**
 * Interstellar Graph Database - Node.js Tests
 * 
 * Uses Node.js built-in test runner (node --test)
 */

import { describe, it, before, beforeEach } from 'node:test';
import assert from 'node:assert';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
const { Graph, P } = require('../../../pkg-node/interstellar.js');

describe('Graph CRUD Operations', () => {
    let graph;

    beforeEach(() => {
        graph = new Graph();
    });

    describe('Vertex Operations', () => {
        it('should create a vertex with properties', () => {
            const id = graph.addVertex('person', { name: 'Alice', age: 30n });
            assert.ok(typeof id === 'bigint', 'ID should be bigint');
            
            const vertex = graph.getVertex(id);
            assert.equal(vertex.label, 'person');
            assert.equal(vertex.properties.name, 'Alice');
            assert.equal(vertex.properties.age, 30n);
        });

        it('should create vertex with empty properties', () => {
            const id = graph.addVertex('empty', {});
            const vertex = graph.getVertex(id);
            assert.equal(vertex.label, 'empty');
            assert.deepEqual(vertex.properties, {});
        });

        it('should update vertex property', () => {
            const id = graph.addVertex('person', { name: 'Alice' });
            graph.setVertexProperty(id, 'age', 30n);
            
            const vertex = graph.getVertex(id);
            assert.equal(vertex.properties.age, 30n);
        });

        it('should remove vertex', () => {
            const id = graph.addVertex('person', { name: 'Alice' });
            assert.ok(graph.getVertex(id) !== undefined);
            
            const removed = graph.removeVertex(id);
            assert.ok(removed);
            assert.equal(graph.getVertex(id), undefined);
        });

        it('should return false when removing non-existent vertex', () => {
            const removed = graph.removeVertex(999999n);
            assert.ok(!removed);
        });
    });

    describe('Edge Operations', () => {
        let alice, bob;

        beforeEach(() => {
            alice = graph.addVertex('person', { name: 'Alice' });
            bob = graph.addVertex('person', { name: 'Bob' });
        });

        it('should create an edge with properties', () => {
            const edgeId = graph.addEdge(alice, bob, 'knows', { since: 2020n });
            assert.ok(typeof edgeId === 'bigint');
            
            const edge = graph.getEdge(edgeId);
            assert.equal(edge.label, 'knows');
            assert.equal(edge.from, alice);
            assert.equal(edge.to, bob);
            assert.equal(edge.properties.since, 2020n);
        });

        it('should update edge property', () => {
            const edgeId = graph.addEdge(alice, bob, 'knows', {});
            graph.setEdgeProperty(edgeId, 'weight', 0.5);
            
            const edge = graph.getEdge(edgeId);
            assert.equal(edge.properties.weight, 0.5);
        });

        it('should remove edge', () => {
            const edgeId = graph.addEdge(alice, bob, 'knows', {});
            const removed = graph.removeEdge(edgeId);
            assert.ok(removed);
            assert.equal(graph.getEdge(edgeId), undefined);
        });
    });

    describe('Graph Statistics', () => {
        it('should count vertices', () => {
            assert.equal(graph.vertexCount(), 0n);
            graph.addVertex('a', {});
            assert.equal(graph.vertexCount(), 1n);
            graph.addVertex('b', {});
            assert.equal(graph.vertexCount(), 2n);
        });

        it('should count edges', () => {
            const a = graph.addVertex('a', {});
            const b = graph.addVertex('b', {});
            assert.equal(graph.edgeCount(), 0n);
            graph.addEdge(a, b, 'rel', {});
            assert.equal(graph.edgeCount(), 1n);
        });
    });
});

describe('Traversal API', () => {
    let graph, alice, bob, charlie, acme;

    before(() => {
        graph = new Graph();
        alice = graph.addVertex('person', { name: 'Alice', age: 30n });
        bob = graph.addVertex('person', { name: 'Bob', age: 25n });
        charlie = graph.addVertex('person', { name: 'Charlie', age: 35n });
        acme = graph.addVertex('company', { name: 'Acme Corp' });
        
        graph.addEdge(alice, bob, 'knows', { since: 2020n });
        graph.addEdge(alice, charlie, 'knows', { since: 2019n });
        graph.addEdge(bob, charlie, 'knows', {});
        graph.addEdge(alice, acme, 'works_at', {});
    });

    describe('Start Steps', () => {
        it('V() should return all vertices', () => {
            const count = graph.V().toCount();
            assert.equal(count, 4n);
        });

        it('V_(id) should start from specific vertex', () => {
            const name = graph.V_(alice).values('name').first();
            assert.equal(name, 'Alice');
        });

        it('E() should return all edges', () => {
            const count = graph.E().toCount();
            assert.equal(count, 4n);
        });
    });

    describe('Filter Steps', () => {
        it('hasLabel should filter by label', () => {
            const people = graph.V().hasLabel('person').toCount();
            assert.equal(people, 3n);
            
            const companies = graph.V().hasLabel('company').toCount();
            assert.equal(companies, 1n);
        });

        it('has should filter by property existence', () => {
            const withAge = graph.V().has('age').toCount();
            assert.equal(withAge, 3n);
        });

        it('hasValue should filter by exact property value', () => {
            const alice = graph.V().hasValue('name', 'Alice').toCount();
            assert.equal(alice, 1n);
        });

        it('hasWhere with P.gt should filter by comparison', () => {
            const names = graph.V()
                .hasLabel('person')
                .hasWhere('age', P.gt(25n))
                .values('name')
                .toList();
            assert.ok(names.includes('Alice'));
            assert.ok(names.includes('Charlie'));
            assert.ok(!names.includes('Bob'));
        });

        it('hasWhere with P.between should filter range', () => {
            const names = graph.V()
                .hasLabel('person')
                .hasWhere('age', P.between(26n, 34n))
                .values('name')
                .toList();
            assert.deepEqual(names, ['Alice']);
        });

        it('limit should restrict results', () => {
            const limited = graph.V().hasLabel('person').limit(2n).toCount();
            assert.equal(limited, 2n);
        });

        it('dedup should remove duplicates', () => {
            const labels = graph.V().label().dedup().toList();
            assert.equal(labels.length, 2);
            assert.ok(labels.includes('person'));
            assert.ok(labels.includes('company'));
        });
    });

    describe('Navigation Steps', () => {
        it('out should traverse outgoing edges', () => {
            const friends = graph.V_(alice).out().toCount();
            assert.equal(friends, 3n); // bob, charlie, acme
        });

        it('outLabels should filter by edge label', () => {
            const friends = graph.V_(alice)
                .outLabels(['knows'])
                .values('name')
                .toList();
            assert.equal(friends.length, 2);
            assert.ok(friends.includes('Bob'));
            assert.ok(friends.includes('Charlie'));
        });

        it('in_ should traverse incoming edges', () => {
            const whoKnowsBob = graph.V_(bob)
                .in_()
                .values('name')
                .toList();
            assert.ok(whoKnowsBob.includes('Alice'));
        });

        it('both should traverse both directions', () => {
            const connected = graph.V_(bob).both().dedup().toCount();
            assert.ok(connected >= 2n);
        });

        it('outE/inV should navigate via edges', () => {
            const edgeLabels = graph.V_(alice).outE().label().toList();
            assert.ok(edgeLabels.includes('knows'));
            assert.ok(edgeLabels.includes('works_at'));
        });
    });

    describe('Transform Steps', () => {
        it('values should extract property', () => {
            const names = graph.V().hasLabel('person').values('name').toList();
            assert.equal(names.length, 3);
        });

        it('valueMap should return property map', () => {
            // Note: valueMap() returns arrays per Gremlin spec (properties can be multi-valued)
            const map = graph.V_(alice).valueMap().first();
            assert.deepEqual(map.name, ['Alice']);
            assert.deepEqual(map.age, [30n]);
        });

        it('elementMap should include id and label', () => {
            const map = graph.V_(alice).elementMap().first();
            assert.equal(map.id, alice);
            assert.equal(map.label, 'person');
            assert.equal(map.name, 'Alice');
        });

        it('id should extract element id', () => {
            const id = graph.V_(alice).id().first();
            assert.equal(id, alice);
        });

        it('label should extract element label', () => {
            const label = graph.V_(alice).label().first();
            assert.equal(label, 'person');
        });

        it('constant should emit fixed value', () => {
            const ones = graph.V().hasLabel('person').constant(1n).toList();
            assert.deepEqual(ones, [1n, 1n, 1n]);
        });
    });

    describe('Terminal Steps', () => {
        it('toList should return array', () => {
            const result = graph.V().hasLabel('person').values('name').toList();
            assert.ok(Array.isArray(result));
        });

        it('first should return first or undefined', () => {
            const first = graph.V().hasLabel('person').values('name').first();
            assert.ok(typeof first === 'string');
            
            const none = graph.V().hasLabel('nonexistent').first();
            assert.equal(none, undefined);
        });

        it('one should return exactly one or throw', () => {
            const result = graph.V_(alice).values('name').one();
            assert.equal(result, 'Alice');
            
            assert.throws(() => {
                graph.V().hasLabel('person').one();
            }, /Expected exactly one result/);
        });

        it('toCount should return bigint count', () => {
            const count = graph.V().toCount();
            assert.equal(typeof count, 'bigint');
            assert.equal(count, 4n);
        });

        it('hasNext should check for results', () => {
            assert.ok(graph.V().hasLabel('person').hasNext());
            assert.ok(!graph.V().hasLabel('nonexistent').hasNext());
        });
    });
});

describe('Predicate System', () => {
    let graph;

    before(() => {
        graph = new Graph();
        graph.addVertex('num', { value: 10n });
        graph.addVertex('num', { value: 20n });
        graph.addVertex('num', { value: 30n });
        graph.addVertex('str', { name: 'hello world' });
        graph.addVertex('str', { name: 'hello universe' });
        graph.addVertex('str', { name: 'goodbye' });
    });

    it('P.eq should match equal values', () => {
        const count = graph.V().hasWhere('value', P.eq(20n)).toCount();
        assert.equal(count, 1n);
    });

    it('P.neq should match non-equal values', () => {
        const count = graph.V().hasWhere('value', P.neq(20n)).toCount();
        assert.equal(count, 2n);
    });

    it('P.lt/P.lte should match less than', () => {
        const lt = graph.V().hasWhere('value', P.lt(20n)).toCount();
        assert.equal(lt, 1n);
        
        const lte = graph.V().hasWhere('value', P.lte(20n)).toCount();
        assert.equal(lte, 2n);
    });

    it('P.gt/P.gte should match greater than', () => {
        const gt = graph.V().hasWhere('value', P.gt(20n)).toCount();
        assert.equal(gt, 1n);
        
        const gte = graph.V().hasWhere('value', P.gte(20n)).toCount();
        assert.equal(gte, 2n);
    });

    it('P.within should match set membership', () => {
        const count = graph.V().hasWhere('value', P.within([10n, 30n])).toCount();
        assert.equal(count, 2n);
    });

    it('P.without should exclude set members', () => {
        const count = graph.V().hasWhere('value', P.without([20n])).toCount();
        assert.equal(count, 2n);
    });

    it('P.inside should match exclusive range', () => {
        const count = graph.V().hasWhere('value', P.inside(10n, 30n)).toCount();
        assert.equal(count, 1n); // only 20
    });

    it('P.between should match inclusive start, exclusive end', () => {
        const count = graph.V().hasWhere('value', P.between(10n, 30n)).toCount();
        assert.equal(count, 2n); // 10 and 20
    });

    it('P.containing should match substring', () => {
        const count = graph.V().hasWhere('name', P.containing('hello')).toCount();
        assert.equal(count, 2n);
    });

    it('P.startingWith should match prefix', () => {
        const count = graph.V().hasWhere('name', P.startingWith('hello')).toCount();
        assert.equal(count, 2n);
    });

    it('P.endingWith should match suffix', () => {
        const count = graph.V().hasWhere('name', P.endingWith('world')).toCount();
        assert.equal(count, 1n);
    });

    it('P.and should combine predicates', () => {
        const count = graph.V()
            .hasWhere('value', P.and(P.gt(5n), P.lt(25n)))
            .toCount();
        assert.equal(count, 2n); // 10 and 20
    });

    it('P.or should match either predicate', () => {
        const count = graph.V()
            .hasWhere('value', P.or(P.eq(10n), P.eq(30n)))
            .toCount();
        assert.equal(count, 2n);
    });

    it('P.not should negate predicate', () => {
        const count = graph.V()
            .hasWhere('value', P.not(P.eq(20n)))
            .toCount();
        assert.equal(count, 2n);
    });
});

describe('GraphSON Serialization', () => {
    it('should export and import graph', () => {
        const graph1 = new Graph();
        const a = graph1.addVertex('node', { value: 42n });
        const b = graph1.addVertex('node', { value: 43n });
        graph1.addEdge(a, b, 'connects', { weight: 1.5 });

        const json = graph1.toGraphSON();
        assert.ok(json.includes('node'));
        assert.ok(json.includes('connects'));

        const graph2 = new Graph();
        const result = graph2.fromGraphSON(json);
        
        assert.equal(graph2.vertexCount(), 2n);
        assert.equal(graph2.edgeCount(), 1n);
    });
});

describe('Value Type Handling', () => {
    let graph;

    beforeEach(() => {
        graph = new Graph();
    });

    it('should handle bigint values', () => {
        const id = graph.addVertex('test', { big: 9007199254740993n });
        const v = graph.getVertex(id);
        assert.equal(v.properties.big, 9007199254740993n);
    });

    it('should handle float values', () => {
        const id = graph.addVertex('test', { pi: 3.14159 });
        const v = graph.getVertex(id);
        assert.ok(Math.abs(v.properties.pi - 3.14159) < 0.00001);
    });

    it('should handle boolean values', () => {
        const id = graph.addVertex('test', { active: true, deleted: false });
        const v = graph.getVertex(id);
        assert.equal(v.properties.active, true);
        assert.equal(v.properties.deleted, false);
    });

    it('should handle string values', () => {
        const id = graph.addVertex('test', { message: 'Hello, World!' });
        const v = graph.getVertex(id);
        assert.equal(v.properties.message, 'Hello, World!');
    });

    it('should handle null values', () => {
        const id = graph.addVertex('test', { nothing: null });
        const v = graph.getVertex(id);
        assert.equal(v.properties.nothing, null);
    });

    it('should handle array values', () => {
        const id = graph.addVertex('test', { tags: ['a', 'b', 'c'] });
        const v = graph.getVertex(id);
        assert.deepEqual(v.properties.tags, ['a', 'b', 'c']);
    });

    it('should handle nested object values', () => {
        const id = graph.addVertex('test', { 
            metadata: { 
                created: 2024n, 
                author: 'test' 
            } 
        });
        const v = graph.getVertex(id);
        assert.deepEqual(v.properties.metadata, { created: 2024n, author: 'test' });
    });
});
