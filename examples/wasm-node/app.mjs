/**
 * Interstellar Graph Database - Node.js Example
 * 
 * Demonstrates the WASM bindings in a Node.js environment.
 */

import { createRequire } from 'module';
const require = createRequire(import.meta.url);

// Import from the built CommonJS package
const { Graph, P } = require('../../pkg-node/interstellar.js');

console.log('Interstellar Graph Database - Node.js Example');
console.log('='.repeat(50));

// Create a new graph
const graph = new Graph();
console.log('\n1. Created new graph');

// Add vertices
console.log('\n2. Adding vertices...');
const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
const charlie = graph.addVertex('person', { name: 'Charlie', age: 35n });
const acme = graph.addVertex('company', { name: 'Acme Corp', founded: 2010n });

console.log(`   Added Alice (id: ${alice})`);
console.log(`   Added Bob (id: ${bob})`);
console.log(`   Added Charlie (id: ${charlie})`);
console.log(`   Added Acme Corp (id: ${acme})`);

// Add edges
console.log('\n3. Adding edges...');
const e1 = graph.addEdge(alice, bob, 'knows', { since: 2020n });
const e2 = graph.addEdge(alice, charlie, 'knows', { since: 2019n });
const e3 = graph.addEdge(bob, charlie, 'knows', { since: 2021n });
const e4 = graph.addEdge(alice, acme, 'works_at', { role: 'Engineer' });
const e5 = graph.addEdge(bob, acme, 'works_at', { role: 'Manager' });

console.log(`   Alice knows Bob (edge: ${e1})`);
console.log(`   Alice knows Charlie (edge: ${e2})`);
console.log(`   Bob knows Charlie (edge: ${e3})`);
console.log(`   Alice works at Acme (edge: ${e4})`);
console.log(`   Bob works at Acme (edge: ${e5})`);

// Query: Get all person names
console.log('\n4. Query: All person names');
const names = graph.V().hasLabel('person').values('name').toList();
console.log(`   Result: ${JSON.stringify(names)}`);

// Query: Find Alice's friends
console.log('\n5. Query: Who does Alice know?');
const aliceFriends = graph.V_(alice)
    .outLabels(['knows'])
    .values('name')
    .toList();
console.log(`   Result: ${JSON.stringify(aliceFriends)}`);

// Query: Find people older than 25
console.log('\n6. Query: People older than 25');
const olderThan25 = graph.V()
    .hasLabel('person')
    .hasWhere('age', P.gt(25n))
    .values('name')
    .toList();
console.log(`   Result: ${JSON.stringify(olderThan25)}`);

// Query: Get company employees
console.log('\n7. Query: Who works at Acme?');
const employees = graph.V_(acme)
    .inLabels(['works_at'])
    .values('name')
    .toList();
console.log(`   Result: ${JSON.stringify(employees)}`);

// Query: Get vertex with properties
console.log('\n8. Query: Get Alice with all properties');
const aliceData = graph.V_(alice).elementMap().first();
console.log(`   Result: ${JSON.stringify(aliceData, (k, v) => typeof v === 'bigint' ? v.toString() + 'n' : v, 2)}`);

// Query: Count vertices
console.log('\n9. Query: Count all vertices');
const count = graph.V().toCount();
console.log(`   Result: ${count}`);

// Query: Complex traversal - friends of friends
console.log('\n10. Query: Friends of Alice\'s friends (excluding Alice)');
const fof = graph.V_(alice)
    .outLabels(['knows'])
    .outLabels(['knows'])
    .dedup()
    .values('name')
    .toList();
console.log(`   Result: ${JSON.stringify(fof)}`);

// GraphSON export
console.log('\n11. Export to GraphSON');
const graphson = graph.toGraphSON();
console.log(`   GraphSON length: ${graphson.length} characters`);
console.log(`   Preview: ${graphson.substring(0, 100)}...`);

// Graph statistics
console.log('\n12. Graph Statistics');
console.log(`   Vertices: ${graph.vertexCount()}`);
console.log(`   Edges: ${graph.edgeCount()}`);

console.log('\n' + '='.repeat(50));
console.log('Example completed successfully!');
