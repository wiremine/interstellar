#!/usr/bin/env node
/**
 * Interstellar Graph Database - Example Usage
 * 
 * Run with: node examples/social-network.mjs
 */

import { Graph, P, __ } from '../index.js';

console.log('='.repeat(60));
console.log('  Interstellar Graph Database - Social Network Example');
console.log('='.repeat(60));
console.log();

// Create a new graph
const g = new Graph();

// ============================================================================
// 1. Create vertices (people and companies)
// ============================================================================
console.log('1. Creating vertices...');

const alice = g.addVertex('person', { 
  name: 'Alice', 
  age: 30n, 
  city: 'New York',
  skills: ['javascript', 'rust']
});

const bob = g.addVertex('person', { 
  name: 'Bob', 
  age: 25n, 
  city: 'San Francisco',
  skills: ['python', 'ml']
});

const charlie = g.addVertex('person', { 
  name: 'Charlie', 
  age: 35n, 
  city: 'New York',
  skills: ['java', 'scala']
});

const diana = g.addVertex('person', { 
  name: 'Diana', 
  age: 28n, 
  city: 'Seattle',
  skills: ['go', 'kubernetes']
});

const acme = g.addVertex('company', { 
  name: 'Acme Corp', 
  industry: 'Tech',
  founded: 2010n
});

const techStartup = g.addVertex('company', { 
  name: 'TechStartup', 
  industry: 'Tech',
  founded: 2020n
});

console.log(`   Created ${g.vertexCount} vertices`);

// ============================================================================
// 2. Create edges (relationships)
// ============================================================================
console.log('\n2. Creating edges...');

// Social connections
g.addEdge(alice, bob, 'knows', { since: 2020n, strength: 'strong' });
g.addEdge(alice, charlie, 'knows', { since: 2018n, strength: 'medium' });
g.addEdge(bob, diana, 'knows', { since: 2021n, strength: 'weak' });
g.addEdge(charlie, diana, 'knows', { since: 2019n, strength: 'strong' });

// Employment
g.addEdge(alice, acme, 'works_at', { role: 'Engineer', startYear: 2019n });
g.addEdge(bob, techStartup, 'works_at', { role: 'Data Scientist', startYear: 2021n });
g.addEdge(charlie, acme, 'works_at', { role: 'Architect', startYear: 2015n });
g.addEdge(diana, techStartup, 'works_at', { role: 'DevOps Lead', startYear: 2020n });

console.log(`   Created ${g.edgeCount} edges`);

// ============================================================================
// 3. Basic Queries
// ============================================================================
console.log('\n3. Basic Queries');
console.log('-'.repeat(40));

// Get all person names
const allPeople = g.V().hasLabel('person').values('name').toList();
console.log(`   All people: ${allPeople.join(', ')}`);

// Count vertices by label
const personCount = g.V().hasLabel('person').count();
const companyCount = g.V().hasLabel('company').count();
console.log(`   People: ${personCount}, Companies: ${companyCount}`);

// ============================================================================
// 4. Filter Queries with Predicates
// ============================================================================
console.log('\n4. Filter Queries with Predicates');
console.log('-'.repeat(40));

// People over 28
const over28 = g.V().hasLabel('person').hasWhere('age', P.gt(28n)).values('name').toList();
console.log(`   People over 28: ${over28.join(', ')}`);

// People in New York
const newYorkers = g.V().hasWhere('city', P.eq('New York')).values('name').toList();
console.log(`   New Yorkers: ${newYorkers.join(', ')}`);

// People aged 25-35 (inclusive)
const midAge = g.V()
  .hasLabel('person')
  .hasWhere('age', P.and(P.gte(25n), P.lte(35n)))
  .values('name')
  .toList();
console.log(`   Aged 25-35: ${midAge.join(', ')}`);

// Names starting with 'A' or 'B'
const abNames = g.V()
  .hasWhere('name', P.or(P.startingWith('A'), P.startingWith('B')))
  .values('name')
  .toList();
console.log(`   Names A* or B*: ${abNames.join(', ')}`);

// ============================================================================
// 5. Navigation Queries
// ============================================================================
console.log('\n5. Navigation Queries');
console.log('-'.repeat(40));

// Who does Alice know?
const aliceKnows = g.V(alice).out('knows').values('name').toList();
console.log(`   Alice knows: ${aliceKnows.join(', ')}`);

// Who knows Diana?
const knowsDiana = g.V(diana).in('knows').values('name').toList();
console.log(`   Who knows Diana: ${knowsDiana.join(', ')}`);

// Where does Bob work?
const bobCompany = g.V(bob).out('works_at').values('name').toList();
console.log(`   Bob works at: ${bobCompany.join(', ')}`);

// Who works at Acme?
const acmeEmployees = g.V(acme).in('works_at').values('name').toList();
console.log(`   Acme employees: ${acmeEmployees.join(', ')}`);

// ============================================================================
// 6. Multi-hop Traversals
// ============================================================================
console.log('\n6. Multi-hop Traversals');
console.log('-'.repeat(40));

// Friends of friends of Alice (2 hops)
const fof = g.V(alice).out('knows').out('knows').dedup().values('name').toList();
console.log(`   Alice's friends of friends: ${fof.join(', ')}`);

// Companies where Alice's friends work
const friendCompanies = g.V(alice)
  .out('knows')
  .out('works_at')
  .dedup()
  .values('name')
  .toList();
console.log(`   Companies of Alice's friends: ${friendCompanies.join(', ')}`);

// ============================================================================
// 7. Aggregations
// ============================================================================
console.log('\n7. Aggregations');
console.log('-'.repeat(40));

const totalAge = g.V().hasLabel('person').values('age').sum().one();
console.log(`   Total age: ${totalAge}`);

const avgAge = g.V().hasLabel('person').values('age').mean().one();
console.log(`   Average age: ${avgAge}`);

const minAge = g.V().hasLabel('person').values('age').min().one();
const maxAge = g.V().hasLabel('person').values('age').max().one();
console.log(`   Age range: ${minAge} - ${maxAge}`);

// ============================================================================
// 8. Sorting
// ============================================================================
console.log('\n8. Sorting');
console.log('-'.repeat(40));

const byAgeAsc = g.V().hasLabel('person').values('age').orderAsc().toList();
console.log(`   Ages ascending: ${byAgeAsc.join(', ')}`);

const byAgeDesc = g.V().hasLabel('person').values('age').orderDesc().toList();
console.log(`   Ages descending: ${byAgeDesc.join(', ')}`);

// ============================================================================
// 9. Value Maps
// ============================================================================
console.log('\n9. Value Maps');
console.log('-'.repeat(40));

const aliceData = g.V(alice).valueMap().toList();
console.log('   Alice properties:', JSON.stringify(aliceData[0], (_, v) => 
  typeof v === 'bigint' ? v.toString() + 'n' : v
));

// ============================================================================
// 10. Path Queries
// ============================================================================
console.log('\n10. Path Queries');
console.log('-'.repeat(40));

const paths = g.V(alice).out('knows').out('works_at').path().toList();
console.log(`   Paths from Alice to companies (via friends): ${paths.length} paths found`);

// ============================================================================
// 11. Anonymous Traversals
// ============================================================================
console.log('\n11. Anonymous Traversals');
console.log('-'.repeat(40));

// The __ (double underscore) module provides anonymous traversal fragments
// that can be composed into larger queries

// where() with anonymous traversal - filter people who know someone over 30
// Note: use outLabels() for label-filtered navigation in anonymous traversals
const knowsOlder = g.V()
  .hasLabel('person')
  .where(__.outLabels('knows').hasWhere('age', P.gt(30n)))
  .values('name')
  .toList();
console.log(`   People who know someone over 30: ${knowsOlder.join(', ')}`);

// not() - find people who don't work at any company
const unemployed = g.V()
  .hasLabel('person')
  .not(__.out('works_at'))
  .values('name')
  .toList();
console.log(`   People without a job: ${unemployed.join(', ') || '(none)'}`);

// coalesce() - get employer name, or "Self-employed" if none
// Note: coalesce takes an array of traversals
const employment = g.V()
  .hasLabel('person')
  .coalesce([
    __.out('works_at').values('name'),
    __.constant('Self-employed')
  ])
  .toList();
console.log(`   Employment status: ${employment.join(', ')}`);

// union() - get both outgoing and incoming 'knows' relationships
// Note: union takes an array of traversals
// Use outLabels/inLabels for anonymous traversals with label filters
const allConnections = g.V(alice)
  .union([
    __.outLabels('knows').values('name'),
    __.inLabels('knows').values('name')
  ])
  .dedup()
  .toList();
console.log(`   Alice's connections (knows + known by): ${allConnections.join(', ')}`);

// optional() - traverse to employer if exists, otherwise stay at person
const withOptionalEmployer = g.V()
  .hasLabel('person')
  .as('person')
  .optional(__.out('works_at'))
  .label()
  .toList();
console.log(`   Labels after optional employer traversal: ${withOptionalEmployer.join(', ')}`);

// local() - apply aggregation locally per element
const localCounts = g.V()
  .hasLabel('person')
  .local(__.outLabels('knows').count_())
  .toList();
console.log(`   Friend counts per person: ${localCounts.join(', ')}`);

// Nested where() - find people who know someone who works at a company
const knowsEmployee = g.V()
  .hasLabel('person')
  .where(__.outLabels('knows').out('works_at'))
  .values('name')
  .toList();
console.log(`   People who know an employee: ${knowsEmployee.join(', ')}`);

// Using anonymous traversal with select
const connections = g.V(alice)
  .as('source')
  .out('knows')
  .as('friend')
  .out('works_at')
  .as('company')
  .select(['source', 'friend', 'company'])
  .toList();
console.log(`   Alice -> friend -> company paths: ${connections.length} found`);

// ============================================================================
// Summary
// ============================================================================
console.log('\n' + '='.repeat(60));
console.log('  Graph Summary');
console.log('='.repeat(60));
console.log(`   Vertices: ${g.vertexCount}`);
console.log(`   Edges: ${g.edgeCount}`);
console.log(`   Version: ${g.version}`);
console.log();
console.log('   Example completed successfully!');
console.log();
