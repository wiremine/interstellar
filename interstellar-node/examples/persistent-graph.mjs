#!/usr/bin/env node
/**
 * Interstellar Graph Database - Persistent Graph Example
 * 
 * Demonstrates using Graph.open() for disk-backed, persistent storage.
 * Data survives process restarts.
 * 
 * Run with: node examples/persistent-graph.mjs
 * 
 * Try running it twice to see data persistence in action!
 */

import { Graph, P } from '../index.js';
import { existsSync, unlinkSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH = join(__dirname, 'knowledge-graph.db');

console.log('='.repeat(60));
console.log('  Interstellar Graph Database - Persistent Storage Example');
console.log('='.repeat(60));
console.log();

// Check if database already exists
const isNewDatabase = !existsSync(DB_PATH);

if (isNewDatabase) {
  console.log(`Creating new database at: ${DB_PATH}`);
} else {
  console.log(`Opening existing database at: ${DB_PATH}`);
}
console.log();

// Open or create the persistent graph
const g = Graph.open(DB_PATH);

// ============================================================================
// Handle first run vs subsequent runs
// ============================================================================

if (isNewDatabase) {
  console.log('First run detected - populating knowledge graph...');
  console.log();
  
  // ============================================================================
  // Build a Knowledge Graph about Programming Languages
  // ============================================================================
  
  console.log('1. Creating programming language vertices...');
  
  // Languages
  const rust = g.addVertex('language', {
    name: 'Rust',
    paradigm: 'systems',
    year: 2010n,
    memoryModel: 'ownership',
    popularity: 95n
  });
  
  const javascript = g.addVertex('language', {
    name: 'JavaScript',
    paradigm: 'multi',
    year: 1995n,
    memoryModel: 'gc',
    popularity: 100n
  });
  
  const python = g.addVertex('language', {
    name: 'Python',
    paradigm: 'multi',
    year: 1991n,
    memoryModel: 'gc',
    popularity: 99n
  });
  
  const go = g.addVertex('language', {
    name: 'Go',
    paradigm: 'concurrent',
    year: 2009n,
    memoryModel: 'gc',
    popularity: 85n
  });
  
  const typescript = g.addVertex('language', {
    name: 'TypeScript',
    paradigm: 'multi',
    year: 2012n,
    memoryModel: 'gc',
    popularity: 92n
  });
  
  const cpp = g.addVertex('language', {
    name: 'C++',
    paradigm: 'multi',
    year: 1983n,
    memoryModel: 'manual',
    popularity: 88n
  });
  
  console.log(`   Created ${g.vertexCount} language vertices`);
  
  // ============================================================================
  // Create Concepts
  // ============================================================================
  
  console.log('\n2. Creating concept vertices...');
  
  const ownership = g.addVertex('concept', {
    name: 'Ownership',
    category: 'memory',
    description: 'Compile-time memory management'
  });
  
  const gc = g.addVertex('concept', {
    name: 'Garbage Collection',
    category: 'memory',
    description: 'Runtime memory management'
  });
  
  const async_ = g.addVertex('concept', {
    name: 'Async/Await',
    category: 'concurrency',
    description: 'Asynchronous programming pattern'
  });
  
  const generics = g.addVertex('concept', {
    name: 'Generics',
    category: 'types',
    description: 'Parametric polymorphism'
  });
  
  const traits = g.addVertex('concept', {
    name: 'Traits/Interfaces',
    category: 'types',
    description: 'Behavior abstraction'
  });
  
  console.log(`   Total vertices: ${g.vertexCount}`);
  
  // ============================================================================
  // Create Relationships
  // ============================================================================
  
  console.log('\n3. Creating relationships...');
  
  // Language influences
  g.addEdge(cpp, rust, 'influenced', { aspect: 'syntax' });
  g.addEdge(javascript, typescript, 'influenced', { aspect: 'ecosystem' });
  g.addEdge(python, go, 'influenced', { aspect: 'simplicity' });
  g.addEdge(rust, go, 'competes_with', { domain: 'systems' });
  g.addEdge(javascript, python, 'competes_with', { domain: 'scripting' });
  
  // Language features
  g.addEdge(rust, ownership, 'uses', { core: true });
  g.addEdge(javascript, gc, 'uses', { core: true });
  g.addEdge(python, gc, 'uses', { core: true });
  g.addEdge(go, gc, 'uses', { core: true });
  g.addEdge(typescript, gc, 'uses', { core: true });
  
  g.addEdge(rust, async_, 'supports', { since: '1.39' });
  g.addEdge(javascript, async_, 'supports', { since: 'ES2017' });
  g.addEdge(python, async_, 'supports', { since: '3.5' });
  g.addEdge(typescript, async_, 'supports', { since: '1.7' });
  
  g.addEdge(rust, generics, 'supports', { style: 'monomorphization' });
  g.addEdge(typescript, generics, 'supports', { style: 'erasure' });
  g.addEdge(go, generics, 'supports', { style: 'recent', since: '1.18' });
  
  g.addEdge(rust, traits, 'supports', { name: 'traits' });
  g.addEdge(go, traits, 'supports', { name: 'interfaces' });
  g.addEdge(typescript, traits, 'supports', { name: 'interfaces' });
  
  console.log(`   Created ${g.edgeCount} edges`);
  console.log();
  console.log('   Data has been persisted to disk!');
  
} else {
  console.log('Existing database found - reading persisted data...');
}

// ============================================================================
// Query the Graph (works on both fresh and reopened database)
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('  Querying the Knowledge Graph');
console.log('='.repeat(60));

console.log('\n4. Graph Statistics');
console.log('-'.repeat(40));
console.log(`   Vertices: ${g.vertexCount}`);
console.log(`   Edges: ${g.edgeCount}`);
console.log(`   Version: ${g.version}`);

console.log('\n5. All Programming Languages');
console.log('-'.repeat(40));
const languages = g.V().hasLabel('language').values('name').toList();
console.log(`   Languages: ${languages.join(', ')}`);

console.log('\n6. Languages by Popularity');
console.log('-'.repeat(40));
const byPopularity = g.V()
  .hasLabel('language')
  .valueMap()
  .toList()
  .sort((a, b) => Number(b.popularity[0]) - Number(a.popularity[0]));

for (const lang of byPopularity) {
  console.log(`   ${lang.name[0]}: popularity ${lang.popularity[0]}`);
}

console.log('\n7. Languages with Garbage Collection');
console.log('-'.repeat(40));
const gcLanguages = g.V()
  .hasLabel('language')
  .hasWhere('memoryModel', P.eq('gc'))
  .values('name')
  .toList();
console.log(`   GC Languages: ${gcLanguages.join(', ')}`);

console.log('\n8. Languages Supporting Async/Await');
console.log('-'.repeat(40));
const asyncLangs = g.V()
  .hasLabel('concept')
  .hasWhere('name', P.eq('Async/Await'))
  .in('supports')
  .values('name')
  .toList();
console.log(`   Async Languages: ${asyncLangs.join(', ')}`);

console.log('\n9. What Influenced Rust?');
console.log('-'.repeat(40));
const rustInfluencers = g.V()
  .hasLabel('language')
  .hasWhere('name', P.eq('Rust'))
  .in('influenced')
  .values('name')
  .toList();
console.log(`   Rust was influenced by: ${rustInfluencers.join(', ') || '(none found)'}`);

console.log('\n10. Memory Concepts Used');
console.log('-'.repeat(40));
const memoryConcepts = g.V()
  .hasLabel('concept')
  .hasWhere('category', P.eq('memory'))
  .valueMap()
  .toList();
for (const concept of memoryConcepts) {
  console.log(`   ${concept.name[0]}: ${concept.description[0]}`);
}

console.log('\n11. Languages Created After 2000');
console.log('-'.repeat(40));
const modernLanguages = g.V()
  .hasLabel('language')
  .hasWhere('year', P.gte(2000n))
  .values('name')
  .toList();
console.log(`   Modern languages: ${modernLanguages.join(', ')}`);

// ============================================================================
// Demonstrate Updates Persist
// ============================================================================

console.log('\n12. Adding a Visit Counter (Persists Across Runs)');
console.log('-'.repeat(40));

// Find or create a metadata vertex to track visits
let metaId = g.V().hasLabel('_metadata').first();

if (!metaId) {
  // First time - create metadata
  metaId = g.addVertex('_metadata', { visits: 1n });
  console.log(`   Created metadata vertex, visit count: 1`);
} else {
  // Update visit count
  const currentVisits = g.V(metaId).values('visits').first() || 0n;
  const newVisits = currentVisits + 1n;
  g.setVertexProperty(metaId, 'visits', newVisits);
  console.log(`   Visit count updated: ${currentVisits} -> ${newVisits}`);
}

// ============================================================================
// Summary
// ============================================================================

console.log('\n' + '='.repeat(60));
console.log('  Summary');
console.log('='.repeat(60));
console.log(`   Database path: ${DB_PATH}`);
console.log(`   Total vertices: ${g.vertexCount}`);
console.log(`   Total edges: ${g.edgeCount}`);
console.log();
console.log('   Run this example again to see data persistence in action!');
console.log('   The visit counter will increment each time.');
console.log();

// Uncomment the line below to delete the database and start fresh
// unlinkSync(DB_PATH);
// console.log('   (Database deleted for fresh start)');
