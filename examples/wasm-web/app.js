// Interstellar Graph Database - WASM Browser Example
// This demonstrates the JavaScript API for the Interstellar graph database

import init, { Graph, P } from './pkg/interstellar.js';

// Global references
let graph = null;
let personCounter = 0;
let productCounter = 0;

// Console logging helper
function log(message, type = 'info') {
    const consoleOutput = document.getElementById('consoleOutput');
    const timestamp = new Date().toLocaleTimeString();
    const prefix = type === 'error' ? '[ERROR]' : type === 'success' ? '[OK]' : '[INFO]';
    consoleOutput.textContent += `\n${timestamp} ${prefix} ${message}`;
    consoleOutput.scrollTop = consoleOutput.scrollHeight;
}

// Update statistics display
function updateStats() {
    if (!graph) return;
    document.getElementById('vertexCount').textContent = graph.vertexCount().toString();
    document.getElementById('edgeCount').textContent = graph.edgeCount().toString();
}

// Format output for display
function formatOutput(data) {
    if (data === undefined || data === null) {
        return 'undefined';
    }
    if (typeof data === 'bigint') {
        return data.toString() + 'n';
    }
    if (Array.isArray(data)) {
        return JSON.stringify(data.map(item => {
            if (typeof item === 'bigint') return item.toString() + 'n';
            if (typeof item === 'object' && item !== null) {
                return formatObject(item);
            }
            return item;
        }), null, 2);
    }
    if (typeof data === 'object') {
        return JSON.stringify(formatObject(data), null, 2);
    }
    return String(data);
}

function formatObject(obj) {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
        if (typeof value === 'bigint') {
            result[key] = value.toString() + 'n';
        } else if (typeof value === 'object' && value !== null) {
            result[key] = formatObject(value);
        } else {
            result[key] = value;
        }
    }
    return result;
}

// Sample names for generating data
const sampleNames = ['Alice', 'Bob', 'Carol', 'David', 'Eve', 'Frank', 'Grace', 'Henry'];
const sampleProducts = ['Laptop', 'Phone', 'Tablet', 'Watch', 'Headphones', 'Camera'];

// Initialize WASM and set up event handlers
async function main() {
    const statusEl = document.getElementById('status');
    
    try {
        log('Initializing WASM module...');
        await init();
        
        log('Creating new Graph instance...');
        graph = new Graph();
        
        statusEl.textContent = 'Ready!';
        statusEl.className = 'status ready';
        
        log('Interstellar Graph Database initialized successfully!', 'success');
        
        // Enable all buttons
        document.querySelectorAll('button').forEach(btn => btn.disabled = false);
        
        // Update initial stats
        updateStats();
        
        // Set up event handlers
        setupEventHandlers();
        
    } catch (error) {
        statusEl.textContent = 'Error: ' + error.message;
        statusEl.className = 'status error';
        log('Failed to initialize: ' + error.message, 'error');
        console.error(error);
    }
}

function setupEventHandlers() {
    // Refresh stats
    document.getElementById('btnRefreshStats').addEventListener('click', () => {
        updateStats();
        log('Stats refreshed');
    });
    
    // Create sample graph
    document.getElementById('btnCreateSample').addEventListener('click', () => {
        createSampleGraph();
    });
    
    // Clear graph (create new instance since clear() is no-op)
    document.getElementById('btnClearGraph').addEventListener('click', () => {
        graph = new Graph();
        personCounter = 0;
        productCounter = 0;
        updateStats();
        document.getElementById('actionOutput').textContent = 'Graph cleared (new instance created)';
        log('Graph cleared');
    });
    
    // Add person
    document.getElementById('btnAddPerson').addEventListener('click', () => {
        const name = sampleNames[personCounter % sampleNames.length];
        const age = BigInt(20 + Math.floor(Math.random() * 40));
        const id = graph.addVertex('person', { name, age });
        personCounter++;
        updateStats();
        const output = `Added person vertex:\n  ID: ${id}n\n  Name: ${name}\n  Age: ${age}n`;
        document.getElementById('addVertexOutput').textContent = output;
        log(`Added person: ${name} (id=${id})`);
    });
    
    // Add product
    document.getElementById('btnAddProduct').addEventListener('click', () => {
        const name = sampleProducts[productCounter % sampleProducts.length];
        const price = BigInt(100 + Math.floor(Math.random() * 900));
        const id = graph.addVertex('product', { name, price });
        productCounter++;
        updateStats();
        const output = `Added product vertex:\n  ID: ${id}n\n  Name: ${name}\n  Price: $${price}n`;
        document.getElementById('addVertexOutput').textContent = output;
        log(`Added product: ${name} (id=${id})`);
    });
    
    // Get all vertices
    document.getElementById('btnGetAllVertices').addEventListener('click', () => {
        try {
            const count = graph.V().toCount();
            const output = `Total vertices: ${count}`;
            document.getElementById('traversalOutput').textContent = output;
            document.getElementById('traversalOutput').className = 'output success';
            log(`Query: V().toCount() = ${count}`);
        } catch (e) {
            document.getElementById('traversalOutput').textContent = 'Error: ' + e.message;
            document.getElementById('traversalOutput').className = 'output error';
            log('Query error: ' + e.message, 'error');
        }
    });
    
    // Get person names
    document.getElementById('btnGetPersonNames').addEventListener('click', () => {
        try {
            const names = graph.V().hasLabel('person').values('name').toList();
            const output = `Person names:\n${formatOutput(names)}`;
            document.getElementById('traversalOutput').textContent = output;
            document.getElementById('traversalOutput').className = 'output success';
            log(`Query: V().hasLabel('person').values('name') = ${names.length} results`);
        } catch (e) {
            document.getElementById('traversalOutput').textContent = 'Error: ' + e.message;
            document.getElementById('traversalOutput').className = 'output error';
            log('Query error: ' + e.message, 'error');
        }
    });
    
    // Get friends of Alice
    document.getElementById('btnGetFriends').addEventListener('click', () => {
        try {
            const friends = graph.V()
                .hasLabel('person')
                .hasValue('name', 'Alice')
                .outLabels(['knows'])
                .values('name')
                .toList();
            const output = friends.length > 0 
                ? `Alice's friends:\n${formatOutput(friends)}`
                : 'Alice has no friends yet (or Alice not found).\nTry creating a sample graph first!';
            document.getElementById('traversalOutput').textContent = output;
            document.getElementById('traversalOutput').className = 'output success';
            log(`Query: Friends of Alice = ${friends.length} results`);
        } catch (e) {
            document.getElementById('traversalOutput').textContent = 'Error: ' + e.message;
            document.getElementById('traversalOutput').className = 'output error';
            log('Query error: ' + e.message, 'error');
        }
    });
    
    // Count by label
    document.getElementById('btnCountByLabel').addEventListener('click', () => {
        try {
            const personCount = graph.V().hasLabel('person').toCount();
            const productCount = graph.V().hasLabel('product').toCount();
            const output = `Vertices by label:\n  person: ${personCount}\n  product: ${productCount}`;
            document.getElementById('traversalOutput').textContent = output;
            document.getElementById('traversalOutput').className = 'output success';
            log(`Count by label: persons=${personCount}, products=${productCount}`);
        } catch (e) {
            document.getElementById('traversalOutput').textContent = 'Error: ' + e.message;
            document.getElementById('traversalOutput').className = 'output error';
            log('Query error: ' + e.message, 'error');
        }
    });
    
    // Filter by age >= 25
    document.getElementById('btnFilterAge').addEventListener('click', () => {
        try {
            const predicate = P.gte(BigInt(25));
            const results = graph.V()
                .hasLabel('person')
                .hasWhere('age', predicate)
                .values('name')
                .toList();
            const output = results.length > 0
                ? `People aged 25+:\n${formatOutput(results)}`
                : 'No people aged 25+ found.\nTry creating a sample graph first!';
            document.getElementById('predicateOutput').textContent = output;
            document.getElementById('predicateOutput').className = 'output success';
            log(`Predicate filter: age >= 25 = ${results.length} results`);
        } catch (e) {
            document.getElementById('predicateOutput').textContent = 'Error: ' + e.message;
            document.getElementById('predicateOutput').className = 'output error';
            log('Predicate error: ' + e.message, 'error');
        }
    });
    
    // Filter by name starting with 'A'
    document.getElementById('btnFilterName').addEventListener('click', () => {
        try {
            const predicate = P.startingWith('A');
            const results = graph.V()
                .hasLabel('person')
                .hasWhere('name', predicate)
                .values('name')
                .toList();
            const output = results.length > 0
                ? `People with names starting with 'A':\n${formatOutput(results)}`
                : 'No people with names starting with "A" found.\nTry creating a sample graph first!';
            document.getElementById('predicateOutput').textContent = output;
            document.getElementById('predicateOutput').className = 'output success';
            log(`Predicate filter: name starts with 'A' = ${results.length} results`);
        } catch (e) {
            document.getElementById('predicateOutput').textContent = 'Error: ' + e.message;
            document.getElementById('predicateOutput').className = 'output error';
            log('Predicate error: ' + e.message, 'error');
        }
    });
    
    // Outgoing edges (knows)
    document.getElementById('btnOutgoing').addEventListener('click', () => {
        try {
            const results = graph.V()
                .hasLabel('person')
                .outLabels(['knows'])
                .values('name')
                .toList();
            const output = results.length > 0
                ? `People known by others (outgoing 'knows'):\n${formatOutput(results)}`
                : 'No outgoing "knows" relationships found.\nTry creating a sample graph first!';
            document.getElementById('navigationOutput').textContent = output;
            document.getElementById('navigationOutput').className = 'output success';
            log(`Navigation: out('knows') = ${results.length} results`);
        } catch (e) {
            document.getElementById('navigationOutput').textContent = 'Error: ' + e.message;
            document.getElementById('navigationOutput').className = 'output error';
            log('Navigation error: ' + e.message, 'error');
        }
    });
    
    // Incoming edges (knows)
    document.getElementById('btnIncoming').addEventListener('click', () => {
        try {
            const results = graph.V()
                .hasLabel('person')
                .inLabels(['knows'])
                .values('name')
                .toList();
            const output = results.length > 0
                ? `People who know others (incoming 'knows'):\n${formatOutput(results)}`
                : 'No incoming "knows" relationships found.\nTry creating a sample graph first!';
            document.getElementById('navigationOutput').textContent = output;
            document.getElementById('navigationOutput').className = 'output success';
            log(`Navigation: in('knows') = ${results.length} results`);
        } catch (e) {
            document.getElementById('navigationOutput').textContent = 'Error: ' + e.message;
            document.getElementById('navigationOutput').className = 'output error';
            log('Navigation error: ' + e.message, 'error');
        }
    });
    
    // Both directions
    document.getElementById('btnBoth').addEventListener('click', () => {
        try {
            const results = graph.V()
                .hasLabel('person')
                .bothLabels(['knows'])
                .values('name')
                .dedup()
                .toList();
            const output = results.length > 0
                ? `People connected via 'knows' (both directions):\n${formatOutput(results)}`
                : 'No "knows" relationships found.\nTry creating a sample graph first!';
            document.getElementById('navigationOutput').textContent = output;
            document.getElementById('navigationOutput').className = 'output success';
            log(`Navigation: both('knows').dedup() = ${results.length} results`);
        } catch (e) {
            document.getElementById('navigationOutput').textContent = 'Error: ' + e.message;
            document.getElementById('navigationOutput').className = 'output error';
            log('Navigation error: ' + e.message, 'error');
        }
    });
}

// Create a sample graph with people and relationships
function createSampleGraph() {
    try {
        log('Creating sample graph...');
        
        // Create people
        const alice = graph.addVertex('person', { name: 'Alice', age: BigInt(30) });
        const bob = graph.addVertex('person', { name: 'Bob', age: BigInt(25) });
        const carol = graph.addVertex('person', { name: 'Carol', age: BigInt(35) });
        const david = graph.addVertex('person', { name: 'David', age: BigInt(28) });
        const eve = graph.addVertex('person', { name: 'Eve', age: BigInt(22) });
        
        log(`Created 5 people: Alice(${alice}), Bob(${bob}), Carol(${carol}), David(${david}), Eve(${eve})`);
        
        // Create products
        const laptop = graph.addVertex('product', { name: 'Laptop', price: BigInt(1200) });
        const phone = graph.addVertex('product', { name: 'Phone', price: BigInt(800) });
        
        log(`Created 2 products: Laptop(${laptop}), Phone(${phone})`);
        
        // Create relationships - note: using BigInt for IDs
        graph.addEdge(BigInt(alice), BigInt(bob), 'knows', { since: BigInt(2020) });
        graph.addEdge(BigInt(alice), BigInt(carol), 'knows', { since: BigInt(2019) });
        graph.addEdge(BigInt(bob), BigInt(david), 'knows', { since: BigInt(2021) });
        graph.addEdge(BigInt(carol), BigInt(eve), 'knows', { since: BigInt(2022) });
        graph.addEdge(BigInt(david), BigInt(alice), 'knows', { since: BigInt(2020) });
        
        log('Created 5 "knows" relationships');
        
        // Create purchases
        graph.addEdge(BigInt(alice), BigInt(laptop), 'purchased', { date: '2023-01-15' });
        graph.addEdge(BigInt(bob), BigInt(phone), 'purchased', { date: '2023-02-20' });
        graph.addEdge(BigInt(carol), BigInt(laptop), 'purchased', { date: '2023-03-10' });
        
        log('Created 3 "purchased" relationships');
        
        updateStats();
        
        const output = `Sample graph created successfully!

Vertices (7 total):
  - 5 people: Alice, Bob, Carol, David, Eve
  - 2 products: Laptop, Phone

Edges (8 total):
  - 5 "knows" relationships
  - 3 "purchased" relationships

Relationship diagram:
  Alice --knows--> Bob --knows--> David
    |                              |
    +--knows--> Carol --knows--> Eve
    |
    +--purchased--> Laptop <--purchased-- Carol
  Bob --purchased--> Phone`;
        
        document.getElementById('actionOutput').textContent = output;
        log('Sample graph created successfully!', 'success');
        
    } catch (e) {
        document.getElementById('actionOutput').textContent = 'Error creating sample graph: ' + e.message;
        log('Error creating sample graph: ' + e.message, 'error');
        console.error(e);
    }
}

// Start the application
main();
