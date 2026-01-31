// Interstellar Graph Database - Interactive Console & Visualization
// This demonstrates the JavaScript API for the Interstellar graph database

import init, { Graph, P } from './pkg/interstellar.js';

// =============================================================================
// Global State
// =============================================================================

let graph = null;
let simulation = null;
let svg = null;
let g = null;
let zoom = null;
let selectedElement = null;
let graphData = { nodes: [], links: [] };

// =============================================================================
// Initialization
// =============================================================================

async function main() {
    const statusEl = document.getElementById('status');
    
    try {
        await init();
        graph = new Graph();
        
        statusEl.textContent = 'Ready';
        statusEl.className = 'status ready';
        
        // Enable buttons
        document.querySelectorAll('button').forEach(btn => btn.disabled = false);
        
        updateStats();
        setupEventHandlers();
        setupVisualization();
        
        log('Interstellar Graph Database initialized', 'success');
        
    } catch (error) {
        statusEl.textContent = 'Error: ' + error.message;
        statusEl.className = 'status error';
        console.error(error);
    }
}

// =============================================================================
// Event Handlers
// =============================================================================

function setupEventHandlers() {
    // Tab switching
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            const tabId = tab.dataset.tab;
            switchTab(tabId);
        });
    });
    
    // Create sample graph
    document.getElementById('btnCreateSample').addEventListener('click', () => {
        createSampleGraph();
    });
    
    // Clear graph
    document.getElementById('btnClearGraph').addEventListener('click', () => {
        graph = new Graph();
        updateStats();
        refreshVisualization();
        log('Graph cleared', 'info');
    });
    
    // Query examples dropdown
    document.getElementById('queryExamples').addEventListener('change', (e) => {
        if (e.target.value) {
            const textarea = document.getElementById('queryInput');
            const decoded = e.target.value.replace(/&#10;/g, '\n');
            textarea.value = decoded;
            autoResizeTextarea(textarea);
            textarea.focus();
            e.target.value = '';
        }
    });
    
    // Run query button
    document.getElementById('btnRunQuery').addEventListener('click', runQuery);
    
    // Clear output button
    document.getElementById('btnClearOutput').addEventListener('click', () => {
        document.getElementById('consoleOutput').innerHTML = '';
    });
    
    // Query input: Enter to run, Shift+Enter for newline
    const queryInput = document.getElementById('queryInput');
    queryInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            runQuery();
        }
    });
    
    // Auto-resize textarea as user types
    queryInput.addEventListener('input', () => autoResizeTextarea(queryInput));
    
    // Visualization controls
    document.getElementById('btnZoomIn').addEventListener('click', () => {
        svg.transition().call(zoom.scaleBy, 1.3);
    });
    
    document.getElementById('btnZoomOut').addEventListener('click', () => {
        svg.transition().call(zoom.scaleBy, 0.7);
    });
    
    document.getElementById('btnFitGraph').addEventListener('click', fitGraphToView);
    
    document.getElementById('btnResetLayout').addEventListener('click', () => {
        refreshVisualization();
    });
    
    // Properties panel
    document.getElementById('btnCloseProps').addEventListener('click', () => {
        hidePropertiesPanel();
    });
    
    document.getElementById('btnDeleteSelected').addEventListener('click', deleteSelected);
}

function switchTab(tabId) {
    // Update tab buttons
    document.querySelectorAll('.tab').forEach(t => {
        t.classList.toggle('active', t.dataset.tab === tabId);
    });
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.toggle('active', content.id === `${tabId}-tab`);
    });
    
    // Refresh visualization when switching to it
    if (tabId === 'visualization') {
        refreshVisualization();
    }
}

// =============================================================================
// Console Functions
// =============================================================================

function autoResizeTextarea(textarea) {
    textarea.style.height = 'auto';
    textarea.style.height = Math.min(textarea.scrollHeight, 150) + 'px';
}

function runQuery() {
    const input = document.getElementById('queryInput');
    const query = input.value.trim();
    
    if (!query) return;
    
    const startTime = performance.now();
    
    try {
        const result = executeQuery(query);
        const elapsed = (performance.now() - startTime).toFixed(2);
        
        logQueryResult(query, result, elapsed);
        updateStats();
        
        // Refresh visualization if we're on that tab
        if (document.getElementById('visualization-tab').classList.contains('active')) {
            refreshVisualization();
        }
        
    } catch (error) {
        logQueryResult(query, error, null, true);
    }
    
    // Clear input after execution
    input.value = '';
    autoResizeTextarea(input);
    input.focus();
}

function executeQuery(query) {
    // Create a function that has access to graph and P
    // This is safer than raw eval and provides the necessary context
    const fn = new Function('graph', 'P', 'BigInt', `
        "use strict";
        ${query.includes('return') ? query : `return (${query})`}
    `);
    
    return fn(graph, P, BigInt);
}

function log(message, type = 'info') {
    const output = document.getElementById('consoleOutput');
    const entry = document.createElement('div');
    entry.className = 'output-entry';
    const typeClass = type === 'error' ? ' error' : type === 'info' ? ' info' : '';
    entry.innerHTML = `<div class="output-result${typeClass}">${escapeHtml(message)}</div>`;
    output.appendChild(entry);
    requestAnimationFrame(() => output.scrollTop = output.scrollHeight);
}

function logQueryResult(query, result, elapsed, isError = false) {
    const output = document.getElementById('consoleOutput');
    const entry = document.createElement('div');
    entry.className = 'output-entry';
    
    const formattedQuery = escapeHtml(query).replace(/\n/g, '<br>');
    const formattedResult = isError 
        ? `Error: ${escapeHtml(result.message || result.toString())}`
        : formatResult(result);
    
    entry.innerHTML = `
        <div class="output-query"><code>${formattedQuery}</code></div>
        <div class="output-result${isError ? ' error' : ''}">${formattedResult}</div>
        ${elapsed ? `<div class="output-time">${elapsed}ms</div>` : ''}
    `;
    
    output.appendChild(entry);
    // Scroll after DOM update
    requestAnimationFrame(() => output.scrollTop = output.scrollHeight);
}

function formatResult(result) {
    if (result === undefined) return '<span style="color: #888;">undefined</span>';
    if (result === null) return '<span style="color: #888;">null</span>';
    
    if (typeof result === 'bigint') {
        return `<span style="color: #fbbf24;">${result.toString()}</span>`;
    }
    
    if (Array.isArray(result)) {
        if (result.length === 0) return '[]';
        const items = result.map(item => formatResultValue(item));
        return `[<br>  ${items.join(',<br>  ')}<br>]`;
    }
    
    if (typeof result === 'object') {
        return formatObject(result);
    }
    
    return escapeHtml(String(result));
}

function formatResultValue(value) {
    if (typeof value === 'bigint') {
        return `<span style="color: #fbbf24;">${value.toString()}</span>`;
    }
    if (typeof value === 'string') {
        return `<span style="color: #86efac;">"${escapeHtml(value)}"</span>`;
    }
    if (typeof value === 'object' && value !== null) {
        return formatObject(value);
    }
    return escapeHtml(String(value));
}

function formatObject(obj) {
    const entries = Object.entries(obj).map(([k, v]) => {
        const formattedValue = typeof v === 'bigint' 
            ? `<span style="color: #fbbf24;">${v.toString()}</span>`
            : typeof v === 'string'
            ? `<span style="color: #86efac;">"${escapeHtml(v)}"</span>`
            : typeof v === 'object' && v !== null
            ? formatObject(v)
            : escapeHtml(String(v));
        return `<span style="color: #93c5fd;">${escapeHtml(k)}</span>: ${formattedValue}`;
    });
    return `{ ${entries.join(', ')} }`;
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

// =============================================================================
// Visualization Functions
// =============================================================================

function setupVisualization() {
    const container = document.getElementById('visualization-tab');
    svg = d3.select('#graphCanvas');
    
    // Set up zoom behavior
    zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
        });
    
    svg.call(zoom);
    
    // Create main group for transformations
    g = svg.append('g');
    
    // Create groups for links and nodes (links first so nodes are on top)
    g.append('g').attr('class', 'links');
    g.append('g').attr('class', 'nodes');
    
    // Handle window resize
    window.addEventListener('resize', updateVisualizationSize);
    updateVisualizationSize();
}

function updateVisualizationSize() {
    const container = document.getElementById('visualization-tab');
    const rect = container.getBoundingClientRect();
    svg.attr('width', rect.width).attr('height', rect.height);
}

function refreshVisualization() {
    updateVisualizationSize();
    
    // Get graph data
    graphData = getGraphData();
    
    if (graphData.nodes.length === 0) {
        // Show empty state - just clear the visualization
        g.select('.links').selectAll('*').remove();
        g.select('.nodes').selectAll('*').remove();
        return;
    }
    
    // Create force simulation
    const width = parseInt(svg.attr('width'));
    const height = parseInt(svg.attr('height'));
    
    simulation = d3.forceSimulation(graphData.nodes)
        .force('link', d3.forceLink(graphData.links).id(d => d.id).distance(100))
        .force('charge', d3.forceManyBody().strength(-300))
        .force('center', d3.forceCenter(width / 2, height / 2))
        .force('collision', d3.forceCollide().radius(40));
    
    // Draw links
    const linkGroup = g.select('.links');
    linkGroup.selectAll('*').remove();
    
    const link = linkGroup.selectAll('line')
        .data(graphData.links)
        .join('line')
        .attr('class', d => `link edge-${d.label || 'default'}`)
        .attr('stroke-width', 2)
        .on('click', (event, d) => {
            event.stopPropagation();
            selectElement('edge', d);
        });
    
    // Draw link labels
    const linkLabel = linkGroup.selectAll('text')
        .data(graphData.links)
        .join('text')
        .attr('class', 'link-label')
        .text(d => d.label);
    
    // Draw nodes
    const nodeGroup = g.select('.nodes');
    nodeGroup.selectAll('*').remove();
    
    const node = nodeGroup.selectAll('g')
        .data(graphData.nodes)
        .join('g')
        .attr('class', 'node')
        .call(d3.drag()
            .on('start', dragstarted)
            .on('drag', dragged)
            .on('end', dragended))
        .on('click', (event, d) => {
            event.stopPropagation();
            selectElement('node', d);
        });
    
    node.append('circle')
        .attr('r', 20)
        .attr('class', d => `node-${d.label || 'default'}`);
    
    node.append('text')
        .attr('dy', 4)
        .attr('text-anchor', 'middle')
        .text(d => d.name || d.id.toString().slice(0, 6));
    
    // Update positions on each tick
    simulation.on('tick', () => {
        link
            .attr('x1', d => d.source.x)
            .attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x)
            .attr('y2', d => d.target.y);
        
        linkLabel
            .attr('x', d => (d.source.x + d.target.x) / 2)
            .attr('y', d => (d.source.y + d.target.y) / 2);
        
        node.attr('transform', d => `translate(${d.x},${d.y})`);
    });
    
    // Click on background to deselect
    svg.on('click', () => {
        deselectAll();
    });
    
    // Fit to view after layout stabilizes
    setTimeout(fitGraphToView, 500);
}

function getGraphData() {
    const nodes = [];
    const links = [];
    const nodeMap = new Map();
    
    try {
        // Get all vertices
        const vertices = graph.V().elementMap().toList();
        vertices.forEach(v => {
            // Convert ID to string for D3 consistency
            const nodeId = v.id.toString();
            const node = {
                id: nodeId,
                rawId: v.id,  // Keep original bigint for operations
                label: v.label,
                name: v.name || v.label,
                properties: { ...v }
            };
            delete node.properties.id;
            delete node.properties.label;
            nodes.push(node);
            nodeMap.set(nodeId, node);
        });
        
        // Get all edges
        const edges = graph.E().toList();
        edges.forEach(edgeId => {
            const edge = graph.getEdge(edgeId);
            if (edge && nodeMap.has(edge.from.toString()) && nodeMap.has(edge.to.toString())) {
                links.push({
                    id: edge.id.toString(),
                    rawId: edge.id,  // Keep original bigint for operations
                    source: edge.from.toString(),
                    target: edge.to.toString(),
                    label: edge.label,
                    properties: edge.properties
                });
            }
        });
    } catch (e) {
        console.error('Error getting graph data:', e);
    }
    
    return { nodes, links };
}

function fitGraphToView() {
    if (graphData.nodes.length === 0) return;
    
    const width = parseInt(svg.attr('width'));
    const height = parseInt(svg.attr('height'));
    
    // Calculate bounds
    let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
    graphData.nodes.forEach(node => {
        if (node.x !== undefined) {
            minX = Math.min(minX, node.x);
            minY = Math.min(minY, node.y);
            maxX = Math.max(maxX, node.x);
            maxY = Math.max(maxY, node.y);
        }
    });
    
    if (minX === Infinity) return;
    
    const padding = 60;
    const graphWidth = maxX - minX + padding * 2;
    const graphHeight = maxY - minY + padding * 2;
    
    const scale = Math.min(
        width / graphWidth,
        height / graphHeight,
        1.5 // Max zoom
    ) * 0.9;
    
    const centerX = (minX + maxX) / 2;
    const centerY = (minY + maxY) / 2;
    
    svg.transition().duration(500).call(
        zoom.transform,
        d3.zoomIdentity
            .translate(width / 2, height / 2)
            .scale(scale)
            .translate(-centerX, -centerY)
    );
}

function dragstarted(event, d) {
    if (!event.active) simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
}

function dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
}

function dragended(event, d) {
    if (!event.active) simulation.alphaTarget(0);
    d.fx = null;
    d.fy = null;
}

function selectElement(type, data) {
    deselectAll();
    
    selectedElement = { type, data };
    
    // Highlight selected element
    if (type === 'node') {
        g.selectAll('.node').filter(d => d.id === data.id).classed('selected', true);
    } else {
        g.selectAll('.link').filter(d => d.id === data.id).classed('selected', true);
    }
    
    showPropertiesPanel(type, data);
}

function deselectAll() {
    selectedElement = null;
    g.selectAll('.selected').classed('selected', false);
    hidePropertiesPanel();
}

function showPropertiesPanel(type, data) {
    const panel = document.getElementById('propertiesPanel');
    const title = document.getElementById('propTitle');
    const content = document.getElementById('propContent');
    
    title.textContent = type === 'node' ? `${data.label} Vertex` : `${data.label} Edge`;
    
    let html = `
        <div class="property-row">
            <span class="property-key">ID</span>
            <span class="property-value">${data.id}</span>
        </div>
        <div class="property-row">
            <span class="property-key">Label</span>
            <span class="property-value">${data.label}</span>
        </div>
    `;
    
    if (type === 'edge') {
        html += `
            <div class="property-row">
                <span class="property-key">From</span>
                <span class="property-value">${data.source.id || data.source}</span>
            </div>
            <div class="property-row">
                <span class="property-key">To</span>
                <span class="property-value">${data.target.id || data.target}</span>
            </div>
        `;
    }
    
    const props = data.properties || {};
    Object.entries(props).forEach(([key, value]) => {
        if (key !== 'id' && key !== 'label') {
            html += `
                <div class="property-row">
                    <span class="property-key">${key}</span>
                    <span class="property-value">${formatPropertyValue(value)}</span>
                </div>
            `;
        }
    });
    
    content.innerHTML = html;
    panel.classList.add('visible');
}

function hidePropertiesPanel() {
    document.getElementById('propertiesPanel').classList.remove('visible');
}

function formatPropertyValue(value) {
    if (typeof value === 'bigint') return value.toString() + 'n';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
}

function deleteSelected() {
    if (!selectedElement) return;
    
    try {
        // Use rawId (bigint) for graph operations, id (string) for display
        const rawId = selectedElement.data.rawId;
        if (selectedElement.type === 'node') {
            graph.V_(rawId).drop().iterate();
            log(`Deleted vertex ${selectedElement.data.id}`, 'info');
        } else {
            graph.E_(rawId).drop().iterate();
            log(`Deleted edge ${selectedElement.data.id}`, 'info');
        }
        
        updateStats();
        refreshVisualization();
        hidePropertiesPanel();
        selectedElement = null;
        
    } catch (e) {
        log(`Error deleting: ${e.message}`, 'error');
    }
}

// =============================================================================
// Graph Operations
// =============================================================================

function updateStats() {
    if (!graph) return;
    document.getElementById('vertexCount').textContent = graph.vertexCount().toString();
    document.getElementById('edgeCount').textContent = graph.edgeCount().toString();
}

function createSampleGraph() {
    try {
        // Create people - addVertex returns bigint directly
        const alice = graph.addVertex('person', { name: 'Alice', age: 30n });
        const bob = graph.addVertex('person', { name: 'Bob', age: 25n });
        const carol = graph.addVertex('person', { name: 'Carol', age: 35n });
        const david = graph.addVertex('person', { name: 'David', age: 28n });
        const eve = graph.addVertex('person', { name: 'Eve', age: 22n });
        
        // Create products
        const laptop = graph.addVertex('product', { name: 'Laptop', price: 1200n });
        const phone = graph.addVertex('product', { name: 'Phone', price: 800n });
        
        // Create relationships - IDs are already bigint, no need to wrap
        graph.addEdge(alice, bob, 'knows', { since: 2020n });
        graph.addEdge(alice, carol, 'knows', { since: 2019n });
        graph.addEdge(bob, david, 'knows', { since: 2021n });
        graph.addEdge(carol, eve, 'knows', { since: 2022n });
        graph.addEdge(david, alice, 'knows', { since: 2020n });
        
        // Create purchases
        graph.addEdge(alice, laptop, 'purchased', { date: '2023-01-15' });
        graph.addEdge(bob, phone, 'purchased', { date: '2023-02-20' });
        graph.addEdge(carol, laptop, 'purchased', { date: '2023-03-10' });
        
        updateStats();
        refreshVisualization();
        
        log('Sample graph created: 7 vertices, 8 edges', 'success');
        
    } catch (e) {
        log('Error creating sample graph: ' + e.message, 'error');
        console.error(e);
    }
}

// =============================================================================
// Start Application
// =============================================================================

main();
