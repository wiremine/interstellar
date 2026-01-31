// @ts-check
const { test, expect } = require('@playwright/test');

// Configure the base URL for the tests
test.use({
    baseURL: 'http://localhost:8080',
});

test.describe('Interstellar WASM Graph Database', () => {
    test.beforeEach(async ({ page }) => {
        // Navigate to the page and wait for WASM to initialize
        await page.goto('/');
        
        // Wait for the status to show "Ready"
        await expect(page.locator('#status')).toHaveText('Ready', { timeout: 10000 });
    });

    test('should initialize WASM module successfully', async ({ page }) => {
        // Verify the status shows ready
        const status = page.locator('#status');
        await expect(status).toHaveClass(/ready/);
        await expect(status).toHaveText('Ready');
        
        // Verify initial stats are 0
        await expect(page.locator('#vertexCount')).toHaveText('0');
        await expect(page.locator('#edgeCount')).toHaveText('0');
    });

    test('should create sample graph successfully', async ({ page }) => {
        // Click the Create Sample Graph button
        await page.click('#btnCreateSample');
        
        // Wait for stats to update - this confirms the graph was created
        await expect(page.locator('#vertexCount')).toHaveText('7', { timeout: 5000 });
        await expect(page.locator('#edgeCount')).toHaveText('8');
    });

    test('should run query via console', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#vertexCount')).toHaveText('7');
        
        // Enter a query in the console
        const queryInput = page.locator('#queryInput');
        await queryInput.fill("graph.V().hasLabel('person').values('name').toList()");
        
        // Run the query
        await page.click('#btnRunQuery');
        
        // Verify the output contains person names
        const output = page.locator('#consoleOutput');
        await expect(output).toContainText('Alice');
        await expect(output).toContainText('Bob');
        await expect(output).toContainText('Carol');
    });

    test('should run query with keyboard shortcut', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#vertexCount')).toHaveText('7');
        
        // Enter a query in the console
        const queryInput = page.locator('#queryInput');
        await queryInput.fill("graph.V().toCount()");
        
        // Run with Enter (REPL-style)
        await queryInput.press('Enter');
        
        // Verify the output shows the query and result (without 'n' suffix)
        const output = page.locator('#consoleOutput');
        await expect(output).toContainText('graph.V().toCount()');
        // The last output entry should contain just "7" as the result
        await expect(output.locator('.output-entry').last().locator('.output-result')).toHaveText('7');
    });

    test('should use query examples dropdown', async ({ page }) => {
        // Select an example from the dropdown
        const dropdown = page.locator('#queryExamples');
        await dropdown.selectOption({ label: 'Count vertices' });
        
        // Verify the query was inserted
        const queryInput = page.locator('#queryInput');
        await expect(queryInput).toHaveValue('graph.V().toCount()');
    });

    test('should filter by age predicate', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#vertexCount')).toHaveText('7');
        
        // Run age filter query
        const queryInput = page.locator('#queryInput');
        await queryInput.fill("graph.V().hasLabel('person').hasWhere('age', P.gte(25n)).values('name').toList()");
        await page.click('#btnRunQuery');
        
        // Verify the output contains people 25+
        const output = page.locator('#consoleOutput');
        await expect(output).toContainText('Alice');
        await expect(output).toContainText('Bob');
        await expect(output).toContainText('Carol');
        await expect(output).toContainText('David');
        // Eve (22) should NOT be in the results
        await expect(output).not.toContainText('"Eve"');
    });

    test('should navigate to visualization tab', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#vertexCount')).toHaveText('7');
        
        // Click on visualization tab
        await page.click('.tab[data-tab="visualization"]');
        
        // Verify the visualization tab is active
        await expect(page.locator('#visualization-tab')).toHaveClass(/active/);
        
        // Verify the graph canvas is visible
        await expect(page.locator('#graphCanvas')).toBeVisible();
        
        // Verify nodes are rendered (D3 creates g.node elements)
        // Wait longer for D3 to render
        await expect(page.locator('#graphCanvas .node')).toHaveCount(7, { timeout: 10000 });
    });

    test('should clear graph', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#vertexCount')).toHaveText('7');
        
        // Clear the graph
        await page.click('#btnClearGraph');
        
        // Verify stats are back to 0
        await expect(page.locator('#vertexCount')).toHaveText('0');
        await expect(page.locator('#edgeCount')).toHaveText('0');
    });

    test('should clear console output', async ({ page }) => {
        // Add some output by running a query
        const queryInput = page.locator('#queryInput');
        await queryInput.fill("graph.V().toCount()");
        await page.click('#btnRunQuery');
        
        // Verify there's query output
        const output = page.locator('#consoleOutput');
        await expect(output).toContainText('toCount');
        
        // Clear output
        await page.click('#btnClearOutput');
        
        // Verify it's cleared (empty or just whitespace)
        await expect(output).toBeEmpty();
    });

    test('should handle query errors gracefully', async ({ page }) => {
        // Enter an invalid query
        const queryInput = page.locator('#queryInput');
        await queryInput.fill('invalidFunction()');
        await page.click('#btnRunQuery');
        
        // Verify error is displayed
        const output = page.locator('#consoleOutput');
        await expect(output).toContainText('Error');
    });

    test('should add vertex via mutation query', async ({ page }) => {
        // Run addV mutation
        const queryInput = page.locator('#queryInput');
        await queryInput.fill("graph.V().addV('test').property('name', 'TestNode').toList()");
        await page.click('#btnRunQuery');
        
        // Verify vertex was created
        await expect(page.locator('#vertexCount')).toHaveText('1');
    });
});
