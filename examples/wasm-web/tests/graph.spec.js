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
        
        // Wait for the status to show "Ready!"
        await expect(page.locator('#status')).toHaveText('Ready!', { timeout: 10000 });
    });

    test('should initialize WASM module successfully', async ({ page }) => {
        // Verify the status shows ready
        const status = page.locator('#status');
        await expect(status).toHaveClass(/ready/);
        await expect(status).toHaveText('Ready!');
        
        // Verify initial stats are 0
        await expect(page.locator('#vertexCount')).toHaveText('0');
        await expect(page.locator('#edgeCount')).toHaveText('0');
    });

    test('should create sample graph successfully', async ({ page }) => {
        // Click the Create Sample Graph button
        await page.click('#btnCreateSample');
        
        // Wait for the action output to show success
        const actionOutput = page.locator('#actionOutput');
        await expect(actionOutput).toContainText('Sample graph created successfully!', { timeout: 5000 });
        
        // Verify the stats updated
        await expect(page.locator('#vertexCount')).toHaveText('7');
        await expect(page.locator('#edgeCount')).toHaveText('8');
        
        // Check the console output for success messages
        const consoleOutput = page.locator('#consoleOutput');
        await expect(consoleOutput).toContainText('Created 5 people');
        await expect(consoleOutput).toContainText('Created 2 products');
    });

    test('should add person vertex', async ({ page }) => {
        // Click Add Person button
        await page.click('#btnAddPerson');
        
        // Verify the output shows the added person
        const output = page.locator('#addVertexOutput');
        await expect(output).toContainText('Added person vertex');
        await expect(output).toContainText('Name:');
        
        // Verify stats updated
        await expect(page.locator('#vertexCount')).toHaveText('1');
    });

    test('should add product vertex', async ({ page }) => {
        // Click Add Product button
        await page.click('#btnAddProduct');
        
        // Verify the output shows the added product
        const output = page.locator('#addVertexOutput');
        await expect(output).toContainText('Added product vertex');
        await expect(output).toContainText('Name:');
        await expect(output).toContainText('Price:');
        
        // Verify stats updated
        await expect(page.locator('#vertexCount')).toHaveText('1');
    });

    test('should query person names after creating sample graph', async ({ page }) => {
        // First create the sample graph
        await page.click('#btnCreateSample');
        await expect(page.locator('#actionOutput')).toContainText('Sample graph created successfully!');
        
        // Query person names
        await page.click('#btnGetPersonNames');
        
        // Verify the output contains person names
        const output = page.locator('#traversalOutput');
        await expect(output).toContainText('Alice');
        await expect(output).toContainText('Bob');
        await expect(output).toContainText('Carol');
    });

    test('should find friends of Alice', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#actionOutput')).toContainText('Sample graph created successfully!');
        
        // Query Alice's friends
        await page.click('#btnGetFriends');
        
        // Verify the output
        const output = page.locator('#traversalOutput');
        await expect(output).toContainText("Alice's friends");
        // Alice knows Bob and Carol in the sample graph
        await expect(output).toContainText('Bob');
        await expect(output).toContainText('Carol');
    });

    test('should filter by age predicate', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#actionOutput')).toContainText('Sample graph created successfully!');
        
        // Filter by age >= 25
        await page.click('#btnFilterAge');
        
        // Verify the output contains people 25+
        const output = page.locator('#predicateOutput');
        await expect(output).toContainText('People aged 25+');
        // Alice (30), Bob (25), Carol (35), David (28) are 25+, Eve (22) is not
        await expect(output).toContainText('Alice');
        await expect(output).toContainText('Bob');
        await expect(output).toContainText('Carol');
        await expect(output).toContainText('David');
        // Eve should NOT be in the results
        await expect(output).not.toContainText('Eve');
    });

    test('should filter by name predicate', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#actionOutput')).toContainText('Sample graph created successfully!');
        
        // Filter by name starting with 'A'
        await page.click('#btnFilterName');
        
        // Verify the output
        const output = page.locator('#predicateOutput');
        await expect(output).toContainText("names starting with 'A'");
        await expect(output).toContainText('Alice');
        // Bob, Carol, David, Eve should NOT be in the results
        await expect(output).not.toContainText('Bob');
    });

    test('should navigate outgoing edges', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#actionOutput')).toContainText('Sample graph created successfully!');
        
        // Navigate outgoing 'knows' edges
        await page.click('#btnOutgoing');
        
        // Verify the output contains targets of 'knows' relationships
        const output = page.locator('#navigationOutput');
        await expect(output).toContainText('outgoing');
    });

    test('should count vertices by label', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#actionOutput')).toContainText('Sample graph created successfully!');
        
        // Count by label
        await page.click('#btnCountByLabel');
        
        // Verify the counts
        const output = page.locator('#traversalOutput');
        await expect(output).toContainText('person: 5');
        await expect(output).toContainText('product: 2');
    });

    test('should clear graph by creating new instance', async ({ page }) => {
        // Create sample graph first
        await page.click('#btnCreateSample');
        await expect(page.locator('#vertexCount')).toHaveText('7');
        
        // Clear the graph
        await page.click('#btnClearGraph');
        
        // Verify stats are back to 0
        await expect(page.locator('#vertexCount')).toHaveText('0');
        await expect(page.locator('#edgeCount')).toHaveText('0');
    });
});
