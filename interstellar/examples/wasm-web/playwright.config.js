// @ts-check
const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
    testDir: './tests',
    fullyParallel: true,
    forbidOnly: !!process.env.CI,
    retries: process.env.CI ? 2 : 0,
    workers: process.env.CI ? 1 : undefined,
    reporter: 'html',
    
    use: {
        baseURL: 'http://localhost:8080',
        trace: 'on-first-retry',
    },

    projects: [
        {
            name: 'chromium',
            use: { ...devices['Desktop Chrome'] },
        },
    ],

    // Start the local server before running tests
    webServer: {
        command: 'python3 -m http.server 8080',
        url: 'http://localhost:8080',
        reuseExistingServer: !process.env.CI,
        timeout: 120 * 1000,
    },
});
