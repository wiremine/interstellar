#!/bin/bash
# Build WASM packages for browser and Node.js targets
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "Building WASM packages..."

# Clean previous builds
rm -rf pkg pkg-web pkg-node

# Build for web (ES modules, browsers)
echo "  Building for web target..."
wasm-pack build --target web --features wasm
mv pkg pkg-web

# Add proper package.json for web
cat > pkg-web/package.json << 'PKGJSON'
{
  "name": "@interstellar/graph-web",
  "version": "0.1.0",
  "description": "Interstellar graph database - Browser/ESM build",
  "type": "module",
  "main": "./interstellar.js",
  "types": "./interstellar.d.ts",
  "files": ["interstellar_bg.wasm", "interstellar.js", "interstellar.d.ts", "interstellar_bg.wasm.d.ts"],
  "sideEffects": false
}
PKGJSON

# Build for Node.js (CommonJS)
echo "  Building for Node.js target..."
wasm-pack build --target nodejs --features wasm
mv pkg pkg-node

# Add proper package.json for node
cat > pkg-node/package.json << 'PKGJSON'
{
  "name": "@interstellar/graph-node",
  "version": "0.1.0",
  "description": "Interstellar graph database - Node.js/CommonJS build",
  "main": "./interstellar.js",
  "types": "./interstellar.d.ts",
  "files": ["interstellar_bg.wasm", "interstellar.js", "interstellar.d.ts", "interstellar_bg.wasm.d.ts"],
  "engines": { "node": ">=18.0.0" }
}
PKGJSON

# Create symlink for default 'pkg' pointing to web (most common use case)
ln -sf pkg-web pkg

echo ""
echo "Build complete!"
echo ""
echo "  pkg-web/  - Browser/ESM build (also accessible via pkg/)"
echo "  pkg-node/ - Node.js/CommonJS build"
echo ""
echo "Usage:"
echo "  Browser: import { Graph } from './pkg-web/interstellar.js'"
echo "  Node.js: const { Graph } = require('./pkg-node/interstellar.js')"
