import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';

// Serves the page with the SDK straight from ../../src.
export default defineConfig({
  resolve: { alias: { '@openagentlink/client': fileURLToPath(new URL('../../src/index.ts', import.meta.url)) } },
});
