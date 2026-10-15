import { defineConfig } from 'tsup';

export default defineConfig([
  {
    entry: ['src/index.ts'],
    format: ['esm', 'cjs'],
    dts: true,
    sourcemap: true,
    clean: true,
    splitting: false,
  },
  {
    // Plain <script> use: window.AgenticGraphStream. Output dist/index.global.js.
    entry: ['src/index.ts'],
    format: ['iife'],
    globalName: 'AgenticGraphStream',
    minify: true,
    sourcemap: true,
    platform: 'browser',
    target: 'es2020',
  },
]);
