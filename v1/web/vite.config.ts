import { defineConfig } from "vite";

export default defineConfig({
  build: { target: "es2022", outDir: "dist", sourcemap: true },
  server: {
    proxy: {
      // The dev server proxies to the gateway so the browser sees one
      // origin -- otherwise the WebSocket upgrade is a cross-origin
      // request and the cookie story gets complicated for no benefit.
      "/stream": { target: "ws://localhost:8080", ws: true },
      "/stats": "http://localhost:8080",
    },
  },
});
