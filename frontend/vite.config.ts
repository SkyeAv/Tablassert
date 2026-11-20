import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vite";
import solidPlugin from "vite-plugin-solid";

export default defineConfig({
  plugins: [solidPlugin(), tailwindcss()],
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8765",
    },
  },
  build: {
    outDir: "../src/tablassert/_webui_dist",
    emptyOutDir: true,
    target: "esnext",
  },
});
