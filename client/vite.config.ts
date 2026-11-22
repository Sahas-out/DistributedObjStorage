import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

// The GUI talks only to the controller. In development Vite forwards the
// controller's paths to it, so the browser sees one origin and the controller
// needs no CORS headers. Workers are never contacted directly.
export default defineConfig(({ mode }) => {
  const env = { ...process.env, ...loadEnv(mode, process.cwd(), "") };
  const target = env.CONTROLLER_URL || "http://localhost:8080";
  const proxied = { target, changeOrigin: true };
  return {
    plugins: [react()],
    build: { chunkSizeWarningLimit: 1000 },
    server: {
      host: "localhost",
      port: Number(env.PORT) || 5173,
      proxy: {
        "/api": proxied,
        "/dashboard": proxied,
        "/healthz": proxied,
      },
    },
    preview: {
      host: "localhost",
      port: 4173,
      proxy: {
        "/api": proxied,
        "/dashboard": proxied,
        "/healthz": proxied,
      },
    },
  };
});
