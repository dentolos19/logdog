import { cloudflare } from "@cloudflare/vite-plugin";
import tailwindcss from "@tailwindcss/vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const config = defineConfig({
  server: {
    port: 3000,
    strictPort: true,
  },
  resolve: {
    tsconfigPaths: true,
  },
  plugins: [
    cloudflare({
      viteEnvironment: {
        name: "ssr",
      },
    }),
    tanstackStart(),
    viteReact(),
    tailwindcss(),
  ],
});

export default config;
