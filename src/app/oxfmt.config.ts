import { defineConfig } from "oxfmt";

export default defineConfig({
  ignorePatterns: ["src/components/ui/**"],
  printWidth: 120,
  sortImports: true,
  sortTailwindcss: true,
});
