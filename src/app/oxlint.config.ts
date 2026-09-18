import { defineConfig } from "oxlint";

export default defineConfig({
  ignorePatterns: ["src/components/ui/**"],
  plugins: ["react"],
  rules: {
    "sort-keys": "allow",
  },
});
