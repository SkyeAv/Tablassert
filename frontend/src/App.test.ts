import { describe, expect, test } from "bun:test";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

describe("Tablassert Web UI build", () => {
  test("package.json pins SolidJS 2.x and Tailwind v4", () => {
    const pkg = JSON.parse(readFileSync(join(import.meta.dir, "..", "package.json"), "utf8")) as {
      dependencies: Record<string, string>;
      devDependencies: Record<string, string>;
    };
    expect(pkg.dependencies["solid-js"]).toMatch(/^\^?2\./);
    expect(pkg.dependencies["@solidjs/web"]).toMatch(/^\^?2\./);
    expect(pkg.devDependencies["@tailwindcss/vite"]).toMatch(/^\^?4\./);
  });

  test("built static bundle exists for FastAPI serving", () => {
    const distDir = join(import.meta.dir, "..", "..", "src", "tablassert", "_webui_dist");
    expect(existsSync(join(distDir, "index.html"))).toBe(true);
  });
});
