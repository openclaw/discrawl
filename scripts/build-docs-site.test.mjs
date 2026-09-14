import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const builder = fileURLToPath(new URL("./build-docs-site.mjs", import.meta.url));

test("documentation identity is independent of the checkout name", (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "renamed-discrawl-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const docs = path.join(root, "docs");
  fs.mkdirSync(path.join(docs, "commands"), { recursive: true });
  fs.mkdirSync(path.join(docs, "guides"));
  fs.writeFileSync(path.join(docs, "README.md"), "# Discrawl\n\nSynthetic documentation.\n");
  fs.copyFileSync(new URL("../docs/social-card.png", import.meta.url), path.join(docs, "social-card.png"));

  execFileSync(process.execPath, [builder], { cwd: root, timeout: 30_000 });
  const index = fs.readFileSync(path.join(root, "dist/docs-site/llms.txt"), "utf8");
  assert.match(index, /^# discrawl\n\ndiscrawl documentation index\.\n/);
  assert.ok(index.includes("https://discrawl.sh/"));
  assert.ok(index.includes("Source: https://github.com/openclaw/discrawl"));
  assert.ok(!index.includes(path.basename(root)));
});
