#!/usr/bin/env node
import fs from "node:fs";
import path from "node:path";

const root = process.cwd();
const docsDir = path.join(root, "docs");
const outDir = path.join(root, "dist", "docs-site");
const repoEditBase = "https://github.com/openclaw/discrawl/edit/main/docs";
const siteUrl = "https://discrawl.sh";
const brewInstall = "brew install openclaw/tap/discrawl";

const sections = [
  ["Start", ["README.md", "install.md", "configuration.md", "bot-setup.md", "security.md", "CHANGELOG.md", "contact.md"]],
  ["Guides", rels("guides")],
  ["Commands", rels("commands")],
];

fs.rmSync(outDir, { recursive: true, force: true });
fs.mkdirSync(outDir, { recursive: true });

const pages = allMarkdown(docsDir).map((file) => {
  const rel = path.relative(docsDir, file).replaceAll(path.sep, "/");
  const markdown = fs.readFileSync(file, "utf8");
  const title = firstHeading(markdown) || titleize(path.basename(rel, ".md"));
  return { file, rel, title, outRel: outPath(rel), markdown };
});

const pageMap = new Map(pages.map((page) => [page.rel, page]));
const nav = sections
  .map(([name, rels]) => ({
    name,
    pages: rels.map((rel) => pageMap.get(rel)).filter(Boolean),
  }))
  .filter((section) => section.pages.length);

const sectionByRel = new Map();
for (const section of nav) for (const page of section.pages) sectionByRel.set(page.rel, section.name);
const orderedPages = nav.flatMap((s) => s.pages);

for (const page of pages) {
  const html = markdownToHtml(page.markdown, page.rel);
  const toc = tocFromHtml(html);
  const idx = orderedPages.findIndex((p) => p.rel === page.rel);
  const prev = idx > 0 ? orderedPages[idx - 1] : null;
  const next = idx >= 0 && idx < orderedPages.length - 1 ? orderedPages[idx + 1] : null;
  const sectionName = sectionByRel.get(page.rel) || "Discrawl docs";
  const pageOut = path.join(outDir, page.outRel);
  fs.mkdirSync(path.dirname(pageOut), { recursive: true });
  fs.writeFileSync(pageOut, layout({ page, html, toc, prev, next, sectionName }), "utf8");
}

fs.writeFileSync(path.join(outDir, "discrawl.svg"), discrawlSvg(), "utf8");
fs.copyFileSync(path.join(docsDir, "social-card.png"), path.join(outDir, "social-card.png"));
fs.writeFileSync(path.join(outDir, "CNAME"), "discrawl.sh\n", "utf8");
fs.writeFileSync(path.join(outDir, ".nojekyll"), "", "utf8");
fs.writeFileSync(path.join(outDir, "llms.txt"), llmsTxt(), "utf8");
console.log(`built docs site: ${path.relative(root, outDir)}`);

function llmsTxt() {
  const origin = docsOrigin();
  const source = docsSourceUrl();
  const name = typeof productName !== "undefined" ? productName : path.basename(root);
  const description = typeof productDescription !== "undefined" ? productDescription : `${name} documentation index.`;
  const install = docsInstallHint();
  const docPages = docsLlmsPages().map((page) => `- ${page.title}: ${pageUrl(origin, page.outRel)}`);
  const lines = [
    `# ${name}`,
    "",
    description,
    "",
    "Canonical documentation:",
    ...docPages,
  ];
  if (install) {
    lines.push("", "Install:", `- ${install}`);
  }
  if (source) {
    lines.push("", `Source: ${source}`);
  }
  lines.push("", "Guidance for agents:", "- Prefer the canonical documentation URLs above over README excerpts or package metadata.", "- Fetch only the pages needed for the current task; this is an index, not a full-site corpus.");
  return `${lines.join("\n")}\n`;
}

function docsLlmsPages() {
  const seen = new Set();
  const ordered = typeof orderedPages !== "undefined" ? orderedPages : [];
  return [...ordered, ...pages].filter((page) => page.outRel && !seen.has(page.outRel) && seen.add(page.outRel));
}

function docsOrigin() {
  const value =
    (typeof siteBase !== "undefined" && siteBase) ||
    (typeof siteUrl !== "undefined" && siteUrl) ||
    (typeof customDomain !== "undefined" && customDomain ? `https://${customDomain}` : "");
  return value.replace(/\/$/, "");
}

function docsSourceUrl() {
  if (typeof repoBase !== "undefined") return repoBase;
  if (typeof repoUrl !== "undefined") return repoUrl;
  if (typeof repoEditBase !== "undefined") return repoEditBase.replace(/\/edit\/main\/docs\/?$/, "");
  return "";
}

function docsInstallHint() {
  if (typeof installCommand !== "undefined") return installCommand;
  if (typeof installLine !== "undefined") return installLine;
  if (typeof installCmd !== "undefined") return installCmd;
  if (typeof installSnippet !== "undefined") return installSnippet;
  if (typeof brewInstall !== "undefined") return brewInstall;
  return "";
}

function pageUrl(origin, outRel) {
  const normalized = outRel === "index.html" ? "" : outRel.replace(/(?:^|\/)index\.html$/, (match) => match === "index.html" ? "" : "/");
  if (!origin) return normalized || "index.html";
  return normalized ? `${origin}/${normalized}` : `${origin}/`;
}

function rels(dir) {
  const full = path.join(docsDir, dir);
  if (!fs.existsSync(full)) return [];
  return fs
    .readdirSync(full)
    .filter((name) => name.endsWith(".md"))
    .sort((a, b) => (a === "README.md" ? -1 : b === "README.md" ? 1 : a.localeCompare(b)))
    .map((name) => `${dir}/${name}`);
}

function allMarkdown(dir) {
  return fs
    .readdirSync(dir, { withFileTypes: true })
    .flatMap((entry) => {
      const full = path.join(dir, entry.name);
      if (entry.isDirectory()) return allMarkdown(full);
      return entry.name.endsWith(".md") ? [full] : [];
    })
    .sort();
}

function outPath(rel) {
  if (rel === "README.md") return "index.html";
  if (rel.endsWith("/README.md")) return rel.replace(/README\.md$/, "index.html");
  return rel.replace(/\.md$/, ".html");
}

function firstHeading(markdown) {
  return markdown.match(/^#\s+(.+)$/m)?.[1]?.trim();
}

function titleize(input) {
  return input.replaceAll("-", " ").replace(/\b\w/g, (m) => m.toUpperCase());
}

function markdownToHtml(markdown, currentRel) {
  const lines = markdown.replace(/\r\n/g, "\n").split("\n");
  const html = [];
  let paragraph = [];
  let list = null;
  let fence = null;

  const flushParagraph = () => {
    if (!paragraph.length) return;
    html.push(`<p>${inline(paragraph.join(" "), currentRel)}</p>`);
    paragraph = [];
  };
  const closeList = () => {
    if (!list) return;
    html.push(`</${list}>`);
    list = null;
  };
  const splitRow = (line) => line.replace(/^\s*\|/, "").replace(/\|\s*$/, "").split("|").map((s) => s.trim());
  const isDivider = (line) => /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)+\|?\s*$/.test(line);

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const fenceMatch = line.match(/^```(\w+)?\s*$/);
    if (fenceMatch) {
      flushParagraph();
      closeList();
      if (fence) {
        html.push(`<pre><code class="language-${fence.lang}">${escapeHtml(fence.lines.join("\n"))}</code></pre>`);
        fence = null;
      } else {
        fence = { lang: fenceMatch[1] || "text", lines: [] };
      }
      continue;
    }
    if (fence) {
      fence.lines.push(line);
      continue;
    }
    if (!line.trim()) {
      flushParagraph();
      closeList();
      continue;
    }
    const heading = line.match(/^(#{1,4})\s+(.+)$/);
    if (heading) {
      flushParagraph();
      closeList();
      const level = heading[1].length;
      const text = heading[2].trim();
      const id = slug(text);
      const inner = inline(text, currentRel);
      if (level === 1) {
        html.push(`<h1 id="${id}">${inner}</h1>`);
      } else {
        html.push(`<h${level} id="${id}"><a class="anchor" href="#${id}" aria-label="Anchor link">#</a>${inner}</h${level}>`);
      }
      continue;
    }
    if (line.trimStart().startsWith("|") && line.includes("|", line.indexOf("|") + 1) && isDivider(lines[i + 1] || "")) {
      flushParagraph();
      closeList();
      const header = splitRow(line);
      const aligns = splitRow(lines[i + 1]).map((cell) => {
        const left = cell.startsWith(":");
        const right = cell.endsWith(":");
        return right && left ? "center" : right ? "right" : left ? "left" : "";
      });
      i += 1;
      const rows = [];
      while (i + 1 < lines.length && lines[i + 1].trimStart().startsWith("|")) {
        i += 1;
        rows.push(splitRow(lines[i]));
      }
      const th = header.map((c, idx) => `<th${aligns[idx] ? ` style="text-align:${aligns[idx]}"` : ""}>${inline(c, currentRel)}</th>`).join("");
      const tb = rows.map((r) => `<tr>${r.map((c, idx) => `<td${aligns[idx] ? ` style="text-align:${aligns[idx]}"` : ""}>${inline(c, currentRel)}</td>`).join("")}</tr>`).join("");
      html.push(`<table><thead><tr>${th}</tr></thead><tbody>${tb}</tbody></table>`);
      continue;
    }
    const bullet = line.match(/^\s*-\s+(.+)$/);
    const numbered = line.match(/^\s*\d+\.\s+(.+)$/);
    if (bullet || numbered) {
      flushParagraph();
      const tag = bullet ? "ul" : "ol";
      if (list && list !== tag) closeList();
      if (!list) {
        list = tag;
        html.push(`<${tag}>`);
      }
      html.push(`<li>${inline((bullet || numbered)[1], currentRel)}</li>`);
      continue;
    }
    paragraph.push(line.trim());
  }
  flushParagraph();
  closeList();
  return html.join("\n");
}

function inline(text, currentRel) {
  const stash = [];
  let out = text.replace(/`([^`]+)`/g, (_, code) => {
    stash.push(`<code>${escapeHtml(code)}</code>`);
    return `\u0000${stash.length - 1}\u0000`;
  });
  out = escapeHtml(out)
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, href) => `<a href="${escapeAttr(rewriteHref(href, currentRel))}">${label}</a>`);
  return out.replace(/\u0000(\d+)\u0000/g, (_, i) => stash[Number(i)]);
}

function rewriteHref(href, currentRel) {
  if (/^(https?:|mailto:|#)/.test(href)) return href;
  const [raw, hash = ""] = href.split("#");
  if (!raw) return `#${hash}`;
  if (!raw.endsWith(".md")) return href;
  const from = path.posix.dirname(currentRel);
  const target = path.posix.normalize(path.posix.join(from, raw));
  let rewritten = outPath(target);
  const currentOut = outPath(currentRel);
  rewritten = path.posix.relative(path.posix.dirname(currentOut), rewritten) || "index.html";
  return `${rewritten}${hash ? `#${hash}` : ""}`;
}

function tocFromHtml(html) {
  const items = [];
  const re = /<h([23]) id="([^"]+)">([\s\S]*?)<\/h[23]>/g;
  let m;
  while ((m = re.exec(html))) {
    const text = m[3]
      .replace(/<a class="anchor"[^>]*>.*?<\/a>/, "")
      .replace(/<[^>]+>/g, "")
      .trim();
    items.push({ level: Number(m[1]), id: m[2], text });
  }
  if (items.length < 2) return "";
  return `<nav class="toc" aria-label="On this page"><h2>On this page</h2>${items
    .map((i) => `<a class="toc-l${i.level}" href="#${i.id}">${escapeHtml(i.text)}</a>`)
    .join("")}</nav>`;
}

function layout({ page, html, toc, prev, next, sectionName }) {
  const depth = page.outRel.split("/").length - 1;
  const rootPrefix = depth ? "../".repeat(depth) : "";
  const canonicalUrl = absolutePageUrl(page);
  const editUrl = `${repoEditBase}/${page.rel}`;
  const isHome = page.rel === "README.md";
  const prevNext = !isHome && (prev || next) ? pageNavHtml(prev, next, rootPrefix) : "";
  const heroBlock = isHome ? landingHero(rootPrefix) : standardHero(page, sectionName, editUrl);
  const articleClass = isHome ? "doc doc-home" : "doc";
  const tocBlock = isHome ? "" : toc;
  return `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${escapeHtml(isHome ? "Discrawl" : `${page.title} - Discrawl`)}</title>
  <meta name="description" content="Discrawl: mirror Discord into local SQLite for offline search and analysis.">
  <link rel="canonical" href="${canonicalUrl}">
  <meta property="og:site_name" content="Discrawl">
  <meta property="og:type" content="website">
  <meta property="og:title" content="${escapeHtml(isHome ? "Discrawl" : `${page.title} - Discrawl`)}">
  <meta property="og:description" content="Mirror Discord into SQLite for local search, SQL, analytics, wiretap DMs, and Git-backed archive snapshots.">
  <meta property="og:url" content="${canonicalUrl}">
  <meta property="og:image" content="${siteUrl}/social-card.png">
  <meta property="og:image:width" content="1200">
  <meta property="og:image:height" content="630">
  <meta property="og:image:alt" content="Discrawl - Discord history, local answers.">
  <meta name="twitter:card" content="summary_large_image">
  <meta name="twitter:title" content="${escapeHtml(isHome ? "Discrawl" : `${page.title} - Discrawl`)}">
  <meta name="twitter:description" content="Mirror Discord into SQLite for local search, SQL, analytics, wiretap DMs, and Git-backed archive snapshots.">
  <meta name="twitter:image" content="${siteUrl}/social-card.png">
  <meta name="theme-color" content="#0c0f14">
  <link rel="icon" href="${rootPrefix}discrawl.svg">
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap" rel="stylesheet">
  <style>${css()}</style>
</head>
<body${isHome ? ' class="home"' : ""}>
  <button class="nav-toggle" type="button" aria-label="Toggle navigation" aria-expanded="false">
    <span aria-hidden="true"></span><span aria-hidden="true"></span><span aria-hidden="true"></span>
  </button>
  <div class="shell">
    <aside class="sidebar">
      <a class="brand" href="${rootPrefix}index.html" aria-label="Discrawl docs home">
        <img src="${rootPrefix}discrawl.svg" alt="">
        <span><strong>discrawl</strong><small>discord -> sqlite</small></span>
      </a>
      <label class="search"><span>filter</span><input id="doc-search" type="search" placeholder="sync, wiretap, search..."></label>
      <nav>${navHtml(page.rel, rootPrefix)}</nav>
      <footer class="side-foot">
        <a href="https://github.com/openclaw/discrawl" rel="noopener">github</a>
        <a href="${rootPrefix}contact.html">contact</a>
      </footer>
    </aside>
    <main>
      ${heroBlock}
      <div class="doc-grid${isHome ? " doc-grid-home" : ""}">
        <article class="${articleClass}">${html}${prevNext}</article>
        ${tocBlock}
      </div>
    </main>
  </div>
  <script>${js()}</script>
</body>
</html>`;
}

function absolutePageUrl(page) {
  if (page.outRel === "index.html") return `${siteUrl}/`;
  return `${siteUrl}/${page.outRel}`;
}

function standardHero(page, sectionName, editUrl) {
  return `<header class="hero">
        <div class="hero-text">
          <p class="eyebrow">${escapeHtml(sectionName.toLowerCase())}</p>
          <h1>${escapeHtml(page.title)}</h1>
        </div>
        <div class="hero-meta">
          <a class="repo" href="https://github.com/openclaw/discrawl" rel="noopener">github</a>
          <a class="edit" href="${escapeAttr(editUrl)}" rel="noopener">edit</a>
        </div>
      </header>`;
}

function landingHero(rootPrefix) {
  const features = [
    ["bot api sync", "Fan out across every guild a bot can see. Channels, threads, members, attachments, mentions, FTS5 - all into one SQLite file."],
    ["desktop wiretap", "Read local Discord Desktop cache for classifiable messages and proven DMs. No user token. No selfbot. Auth tokens never extracted."],
    ["fts + semantic", "<code>unicode61</code> tokenizer for fast literal search. Optional embeddings (OpenAI, Ollama) for semantic and hybrid recall."],
    ["git-backed mirrors", "Publish a sharded NDJSON snapshot to a private repo. Readers <code>subscribe</code>, search offline, and never need a bot token."],
    ["live tail", "Gateway tail keeps the archive warm. Periodic repair sweeps catch anything the live stream missed."],
    ["offline analysis", "<code>digest</code>, <code>analytics</code>, <code>members</code>, raw read-only <code>sql</code> against the local archive."],
  ];
  const cards = features
    .map(([title, body]) => `<article class="feature"><h3>${escapeHtml(title)}</h3><p>${body}</p></article>`)
    .join("");
  return `<header class="hero hero-home">
        <div class="hero-text">
          <p class="eyebrow">discord -> sqlite -> answers</p>
          <h1>Server history you can actually <em>search</em>.</h1>
          <p class="lede">Discrawl mirrors Discord guilds into local SQLite so you can grep, query, and run analytics on org memory without depending on Discord search. Bring a bot token, or read everything offline from a Git snapshot.</p>
          <div class="cta">
            <a class="cta-primary" href="${rootPrefix}install.html">Get started</a>
            <a class="cta-secondary" href="https://github.com/openclaw/discrawl" rel="noopener">View on GitHub</a>
            <div class="home-install" aria-label="Install with Homebrew">
              <span class="prompt" aria-hidden="true">$</span>
              <code>${escapeHtml(brewInstall)}</code>
            </div>
          </div>
        </div>
        <pre class="hero-snippet" aria-hidden="true"><code><span class="prompt">$</span> discrawl init
<span class="comment"># discovered 3 guilds, default = maintainers</span>
<span class="prompt">$</span> discrawl sync --full
<span class="comment"># 312k messages, 14k attachments, fts ready</span>
<span class="prompt">$</span> discrawl search "panic: nil pointer"
<span class="comment"># 23 hits across 5 channels</span></code></pre>
      </header>
      <section class="features" aria-label="Highlights">${cards}</section>`;
}

function pageNavHtml(prev, next, rootPrefix) {
  const cell = (page, dir) => {
    if (!page) return "";
    return `<a class="page-nav-${dir}" href="${rootPrefix}${page.outRel}"><small>${dir === "prev" ? "<- prev" : "next ->"}</small><span>${escapeHtml(page.title)}</span></a>`;
  };
  return `<nav class="page-nav" aria-label="Pager">${cell(prev, "prev")}${cell(next, "next")}</nav>`;
}

function navHtml(currentRel, rootPrefix) {
  return nav
    .map((section) => `<section><h2>${section.name.toLowerCase()}</h2>${section.pages.map((page) => {
      const href = rootPrefix + page.outRel;
      const active = page.rel === currentRel ? " active" : "";
      return `<a class="nav-link${active}" href="${href}">${escapeHtml(page.title)}</a>`;
    }).join("")}</section>`)
    .join("");
}

function css() {
  return `
:root{--bg:#0c0f14;--panel:#11151c;--panel-2:#161b24;--ink:#e6ecf3;--ink-dim:#aab3c1;--muted:#6b7585;--line:#1f2530;--line-soft:#262d39;--cyan:#5fe3d4;--magenta:#f364a2;--amber:#f7c177;--violet:#a594ff;--shadow:0 14px 40px rgba(0,0,0,.45)}
@media (prefers-color-scheme: light){:root{--bg:#f4f6fa;--panel:#ffffff;--panel-2:#f8fafc;--ink:#0f172a;--ink-dim:#3f4a5c;--muted:#64748b;--line:#dde3ec;--line-soft:#e7ecf3;--cyan:#0d9488;--magenta:#c026d3;--amber:#b45309;--violet:#6d28d9;--shadow:0 10px 30px rgba(15,23,42,.08)}}
*{box-sizing:border-box}
html{scroll-behavior:smooth;scroll-padding-top:24px}
body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,system-ui,-apple-system,sans-serif;line-height:1.65;overflow-x:hidden;-webkit-font-smoothing:antialiased;font-feature-settings:"ss01","cv11"}
body:before{content:"";position:fixed;inset:0;pointer-events:none;background:radial-gradient(1200px 600px at 90% -20%,rgba(95,227,212,.08),transparent 60%),radial-gradient(900px 500px at -10% 110%,rgba(243,100,162,.07),transparent 60%);z-index:0}
::selection{background:var(--magenta);color:var(--bg)}
a{color:var(--cyan);text-decoration:none;border-bottom:1px solid transparent;transition:color .15s,border-color .15s}
a:hover{color:var(--magenta);border-bottom-color:var(--magenta)}
.shell{position:relative;z-index:1;display:grid;grid-template-columns:264px minmax(0,1fr);min-height:100vh}

/* sidebar */
.sidebar{position:sticky;top:0;height:100vh;overflow:auto;padding:22px 18px 14px;background:var(--panel);border-right:1px solid var(--line);scrollbar-width:thin;scrollbar-color:var(--line) transparent;display:flex;flex-direction:column}
.sidebar::-webkit-scrollbar{width:6px}
.sidebar::-webkit-scrollbar-thumb{background:var(--line);border-radius:6px}
.brand{display:flex;align-items:center;gap:10px;color:var(--ink);text-decoration:none;border:0;margin-bottom:18px;padding-bottom:14px;border-bottom:1px solid var(--line)}
.brand:hover{color:var(--ink)}
.brand img{width:32px;height:32px;border-radius:7px}
.brand strong{display:block;font-family:"JetBrains Mono",ui-monospace,monospace;font-size:1.05rem;line-height:1;letter-spacing:-.01em;font-weight:700}
.brand small{display:block;color:var(--muted);font-size:.66rem;margin-top:5px;font-family:"JetBrains Mono",ui-monospace,monospace;letter-spacing:.06em}
.search{display:block;margin:0 0 18px}
.search span{display:block;color:var(--muted);font-size:.62rem;font-weight:600;text-transform:uppercase;letter-spacing:.18em;margin-bottom:6px;font-family:"JetBrains Mono",ui-monospace,monospace}
.search input{width:100%;border:1px solid var(--line);background:var(--panel-2);border-radius:6px;padding:8px 10px;font:500 .82rem/1.4 "JetBrains Mono",ui-monospace,monospace;color:var(--ink);outline:none;transition:border-color .15s,box-shadow .15s}
.search input::placeholder{color:var(--muted)}
.search input:focus{border-color:var(--cyan);box-shadow:0 0 0 2px rgba(95,227,212,.15)}
nav{flex:1}
nav section{margin:0 0 18px}
nav h2{font-size:.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:.2em;margin:0 0 6px;font-weight:700;font-family:"JetBrains Mono",ui-monospace,monospace;padding:0 4px}
.nav-link{display:block;color:var(--ink-dim);text-decoration:none;border:0;border-radius:5px;padding:5px 10px;margin:1px 0;font-size:.86rem;line-height:1.4;font-family:"JetBrains Mono",ui-monospace,monospace;transition:background .12s,color .12s}
.nav-link:hover{background:var(--panel-2);color:var(--cyan)}
.nav-link.active{background:linear-gradient(90deg,rgba(95,227,212,.14),rgba(95,227,212,.04));color:var(--cyan);position:relative}
.nav-link.active:before{content:"";position:absolute;left:-1px;top:6px;bottom:6px;width:2px;background:var(--cyan);border-radius:2px}
.side-foot{display:flex;gap:14px;padding-top:14px;margin-top:8px;border-top:1px solid var(--line);font-size:.74rem;font-family:"JetBrains Mono",ui-monospace,monospace}
.side-foot a{color:var(--muted);border:0}
.side-foot a:hover{color:var(--magenta)}

/* main */
main{min-width:0;padding:30px clamp(20px,4.5vw,56px) 80px;max-width:1180px;margin:0 auto;width:100%;position:relative}
.hero{display:flex;align-items:flex-end;justify-content:space-between;gap:22px;border-bottom:1px solid var(--line);padding:14px 0 20px;position:relative;flex-wrap:wrap}
.hero:after{content:"";position:absolute;left:0;bottom:-1px;width:64px;height:2px;background:linear-gradient(90deg,var(--cyan),var(--magenta));border-radius:2px}
.hero-text{min-width:0;flex:1 1 320px}
.eyebrow{margin:0 0 10px;color:var(--magenta);font-weight:700;text-transform:uppercase;letter-spacing:.18em;font-size:.66rem;font-family:"JetBrains Mono",ui-monospace,monospace}
.hero h1{font-family:"JetBrains Mono",ui-monospace,monospace;font-size:clamp(1.8rem,3.2vw,2.6rem);line-height:1.05;letter-spacing:-.02em;margin:0;font-weight:700;color:var(--ink)}
.hero-meta{display:flex;gap:6px;flex:0 0 auto}
.repo,.edit{border:1px solid var(--line);color:var(--ink-dim);text-decoration:none;border-radius:6px;padding:6px 12px;font-weight:500;font-size:.78rem;background:var(--panel);font-family:"JetBrains Mono",ui-monospace,monospace;transition:border-color .15s,color .15s}
.repo:hover,.edit:hover{border-color:var(--cyan);color:var(--cyan)}

/* landing hero */
.hero-home{display:grid;grid-template-columns:minmax(0,1.1fr) minmax(0,1fr);gap:40px;align-items:center;border-bottom:0;padding:32px 0 14px}
.hero-home:after{display:none}
.hero-home .eyebrow{margin-bottom:16px;color:var(--cyan)}
.hero-home h1{font-size:clamp(2.1rem,4.6vw,3.6rem);line-height:1.02;letter-spacing:-.025em;font-weight:700;margin:0 0 18px;max-width:18ch}
.hero-home h1 em{font-style:normal;color:var(--magenta);font-weight:700;background:linear-gradient(180deg,transparent 60%,rgba(243,100,162,.18) 60%);padding:0 .12em}
.lede{margin:0 0 24px;color:var(--ink-dim);font-size:clamp(1rem,1.2vw,1.06rem);line-height:1.6;max-width:48ch}
.cta{display:flex;gap:10px;align-items:stretch;flex-wrap:nowrap}
.cta-primary,.cta-secondary{display:inline-flex;align-items:center;justify-content:center;flex:0 0 auto;white-space:nowrap;word-break:keep-all;border-radius:7px;padding:10px 18px;font-weight:600;font-size:.86rem;line-height:1.2;text-decoration:none;font-family:"JetBrains Mono",ui-monospace,monospace;transition:transform .15s,box-shadow .15s,background .15s,border-color .15s,color .15s;border:1px solid transparent}
.cta-primary{background:var(--cyan);color:var(--bg);border-color:var(--cyan)}
.cta-primary:hover{background:var(--magenta);border-color:var(--magenta);color:var(--bg);transform:translateY(-1px);box-shadow:0 8px 22px rgba(243,100,162,.25)}
.cta-secondary{border-color:var(--line);color:var(--ink);background:transparent}
.cta-secondary:hover{border-color:var(--cyan);color:var(--cyan);transform:translateY(-1px)}
.home-install{display:flex;align-items:center;gap:8px;flex:1 1 23rem;min-width:22rem;max-width:34em;border:1px solid var(--line);background:var(--panel);color:var(--ink);border-radius:7px;padding:10px 10px 10px 14px;font:600 .84rem/1.35 "JetBrains Mono",ui-monospace,monospace;box-shadow:inset 0 0 0 1px rgba(255,255,255,.02)}
.home-install .prompt{color:var(--cyan);user-select:none;flex:0 0 auto}
.home-install code{flex:1;min-width:0;background:transparent;border:0;color:var(--ink);padding:0;font:inherit;white-space:pre;overflow:hidden;text-overflow:ellipsis}
.home-install .copy{flex:0 0 auto;background:rgba(255,255,255,.05);color:var(--ink-dim);border:1px solid var(--line);border-radius:5px;padding:5px 10px;font:600 .68rem/1 "JetBrains Mono",monospace;cursor:pointer;transition:background .15s,border-color .15s,color .15s}
.home-install .copy:hover{border-color:var(--cyan);color:var(--cyan)}
.home-install .copy.copied{background:var(--cyan);border-color:var(--cyan);color:var(--bg)}
.hero-snippet{margin:0;background:var(--panel);color:var(--ink);border-radius:10px;padding:20px 22px;font:500 .84rem/1.7 "JetBrains Mono",ui-monospace,monospace;border:1px solid var(--line);box-shadow:var(--shadow);overflow:hidden;position:relative}
.hero-snippet:before{content:"$ wiretap";position:absolute;top:8px;right:14px;font-size:.62rem;color:var(--muted);letter-spacing:.14em;text-transform:uppercase}
.hero-snippet code{background:transparent;border:0;padding:0;color:inherit;font:inherit;display:block;white-space:pre}
.hero-snippet .prompt{color:var(--cyan)}
.hero-snippet .comment{color:var(--muted)}

/* feature grid */
.features{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px;margin:32px 0 8px}
.feature{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:18px 18px 16px;transition:border-color .15s,transform .15s,box-shadow .15s;position:relative;overflow:hidden}
.feature:before{content:"";position:absolute;top:0;left:0;width:100%;height:2px;background:linear-gradient(90deg,var(--cyan),var(--magenta));opacity:0;transition:opacity .15s}
.feature:hover{border-color:var(--cyan);transform:translateY(-2px);box-shadow:var(--shadow)}
.feature:hover:before{opacity:1}
.feature h3{font-family:"JetBrains Mono",ui-monospace,monospace;font-size:.95rem;margin:0 0 8px;font-weight:600;letter-spacing:-.01em;line-height:1.2;color:var(--ink)}
.feature p{margin:0;color:var(--ink-dim);font-size:.9rem;line-height:1.55}
.feature code{font-size:.86em;background:var(--panel-2);border:1px solid var(--line-soft);border-radius:4px;padding:.04em .3em;color:var(--cyan)}

/* layout: doc + toc */
.doc-grid{display:grid;grid-template-columns:minmax(0,1fr);gap:36px;margin-top:32px}
.doc-grid-home{margin-top:14px}
.doc-home{background:transparent;box-shadow:none;border:0;padding:8px clamp(18px,3vw,30px) 0;max-width:74ch;margin-inline:auto;width:100%}
.doc-home>:first-child{margin-top:0}
@media(min-width:1180px){.doc-grid{grid-template-columns:minmax(0,72ch) 200px;justify-content:start}.doc-grid-home{grid-template-columns:minmax(0,1fr)}}
.doc{min-width:0;max-width:74ch;background:var(--panel);box-shadow:var(--shadow);border:1px solid var(--line);border-radius:10px;padding:clamp(22px,3.6vw,42px);overflow-wrap:break-word}
.doc-home{max-width:none}
.doc h1{display:none}
.doc h2{font-family:"JetBrains Mono",ui-monospace,monospace;font-size:1.45rem;line-height:1.2;margin:1.9em 0 .6em;font-weight:600;letter-spacing:-.015em;position:relative;color:var(--ink)}
.doc h3{font-size:1.08rem;margin:1.6em 0 .35em;position:relative;font-weight:600;font-family:"JetBrains Mono",ui-monospace,monospace;color:var(--ink)}
.doc h4{font-size:.92rem;margin:1.3em 0 .2em;color:var(--cyan);position:relative;font-weight:600;font-family:"JetBrains Mono",ui-monospace,monospace;text-transform:uppercase;letter-spacing:.08em}
.doc h2:first-child,.doc h3:first-child,.doc h4:first-child{margin-top:0}
.doc :is(h2,h3,h4) .anchor{position:absolute;left:-1em;top:0;color:var(--muted);opacity:0;text-decoration:none;font-weight:400;padding-right:.3em;transition:opacity .12s,color .12s;border:0}
.doc :is(h2,h3,h4):hover .anchor{opacity:.55}
.doc :is(h2,h3,h4) .anchor:hover{opacity:1;color:var(--magenta)}
.doc p{margin:0 0 1.05em;color:var(--ink-dim)}
.doc ul,.doc ol{padding-left:1.35rem;margin:0 0 1.2em;color:var(--ink-dim)}
.doc li{margin:.25em 0}
.doc li>p{margin:0 0 .4em}
.doc strong{font-weight:600;color:var(--ink)}
.doc code{font-family:"JetBrains Mono",ui-monospace,monospace;font-size:.84em;background:var(--panel-2);border:1px solid var(--line-soft);border-radius:4px;padding:.08em .34em;color:var(--cyan)}
.doc pre{position:relative;overflow:auto;background:var(--panel-2);color:var(--ink);border-radius:8px;padding:18px 22px;border:1px solid var(--line);margin:1.35em 0;font-size:.86em;scrollbar-width:thin;scrollbar-color:var(--line) transparent}
.doc pre::-webkit-scrollbar{height:8px}
.doc pre::-webkit-scrollbar-thumb{background:var(--line);border-radius:8px}
.doc pre code{display:block;background:transparent;border:0;color:inherit;padding:0;font-size:1em;white-space:pre-wrap;overflow-wrap:anywhere}
.doc pre .copy{position:absolute;top:8px;right:8px;background:var(--panel);color:var(--ink-dim);border:1px solid var(--line);border-radius:5px;padding:3px 9px;font:600 .68rem/1 "JetBrains Mono",monospace;cursor:pointer;opacity:0;transition:opacity .15s,background .15s,border-color .15s,color .15s}
.doc pre:hover .copy,.doc pre .copy:focus{opacity:1}
.doc pre .copy:hover{border-color:var(--cyan);color:var(--cyan)}
.doc pre .copy.copied{background:var(--cyan);border-color:var(--cyan);color:var(--bg);opacity:1}
.doc blockquote{margin:1.4em 0;padding:12px 16px;border-left:2px solid var(--magenta);background:var(--panel-2);border-radius:0 6px 6px 0;color:var(--ink-dim)}
.doc blockquote p:last-child{margin-bottom:0}
.doc table{width:100%;border-collapse:collapse;margin:1.2em 0;font-size:.92em}
.doc th,.doc td{border-bottom:1px solid var(--line);padding:9px 10px;text-align:left}
.doc th{font-weight:600;color:var(--cyan);font-family:"JetBrains Mono",ui-monospace,monospace;font-size:.85em;text-transform:uppercase;letter-spacing:.06em}
.doc td{color:var(--ink-dim)}
.doc hr{border:0;border-top:1px solid var(--line);margin:2em 0}

/* toc */
.toc{position:sticky;top:24px;align-self:start;font-size:.82rem;padding-left:14px;border-left:1px solid var(--line);max-height:calc(100vh - 48px);overflow:auto;scrollbar-width:thin;scrollbar-color:var(--line) transparent;font-family:"JetBrains Mono",ui-monospace,monospace}
.toc::-webkit-scrollbar{width:5px}
.toc::-webkit-scrollbar-thumb{background:var(--line);border-radius:5px}
.toc h2{font-size:.6rem;color:var(--muted);text-transform:uppercase;letter-spacing:.2em;margin:0 0 10px;font-weight:700}
.toc a{display:block;color:var(--muted);text-decoration:none;padding:4px 0 4px 10px;line-height:1.4;border-left:2px solid transparent;margin-left:-12px;transition:color .12s,border-color .12s;border-bottom:0}
.toc a:hover{color:var(--cyan)}
.toc a.active{color:var(--cyan);border-left-color:var(--cyan);font-weight:600}
.toc-l3{padding-left:22px!important;font-size:.92em}
@media(max-width:1179px){.toc{display:none}}

/* prev/next pager */
.page-nav{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-top:48px}
.page-nav>a{display:block;border:1px solid var(--line);background:var(--panel);border-radius:8px;padding:14px 18px;text-decoration:none;color:var(--ink);transition:border-color .15s,transform .15s,box-shadow .15s;border-bottom:1px solid var(--line)}
.page-nav>a:hover{border-color:var(--cyan);transform:translateY(-1px);box-shadow:var(--shadow)}
.page-nav small{display:block;color:var(--muted);font-size:.66rem;text-transform:uppercase;letter-spacing:.16em;margin-bottom:5px;font-weight:700;font-family:"JetBrains Mono",ui-monospace,monospace}
.page-nav span{display:block;font-weight:600;line-height:1.3;font-family:"JetBrains Mono",ui-monospace,monospace;font-size:.92rem}
.page-nav-prev{text-align:left}
.page-nav-next{text-align:right;grid-column:2}
.page-nav-prev:only-child{grid-column:1}

/* mobile nav toggle */
.nav-toggle{display:none;position:fixed;top:14px;right:14px;top:calc(14px + env(safe-area-inset-top, 0px));right:calc(14px + env(safe-area-inset-right, 0px));z-index:20;width:40px;height:40px;border-radius:7px;background:var(--panel);border:1px solid var(--line);color:var(--ink);cursor:pointer;padding:10px 9px;flex-direction:column;align-items:stretch;justify-content:space-between;box-shadow:var(--shadow)}
.nav-toggle span{display:block;width:100%;height:2px;flex:0 0 2px;background:currentColor;border-radius:2px;transition:transform .2s,opacity .2s}
.nav-toggle[aria-expanded="true"] span:nth-child(1){transform:translateY(8px) rotate(45deg)}
.nav-toggle[aria-expanded="true"] span:nth-child(2){opacity:0}
.nav-toggle[aria-expanded="true"] span:nth-child(3){transform:translateY(-8px) rotate(-45deg)}

/* mobile */
@media(max-width:900px){
  .shell{display:block}
  .sidebar{position:fixed;inset:0 30% 0 0;max-width:300px;height:100vh;z-index:15;transform:translateX(-100%);transition:transform .25s ease;box-shadow:var(--shadow);background:var(--panel);pointer-events:none}
  .sidebar.open{transform:translateX(0);pointer-events:auto}
  .nav-toggle{display:flex}
  main{padding:64px 18px 56px}
  .hero{padding-top:8px}
  .hero h1{font-size:clamp(1.6rem,7vw,2rem)}
  .hero-meta{width:100%;justify-content:flex-start}
  .hero-home{grid-template-columns:1fr;gap:24px;padding-top:8px}
  .hero-home h1{font-size:clamp(1.95rem,8vw,2.5rem);max-width:none}
  .home-install{min-width:20rem}
  .hero-snippet{font-size:.76rem;padding:16px 16px}
  .features{grid-template-columns:1fr;margin-top:22px}
  .doc{padding:20px;border-radius:8px}
  .doc-home{padding:0 18px}
  .doc-grid{margin-top:22px;gap:24px}
  .doc :is(h2,h3,h4) .anchor{display:none}
}
@media(max-width:520px){
  main{padding:60px 14px 48px}
  .doc{padding:18px 16px}
  .doc-home{padding-inline:16px}
  .cta{flex-wrap:wrap}
  .cta-primary,.cta-secondary{flex:1 1 calc(50% - 5px);padding-inline:12px}
  .home-install{width:100%;min-width:0;flex-basis:100%}
  .home-install code{white-space:normal;overflow-wrap:anywhere}
  .doc pre{margin-left:-16px;margin-right:-16px;border-radius:0;border-left:0;border-right:0}
}
`;
}

function js() {
  return `
const sidebar=document.querySelector('.sidebar');
const toggle=document.querySelector('.nav-toggle');
const mobileNav=window.matchMedia('(max-width: 900px)');
const sidebarFocusable='a[href],button,input,select,textarea,[tabindex]';
function setSidebarFocusable(enabled){
  sidebar?.querySelectorAll(sidebarFocusable).forEach((el)=>{
    if(enabled){
      if(el.dataset.sidebarTabindex!==undefined){
        if(el.dataset.sidebarTabindex)el.setAttribute('tabindex',el.dataset.sidebarTabindex);
        else el.removeAttribute('tabindex');
        delete el.dataset.sidebarTabindex;
      }
    }else if(el.dataset.sidebarTabindex===undefined){
      el.dataset.sidebarTabindex=el.getAttribute('tabindex')??'';
      el.setAttribute('tabindex','-1');
    }
  });
}
function setSidebarOpen(open){
  if(!sidebar||!toggle)return;
  sidebar.classList.toggle('open',open);
  toggle.setAttribute('aria-expanded',open?'true':'false');
  if(mobileNav.matches){
    sidebar.inert=!open;
    if(open)sidebar.removeAttribute('aria-hidden');
    else sidebar.setAttribute('aria-hidden','true');
    setSidebarFocusable(open);
  }else{
    sidebar.inert=false;
    sidebar.removeAttribute('aria-hidden');
    setSidebarFocusable(true);
  }
}
setSidebarOpen(false);
toggle?.addEventListener('click',()=>setSidebarOpen(!sidebar?.classList.contains('open')));
document.addEventListener('click',(e)=>{if(!sidebar?.classList.contains('open'))return;if(sidebar.contains(e.target)||toggle?.contains(e.target))return;setSidebarOpen(false)});
document.addEventListener('keydown',(e)=>{if(e.key==='Escape')setSidebarOpen(false)});
const syncSidebarForViewport=()=>setSidebarOpen(sidebar?.classList.contains('open')??false);
if(mobileNav.addEventListener)mobileNav.addEventListener('change',syncSidebarForViewport);
else mobileNav.addListener?.(syncSidebarForViewport);

const input=document.getElementById('doc-search');
input?.addEventListener('input',()=>{const q=input.value.trim().toLowerCase();document.querySelectorAll('nav section').forEach(sec=>{let any=false;sec.querySelectorAll('.nav-link').forEach(a=>{const m=!q||a.textContent.toLowerCase().includes(q);a.style.display=m?'block':'none';if(m)any=true});sec.style.display=any?'block':'none'})});

function attachCopy(target,getText){const btn=document.createElement('button');btn.type='button';btn.className='copy';btn.textContent='copy';btn.addEventListener('click',async()=>{try{await navigator.clipboard.writeText(getText());btn.textContent='copied';btn.classList.add('copied');setTimeout(()=>{btn.textContent='copy';btn.classList.remove('copied')},1400)}catch{btn.textContent='failed';setTimeout(()=>{btn.textContent='copy'},1400)}});target.appendChild(btn)}
document.querySelectorAll('.doc pre').forEach(pre=>attachCopy(pre,()=>pre.querySelector('code')?.textContent??''));
document.querySelectorAll('.home-install').forEach(el=>attachCopy(el,()=>el.querySelector('code')?.textContent??''));

const tocLinks=document.querySelectorAll('.toc a');
if(tocLinks.length){const map=new Map();tocLinks.forEach(a=>{const id=a.getAttribute('href').slice(1);const el=document.getElementById(id);if(el)map.set(el,a)});const setActive=l=>{tocLinks.forEach(x=>x.classList.remove('active'));l.classList.add('active')};const obs=new IntersectionObserver(entries=>{const visible=entries.filter(e=>e.isIntersecting).sort((a,b)=>a.boundingClientRect.top-b.boundingClientRect.top);if(visible.length){const link=map.get(visible[0].target);if(link)setActive(link)}},{rootMargin:'-15% 0px -65% 0px',threshold:0});map.forEach((_,el)=>obs.observe(el))}
`;
}

function discrawlSvg() {
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 120 120" role="img" aria-label="Discrawl">
<rect width="120" height="120" rx="22" fill="#0c0f14"/>
<rect x="20" y="28" width="80" height="58" rx="6" fill="none" stroke="#5fe3d4" stroke-width="3"/>
<line x1="20" y1="44" x2="100" y2="44" stroke="#5fe3d4" stroke-width="2"/>
<circle cx="28" cy="36" r="2" fill="#f364a2"/>
<circle cx="36" cy="36" r="2" fill="#f7c177"/>
<circle cx="44" cy="36" r="2" fill="#5fe3d4"/>
<text x="28" y="60" font-family="JetBrains Mono, monospace" font-size="9" font-weight="700" fill="#5fe3d4">SELECT *</text>
<text x="28" y="72" font-family="JetBrains Mono, monospace" font-size="9" font-weight="700" fill="#aab3c1">FROM msgs</text>
<text x="28" y="82" font-family="JetBrains Mono, monospace" font-size="9" font-weight="700" fill="#f364a2">_</text>
<rect x="20" y="92" width="80" height="6" rx="2" fill="#161b24"/>
<rect x="20" y="92" width="48" height="6" rx="2" fill="#5fe3d4"/>
<circle cx="22" cy="106" r="3" fill="#5fe3d4"/>
<circle cx="32" cy="106" r="3" fill="#a594ff"/>
<circle cx="42" cy="106" r="3" fill="#f364a2"/>
</svg>`;
}

function slug(text) {
  return text.toLowerCase().replace(/`/g, "").replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "");
}

function escapeHtml(value) {
  return String(value).replace(/[&<>"']/g, (char) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]);
}

function escapeAttr(value) {
  return escapeHtml(value);
}
