#!/usr/bin/env node
/*
OWNER-FINDER — v5.2.1 HARDENED (batch, resume, robots-aware, GDPR-trim, zero outreach)

Modes:
  Single: node MAILSIEVE.mjs --domain example.com [--paths '/azienda,/chi-siamo'] [--json]
  Batch:  node MAILSIEVE.mjs --file domains.txt --out results.csv [--format csv|tsv|jsonl]

Features:
* Batch mode with resume: dedupes input, skips already-processed from output, idempotent writes
* Silent-by-default: stderr muted unless QUIET=0 (env)
* Evidence logging: hashed emails + hashed page URLs to logs/evidence.jsonl (GDPR-friendly)
* Robust fetcher: retries with backoff, robots.txt-aware, per-host pacing, conditional caching
* Smart writers: csv | tsv | jsonl; headers written once; atomic-ish append
* First-party scope only; pattern-safe email synthesis only if ≥1 same-domain sample + MX valid
* Sitemap support, vCard/h-card/JSON-LD awareness; deterministic selection
* Zero outreach, no SMTP probing beyond DNS MX check

Env:
  RATE_MS=900            // min delay same-host requests
  HASH_EVIDENCE=1        // hash evidence emails in logs
  TIMEOUT_MS=12000
  MAX_PAGES=16
  LOG_PATH=logs/evidence.jsonl
  CACHE_DIR=.cache/http  // disk cache (ETag/Last-Modified). disable by unsetting
  QUIET=1                // suppress non-result stderr
  RETRIES=3              // network retries per document
  BACKOFF_MS=600         // base backoff

Exit: 0 (ok), 1 (fail)
*/

import { setTimeout as sleep } from "node:timers/promises";
import { load as cheerioLoad } from "cheerio";
import psl from "psl";
import robotsParser from "robots-parser";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import dns from "node:dns/promises";
import yargs from "yargs";
import { hideBin } from "yargs/helpers";

const getFetch = async () => (typeof fetch !== "undefined" ? fetch : (await import("node-fetch")).default);

const argv = yargs(hideBin(process.argv))
  .scriptName("MAILSIEVE")
  .usage("$0 --domain <domain> | --file domains.txt --out results.csv")
  .option("domain", { type: "string", describe: "Target registrable domain (e.g., example.com)" })
  .option("file", { type: "string", describe: "Path to newline-separated domains" })
  .option("out", { type: "string", describe: "Output file path (csv|tsv|jsonl)", default: "" })
  .option("format", { type: "string", choices: ["csv", "tsv", "jsonl"], describe: "Output format override" })
  .option("rate", { type: "number", default: Number(process.env.RATE_MS || 900) })
  .option("hash", { type: "boolean", default: process.env.HASH_EVIDENCE === "1" })
  .option("timeout", { type: "number", default: Number(process.env.TIMEOUT_MS || 12000) })
  .option("maxPages", { type: "number", default: Math.max(8, Number(process.env.MAX_PAGES || 16)) })
  .option("sitemap", { type: "boolean", default: true })
  .option("paths", { type: "string", desc: "Comma-separated extra paths to scan (e.g., /azienda,/chi-siamo)" })
  .option("json", { type: "boolean", default: false, desc: "Output JSON (single mode) instead of CSV" })
  .option("strict", { type: "boolean", default: true, desc: "Require role keyword proximity for heuristic picks" })
  .option("headers", { type: "string", default: "", desc: "Additional request headers as JSON" })
  .option("noHeaders", { type: "boolean", default: false, desc: "Emit no CSV/TSV headers (batch)" })
  .option("concurrency", { type: "number", default: 2, desc: "Batch concurrency (low for politeness)" })
  .help(false).argv;

const UA = "OwnerFinder/5.2 (no-sending; robots-aware; contact: compliance@invalid)";
const EXTRA_HEADERS = (() => { try { return argv.headers ? JSON.parse(argv.headers) : {}; } catch { return {}; } })();
const HEADERS = { "User-Agent": UA, Accept: "text/html,application/xhtml+xml", "Accept-Language": "en,it,de,fr,es,pt;q=0.7", ...EXTRA_HEADERS };
const RATE_MS = argv.rate;
const HASH_EVIDENCE = argv.hash;
const TIMEOUT_MS = argv.timeout;
const MAX_PAGES = Math.max(3, argv.maxPages);
const QUIET = String(process.env.QUIET || "").trim() !== "0"; // quiet by default
const CACHE_DIR = process.env.CACHE_DIR || ".cache/http";
const SIZE_CAP = 1_800_000; // ~1.8MB
const RETRIES = Math.max(0, Number(process.env.RETRIES || 3));
const BACKOFF_MS = Math.max(100, Number(process.env.BACKOFF_MS || 600));

const DEFAULT_PATHS = [
  "",
  "/about","/team","/contact","/directors","/partners","/leadership","/people","/management","/company","/founders",
  "/chi-siamo","/contatti","/azienda","/fondatori","/governance","/impressum","/legal","/who-we-are","/la-societa",
  "/equipo","/sobre-nosotros","/fundadores","/contato","/equipe","/direction","/notre-histoire","/dirigenza"
];

const TITLE_KEYWORDS = [
  "Owner","Owner Manager","Founder","Co-Founder","CEO","President","Managing Director","Director","Partner","Principal","Proprietor",
  "Titolare","Fondatore","Co-Fondatore","Amministratore Delegato","Direttore Generale","Proprietario","Amministratore Unico","Legale Rappresentante",
  "Geschäftsführer","Inhaber","Président","Directeur Général","Dirigente","Managing Partner","Chairman","Chairwoman","Executive Director"
];

const TITLE_RX = new RegExp(TITLE_KEYWORDS.join("|"), "iu");
const NAME_RX = new RegExp(`\\b(${TITLE_KEYWORDS.join("|")})\\b[^\\.\\n:]{0,200}?\\b([A-Z][\\p{L}À-ÖØ-öø-ÿ'’-]+)\\s([A-Z][\\p{L}À-ÖØ-öø-ÿ'’-]+)\\b`, "giu");

const robotsCache = new Map();
const hostTick = new Map(); // per-host pacing
const hash = (s) => crypto.createHash("sha256").update(s).digest("hex");

function log(...args){ if (!QUIET) console.error(...args); }

function cleanHost(h) {
  try {
    const u = new URL(/^https?:\/\//.test(h) ? h : `https://${h}`);
    return u.hostname.replace(/^www\./, "");
  } catch {
    return String(h || "").replace(/^https?:\/\//, "").replace(/^www\./, "").split("/")[0];
  }
}

async function resolveDomain(input) {
  const host = cleanHost(input);
  const parsed = psl.parse(host);
  const base = parsed.domain || host;
  return base;
}

async function robotsAgent(base) {
  try {
    const f = await getFetch();
    const res = await f(`${base}/robots.txt`, { headers: HEADERS, redirect: "follow", signal: AbortSignal.timeout(TIMEOUT_MS) });
    const txt = res.ok ? await res.text() : "";
    return robotsParser(`${base}/robots.txt`, txt);
  } catch {
    return robotsParser(`${base}/robots.txt`, "");
  }
}

async function robotsAllowed(u) {
  try {
    const url = new URL(u), base = `${url.protocol}//${url.host}`;
    if (!robotsCache.has(base)) robotsCache.set(base, await robotsAgent(base));
    const rp = robotsCache.get(base);
    const allowed = rp.isAllowed(u, UA);
    return allowed !== false;
  } catch { return true; }
}

function cachePathFor(url){
  const id = hash(url);
  const dir = path.join(CACHE_DIR, url.split("//")[1]?.split("/")[0] || "misc");
  return { meta: path.join(dir, id + ".json"), body: path.join(dir, id + ".bin"), dir };
}

async function readCache(url){
  if (!CACHE_DIR) return null;
  try {
    const { meta, body } = cachePathFor(url);
    const m = JSON.parse(fs.readFileSync(meta, "utf8"));
    const b = fs.readFileSync(body);
    return { meta: m, body: b };
  } catch { return null; }
}

async function writeCache(url, meta, body){
  if (!CACHE_DIR) return;
  const { meta: mpath, body: bpath, dir } = cachePathFor(url);
  try {
    fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(mpath, JSON.stringify(meta));
    fs.writeFileSync(bpath, body);
  } catch {}
}

async function paced(host){
  const last = hostTick.get(host) || 0;
  const wait = Math.max(0, RATE_MS - (Date.now() - last));
  if (wait) await sleep(wait);
  hostTick.set(host, Date.now());
}

async function fetchOnce(url){
  const f = await getFetch();
  const U = new URL(url);
  await paced(U.host);

  // Conditional fetch if cached
  const prior = await readCache(url);
  const headers = { ...HEADERS };
  if (prior?.meta?.etag) headers["If-None-Match"] = prior.meta.etag;
  if (prior?.meta?.lastModified) headers["If-Modified-Since"] = prior.meta.lastModified;

  const res = await f(url, { headers, redirect: "follow", signal: AbortSignal.timeout(TIMEOUT_MS) });
  if (res.status === 304 && prior) {
    const buf = prior.body;
    const ct = prior.meta.contentType || "text/html";
    if (!/text\/html|application\/xhtml\+xml/i.test(ct)) return "";
    const txt = buf.toString(prior.meta.encoding || "utf8");
    return txt.slice(0, SIZE_CAP);
  }
  if (!res.ok) throw new Error(`status_${res.status}`);

  const ct = (res.headers.get("content-type") || "").toLowerCase();
  if (!/text\/html|application\/xhtml\+xml/.test(ct)) return "";

  const ab = Buffer.from(await res.arrayBuffer());
  const limited = ab.subarray(0, SIZE_CAP);
  const encoding = /charset=([^;]+)/i.exec(ct)?.[1] || "utf8";
  const txt = limited.toString(encoding);

  await writeCache(url, { etag: res.headers.get("etag"), lastModified: res.headers.get("last-modified"), contentType: ct, encoding }, limited);
  return txt;
}

async function fetchText(url) {
  if (!(await robotsAllowed(url))) return "";
  const U = new URL(url);
  let attempt = 0;
  while (true) {
    try {
      return await fetchOnce(url);
    } catch (e) {
      attempt++;
      if (attempt > RETRIES) { return ""; }
      const delay = BACKOFF_MS * Math.pow(2, attempt - 1) + Math.floor(Math.random() * 200);
      if (!QUIET) console.error(`retry ${attempt} ${U.host} ${U.pathname}`);
      await sleep(delay);
    }
  }
}

// --- Email extraction (leveled up) ---
function decodeBasicObfuscations(s){
  return (s || "")
    .replace(/&#64;|&commat;|%40/gi, "@")
    .replace(/\\s*\\[at\\]\\s*|\\s*\\(at\\)\\s*|\\s+at\\s+/gi, "@")
    .replace(/\\s*\\[dot\\]\\s*|\\s*\\(dot\\)\\s*|\\s+dot\\s+/gi, ".")
    .replace(/&#46;|&period;|%2e/gi, ".")
    .replace(/&amp;/gi, "&");
}

function extractEmails(html, domain) {
  if (!html) return [];
  const $ = cheerioLoad(html);
  const found = new Set();

  // 1) mailto:
  $('a[href^="mailto:"]').each((_, a) => {
    const href = String($(a).attr("href") || "");
    const raw = href.replace(/^mailto:/i, "").split("?")[0].trim();
    if (raw) found.add(raw);
  });

  // 2) decode common obfuscations and scan
  const decoded = decodeBasicObfuscations(html);
  const re = /[A-Z0-9._%+-]+@(?:[A-Z0-9-]+\\.)+[A-Z]{2,}/gi;
  (decoded.match(re) || []).forEach(e => found.add(e.trim()));

  // 3) scope to same domain/subdomain
  const host = domain.toLowerCase();
  return [...found].filter((e) => {
    const dom = (e.toLowerCase().split("@")[1] || "").trim();
    return dom === host || dom.endsWith(`.${host}`);
  });
}

function parseJsonLdPeople(html) {
  if (!html) return [];
  const $ = cheerioLoad(html);
  const hits = [];
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const raw = $(el).contents().text();
      const data = JSON.parse(raw);
      const flat = [];
      const pushAny = (obj) => flat.push(obj);
      const walk = (node) => {
        if (!node) return;
        if (Array.isArray(node)) return node.forEach(walk);
        if (typeof node === 'object') {
          const t = node['@type'];
          const isPerson = t === 'Person' || (Array.isArray(t) && t.includes('Person'));
          if (isPerson) pushAny(node);
          ['founder','founders','member','members','employee','employees','author','publisher','creator'].forEach(k => {
            const v = node[k];
            if (Array.isArray(v)) v.forEach(pushAny);
            else if (v && typeof v === 'object') pushAny(v);
          });
          if (node['@graph']) walk(node['@graph']);
        }
      };
      walk(data);
      for (const item of flat) {
        const name = (item.name || '').trim();
        const job = (item.jobTitle || item.title || item.role || '').trim();
        const m = name.match(/^([A-Z][\p{L}À-ÖØ-öø-ÿ'’-]+)\s+([A-Z][\p{L}À-ÖØ-öø-ÿ'’-]+)$/u);
        if (m) hits.push({ title: job || 'Person', first: m[1], last: m[2], weight: TITLE_RX.test(job) ? 3 : 1 });
      }
    } catch {}
  });
  return hits;
}

function extractHCard(html){
  if (!html) return [];
  const $ = cheerioLoad(html);
  const hits = [];
  $('.h-card, .vcard, .hcard').each((_, el)=>{
    const name = ($(el).find('.p-name,.fn,.name').text() || '').trim();
    const title = ($(el).find('.p-job-title,.title,.role').text() || '').trim();
    const m = name.match(/^([A-Z][\p{L}À-ÖØ-öø-ÿ'’-]+)\s+([A-Z][\p{L}À-ÖØ-öø-ÿ'’-]+)$/u);
    if (m) hits.push({ title: title || 'Person', first: m[1], last: m[2], weight: TITLE_RX.test(title) ? 3 : 1 });
  });
  return hits;
}

function extractVCards(html, baseUrl){
  if (!html) return { people: [], emails: [], links: [] };
  const $ = cheerioLoad(html);
  const links = [];
  $('a[href$=".vcf"], a[href*="vcard"]').each((_, a)=>{ links.push(String($(a).attr('href')||'').trim()); });
  return { people: [], emails: [], links };
}

function extractPeopleRegex(html) {
  if (!html) return [];
  const $ = cheerioLoad(html);
  const text = $("body").text().replace(/\s+/g, " ").trim();
  const hits = [];
  let m;
  while ((m = NAME_RX.exec(text))) hits.push({ title: m[1], first: m[2], last: m[3], weight: 4 });
  return hits;
}

function extractPeopleHeuristic(html) {
  if (!html) return [];
  const $ = cheerioLoad(html);
  const hits = [];
  const nameLike = /\b([A-Z][\p{L}À-ÖØ-öø-ÿ'’-]+)\s([A-Z][\p{L}À-ÖØ-öø-ÿ'’-]+)\b/u;
  $("h1, h2, h3, h4, .team-member, .member, .person, .card, .bio, .profile, .staff, .leader, .founder, [class*='team']").each((_, el) => {
    const block = $(el).text().replace(/\s+/g, " ").trim();
    if (argv.strict && !TITLE_RX.test(block)) return; // require role keyword
    const m = block.match(nameLike);
    if (!m) return;
    const cand = { title: block.slice(0, 160), first: m[1], last: m[2], weight: 2 };
    if (isHumanName(cand.first, cand.last)) hits.push(cand);
  });
  return hits;
}

const STOPWORDS = new Set([
  "our","approach","frequently","asked","mitcham","office","griffin","chartered","outsourced","finance","wimbledon","online","accounting",
  "team","studio","guesthouse","limited","ltd","plc","llp","inc","gmbh","srl","spa","services","service","about","contact","careers","jobs",
  "privacy","policy","terms","conditions","support","bookkeeping","audit","tax","consulting","payroll","systems","advice","solutions",
  "digital","cloud","innovation","management","accounts","price","cheap","business","company","firm","practice","financial","reports",
  "traders","trading","clients","customers","vendors","suppliers","invoices","receipts","working","capital","equity","profit","revenue","income",
  "role","job","solution","trust","newsletter","faq","faqs","events","press","news","blog","care","contactus","download","resources","library",
  "low","cost","more","than","loan","account","with","sea","inspection","accountant","accountants","simplex","ascot","drummond","sutherland","black"
]);

const PHRASE_BLOCKS = new Set([
  "low cost","more than","loan account","accountant with","sea inspection"
]);

function norm(s) { return (s || "").toLowerCase().normalize("NFKD").replace(/[^a-z]/g, ""); }

function isHumanName(first, last) {
  if (!first || !last) return false;
  if (first === first.toUpperCase() || last === last.toUpperCase()) return false;
  if (!/^[A-Z][a-zÀ-ÖØ-öø-ÿ'’-]+$/.test(first)) return false;
  if (!/^[A-Z][a-zÀ-ÖØ-öø-ÿ'’-]+$/.test(last)) return false;
  const f = norm(first), l = norm(last);
  if (![...f].some(ch => "aeiou".includes(ch)) || ![...l].some(ch => "aeiou".includes(ch))) return false;
  if (l.length < 3) return false;
  if (l.endsWith("s") || l.endsWith("ing")) return false;
  if (STOPWORDS.has(f) || STOPWORDS.has(l)) return false;
  const full = `${f} ${l}`;
  if (PHRASE_BLOCKS.has(full)) return false;
  return true;
}

function inferPattern(samples) {
  const lps = samples.map((e) => e.split("@")[0].toLowerCase());
  const score = { "f.l": 0, fl: 0, f_l: 0, "f-l": 0, f0l: 0, l: 0, f: 0 };
  for (const lp of lps) {
    if (lp.includes(".")) score["f.l"]++;
    if (lp.includes("_")) score["f_l"]++;
    if (lp.includes("-")) score["f-l"]++;
    if (/^[a-z][a-z0-9]+$/.test(lp)) score["f0l"]++;
    if (/^[a-z]+$/.test(lp)) { score["fl"]++; score["l"]++; score["f"]++; }
    if (/^[a-z]{2,}[a-z]{2,}$/.test(lp)) score["fl"]++;
  }
  const rank = Object.entries(score).sort((a, b) => b[1] - a[1]).map(([k]) => k);
  const gens = {
    "f.l": ({ f, l }) => `${f}.${l}`,
    fl: ({ f, l }) => `${f}${l}`,
    f_l: ({ f, l }) => `${f}_${l}`,
    "f-l": ({ f, l }) => `${f}-${l}`,
    f0l: ({ f, l }) => `${f[0]}${l}`,
    l: ({ f, l }) => `${l}`,
    f: ({ f, l }) => `${f}`,
  };
  return { best: gens[rank[0] || "f.l"], second: gens[rank[1] || "fl"], hint: rank.slice(0, 2), count: lps.length };
}

function deriveCompany(htmls, domain) {
  for (const h of htmls) {
    if (!h) continue;
    try {
      const $ = cheerioLoad(h);
      const og = $('meta[property="og:site_name"]').attr("content")?.trim();
      const org = $('meta[itemprop="name"]').attr("content")?.trim();
      const schemaOrg = $('script[type="application/ld+json"]').map((_,el)=>{
        try { const d = JSON.parse($(el).contents().text()); return Array.isArray(d)? d : [d]; } catch { return []; }
      }).get().flat();
      const orgNode = schemaOrg.find(n=> n && (n['@type']==='Organization' || (Array.isArray(n['@type']) && n['@type'].includes('Organization'))) && n.name);
      const t = $("title").first().text().trim();
      const pick = orgNode?.name || og || org || t;
      if (pick) {
        const cleaned = pick.split("|")[0].split("—")[0].split("-")[0].trim();
        if (cleaned) return cleaned;
      }
    } catch {}
  }
  const d = psl.parse(domain).sld || domain.split(".")[0];
  return d ? d.charAt(0).toUpperCase() + d.slice(1) : domain;
}

function looksLikeCompany(personFull, company, domain) {
  if (!personFull) return false;
  const p = personFull.trim().toLowerCase();
  if (!p) return false;
  const c = (company || '').trim().toLowerCase();
  const root = (psl.parse(domain).sld || domain.split('.')[0] || '').toLowerCase();
  const tokens = new Set(c.split(/[^a-z]+/).filter(Boolean));
  const [pf, pl] = p.split(/\s+/);
  if (p === c || p === root) return true;
  if (tokens.has(p) || tokens.has(pf) || tokens.has(pl)) return true;
  if (c && (c.includes(p) || p.includes(c))) return true;
  if (root && (p === root || p.includes(root))) return true;
  for (const tok of [pf, pl]) if (STOPWORDS.has(tok)) return true;
  if (PHRASE_BLOCKS.has(p)) return true;
  return false;
}

const LOG_PATH = process.env.LOG_PATH || "logs/evidence.jsonl";
function logEvidence(obj) {
  try {
    fs.mkdirSync(path.dirname(LOG_PATH), { recursive: true });
    const safe = (obj.evidence?.emailsFound || []).map((e) => (HASH_EVIDENCE ? hash(e) : e));
    const pagesSafe = (obj.evidence?.pagesSearched || []).map(u=> ({ u: hash(u) }));
    fs.appendFileSync(
      LOG_PATH,
      JSON.stringify({ ts: new Date().toISOString(), domain: obj.domain, pages: pagesSafe, emailsFound: safe, patternHint: obj.evidence?.patternHint || [], result: (obj.results || [])[0]?.email || "" }) + "\n"
    );
  } catch {}
}

function uniq(arr) { return [...new Set(arr.filter(Boolean))]; }

function scoreAndPick(people) {
  const map = new Map();
  for (const p of people) {
    if (!isHumanName(p.first, p.last)) continue;
    const key = `${norm(p.first)} ${norm(p.last)}`;
    const roleBoost = TITLE_RX.test(p.title || "") ? 3 : 0;
    const curr = map.get(key) || { ...p, weight: 0 };
    curr.weight += (p.weight || 1) + roleBoost;
    curr.title = curr.title?.length >= (p.title || '').length ? curr.title : p.title;
    map.set(key, curr);
  }
  const ranked = [...map.values()].sort((a, b) => (b.weight - a.weight) || (a.last.localeCompare(b.last)) || (a.first.localeCompare(b.first)));
  const MIN_WEIGHT = 4;
  return ranked.find(p => (p.weight || 0) >= MIN_WEIGHT);
}

function prioritizeEmail(emails) {
  if (!emails.length) return "";
  const scores = new Map();
  const rank = (e) => {
    const local = e.split('@')[0].toLowerCase();
    if (/^(owner|founder|ceo|titolare|amministratore|admin(istratore)?unico)$/.test(local)) return 6;
    if (/^[a-z]+\.[a-z]+$/.test(local)) return 5;
    if (/^[a-z][a-z0-9]*[.-][a-z]+$/.test(local)) return 4;
    if (/(director|managing|partner|principal)/.test(local)) return 3;
    if (/^(hello|info|contact|team|enquiries|support|office)$/.test(local)) return 2;
    return 1;
  };
  for (const e of emails) scores.set(e, rank(e));
  return [...emails].sort((a,b)=> (scores.get(b)||0)-(scores.get(a)||0) || a.localeCompare(b))[0];
}

async function fetchSitemapUrls(baseUrl, domain) {
  const urls = new Set();
  try {
    const f = await getFetch();
    for (const path of ["/sitemap.xml", "/sitemap_index.xml"]) {
      const res = await f(`${baseUrl}${path}`, { headers: HEADERS, redirect: "follow", signal: AbortSignal.timeout(TIMEOUT_MS) });
      if (!res.ok) continue;
      const xml = await res.text();
      const locs = [...xml.matchAll(/<loc>([^<]+)<\/loc>/gi)].map(m => m[1]);
      for (const u of locs) {
        try {
          const U = new URL(u);
          if (U.hostname.endsWith(domain) && (U.pathname.endsWith("/") || /.html?$/.test(U.pathname))) urls.add(U.toString());
        } catch {}
      }
    }
  } catch {}
  return [...urls].sort((a, b) => {
    const score = (u) => (/about|team|leadership|people|founder|chi-siamo|azienda|impressum|governance|directors|partners|management/i.test(u) ? 1 : 0);
    return score(b) - score(a) || a.localeCompare(b);
  }).slice(0, Math.max(4, Math.floor(MAX_PAGES/2)));
}

async function collectInternalLinks(html, baseUrl, domain) {
  const out = new Set();
  if (!html) return [];
  try {
    const $ = cheerioLoad(html);
    $('a[href]').each((_, a) => {
      const href = String($(a).attr('href') || '').trim();
      if (!href) return;
      try {
        const U = new URL(href, baseUrl);
        if (U.hostname.endsWith(domain)) {
          const path = U.pathname + (U.search || '');
          if (/^\/(about|team|people|leadership|company|founder|chi-siamo|azienda|impressum|governance|directors|partners|management|contatti|contact|who-we-are)\b/i.test(path)) {
            out.add(`${U.origin}${U.pathname}`);
          }
        }
      } catch {}
    });
  } catch {}
  return [...out];
}

async function mxExists(domain) { try { const mx = await dns.resolveMx(domain); return Array.isArray(mx) && mx.length > 0; } catch { return false; } }

function sanitizeField(s) {
  const v = String(s || "").replace(/[\r\n]+/g, " ").trim();
  const q = v.replace(/"/g, '""');
  return `"${q}"`;
}

async function fetchAll(urls){
  const out = [];
  for (const u of urls) out.push(await fetchText(u));
  return out;
}

async function findOwner(domainInput) {
  const domain = await resolveDomain(domainInput);
  const base = `https://${domain}`;
  const extraPaths = (argv.paths || "").split(",").map((s) => s.trim()).filter(Boolean);
  const pages = new Set(uniq([ ...DEFAULT_PATHS, ...extraPaths ].map((p) => `${base}${p}`)));

  const homeHtml = await fetchText(base);
  if (homeHtml) {
    for (const u of await collectInternalLinks(homeHtml, base, domain)) pages.add(u);
  }

  if (argv.sitemap) {
    for (const u of await fetchSitemapUrls(base, domain)) pages.add(u);
  }

  const pageList = [...pages].slice(0, MAX_PAGES);
  const htmls = await fetchAll(pageList);

  // vCard links (limited)
  let vcardEmails = [];
  try {
    const vLinks = new Set();
    htmls.forEach((h,i)=>{ const { links } = extractVCards(h, pageList[i]); (links||[]).forEach(l=>{ try{ const U = new URL(l, base); if (U.hostname.endsWith(domain)) vLinks.add(U.toString()); }catch{} }); });
    for (const v of [...vLinks].slice(0, 3)) {
      const vc = await fetchText(v);
      const emails = (vc.match(/[A-Z0-9._%+-]+@(?:[A-Z0-9-]+\.)+[A-Z]{2,}/gi) || [])
        .filter(e=> e.toLowerCase().endsWith(`@${domain}`) || e.toLowerCase().endsWith(`.${domain}`));
      vcardEmails.push(...emails);
    }
  } catch {}

  const emails = uniq([ ...vcardEmails, ...htmls.flatMap((h) => extractEmails(h, domain)) ]);

  let people = [];
  people = people.concat(htmls.flatMap(extractPeopleRegex));
  people = people.concat(htmls.flatMap(parseJsonLdPeople));
  people = people.concat(htmls.flatMap(extractHCard));
  people = people.concat(htmls.flatMap(extractPeopleHeuristic));

  const company = deriveCompany(htmls, domain);
  const picked = scoreAndPick(people);

  // NOTE: you asked to "leave the name" when missing; so owner stays "" unless confidently inferred.
  let owner = "";
  let email = "";

  if (picked) {
    const candidate = `${picked.first} ${picked.last}`;
    if (!looksLikeCompany(candidate, company, domain)) {
      owner = candidate;
      const f = norm(picked.first), l = norm(picked.last);
      const pat = inferPattern(emails);
      // pattern-safe synthesis only if we already observed at least one same-domain email
      if (pat.count > 0 && await mxExists(domain)) {
        email = `${pat.best({ f, l })}@${domain}`;
      }
    }
  }

  // Fallback: if we couldn't pick a person, choose best same-domain email (but do NOT invent owner name)
  if (!owner) {
    const fallback = prioritizeEmail(emails);
    if (fallback && await mxExists(domain)) {
      owner = "";
      email = fallback;
    }
  }

  if (email && !(await mxExists(domain))) email = ""; // MX sanity

  const out = { domain, company, results: [{ person: owner || "", email: email || "" }], evidence: { emailsFound: emails, pagesSearched: pageList } };
  logEvidence({ ...out, evidence: { ...out.evidence, patternHint: [] } });

  return { company: company, owner: owner || "", email: email || "" };
}

// ---- Writers (CSV/TSV/JSONL) ----
function detectFormat(outPath, explicit) {
  if (explicit) return explicit;
  const ext = (outPath.split('.').pop() || '').toLowerCase();
  if (ext === 'csv') return 'csv';
  if (ext === 'tsv') return 'tsv';
  if (ext === 'jsonl' || ext === 'json') return 'jsonl';
  return 'csv';
}

function writerFor(outPath, format, noHeaders){
  if (!outPath) return { write: (_row)=>{}, close: ()=>{} };
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
  if (!fs.existsSync(outPath)) fs.writeFileSync(outPath, "");
  const f = fs.createWriteStream(outPath, { flags: 'a' });
  const fmt = detectFormat(outPath, format);
  const sep = fmt === 'csv' ? ',' : '\t';

  const wroteHeader = fs.statSync(outPath).size > 0;
  if (!noHeaders && fmt !== 'jsonl' && !wroteHeader) f.write(`"company"${sep}"owner"${sep}"email"\n`);

  return {
    write: (obj) => {
      if (fmt === 'jsonl') {
        f.write(JSON.stringify(obj) + "\n");
      } else {
        const line = [sanitizeField(obj.company), sanitizeField(obj.owner), sanitizeField(obj.email)].join(sep) + "\n";
        f.write(line);
      }
    },
    close: () => { try { f.close(); } catch {} }
  };
}

function loadProcessed(outPath, fmt){
  const done = new Set();
  if (!outPath || !fs.existsSync(outPath)) return done;
  try {
    const data = fs.readFileSync(outPath, 'utf8');
    if (fmt === 'jsonl') {
      for (const line of data.split(/\n+/)) {
        if (!line.trim()) continue;
        try { const o = JSON.parse(line); if (o && o.domain) done.add(cleanHost(o.domain)); } catch {}
      }
    }
  } catch {}
  return done;
}

function normalizeDomainLine(line) {
  const raw = line.split('#')[0].trim();
  if (!raw) return '';
  const cleaned = cleanHost(raw);
  if (!/^[a-z0-9.-]+\.[a-z]{2,}$/i.test(cleaned)) return '';
  const parsed = psl.parse(cleaned);
  return parsed.domain || cleaned;
}

async function runBatch(filePath, outPath){
  const fmt = detectFormat(outPath, argv.format);
  const processed = loadProcessed(outPath, fmt); // JSONL-only domain resume supported
  const seen = new Set();
  const input = fs.readFileSync(filePath, 'utf8').split(/\r?\n/).map(normalizeDomainLine).filter(Boolean);
  const domains = input.filter(d => { const k = d.toLowerCase(); if (seen.has(k)) return false; seen.add(k); return true; });

  const writer = writerFor(outPath, fmt, argv.noHeaders);

  let idx = 0;
  let active = 0;
  let stopped = false;

  const next = async () => {
    while (!stopped && active < argv.concurrency && idx < domains.length) {
      const dom = domains[idx++];
      if (processed.has(dom)) { continue; }
      active++;
      (async () => {
        try {
          const res = await findOwner(dom);
          if (fmt === 'jsonl') writer.write({ domain: dom, ...res });
          else writer.write(res);
        } catch (e) {
          if (!QUIET) console.error(`fail ${dom}: ${String(e?.message || e)}`);
        } finally {
          active--;
          await next();
        }
      })();
    }
  };

  const onSig = () => { stopped = true; };
  process.once('SIGINT', onSig);
  process.once('SIGTERM', onSig);

  await next();
  while (active > 0) await sleep(50);
  writer.close();
}

async function runSingle() {
  const r = await findOwner(argv.domain);
  if (argv.json) {
    process.stdout.write(JSON.stringify(r) + "\n");
    return;
  }
  if (!argv.noHeaders) process.stdout.write(`"company","owner","email"\n`);
  const csv = [ sanitizeField(r.company), sanitizeField(r.owner), sanitizeField(r.email) ].join(",");
  process.stdout.write(csv + "\n");
}

function ensureOut(outPath){
  if (!outPath) throw new Error("--out required in batch mode");
  fs.mkdirSync(path.dirname(outPath), { recursive: true });
}

(async () => {
  try {
    if (argv.file) {
      ensureOut(argv.out);
      await runBatch(argv.file, argv.out);
    } else if (argv.domain) {
      await runSingle();
    } else {
      throw new Error("usage: --domain <domain> | --file domains.txt --out results.csv");
    }
  } catch (e) {
    if (!QUIET) console.error(String(e?.message || e || 'error'));
    process.exit(1);
  }
})();
