#!/usr/bin/env python3
import re
import sys
import csv
import time
import html
import urllib.parse
from collections import deque
from dataclasses import dataclass
from typing import Set, List, Tuple, Optional

import requests
from bs4 import BeautifulSoup

# -------- Email patterns (includes common obfuscations) --------
EMAIL_RE = re.compile(r"""
\b
[a-z0-9._%+\-]+      # local
@
[a-z0-9.\-]+         # domain
\.[a-z]{2,}          # tld
\b
""", re.IGNORECASE | re.VERBOSE)

# e.g. "name (at) domain (dot) com", "name [at] domain dot com"
OBFUSCATED_RE = re.compile(r"""
(?P<local>[a-z0-9._%+\-]+)\s*
(?:\(|\[)?\s*(?:at|@)\s*(?:\)|\])?\s*
(?P<domain>[a-z0-9.\-]+)\s*
(?:\(|\[)?\s*(?:dot|\.)\s*(?:\)|\])?\s*
(?P<tld>[a-z]{2,})
""", re.IGNORECASE | re.VERBOSE)

PRIORITY_HINTS = (
    "contact", "contatt", "about", "chi-siamo", "company", "team",
    "privacy", "cookie", "legal", "impressum", "support", "assistenza",
)

@dataclass
class CrawlConfig:
    max_pages: int = 60
    max_depth: int = 2
    timeout: int = 20
    sleep_s: float = 0.2
    user_agent: str = "Mozilla/5.0 (compatible; email-extractor/1.0)"

def normalize_url(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    href = href.strip()
    if href.startswith("#"):
        return None
    # discard non-http links
    if href.startswith(("mailto:", "tel:", "javascript:")):
        return None
    url = urllib.parse.urljoin(base, href)
    u = urllib.parse.urlparse(url)
    if u.scheme not in ("http", "https"):
        return None
    # remove fragment
    u = u._replace(fragment="")
    return u.geturl()

def same_host(url: str, host: str) -> bool:
    u = urllib.parse.urlparse(url)
    h = (u.netloc or "").lower()
    if h.startswith("www."):
        h = h[4:]
    return h == host

def host_of(domain_or_url: str) -> str:
    u = urllib.parse.urlparse(domain_or_url if "://" in domain_or_url else "https://" + domain_or_url)
    host = (u.netloc or u.path).strip().lower().split("/")[0]
    if host.startswith("www."):
        host = host[4:]
    return host

def score_url(url: str) -> int:
    low = url.lower()
    return sum(1 for k in PRIORITY_HINTS if k in low)

def extract_emails_from_text(text: str) -> Set[str]:
    found = set()
    if not text:
        return found

    # Unescape HTML entities
    text = html.unescape(text)

    # Direct emails
    for m in EMAIL_RE.finditer(text):
        found.add(m.group(0).lower())

    # Obfuscated
    for m in OBFUSCATED_RE.finditer(text):
        email = f"{m.group('local')}@{m.group('domain')}.{m.group('tld')}".lower()
        found.add(email)

    return found

def extract_emails_from_html(url: str, html_text: str) -> Tuple[Set[str], List[str]]:
    soup = BeautifulSoup(html_text, "html.parser")

    # Collect mailto:
    emails = set()
    for a in soup.select("a[href^='mailto:']"):
        href = a.get("href", "")
        # mailto:addr1,addr2?subject=
        addr = href.split(":", 1)[1] if ":" in href else ""
        addr = addr.split("?", 1)[0]
        for part in re.split(r"[,\s;]+", addr):
            part = part.strip()
            if part and EMAIL_RE.fullmatch(part):
                emails.add(part.lower())
            else:
                emails |= extract_emails_from_text(part)

    # Visible text + raw HTML (some emails hide in attributes)
    text = soup.get_text(" ", strip=True)
    emails |= extract_emails_from_text(text)
    emails |= extract_emails_from_text(html_text)

    # Collect internal links
    links = []
    for a in soup.find_all("a"):
        href = a.get("href")
        nu = normalize_url(url, href)
        if nu:
            links.append(nu)

    return emails, links

def fetch(session: requests.Session, url: str, cfg: CrawlConfig) -> Tuple[int, str, str]:
    r = session.get(url, timeout=cfg.timeout, allow_redirects=True)
    ct = (r.headers.get("content-type") or "").lower()
    return r.status_code, ct, r.text if "text" in ct or "html" in ct or ct == "" else ""

def get_sitemap_urls(session: requests.Session, base: str, host: str, cfg: CrawlConfig) -> List[str]:
    # Try common sitemap locations
    candidates = [
        urllib.parse.urljoin(base, "/sitemap.xml"),
        urllib.parse.urljoin(base, "/sitemap_index.xml"),
    ]
    urls = []
    for sm in candidates:
        try:
            code, ct, text = fetch(session, sm, cfg)
            if code >= 400 or not text:
                continue
            # naive parse for <loc>...</loc>
            locs = re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", text, flags=re.IGNORECASE)
            for loc in locs:
                loc = loc.strip()
                if loc and same_host(loc, host):
                    urls.append(loc)
        except Exception:
            continue
    # Prioritize sitemap URLs likely to have contacts/legal, then cap
    urls = sorted(set(urls), key=lambda u: (-score_url(u), u))
    return urls[: min(len(urls), cfg.max_pages)]

def crawl_domain(domain: str, cfg: CrawlConfig) -> Tuple[Set[str], List[Tuple[str,int,str]]]:
    host = host_of(domain)
    base = "https://" + host

    session = requests.Session()
    session.headers.update({"User-Agent": cfg.user_agent})

    # Seed URLs: homepage + common contact/legal routes
    seeds = [
        base + "/",
        base + "/contact",
        base + "/contacts",
        base + "/contatti",
        base + "/about",
        base + "/chi-siamo",
        base + "/privacy",
        base + "/cookie-policy",
        base + "/legal",
        base + "/impressum",
    ]

    # Add sitemap URLs (big win)
    seeds += get_sitemap_urls(session, base, host, cfg)

    # BFS queue: (url, depth)
    q = deque()
    for s in seeds:
        if same_host(s, host):
            q.append((s, 0))

    seen = set()
    emails = set()

    # Debug records for “why empty”
    debug: List[Tuple[str,int,str]] = []

    pages = 0
    while q and pages < cfg.max_pages:
        url, depth = q.popleft()
        if url in seen or depth > cfg.max_depth:
            continue
        seen.add(url)

        try:
            code, ct, text = fetch(session, url, cfg)
            debug.append((url, code, ct))
            if code >= 400 or not text:
                continue

            found, links = extract_emails_from_html(url, text)
            emails |= found
            pages += 1

            # prioritize next URLs by heuristic score
            next_links = []
            for link in links:
                if same_host(link, host) and link not in seen:
                    next_links.append(link)

            next_links = sorted(set(next_links), key=lambda u: (-score_url(u), u))
            for link in next_links:
                q.append((link, depth + 1))

            time.sleep(cfg.sleep_s)

        except Exception:
            continue

    # Keep only emails that belong to this domain OR common provider ones (gmail etc.)?:
    # If you want strictly “on-domain” emails only, uncomment filter below.
    # emails = {e for e in emails if e.endswith(host) or e.endswith("." + host)}

    return emails, debug

def main():
    if len(sys.argv) < 2:
        print("Usage: extract_emails.py domain1 [domain2 ...]", file=sys.stderr)
        sys.exit(2)

    cfg = CrawlConfig()

    # Output one row per email (no silent dropping)
    w = csv.writer(sys.stdout, quoting=csv.QUOTE_ALL)
    w.writerow(["domain", "email"])

    for dom in sys.argv[1:]:
        dom = dom.strip()
        if not dom:
            continue
        emails, debug = crawl_domain(dom, cfg)

        if emails:
            for e in sorted(emails):
                w.writerow([host_of(dom), e])
        else:
            # Emit a sentinel row so you see failures explicitly
            w.writerow([host_of(dom), ""])
            # Minimal diagnostics to stderr (so CSV stays clean)
            print(f"[WARN] No emails found for {dom}. Sample fetches:", file=sys.stderr)
            for u, code, ct in debug[:5]:
                print(f"  - {code} {ct} {u}", file=sys.stderr)

if __name__ == "__main__":
    main()
