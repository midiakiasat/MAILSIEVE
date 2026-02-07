# MAILSIEVE

MAILSIEVE is a command-line tool for discovering publicly listed business email addresses from domains, with an emphasis on **rate-limiting, resumability, and evidence logging**.

It is designed for research, compliance checks, and operational workflows where **polite crawling and auditability** matter.

---

## Features

- Domain-based email discovery
- Safe resume via `processed.txt`
- Polite rate-limiting and concurrency controls
- CSV output (append-only)
- Evidence logging (GDPR-trimmed)
- Parallel execution with controlled fan-out

---

## Installation

Requires **Node.js ≥ 18**.

```bash
git clone https://github.com/midiakiasat/MAILSIEVE.git
cd MAILSIEVE
npm install
chmod +x batch-run.sh
````

---

## Basic Usage

### 1. Prepare input domains

Create a file named `domains.txt`:

```txt
example.com
https://anotherdomain.it
www.somedomain.org
```

One domain per line.
URLs are normalized automatically.

---

### 2. Run the batch processor

```bash
./batch-run.sh
```

MAILSIEVE will:

* skip domains already listed in `processed.txt`
* append results to `results.csv`
* log trimmed evidence to `logs/evidence.jsonl`

---

### 3. Check progress

```bash
wc -l domains.txt processed.txt results.csv
tail -n 5 results.csv
```

---

## Configuration (Environment Variables)

You can tune behavior without editing code:

### Concurrency

```bash
POLITE_CONCURRENCY=3 ./batch-run.sh
```

### Slower / safer crawling

```bash
RATE_MS=1500 TIMEOUT_MS=20000 POLITE_CONCURRENCY=1 ./batch-run.sh
```

### Verbose output

```bash
QUIET_ENV=0 ./batch-run.sh
```

---

## Reset a Run

To start fresh:

```bash
rm -f processed.txt results.csv
rm -rf .cache/http
rm -f logs/evidence.jsonl
```

Then rerun:

```bash
./batch-run.sh
```

---

## Output Files

| File                  | Purpose                   |
| --------------------- | ------------------------- |
| `results.csv`         | Discovered emails         |
| `processed.txt`       | Domains already processed |
| `logs/evidence.jsonl` | Minimal evidence trail    |

---

## Legal & Ethical Use

MAILSIEVE **only processes publicly available information**.

You are responsible for ensuring that your usage complies with:

* local laws and regulations
* website terms of service
* data protection frameworks (e.g. GDPR)

This tool is provided **as-is**, without warranty.

---

## License

See [`LICENSE`](./LICENSE).
