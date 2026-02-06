# What to do next (batch)

### 1) Put multiple targets in `domains.txt`

One per line; URLs are fine, they’ll be normalized:

```txt
cotini.it
https://example.com/
www.anotherdomain.it
```

### 2) Run

```bash
./batch-run.sh
```

It will:

* skip anything already in `processed.txt`
* append new rows to `results.csv`
* write GDPR-trimmed evidence to `logs/evidence.jsonl`

### 3) Verify progress

```bash
wc -l domains.txt processed.txt results.csv
tail -n 5 results.csv
```

---

## If you want speed vs politeness

### Faster (more parallel)

```bash
POLITE_CONCURRENCY=3 ./batch-run.sh
```

### Slower / safer on fragile sites

```bash
RATE_MS=1500 TIMEOUT_MS=20000 POLITE_CONCURRENCY=1 ./batch-run.sh
```

### Debug what it’s doing

```bash
QUIET_ENV=0 ./batch-run.sh
```

---

## Reset a run (start fresh)

```bash
rm -f processed.txt results.csv
rm -rf .cache/http
rm -f logs/evidence.jsonl
./batch-run.sh
```
