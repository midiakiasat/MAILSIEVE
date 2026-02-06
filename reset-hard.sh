
set -euo pipefail
stamp="$(date +%F_%H%M%S)"
cp results.csv "results-${stamp}.csv" 2>/dev/null || true
cp logs/evidence.jsonl "evidence-${stamp}.jsonl" 2>/dev/null || true
rm -f processed.txt .processed.set
rm -rf .cache/http 2>/dev/null || true
echo '"company","owner","email"' > results.csv
./batch-run.sh
