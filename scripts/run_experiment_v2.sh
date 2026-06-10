#!/bin/bash
set -e
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
NUM_WORKERS=${1:-8}

run_mode() {
    local MODE=$1; local SCHED_CMD=$2; local OUT="metrics/$MODE"
    mkdir -p "$OUT" logs
    echo -e "${YELLOW}=== $MODE ===${NC}"
    redis-cli FLUSHDB > /dev/null
    for i in $(seq 1 8); do redis-cli DEL "worker:$i" > /dev/null 2>&1; done
    bash -lc "$SCHED_CMD" > "logs/scheduler_${MODE}.log" 2>&1 & local SPID=$!; sleep 0.5
    for i in $(seq 1 $NUM_WORKERS); do
        ./worker/worker_v2 $i > "logs/worker_${i}_${MODE}.log" 2>&1 & sleep 0.05
    done
    echo -e "${GREEN}  $NUM_WORKERS workers started${NC}"
    k6 run --out csv="metrics/${MODE}/k6_results.csv" k6/k6_load_test.js 2>&1 | tail -15 || true
    sleep 15
    kill $SPID 2>/dev/null; pkill -f "worker_v2" 2>/dev/null; sleep 2
    for f in metrics/worker_*_v2_metrics.csv; do [ -f "$f" ] && mv "$f" "$OUT/"; done
    [ -f metrics/scheduler_v2_metrics.csv ] && mv metrics/scheduler_v2_metrics.csv "$OUT/"
    echo -e "${GREEN}  Done -> $OUT/${NC}"
}

curl -s http://localhost:3000/health > /dev/null || { echo "Start UI server first: cd ui && node server.js"; exit 1; }
run_mode "static"      "SCHED_FIXED_ALGO=FCFS ./scheduler/scheduler_v2"
run_mode "adaptive"    "./scheduler/scheduler_v2"
run_mode "adaptive_lb" "./scheduler/scheduler_v2"
python3 analysis/analysis_v2.py --modes static adaptive adaptive_lb
echo -e "${GREEN}Done! Open analysis/dashboard_v2.png${NC}"
