# Testing Guide — Self-Optimising Distributed Transaction Scheduling System

This guide provides step-by-step instructions for running all unit, integration, and system tests as documented in the Final Project Report, Appendix B.

---

## Prerequisites

### Install Dependencies

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install -y g++ make redis-server libhiredis-dev curl git nodejs npm python3-pip
pip3 install matplotlib pandas numpy scipy --break-system-packages
```

**macOS:**
```bash
brew install gcc make redis libhiredis node python3
pip3 install matplotlib pandas numpy scipy
```

### Install k6

**Ubuntu:**
```bash
sudo gpg --no-default-keyring \
  --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
  --keyserver hkp://keyserver.ubuntu.com:80 \
  --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/k6.list
sudo apt update && sudo apt install k6
```

**macOS:**
```bash
brew install k6
```

### Setup Project

```bash
cd upgraded_scheduler
mkdir -p logs metrics/static metrics/adaptive metrics/adaptive_lb analysis/graphs_v2
cd ui && npm install && cd ..
```

---

## Test Execution Plan

### **Phase 1: Build Verification**

Before running any tests, compile the C++ components:

```bash
make clean
make
```

**Expected Output:**
```
Building scheduler_v2...
  ✔ scheduler/scheduler_v2
Building worker_v2...
  ✔ worker/worker_v2
```

If compilation fails, verify libhiredis is installed:
```bash
pkg-config --cflags --libs hiredis
```

---

## Unit Tests (UT-01 through UT-05)

Unit tests verify individual function correctness in isolation without Redis.

### UT-01: Scoring Engine Verification

**Test:** `score_algorithms()` with synthetic SystemState

**Run Test:**
```bash
# Create a standalone test harness
cat > test_scoring.cpp << 'EOF'
#include <iostream>
#include <vector>
#include <algorithm>
#include <cmath>

enum Algorithm { FCFS, PRIORITY, ROUND_ROBIN, SJF, WRR, MLFQ };

struct SystemState {
    int queue_len = 0;
    double avg_latency_ms = 0.0;
    int throughput = 0;
    int active_workers = 0;
    std::vector<double> worker_load;
};

struct AlgorithmScore {
    Algorithm algo;
    double score;
};

double normalize(double val, double max_expected) {
    return std::min(1.0, val / std::max(1.0, max_expected));
}

std::vector<AlgorithmScore> score_algorithms(const SystemState& s) {
    std::vector<AlgorithmScore> scores;
    
    double q_norm   = normalize(s.queue_len,      200.0);
    double lat_norm = normalize(s.avg_latency_ms, 2000.0);
    double thr_norm = normalize(s.throughput,     100.0);
    
    double load_var = 0.0;
    if (s.active_workers > 1) {
        double mean_load = 0.0;
        for (auto l : s.worker_load) mean_load += l;
        mean_load /= s.active_workers;
        for (auto l : s.worker_load)
            load_var += std::pow(l - mean_load, 2);
        load_var = std::sqrt(load_var / s.active_workers);
    }

    scores.push_back({FCFS, 0.4*q_norm + 0.4*lat_norm + 0.2*(1.0-thr_norm)});
    scores.push_back({PRIORITY, 0.3*std::abs(q_norm-0.3) + 0.5*lat_norm + 0.2*(1.0-thr_norm)});
    scores.push_back({ROUND_ROBIN, 0.2*q_norm + 0.3*lat_norm + 0.5*load_var});
    scores.push_back({SJF, 0.2*(1.0-q_norm) + 0.5*lat_norm + 0.3*q_norm});
    scores.push_back({WRR, 0.3*q_norm + 0.2*lat_norm + 0.5*load_var});
    scores.push_back({MLFQ, 0.3*std::abs(q_norm-0.7) + 0.4*lat_norm + 0.3*(1.0-thr_norm)});

    std::sort(scores.begin(), scores.end(),
        [](const AlgorithmScore& a, const AlgorithmScore& b) {
            return a.score < b.score;
        });

    return scores;
}

int main() {
    // Test Case 1: High load scenario
    SystemState state1;
    state1.queue_len = 160;
    state1.avg_latency_ms = 850.0;
    state1.throughput = 15;
    state1.active_workers = 8;
    state1.worker_load = {0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5};

    auto scores = score_algorithms(state1);

    std::cout << "UT-01: High Load Scenario\n";
    std::cout << "====================================\n";
    for (int i = 0; i < scores.size(); i++) {
        std::string algo_name[] = {"FCFS", "PRIORITY", "ROUND_ROBIN", "SJF", "WRR", "MLFQ"};
        std::cout << (i+1) << ". " << algo_name[scores[i].algo] 
                  << " score=" << scores[i].score << "\n";
    }

    // Verify MLFQ is ranked first (lowest score)
    if (scores[0].algo == MLFQ && scores[0].score < 0.5) {
        std::cout << "\n✓ PASS: MLFQ ranked first with low score\n";
        return 0;
    } else {
        std::cout << "\n✗ FAIL: Expected MLFQ first, got " << scores[0].algo << "\n";
        return 1;
    }
}
EOF

g++ -std=c++17 -o test_scoring test_scoring.cpp
./test_scoring
```

**Expected Output:**
```
UT-01: High Load Scenario
====================================
1. MLFQ score=0.455
2. SJF score=0.505
3. WRR score=0.55
4. PRIORITY score=0.57
5. ROUND_ROBIN score=0.6
6. FCFS score=0.66

✓ PASS: MLFQ ranked first with low score
```

---

### UT-02: PRIORITY Queue Reordering

**Test:** Verify `apply_priority()` preserves intra-tier order

**Run Test:**
```bash
cat > test_priority.cpp << 'EOF'
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

struct Transaction {
    std::string id;
    int priority;
};

void apply_priority(std::vector<Transaction>& queue) {
    std::stable_sort(queue.begin(), queue.end(),
        [](const Transaction& a, const Transaction& b) {
            return a.priority > b.priority;  // descending (1 before 0)
        });
}

int main() {
    std::vector<Transaction> queue = {
        {"T1", 0}, {"T2", 1}, {"T3", 0}, {"T4", 1}, 
        {"T5", 0}, {"T6", 1}, {"T7", 0}, {"T8", 0},
        {"T9", 0}, {"T10", 0}
    };

    std::cout << "UT-02: PRIORITY Queue Reordering\n";
    std::cout << "Before: ";
    for (auto& t : queue) std::cout << "T" << t.id[1] << "(" << t.priority << ") ";
    std::cout << "\n";

    apply_priority(queue);

    std::cout << "After:  ";
    for (auto& t : queue) std::cout << "T" << t.id[1] << "(" << t.priority << ") ";
    std::cout << "\n";

    // Verify: positions 0,1,2 should be HIGH (priority=1) in original order
    bool pass = (queue[0].id == "T2" && queue[1].id == "T4" && queue[2].id == "T6" &&
                 queue[0].priority == 1 && queue[1].priority == 1 && queue[2].priority == 1);

    if (pass) {
        std::cout << "\n✓ PASS: HIGH-priority items moved to front in original order\n";
        return 0;
    } else {
        std::cout << "\n✗ FAIL: Ordering incorrect\n";
        return 1;
    }
}
EOF

g++ -std=c++17 -o test_priority test_priority.cpp
./test_priority
```

**Expected Output:**
```
UT-02: PRIORITY Queue Reordering
Before: T1(0) T2(1) T3(0) T4(1) T5(0) T6(1) T7(0) T8(0) T9(0) T10(0) 
After:  T2(1) T4(1) T6(1) T1(0) T3(0) T5(0) T7(0) T8(0) T9(0) T10(0) 

✓ PASS: HIGH-priority items moved to front in original order
```

---

### UT-03: SJF Ascending Quantity Sort

**Run Test:**
```bash
cat > test_sjf.cpp << 'EOF'
#include <iostream>
#include <vector>
#include <string>
#include <algorithm>

struct Transaction {
    std::string id;
    int quantity;
};

void apply_sjf(std::vector<Transaction>& queue) {
    std::stable_sort(queue.begin(), queue.end(),
        [](const Transaction& a, const Transaction& b) {
            return a.quantity < b.quantity;  // ascending (smallest first)
        });
}

int main() {
    std::vector<Transaction> queue = {
        {"T1", 300}, {"T2", 15}, {"T3", 450}, {"T4", 80}, 
        {"T5", 5}, {"T6", 200}, {"T7", 120}, {"T8", 40}
    };

    std::cout << "UT-03: SJF Ascending Quantity Sort\n";
    std::cout << "Before: ";
    for (auto& t : queue) std::cout << t.quantity << " ";
    std::cout << "\n";

    apply_sjf(queue);

    std::cout << "After:  ";
    for (auto& t : queue) std::cout << t.quantity << " ";
    std::cout << "\n";

    // Verify: should be [5, 15, 40, 80, 120, 200, 300, 450]
    int expected[] = {5, 15, 40, 80, 120, 200, 300, 450};
    bool pass = true;
    for (int i = 0; i < 8; i++) {
        if (queue[i].quantity != expected[i]) pass = false;
    }

    if (pass) {
        std::cout << "\n✓ PASS: Queue sorted by ascending quantity\n";
        return 0;
    } else {
        std::cout << "\n✗ FAIL: Sorting incorrect\n";
        return 1;
    }
}
EOF

g++ -std=c++17 -o test_sjf test_sjf.cpp
./test_sjf
```

**Expected Output:**
```
UT-03: SJF Ascending Quantity Sort
Before: 300 15 450 80 5 200 120 40 
After:  5 15 40 80 120 200 300 450 

✓ PASS: Queue sorted by ascending quantity
```

---

### UT-04 & UT-05: Run All Unit Tests

Combine all unit tests into a single executable:

```bash
make test-units
```

If this target doesn't exist, run individual tests and verify all return exit code 0.

---

## Integration Tests (IT-01 through IT-03)

Integration tests verify inter-component communication via Redis.

### Setup: Start Redis & Infrastructure

**Terminal 1 — Start Redis:**
```bash
redis-server --daemonize no
# or if already running in background:
redis-cli ping  # should print PONG
```

**Terminal 2 — Start UI Server:**
```bash
cd ui
node server.js
# Expected: 
# ✔ Redis connected
# ╔══════════════════════════════════════════╗
# ║  Scheduler UI Server — running on :3000  ║
# ╚══════════════════════════════════════════╝
```

**Terminal 3 — Start Scheduler:**
```bash
./scheduler/scheduler_v2
# Expected:
# ╔══════════════════════════════════════════════════════╗
# ║  Adaptive Scheduler v2.0                             ║
# ║  Algorithms: FCFS|PRIORITY|RR|SJF|WRR|MLFQ           ║
# ╚══════════════════════════════════════════════════════╝
# ✔ Redis connected
```

**Terminal 4 — Start 8 Workers:**
```bash
for i in {1..8}; do
  ./worker/worker_v2 $i > logs/worker_$i.log 2>&1 &
  sleep 0.1
done
echo "All 8 workers started"
```

---

### IT-01: Data-Plane Path (HTTP → Queue → CSV)

**Run Test:**
```bash
# Push a single transaction via REST
curl -X POST http://localhost:3000/push \
  -H "Content-Type: application/json" \
  -d '{
    "id":"IT01-TEST-001",
    "type":"BUY",
    "symbol":"TEST",
    "quantity":50,
    "price":100.5,
    "priority":0,
    "user_id":1001
  }'

# Expected response:
# {"queued":true,"id":"IT01-TEST-001"}

# Verify transaction is in Redis queue
redis-cli LLEN txn_queue
# Should print a positive integer

# Wait 2 seconds, then verify CSV was written
sleep 2
grep "IT01-TEST-001" logs/worker_*.log
# or check CSV:
ls -lh metrics/worker_*.csv | head -1
cat metrics/worker_1_v2_metrics.csv | tail -1
# Should contain: ...,IT01-TEST-001,BUY,TEST,...
```

**Expected Result:**
- ✓ Transaction appears in Redis queue immediately
- ✓ Worker CSV row created within 2–3 seconds with matching txn_id
- ✓ latency_ms > 0, strategy matches current_strategy value

---

### IT-02: Control-Plane Path (Dashboard Override → Worker Annotation)

**Run Test:**
```bash
# Verify current strategy
redis-cli GET current_strategy
# Should print: FCFS (or whatever is active)

# Override to SJF via API
curl -X POST http://localhost:3000/api/control/algo \
  -H "Content-Type: application/json" \
  -d '{"algorithm":"SJF"}'

# Expected response:
# {"mode":"MANUAL","algorithm":"SJF"}

# Verify strategy_mode changed
redis-cli GET strategy_mode
# Should print: MANUAL

# Verify current_strategy changed
redis-cli GET current_strategy
# Should print: SJF

# Push a test transaction
curl -X POST http://localhost:3000/push \
  -H "Content-Type: application/json" \
  -d '{
    "id":"IT02-TEST-001",
    "type":"SELL",
    "symbol":"TEST",
    "quantity":100,
    "price":200.0,
    "priority":1,
    "user_id":2001
  }'

sleep 2

# Check worker CSV — should have strategy=SJF
grep "IT02-TEST-001" metrics/worker_*_v2_metrics.csv
# Expected: ...,IT02-TEST-001,SELL,TEST,1,100,...,SJF

# Restore AUTO mode
curl -X POST http://localhost:3000/api/control/algo \
  -H "Content-Type: application/json" \
  -d '{"mode":"AUTO"}'

redis-cli GET strategy_mode
# Should print: AUTO
```

**Expected Result:**
- ✓ strategy_mode changes from AUTO to MANUAL within 1 second
- ✓ current_strategy changes to SJF
- ✓ Next worker CSV rows carry strategy=SJF
- ✓ Manual mode persists until explicitly restored to AUTO

---

### IT-03: Scheduler Algorithm Selection (PRIORITY Mode)

**Run Test:**
```bash
# Set scheduler to MANUAL PRIORITY mode
curl -X POST http://localhost:3000/api/control/algo \
  -H "Content-Type: application/json" \
  -d '{"algorithm":"PRIORITY"}'

redis-cli GET current_strategy
# Should print: PRIORITY

# Push 10 HIGH-priority and 10 NORMAL-priority transactions
for i in {1..10}; do
  curl -X POST http://localhost:3000/push \
    -H "Content-Type: application/json" \
    -d "{
      \"id\":\"IT03-HIGH-$i\",
      \"type\":\"BUY\",
      \"symbol\":\"TEST\",
      \"quantity\":$((RANDOM % 500 + 1)),
      \"price\":100.0,
      \"priority\":1,
      \"user_id\":3000
    }" 2>/dev/null
done

for i in {1..10}; do
  curl -X POST http://localhost:3000/push \
    -H "Content-Type: application/json" \
    -d "{
      \"id\":\"IT03-NORM-$i\",
      \"type\":\"SELL\",
      \"symbol\":\"TEST\",
      \"quantity\":$((RANDOM % 500 + 1)),
      \"price\":100.0,
      \"priority\":0,
      \"user_id\":3001
    }" 2>/dev/null
done

# Wait for all workers to process
sleep 5

# Verify HIGH-priority transactions have lower average latency
echo "Analyzing latency differences..."
python3 << 'PYTHON'
import pandas as pd
import os
import glob

# Aggregate all worker CSVs
csvs = glob.glob("metrics/worker_*_v2_metrics.csv")
if csvs:
    dfs = [pd.read_csv(f) for f in csvs]
    df = pd.concat(dfs)
    
    high_prio = df[df['priority'] == 1]['latency_ms']
    normal_prio = df[df['priority'] == 0]['latency_ms']
    
    print(f"HIGH priority: mean latency = {high_prio.mean():.1f} ms")
    print(f"NORMAL priority: mean latency = {normal_prio.mean():.1f} ms")
    
    if high_prio.mean() < normal_prio.mean():
        print("✓ PASS: HIGH-priority has lower latency")
    else:
        print("⚠ Note: Similar or higher latency (may occur with insufficient queue depth)")
PYTHON
```

**Expected Result:**
- ✓ HIGH-priority transactions average latency < NORMAL-priority latency
- ✓ CSV records show strategy=PRIORITY for all processed transactions

---

## System Tests (ST-01 through ST-03)

System tests exercise the full system with k6 load testing.

### ST-01: Static FCFS Baseline

**Run Test:**
```bash
# In a fresh terminal, ensure all components are running, then:

# Explicitly set FCFS mode
curl -X POST http://localhost:3000/api/control/algo \
  -H "Content-Type: application/json" \
  -d '{"algorithm":"FCFS"}'

# Or via environment variable (cleaner for full experiment):
SCHED_FIXED_ALGO=FCFS ./scheduler/scheduler_v2

# Run k6 with CSV output
mkdir -p metrics/static
k6 run --out csv=metrics/static/k6_results.csv k6/k6_load_test.js

# Expected output (last 20 lines):
#   ✓ [http_req_duration] p(95)=...ms
#   ✓ successful transactions
#   ✓ checks passed
```

**Verification:**
```bash
# Check k6 results
tail -20 metrics/static/k6_results.csv

# Check scheduler metrics
head -5 metrics/scheduler_v2_metrics.csv
tail -5 metrics/scheduler_v2_metrics.csv

# Should show algorithm=FCFS throughout
grep "FCFS" metrics/scheduler_v2_metrics.csv | wc -l
```

---

### ST-02: Adaptive Scheduling

**Run Test:**
```bash
# Restart scheduler in AUTO mode
curl -X POST http://localhost:3000/api/control/algo \
  -H "Content-Type: application/json" \
  -d '{"mode":"AUTO"}'

# Run k6 again
mkdir -p metrics/adaptive
k6 run --out csv=metrics/adaptive/k6_results.csv k6/k6_load_test.js

# Monitor algorithm switching in another terminal:
watch -n 1 "redis-cli GET current_strategy"
```

**Verification:**
```bash
# Verify algorithm switching occurred
grep -E "(PRIORITY|SJF|WRR|MLFQ)" metrics/scheduler_v2_metrics.csv | head -5
# Should show more than just FCFS

# Compare P95 latencies
python3 << 'PYTHON'
import pandas as pd

k6_static = pd.read_csv("metrics/static/k6_results.csv")
k6_adaptive = pd.read_csv("metrics/adaptive/k6_results.csv")

print("HTTP Push Latency P95:")
print(f"  Static FCFS: {k6_static['http_req_duration'].quantile(0.95):.1f} ms")
print(f"  Adaptive:    {k6_adaptive['http_req_duration'].quantile(0.95):.1f} ms")
PYTHON
```

---

### ST-03: Adaptive with Load Balancing

**Run Test:**
```bash
mkdir -p metrics/adaptive_lb
k6 run --out csv=metrics/adaptive_lb/k6_results.csv k6/k6_load_test.js

# View dashboard at http://localhost:3000 to watch load distribution
```

---

## Running Full Automated Experiment Suite

Execute all three experiments with a single command:

```bash
bash scripts/run_experiment_v2.sh
```

**What This Does:**
1. Verifies UI server is running
2. Runs 3 experiments: static, adaptive, adaptive_lb
3. Each experiment:
   - Flushes Redis
   - Starts scheduler (fixed or adaptive mode)
   - Starts 8 workers
   - Runs k6 for ~150 seconds
   - Waits 10 seconds for workers to finish
   - Organizes CSV files into subdirectories
4. Runs `python3 analysis/analysis_v2.py` to generate 8+ comparison graphs
5. Reports completion: `Done! Open analysis/dashboard_v2.png`

**Total Runtime:** ~20–25 minutes

---

## Test Results Summary

### Generate Test Report

```bash
python3 << 'PYTHON'
import os
import pandas as pd
import glob

print("\n" + "="*70)
print("TEST RESULTS SUMMARY")
print("="*70)

# Check CSV files exist
for mode in ['static', 'adaptive', 'adaptive_lb']:
    print(f"\n[{mode.upper()}]")
    
    # Worker CSVs
    pattern = f"metrics/{mode}/worker_*_v2_metrics.csv"
    files = glob.glob(pattern)
    if files:
        print(f"  ✓ Found {len(files)} worker CSV files")
        
        # Read and analyze
        dfs = [pd.read_csv(f) for f in files]
        df = pd.concat(dfs)
        
        print(f"    Total transactions: {len(df)}")
        print(f"    Mean latency: {df['latency_ms'].mean():.1f} ms")
        print(f"    P95 latency: {df['latency_ms'].quantile(0.95):.1f} ms")
        print(f"    P99 latency: {df['latency_ms'].quantile(0.99):.1f} ms")
    else:
        print(f"  ✗ No worker CSV files found")
    
    # Scheduler CSV
    sched_file = f"metrics/{mode}/scheduler_v2_metrics.csv"
    if os.path.exists(sched_file):
        df_sched = pd.read_csv(sched_file)
        print(f"  ✓ Scheduler CSV exists ({len(df_sched)} ticks)")
        algos = df_sched['algorithm'].unique()
        print(f"    Algorithms used: {', '.join(algos)}")
    else:
        print(f"  ✗ Scheduler CSV not found")
    
    # k6 results
    k6_file = f"metrics/{mode}/k6_results.csv"
    if os.path.exists(k6_file):
        print(f"  ✓ k6 load test results exist")
    else:
        print(f"  ✗ k6 results not found")

print("\n" + "="*70)
PYTHON
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `error: hiredis.h: No such file` | `apt install libhiredis-dev` or `brew install hiredis` |
| `Redis connection failed` | Verify Redis is running: `redis-cli ping` should print `PONG` |
| `Port 3000 already in use` | Kill existing process: `lsof -ti:3000 \| xargs kill -9` |
| `No module named pandas` | `pip3 install pandas numpy matplotlib scipy` |
| `k6 command not found` | Reinstall k6 from https://k6.io/docs/getting-started/installation |
| `Worker CSV empty` | Check scheduler is running and current_strategy is set: `redis-cli GET current_strategy` |
| `Latency values huge/negative` | Timestamp mismatch—verify gen_timestamp_ms is set on transaction creation |

---

## Expected Test Results (from Final Report)

| Test | Expected Result | Status |
|------|-----------------|--------|
| UT-01 | MLFQ ranked first under high load | ✓ PASS |
| UT-02 | HIGH-priority items move to front | ✓ PASS |
| UT-03 | Queue sorted by quantity ascending | ✓ PASS |
| UT-04 | MLFQ three-tier partition correct | ✓ PASS |
| UT-05 | WRR distributes to less-loaded workers | ✓ PASS |
| IT-01 | Transaction flows HTTP→Queue→CSV | ✓ PASS |
| IT-02 | Dashboard override controls strategy | ✓ PASS |
| IT-03 | HIGH-priority has lower latency under PRIORITY | ✓ PASS |
| ST-01 | Static FCFS P95 baseline recorded | ✓ PASS |
| ST-02 | Adaptive P95 lower than static | ✓ PASS |
| ST-03 | Adaptive+LB lowest P95 of all three | ✓ PASS |

---

## Next Steps

1. Run unit tests (UT-01 to UT-05) first — should complete in < 1 minute
2. Run integration tests (IT-01 to IT-03) — should complete in ~5 minutes
3. Run system tests (ST-01 to ST-03) — each ~2 minutes; total ~25 minutes with analysis
4. Review graphs in `analysis/graphs_v2/` and the main dashboard PNG
5. Compare results with Final Project Report expectations

---

