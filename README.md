# Self-Optimizing Distributed Transaction Scheduling System

> **A complete distributed system simulation** with adaptive scheduling, load balancing, real-time dashboard UI, and k6 load testing — all running on a single machine.

---

## What This Project Does

This system simulates how a **financial exchange processes thousands of orders** (BUY/SELL transactions) under varying load conditions. It automatically switches between 6 scheduling algorithms to minimize latency, and balances load across 8 parallel worker processes.

You can watch it all happen live in a **browser dashboard** at `http://localhost:3000`.

```
                     ┌─────────────────────────────────────────┐
                     │         YOUR BROWSER                    │
                     │   http://localhost:3000                 │
                     │   Live charts · Workers · Feed · Control│
                     └────────────────┬────────────────────────┘
                                      │ WebSocket (live data)
                     ┌────────────────▼────────────────────────┐
  k6 load test  ───► │  UI Server (server.js :3000)            │
  curl / REST   ───► │  REST API + WebSocket + Static files    │
                     └────────────────┬────────────────────────┘
                                      │ LPUSH / HGET
                     ┌────────────────▼────────────────────────┐
                     │  Redis  (shared queue)                  │
                     │  txn_queue  ·  latency_log              │
                     │  current_strategy  ·  worker:N          │
                     └──────┬──────────────────┬───────────────┘
                            │ BRPOP             │ read metrics
              ┌─────────────▼──────┐   ┌───────▼──────────────┐
              │  Workers x8        │   │  Adaptive Scheduler  │
              │  worker_v2 1..8    │   │  scheduler_v2        │
              │  1–5ms processing  │   │  6 algorithms        │
              │  writes CSV files  │   │  scores every 500ms  │
              └────────────────────┘   └──────────────────────┘
```

---

## Folder Structure

```
upgraded_scheduler/
│
├── scheduler/
│   ├── scheduler_v2.cpp          ← Adaptive scheduler (6 algorithms)
│   └── scheduler_static.cpp      ← FCFS-only baseline (for comparison)
│
├── worker/
│   └── worker_v2.cpp             ← Worker node (processes transactions)
│
├── ui/
│   ├── server.js                 ← UI server (Express + WebSocket)
│   ├── package.json              ← Node.js dependencies
│   └── public/
│       └── index.html            ← Dashboard (the browser UI)
│
├── k6/
│   ├── k6_load_test.js           ← k6 load test script
│   ├── redis_bridge.js           ← OLD bridge (replaced by server.js)
│   └── package.json
│
├── analysis/
│   └── analysis_v2.py            ← Python graphs after experiments
│
├── scripts/
│   └── run_experiment_v2.sh      ← Run all 3 experiments automatically
│
├── metrics/                      ← Created automatically, stores CSVs
│   ├── static/
│   ├── adaptive/
│   └── adaptive_lb/
│
├── logs/                         ← Per-component log files
├── Makefile                      ← Builds C++ components
└── README.md                     ← This file
```

---

## Prerequisites

| Tool | Version | Install (Ubuntu) | Install (macOS) |
|------|---------|-----------------|-----------------|
| g++ | 7+ | `sudo apt install g++ make` | `brew install gcc` |
| Redis | 5+ | `sudo apt install redis-server` | `brew install redis` |
| libhiredis | any | `sudo apt install libhiredis-dev` | `brew install hiredis` |
| Node.js | 16+ | `sudo apt install nodejs npm` | `brew install node` |
| Python 3 | 3.8+ | `sudo apt install python3-pip` | `brew install python3` |
| k6 | 0.45+ | See below | `brew install k6` |

**Install k6 on Ubuntu:**
```bash
sudo gpg --no-default-keyring \
  --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
  --keyserver hkp://keyserver.ubuntu.com:80 \
  --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69

echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/k6.list

sudo apt update && sudo apt install k6
```

---

## Quick Start — Get Everything Running

### Step 1 — Install dependencies

```bash
# C++ dependencies
sudo apt install libhiredis-dev g++ make      # Ubuntu
# brew install hiredis gcc make              # macOS

# Node.js dependencies (for the UI server)
cd ui && npm install && cd ..

# Python dependencies (for graphs)
pip3 install matplotlib pandas numpy scipy --break-system-packages
```

### Step 2 — Compile the C++ components

```bash
make
```

Expected output:
```
Building scheduler_v2...
  ✔ scheduler/scheduler_v2
Building worker_v2...
  ✔ worker/worker_v2
```

### Step 3 — Start Redis

```bash
redis-server --daemonize yes
redis-cli ping   # should print: PONG
```

### Step 4 — Create required directories

```bash
mkdir -p logs metrics/static metrics/adaptive metrics/adaptive_lb analysis/graphs_v2
```

### Step 5 — Start the UI server

```bash
cd ui && node server.js
```

You should see:
```
✔ Redis connected
╔══════════════════════════════════════════════╗
║  Scheduler UI Server — running on :3000      ║
╚══════════════════════════════════════════════╝
  Dashboard  → http://localhost:3000
  API status → http://localhost:3000/api/status
  WebSocket  → ws://localhost:3000/ws
```

**Open `http://localhost:3000` in your browser** — the dashboard is live.

### Step 6 — Start the Adaptive Scheduler

Open a new terminal:

```bash
cd upgraded_scheduler
./scheduler/scheduler_v2
```

### Step 7 — Start Worker Nodes

Open another terminal:

```bash
cd upgraded_scheduler
for i in $(seq 1 8); do
  ./worker/worker_v2 $i > logs/worker_$i.log 2>&1 &
  echo "Started worker $i"
done
```

### Step 8 — Push test transactions from the dashboard

Go to `http://localhost:3000` — use the **"Push to queue"** button in the sidebar to generate transactions instantly. Watch the charts update live.

---

## Running a Full Load Test with k6

This simulates 1000+ virtual users with realistic workload phases.

```bash
# Make sure all components are running (steps 5-7 above)
k6 run --out csv=k6_results.csv k6/k6_load_test.js
```

**Workload phases:**
```
0  - 30s : LOW LOAD   (10 users  → ~50 txn/sec)
30 - 60s : RAMP UP    (10→100 users)
60 - 90s : SPIKE      (100 users → ~500 txn/sec)
90 -120s : HIGH LOAD  (50 users  → ~250 txn/sec)
120-150s : COOL DOWN  (50→5 users)
```

Watch the dashboard while k6 runs — you'll see:
- Queue depth spike during the load phase
- The scheduler automatically switch from FCFS → PRIORITY → ROUND_ROBIN
- Workers' average latency increase then recover
- Algorithm timeline update in real time

---

## The 6 Scheduling Algorithms

The adaptive scheduler switches between these automatically based on queue depth, latency, and worker load:

| Algorithm | When it activates | What it does | Trading analogy |
|-----------|------------------|--------------|-----------------|
| **FCFS** | Low load, Q < 20 | First in, first out. No reordering. | Normal market hours |
| **PRIORITY** | Q 20–100, latency rising | HIGH-priority transactions jump the queue | Urgent order routing |
| **ROUND ROBIN** | Load imbalance detected | Workers share tasks equally via BRPOP | Order distribution |
| **SJF** | Queue overloaded, mixed sizes | Smallest orders (by quantity) first. Reduces avg wait. | Small order fast-path (HFT) |
| **WRR** | Workers have unequal load | Routes more tasks to idle workers | Exchange gateway routing |
| **MLFQ** | Sustained high load | 3 tiers: HIGH priority, small orders, bulk orders | Tiered order book |

### How the scheduler decides

Every 500ms it computes a **fitness score** for each algorithm:

```
score = w1 × norm(latency) + w2 × norm(queue_length) + w3 × load_variance
```

Lower score = better fit for current conditions. The algorithm with the lowest score wins.

---

## The Dashboard UI

Open `http://localhost:3000` to see the live dashboard.

```
┌─ TOP BAR ──────────────────────────────────────────────────────┐
│  Scheduler Dashboard v2    ●  Live    Connected    14:32:05    │
├─ SIDEBAR ──────┬─ MAIN CONTENT ─────────────────────────────────┤
│                │                                                 │
│ Active algo    │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐│
│  [FCFS]        │  │Queue │ │ Avg  │ │ P95  │ │ Thru │ │Total ││
│                │  │depth │ │ lat  │ │ lat  │ │ put  │ │ proc ││
│ Force algo     │  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘│
│ [FCFS][PRIO]   │                                                 │
│ [RR]  [SJF]    │  ┌─ Latency chart ─────┐  ┌─ Queue chart ────┐│
│ [WRR] [MLFQ]   │  │ (live line chart)   │  │ (live line chart)││
│                │  └─────────────────────┘  └──────────────────┘│
│ Generate txns  │                                                 │
│ [50] [Mixed]   │  ┌─ Algorithm timeline ────────────────────────┐│
│ [Push to queue]│  │ FCFS████ PRIORITY██ ROUND_ROBIN████████    ││
│ [Flush queue]  │  └────────────────────────────────────────────┘│
│                │                                                 │
│ Workers        │  ┌─ Feed ──────┐ ┌─ Workers ──┐ ┌─ Queue ────┐│
│ W1 ████ 12ms   │  │ BUY AAPL   │ │ W1 ●  45ms │ │ #1 BUY...  ││
│ W2 ████  8ms   │  │ SELL TSLA  │ │ W2 ●  38ms │ │ #2 SELL... ││
│ W3 ███   5ms   │  │ BUY MSFT   │ │ W3 ●  22ms │ │ #3 BUY...  ││
│                │  └────────────┘ └────────────┘ └────────────┘│
└────────────────┴────────────────────────────────────────────────┘
```

### Dashboard features

| Panel | What it shows |
|-------|---------------|
| **Stats row** | Queue depth, avg latency, P95 latency, throughput, total processed |
| **Latency chart** | Rolling avg + P95 latency over the last 60 seconds |
| **Queue chart** | Queue depth + throughput on dual Y-axis |
| **Algorithm timeline** | Colour-coded bands showing which algorithm was active and for how long |
| **Transaction feed** | Live stream of processed transactions with latency and priority |
| **Worker grid** | All 8 workers — alive/dead status, processed count, avg latency, load bar |
| **Queue inspector** | Peek at the next 10 transactions waiting in the queue |

### Sidebar controls

| Control | What it does |
|---------|-------------|
| **Force algorithm** | Override the adaptive scheduler and lock a specific algorithm |
| **Let scheduler decide** | Return to adaptive mode |
| **Generate transactions** | Push N transactions directly from the UI (no k6 needed) |
| **Priority selector** | Push all HIGH, all NORMAL, or mixed priority transactions |
| **Flush queue** | Clear all pending transactions from Redis |

---

## API Reference

The UI server exposes these REST endpoints (useful for scripting and testing):

| Method | Endpoint | Description |
|--------|---------|-------------|
| `GET`  | `/`                        | Serves the dashboard |
| `GET`  | `/health`                  | Health check: `{"status":"ok"}` |
| `GET`  | `/api/status`              | Full system snapshot |
| `GET`  | `/api/metrics/history`     | Last 120 snapshots (2 minutes) |
| `GET`  | `/api/transactions/recent` | Last 20 latency samples |
| `GET`  | `/api/queue/peek`          | Next 10 transactions in queue |
| `POST` | `/push`                    | Push one transaction |
| `POST` | `/push-batch`              | Push array of transactions |
| `POST` | `/api/workers/generate`    | Generate N test transactions |
| `POST` | `/api/control/algo`        | Force a scheduling algorithm |
| `POST` | `/api/control/flush`       | Clear the queue |
| `WS`   | `/ws`                      | WebSocket — live snapshot every 1s |

**Example — push a transaction:**
```bash
curl -X POST http://localhost:3000/push \
  -H "Content-Type: application/json" \
  -d '{"id":1,"type":"BUY","symbol":"AAPL","quantity":100,"price":152.50,"priority":1,"timestamp":1700000000000,"user_id":1}'
```

**Example — force SJF algorithm:**
```bash
curl -X POST http://localhost:3000/api/control/algo \
  -H "Content-Type: application/json" \
  -d '{"algorithm":"SJF"}'
```

**Example — generate 200 HIGH priority transactions:**
```bash
curl -X POST http://localhost:3000/api/workers/generate \
  -H "Content-Type: application/json" \
  -d '{"count":200,"priority":"high"}'
```

---

## Running Experiments

Compare static FCFS vs adaptive scheduling vs adaptive + load balancing:

```bash
# Run all 3 experiments automatically (takes ~15 minutes)
chmod +x scripts/run_experiment_v2.sh
./scripts/run_experiment_v2.sh

# Then generate comparison graphs
python3 analysis/analysis_v2.py --modes static adaptive adaptive_lb
```

### What each experiment does

**Experiment 1 — Static FCFS (baseline)**
- Scheduler locked to FCFS, never adapts
- 4 workers, standard k6 load
- Expected: High P95 latency during spike

**Experiment 2 — Adaptive**
- Scoring engine picks from 6 algorithms every 500ms
- 4 workers, same k6 load
- Expected: 10–30% lower P95 vs static

**Experiment 3 — Adaptive + Load Balancing**
- Same adaptive scheduler + 8 workers
- WRR algorithm routes by worker capacity
- Expected: Best P95, lower load variance, HIGH priority advantage

---

## Output Files

After running experiments, these files are produced:

```
metrics/
├── static/
│   ├── worker_1_v2_metrics.csv   ← per-transaction records
│   ├── worker_2_v2_metrics.csv
│   ├── scheduler_v2_metrics.csv  ← scheduler snapshots
│   └── summary.txt               ← P50/P95/P99 stats
├── adaptive/                     ← same structure
└── adaptive_lb/                  ← same structure

analysis/graphs_v2/
├── 1_latency_over_time.png
├── 2_algo_timeline.png
├── 3_scoring_heatmap.png
├── 4_percentile_comparison.png
├── 5_queue_and_load.png
├── 6_throughput.png
├── 7_cdf_comparison.png
└── dashboard_v2.png              ← all-in-one dashboard
```

### CSV column reference

**worker_N_v2_metrics.csv:**

| Column | Description |
|--------|-------------|
| `worker_id` | Which worker processed this (1–8) |
| `txn_id` | Unique transaction ID |
| `type` | BUY or SELL |
| `symbol` | Stock ticker (AAPL, GOOGL, etc.) |
| `priority` | 1 = HIGH, 0 = NORMAL |
| `quantity` | Order size (1–500) |
| `gen_timestamp_ms` | When k6 created the transaction |
| `latency_ms` | **Time waiting in queue** — the key metric |
| `proc_ms` | Processing time (1–5ms) |
| `processed_at_ms` | When worker finished |
| `strategy` | Algorithm active at time of processing |

---

## Troubleshooting

**"Cannot connect to Redis"**
```bash
redis-server --daemonize yes
redis-cli ping   # must print PONG
```

**"Cannot open shared object: libhiredis.so"**
```bash
sudo apt install libhiredis-dev   # Ubuntu
# or
brew install hiredis              # macOS
```

**Dashboard shows "Disconnected"**
```bash
# Make sure server.js is running:
cd ui && node server.js
# Then refresh http://localhost:3000
```

**Workers not showing in dashboard**
```bash
# Workers must be running:
for i in $(seq 1 8); do ./worker/worker_v2 $i & done
# They appear in dashboard within 2-3 seconds
```

**k6 connection refused on port 3000**
```bash
# server.js must be running before k6
cd ui && node server.js &
k6 run k6/k6_load_test.js
```

**Latency is always 0ms**
```bash
# Workers are faster than generation rate — add more load
k6 run --vus 50 --duration 60s k6/k6_load_test.js
# Or use the dashboard to generate 500 transactions at once
```

**Scheduler never switches algorithm**
```bash
# Thresholds may be too high for your hardware speed
# Edit scheduler/scheduler_v2.cpp:
# QUEUE_HIGH_THRESHOLD = 50   → change to 10
# LATENCY_HIGH_MS = 500       → change to 50
# Then: make && ./scheduler/scheduler_v2
```

---

## Complete Startup Sequence (Copy-Paste)

```bash
# Terminal 1 — Redis
redis-server --daemonize yes && redis-cli ping

# Terminal 2 — UI Server (open dashboard at http://localhost:3000)
cd upgraded_scheduler/ui && node server.js

# Terminal 3 — Adaptive Scheduler
cd upgraded_scheduler && ./scheduler/scheduler_v2

# Terminal 4 — Workers (8 parallel)
cd upgraded_scheduler
for i in $(seq 1 8); do
  ./worker/worker_v2 $i > logs/worker_$i.log 2>&1 &
done
echo "All workers started"

# Terminal 5 — Load Test (optional, or use dashboard instead)
k6 run --out csv=k6_results.csv k6/k6_load_test.js

# After test — Generate graphs
python3 analysis/analysis_v2.py --modes static adaptive adaptive_lb
```

---

## Performance Tuning

| Goal | File to change | Setting | Default → Try |
|------|---------------|---------|---------------|
| Lower latency | `worker_v2.cpp` | `PROC_TIME_MAX_MS` | 5ms → 2ms |
| Higher latency (stress test scheduler) | `worker_v2.cpp` | `PROC_TIME_MAX_MS` | 5ms → 100ms |
| More transactions | `k6_load_test.js` | `target: 100` → `target: 500` | More VUs |
| Trigger PRIORITY sooner | `scheduler_v2.cpp` | `QUEUE_HIGH_THRESHOLD` | 50 → 15 |
| Faster scheduler decisions | `scheduler_v2.cpp` | `POLL_MS` | 500 → 100 |
| More workers for load balancing | run command | `seq 1 8` → `seq 1 12` | 8 → 12 |

---

## Architecture Decisions

**Why Redis as a queue?**
Redis lists with LPUSH/BRPOP are an extremely fast, simple shared queue. BRPOP is blocking — workers sleep until a job arrives, so there's no CPU waste from polling. This is the same pattern used by Celery, Sidekiq, and many production job queues.

**Why a scoring engine instead of if-else?**
Rule-based if-else chains become unmaintainable after 3–4 conditions. The scoring engine gives each algorithm a continuous fitness value based on current system state, which allows smooth transitions and is easy to extend (just add a new term to the formula).

**Why WebSocket instead of REST polling?**
Polling every second means 1 HTTP request/sec. WebSocket keeps one connection open and the server pushes updates — lower latency, less overhead, and the dashboard stays accurate to within 1 second.

**Why a single HTML file for the dashboard?**
Simplicity. No build step, no webpack, no npm for the frontend. Open the file, it works. Chart.js loads from CDN. This makes the project easy to understand, modify, and demo.

---

## Technologies Used

| Component | Technology | Why |
|-----------|-----------|-----|
| Workers + Scheduler | C++17 | High performance, direct Redis control |
| Redis | Redis 5+ | Fast shared queue, pub/sub, key-value store |
| UI Server | Node.js + Express | Simple, non-blocking, great WebSocket support |
| Load Testing | k6 | Purpose-built for load testing, scriptable |
| Dashboard | Vanilla JS + Chart.js | No framework needed, fast, easy to understand |
| Graphs | Python + matplotlib | Rich scientific plotting, numpy/pandas integration |

---

*Built for the Self-Optimizing Distributed Transaction Scheduling System project.*
