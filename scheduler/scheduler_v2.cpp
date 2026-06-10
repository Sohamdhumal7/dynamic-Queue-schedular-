/**
 * ══════════════════════════════════════════════════════════════
 *  Advanced Adaptive Scheduler v2.0
 *  Self-Optimizing Distributed Transaction Scheduling System
 * ══════════════════════════════════════════════════════════════
 *
 *  SCHEDULING ALGORITHMS IMPLEMENTED:
 *  ─────────────────────────────────
 *  1. FCFS          – First Come First Served
 *  2. PRIORITY      – High-priority transactions first (reorder queue)
 *  3. ROUND_ROBIN   – Even distribution across workers
 *  4. SJF           – Shortest Job First (by estimated size/qty)
 *  5. WRR           – Weighted Round Robin (workers get diff weights)
 *  6. MLFQ          – Multi-Level Feedback Queue (3 priority tiers)
 *
 *  ADAPTIVE LOGIC:
 *  ──────────────
 *  Uses a SCORING SYSTEM instead of simple if-else:
 *    Score = w1*latency_norm + w2*queue_norm + w3*(1-throughput_norm)
 *  Each algorithm gets a fitness score → best one is selected.
 *
 *  REAL-TRADING ALGORITHMS:
 *  ────────────────────────
 *  - SJF mimics "small order priority" used in HFT systems
 *  - WRR mimics capacity-aware routing in exchange gateways
 *  - MLFQ mimics tiered order books (market vs limit vs stop)
 * ══════════════════════════════════════════════════════════════
 */

#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <deque>
#include <map>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <thread>
#include <atomic>
#include <fstream>
#include <cmath>
#include <iomanip>
#include <cstdlib>
#include <cctype>
#include <signal.h>
#include <hiredis/hiredis.h>

// ── Config ────────────────────────────────────────────────────
static const char* REDIS_HOST       = "127.0.0.1";
static const int   REDIS_PORT       = 6379;
static const char* MAIN_QUEUE       = "txn_queue";
static const char* STRATEGY_KEY     = "current_strategy";
static const char* MODE_KEY         = "strategy_mode";
static const char* LATENCY_LIST     = "latency_log";
static const char* SCHED_CSV        = "metrics/scheduler_v2_metrics.csv";
static const int   MAX_WORKERS      = 8;
static const int   POLL_MS          = 500;      // decision every 500ms
static const int   LATENCY_WINDOW   = 30;       // rolling window size

// ── Scheduling Algorithm Enum ─────────────────────────────────
enum Algorithm {
    FCFS = 0, PRIORITY, ROUND_ROBIN, SJF, WRR, MLFQ
};

const char* algo_name(Algorithm a) {
    switch(a) {
        case FCFS:        return "FCFS";
        case PRIORITY:    return "PRIORITY";
        case ROUND_ROBIN: return "ROUND_ROBIN";
        case SJF:         return "SJF";
        case WRR:         return "WRR";
        case MLFQ:        return "MLFQ";
    }
    return "UNKNOWN";
}

std::string upper_copy(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(),
        [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return s;
}

bool parse_algo_name(const std::string& raw, Algorithm& out) {
    std::string s = upper_copy(raw);
    std::replace(s.begin(), s.end(), '-', '_');
    std::replace(s.begin(), s.end(), ' ', '_');

    if (s == "FCFS") { out = FCFS; return true; }
    if (s == "PRIORITY") { out = PRIORITY; return true; }
    if (s == "ROUND_ROBIN" || s == "RR") { out = ROUND_ROBIN; return true; }
    if (s == "SJF") { out = SJF; return true; }
    if (s == "WRR") { out = WRR; return true; }
    if (s == "MLFQ") { out = MLFQ; return true; }
    return false;
}

// ── Shutdown ──────────────────────────────────────────────────
std::atomic<bool> g_running{true};
void sig_handler(int) { g_running = false; }

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ── System State ─────────────────────────────────────────────
struct SystemState {
    long long queue_len      = 0;
    double    avg_latency_ms = 0.0;
    long long throughput     = 0;   // txns/sec in last interval
    int       active_workers = 0;
    double    worker_load[MAX_WORKERS] = {};  // 0.0-1.0 per worker
};

// ══════════════════════════════════════════════════════════════
//  SCORING ENGINE
//  Each algorithm gets a score based on current system state.
//  Lower score = better fit for current conditions.
// ══════════════════════════════════════════════════════════════
struct AlgorithmScore {
    Algorithm algo;
    double    score;
};

// Normalize a value to [0,1] using soft clamping
double normalize(double val, double max_expected) {
    return std::min(1.0, val / std::max(1.0, max_expected));
}

// Compute fitness score for each algorithm given current state.
// Returns vector sorted by score ascending (best first).
std::vector<AlgorithmScore> score_algorithms(const SystemState& s) {
    double q_norm   = normalize(s.queue_len,      200.0);
    double lat_norm = normalize(s.avg_latency_ms, 2000.0);
    double thr_norm = normalize(s.throughput,     100.0);
    double load_var = 0.0; // load imbalance across workers

    if (s.active_workers > 1) {
        double mean_load = 0.0;
        for (int i = 0; i < s.active_workers; i++)
            mean_load += s.worker_load[i];
        mean_load /= s.active_workers;
        for (int i = 0; i < s.active_workers; i++)
            load_var += std::pow(s.worker_load[i] - mean_load, 2);
        load_var = std::sqrt(load_var / s.active_workers);
    }

    // ── Algorithm fitness rules ───────────────────────────────
    // Each score = weighted sum of bad indicators for that algo.
    // A lower score means this algorithm handles the current
    // situation better.

    std::vector<AlgorithmScore> scores;

    // FCFS: good when queue is short and latency is low
    // Bad: high queue or high latency (everything waits equally)
    scores.push_back({FCFS,
        0.4 * q_norm +
        0.4 * lat_norm +
        0.2 * (1.0 - thr_norm)
    });

    // PRIORITY: good when queue is medium and there are high-prio txns
    // Bad: very empty (overhead not worth it) or over-saturated
    scores.push_back({PRIORITY,
        0.3 * std::abs(q_norm - 0.3) +   // ideal at ~30% queue fullness
        0.5 * lat_norm +
        0.2 * (1.0 - thr_norm)
    });

    // ROUND_ROBIN: good when load is imbalanced across workers
    // Bad: single worker or all equal
    scores.push_back({ROUND_ROBIN,
        0.2 * q_norm +
        0.3 * lat_norm +
        0.5 * load_var   // higher load variance → RR is better
    });

    // SJF: good when queue is overloaded with mixed sizes
    // Reduces average wait time by processing small jobs first
    // Bad: uniform job sizes (no benefit)
    scores.push_back({SJF,
        0.2 * (1.0 - q_norm) +  // needs some queue to be useful
        0.5 * lat_norm +
        0.3 * q_norm
    });

    // WRR: good when workers have different capacities
    // and load is heavily skewed
    scores.push_back({WRR,
        0.3 * q_norm +
        0.2 * lat_norm +
        0.5 * load_var
    });

    // MLFQ: good in high-load, mixed-priority scenarios
    // Best at sustained high load with priority differentiation
    scores.push_back({MLFQ,
        0.3 * std::abs(q_norm - 0.7) +  // ideal near saturation
        0.4 * lat_norm +
        0.3 * (1.0 - thr_norm)
    });

    std::sort(scores.begin(), scores.end(),
        [](const AlgorithmScore& a, const AlgorithmScore& b){
            return a.score < b.score;
        });

    return scores;
}

// ══════════════════════════════════════════════════════════════
//  QUEUE MANIPULATION FUNCTIONS
//  Each algorithm reshapes the Redis queue differently.
// ══════════════════════════════════════════════════════════════

// Extract a JSON field value (reuse from original worker)
std::string json_field(const std::string& json, const std::string& key) {
    std::string search = "\"" + key + "\"";
    size_t pos = json.find(search);
    if (pos == std::string::npos) return "";
    pos += search.size();
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos])))
        pos++;
    if (pos >= json.size() || json[pos] != ':') return "";
    pos++;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos])))
        pos++;
    if (pos >= json.size()) return "";
    if (json[pos] == '"') {
        size_t s = pos+1, e = json.find('"', s);
        if (e == std::string::npos) return "";
        return json.substr(s, e-s);
    }
    size_t e = json.find_first_of(",}", pos);
    if (e == std::string::npos) e = json.size();
    return json.substr(pos, e-pos);
}

// Fetch up to N items from queue (non-destructive)
std::vector<std::string> fetch_queue(redisContext* ctx, int n = 300) {
    std::vector<std::string> items;
    redisReply* r = (redisReply*)redisCommand(
        ctx, "LRANGE %s 0 %d", MAIN_QUEUE, n-1);
    if (r && r->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < r->elements; i++)
            items.push_back(r->element[i]->str);
    }
    if (r) freeReplyObject(r);
    return items;
}

// Replace entire queue with reordered items
void replace_queue(redisContext* ctx, const std::vector<std::string>& items) {
    if (items.empty()) return;
    redisReply* r = (redisReply*)redisCommand(ctx, "DEL %s", MAIN_QUEUE);
    if (r) freeReplyObject(r);
    // RPUSH in order so BRPOP (right-pop) gets first item first
    for (auto it = items.rbegin(); it != items.rend(); ++it) {
        redisReply* pr = (redisReply*)redisCommand(
            ctx, "RPUSH %s %s", MAIN_QUEUE, it->c_str());
        if (pr) freeReplyObject(pr);
    }
}

// ── PRIORITY: sort by priority field desc ─────────────────────
void apply_priority(redisContext* ctx) {
    auto items = fetch_queue(ctx);
    if (items.size() < 2) return;
    std::stable_sort(items.begin(), items.end(),
        [](const std::string& a, const std::string& b) {
            int pa = 0, pb = 0;
            try { pa = std::stoi(json_field(a, "priority")); } catch(...) {}
            try { pb = std::stoi(json_field(b, "priority")); } catch(...) {}
            return pa > pb;
        });
    replace_queue(ctx, items);
    std::cout << "  [PRIORITY] Queue reordered by priority\n";
}

// ── SJF: sort by quantity (proxy for job size) asc ────────────
void apply_sjf(redisContext* ctx) {
    auto items = fetch_queue(ctx);
    if (items.size() < 2) return;
    std::stable_sort(items.begin(), items.end(),
        [](const std::string& a, const std::string& b) {
            int qa = 999, qb = 999;
            try { qa = std::stoi(json_field(a, "quantity")); } catch(...) {}
            try { qb = std::stoi(json_field(b, "quantity")); } catch(...) {}
            return qa < qb; // smallest quantity = shortest job
        });
    replace_queue(ctx, items);
    std::cout << "  [SJF] Queue reordered by job size (quantity)\n";
}

// ── MLFQ: 3 tiers → HIGH_PRIO, NORMAL, BULK ──────────────────
// Tier assignment: priority=1 → tier 0 (fastest)
//                 quantity<100 → tier 1 (normal)
//                 quantity>=100 → tier 2 (bulk/slow)
void apply_mlfq(redisContext* ctx) {
    auto items = fetch_queue(ctx);
    if (items.size() < 2) return;

    std::vector<std::string> tier0, tier1, tier2;
    for (const auto& item : items) {
        int prio = 0, qty = 999;
        try { prio = std::stoi(json_field(item, "priority")); } catch(...) {}
        try { qty  = std::stoi(json_field(item, "quantity")); } catch(...) {}

        if (prio == 1)      tier0.push_back(item);
        else if (qty < 100) tier1.push_back(item);
        else                tier2.push_back(item);
    }

    std::vector<std::string> merged;
    for (auto& x : tier0) merged.push_back(x);
    for (auto& x : tier1) merged.push_back(x);
    for (auto& x : tier2) merged.push_back(x);
    replace_queue(ctx, merged);

    std::cout << "  [MLFQ] Tier0(HIGH)=" << tier0.size()
              << " Tier1(NORM)=" << tier1.size()
              << " Tier2(BULK)=" << tier2.size() << "\n";
}

// ── WRR: Distribute across per-worker queues by weight ────────
// Workers with lower avg_load get a larger slice of tasks.
void apply_wrr(redisContext* ctx, const SystemState& s) {
    if (s.active_workers < 2) return;
    auto items = fetch_queue(ctx);
    if (items.empty()) return;

    // Compute inverse-load weights (less loaded = more work)
    std::vector<double> weights(s.active_workers);
    double total_weight = 0.0;
    for (int i = 0; i < s.active_workers; i++) {
        weights[i] = 1.0 - s.worker_load[i] + 0.01; // avoid 0
        total_weight += weights[i];
    }

    // Distribute items into per-worker queues
    // worker queue key: "worker_queue:N"
    std::vector<int> alloc(s.active_workers, 0);
    int total = (int)items.size();
    for (int i = 0; i < s.active_workers; i++) {
        alloc[i] = (int)std::round(weights[i] / total_weight * total);
    }

    int idx = 0;
    for (int w = 0; w < s.active_workers && idx < total; w++) {
        std::string wq = "worker_queue:" + std::to_string(w+1);
        for (int j = 0; j < alloc[w] && idx < total; j++, idx++) {
            redisReply* r = (redisReply*)redisCommand(
                ctx, "LPUSH %s %s", wq.c_str(), items[idx].c_str());
            if (r) freeReplyObject(r);
        }
    }

    // Clear main queue (items moved to worker queues)
    redisReply* dr = (redisReply*)redisCommand(ctx, "DEL %s", MAIN_QUEUE);
    if (dr) freeReplyObject(dr);
    std::cout << "  [WRR] Distributed " << total
              << " items across " << s.active_workers << " worker queues\n";
}

// ══════════════════════════════════════════════════════════════
//  WORKER LOAD TRACKER
//  Reads per-worker stats from Redis to compute load scores.
// ══════════════════════════════════════════════════════════════
void update_worker_loads(redisContext* ctx, SystemState& s) {
    s.active_workers = 0;
    long long max_proc = 1;

    // First pass: find max processed count
    for (int w = 1; w <= MAX_WORKERS; w++) {
        std::string key = "worker:" + std::to_string(w);
        redisReply* r = (redisReply*)redisCommand(
            ctx, "HGET %s processed", key.c_str());
        if (r && r->type == REDIS_REPLY_STRING) {
            long long cnt = std::stoll(r->str);
            if (cnt > max_proc) max_proc = cnt;
            s.active_workers++;
        }
        if (r) freeReplyObject(r);
    }

    // Second pass: compute relative load (0=idle, 1=busiest)
    for (int w = 1; w <= MAX_WORKERS; w++) {
        std::string key = "worker:" + std::to_string(w);
        redisReply* r = (redisReply*)redisCommand(
            ctx, "HGET %s avg_latency", key.c_str());
        if (r && r->type == REDIS_REPLY_STRING) {
            double lat = std::stod(r->str);
            // Normalize: higher latency → higher load
            s.worker_load[w-1] = std::min(1.0, lat / 200.0);
        }
        if (r) freeReplyObject(r);
    }
}

// ══════════════════════════════════════════════════════════════
//  MAIN SCHEDULER LOOP
// ══════════════════════════════════════════════════════════════
int main() {
    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler);

    std::cout << "╔══════════════════════════════════════════════════════╗\n";
    std::cout << "║  Advanced Adaptive Scheduler v2.0                    ║\n";
    std::cout << "║  Algorithms: FCFS|PRIORITY|RR|SJF|WRR|MLFQ          ║\n";
    std::cout << "╚══════════════════════════════════════════════════════╝\n\n";

    redisContext* ctx = redisConnect(REDIS_HOST, REDIS_PORT);
    if (!ctx || ctx->err) {
        std::cerr << "Redis connection failed\n"; return 1;
    }
    std::cout << "✔ Redis connected\n\n";

    std::ofstream csv(SCHED_CSV);
    csv << "timestamp_ms,queue_len,avg_latency,throughput,algorithm,"
        << "score_fcfs,score_priority,score_rr,score_sjf,score_wrr,score_mlfq,"
        << "active_workers,load_var\n";

    Algorithm current_algo = FCFS;
    Algorithm prev_algo    = FCFS;

    // Mode control:
    // - AUTO   : scheduler selects best algorithm from score engine
    // - MANUAL : scheduler uses current_strategy value
    bool fixed_env_algo = false;
    Algorithm env_algo = FCFS;
    const char* fixed_env = std::getenv("SCHED_FIXED_ALGO");
    if (fixed_env && parse_algo_name(fixed_env, env_algo)) {
        fixed_env_algo = true;
        current_algo = env_algo;
        prev_algo = env_algo;
        redisReply* mr = (redisReply*)redisCommand(ctx, "SET %s MANUAL", MODE_KEY);
        if (mr) freeReplyObject(mr);
        redisReply* sr = (redisReply*)redisCommand(
            ctx, "SET %s %s", STRATEGY_KEY, algo_name(env_algo));
        if (sr) freeReplyObject(sr);
        std::cout << "[Scheduler] Fixed mode via SCHED_FIXED_ALGO="
                  << algo_name(env_algo) << "\n";
    } else {
        redisReply* mr = (redisReply*)redisCommand(ctx, "SET %s AUTO", MODE_KEY);
        if (mr) freeReplyObject(mr);
        redisReply* sr = (redisReply*)redisCommand(
            ctx, "SET %s %s", STRATEGY_KEY, algo_name(current_algo));
        if (sr) freeReplyObject(sr);
    }

    std::deque<long long> lat_window;
    long long last_processed = 0;
    long long start_ts = now_ms();
    int tick = 0;

    std::cout << "[ t ]  Q-len  AvgLat  Thruput  Algorithm  Reason\n";
    std::cout << "────────────────────────────────────────────────────\n";

    while (g_running) {
        auto tick_start = std::chrono::steady_clock::now();
        tick++;

        // ── 1. Collect system state ───────────────────────────
        SystemState state;

        // Queue length
        redisReply* qr = (redisReply*)redisCommand(
            ctx, "LLEN %s", MAIN_QUEUE);
        if (qr && qr->type == REDIS_REPLY_INTEGER)
            state.queue_len = qr->integer;
        if (qr) freeReplyObject(qr);

        // Latency samples from workers
        redisReply* lr = (redisReply*)redisCommand(
            ctx, "LRANGE %s 0 %d", LATENCY_LIST, LATENCY_WINDOW-1);
        if (lr && lr->type == REDIS_REPLY_ARRAY) {
            for (size_t i = 0; i < lr->elements; i++) {
                std::string s = lr->element[i]->str;
                size_t p1 = s.find(','), p2 = s.find(',', p1+1);
                if (p1 != std::string::npos && p2 != std::string::npos) {
                    long long lat = std::stoll(s.substr(p1+1, p2-p1-1));
                    lat_window.push_back(lat);
                    if ((int)lat_window.size() > LATENCY_WINDOW)
                        lat_window.pop_front();
                }
            }
        }
        if (lr) freeReplyObject(lr);

        if (!lat_window.empty()) {
            long long sum = std::accumulate(lat_window.begin(),
                lat_window.end(), 0LL);
            state.avg_latency_ms = (double)sum / lat_window.size();
        }

        // Throughput
        long long total_proc = 0;
        for (int w = 1; w <= MAX_WORKERS; w++) {
            std::string wk = "worker:" + std::to_string(w);
            redisReply* hr = (redisReply*)redisCommand(
                ctx, "HGET %s processed", wk.c_str());
            if (hr && hr->type == REDIS_REPLY_STRING)
                total_proc += std::stoll(hr->str);
            if (hr) freeReplyObject(hr);
        }
        state.throughput = total_proc - last_processed;
        last_processed   = total_proc;

        // Worker loads
        update_worker_loads(ctx, state);

        // ── 2. Score all algorithms ───────────────────────────
        auto scored = score_algorithms(state);

        // Resolve control mode and pick algorithm
        bool manual_mode = fixed_env_algo;
        if (!fixed_env_algo) {
            redisReply* mr = (redisReply*)redisCommand(ctx, "GET %s", MODE_KEY);
            if (mr && mr->type == REDIS_REPLY_STRING) {
                std::string mode = upper_copy(mr->str ? mr->str : "");
                manual_mode = (mode == "MANUAL");
            }
            if (mr) freeReplyObject(mr);
        }

        if (manual_mode) {
            if (fixed_env_algo) {
                current_algo = env_algo;
            } else {
                redisReply* cr = (redisReply*)redisCommand(ctx, "GET %s", STRATEGY_KEY);
                if (cr && cr->type == REDIS_REPLY_STRING) {
                    Algorithm requested = current_algo;
                    if (parse_algo_name(cr->str ? cr->str : "", requested))
                        current_algo = requested;
                }
                if (cr) freeReplyObject(cr);
            }
        } else {
            // AUTO mode: select best algorithm (lowest score)
            current_algo = scored[0].algo;
        }

        // ── 3. Apply algorithm (act on the queue) ─────────────
        if (current_algo != prev_algo || tick % 10 == 0) {
            // Only reorder when strategy changes or every 10 ticks
            switch (current_algo) {
                case PRIORITY:    apply_priority(ctx);            break;
                case SJF:         apply_sjf(ctx);                 break;
                case MLFQ:        apply_mlfq(ctx);                break;
                case WRR:         apply_wrr(ctx, state);          break;
                case FCFS:
                case ROUND_ROBIN: /* no reordering needed */      break;
            }
        }

        // Publish current strategy for workers to read
        if (current_algo != prev_algo) {
            redisReply* sr2 = (redisReply*)redisCommand(
                ctx, "SET %s %s", STRATEGY_KEY, algo_name(current_algo));
            if (sr2) freeReplyObject(sr2);

            std::cout << "\n  ★ SWITCH: " << algo_name(prev_algo)
                      << " → " << algo_name(current_algo) << "\n";
            std::cout << "    Q=" << state.queue_len
                      << " lat=" << (int)state.avg_latency_ms
                      << "ms thru=" << state.throughput << "/s\n";
            std::cout << "    Scores: ";
            for (auto& sc : scored)
                std::cout << algo_name(sc.algo) << "=" << std::fixed
                          << std::setprecision(3) << sc.score << " ";
            std::cout << "\n\n";
            prev_algo = current_algo;
        }

        // ── 4. Print tick status ──────────────────────────────
        long long elapsed = (now_ms() - start_ts) / 1000;
        std::cout << "[" << elapsed << "s]"
                  << "  Q=" << state.queue_len
                  << "  lat=" << (int)state.avg_latency_ms << "ms"
                  << "  thr=" << state.throughput << "/s"
                  << "  algo=" << algo_name(current_algo)
                  << "\n";

        // ── 5. Write CSV snapshot ─────────────────────────────
        double load_var = 0.0;
        if (state.active_workers > 1) {
            double ml = 0.0;
            for (int i = 0; i < state.active_workers; i++)
                ml += state.worker_load[i];
            ml /= state.active_workers;
            for (int i = 0; i < state.active_workers; i++)
                load_var += std::pow(state.worker_load[i] - ml, 2);
            load_var = std::sqrt(load_var / state.active_workers);
        }

        csv << now_ms() << ","
            << state.queue_len << ","
            << (int)state.avg_latency_ms << ","
            << state.throughput << ","
            << algo_name(current_algo);
        for (auto& sc : scored) {
            // Write scores in fixed algo order
        }
        // Write scores in fixed order
        std::map<Algorithm, double> score_map;
        for (auto& sc : scored) score_map[sc.algo] = sc.score;
        csv << "," << score_map[FCFS]
            << "," << score_map[PRIORITY]
            << "," << score_map[ROUND_ROBIN]
            << "," << score_map[SJF]
            << "," << score_map[WRR]
            << "," << score_map[MLFQ]
            << "," << state.active_workers
            << "," << load_var
            << "\n";
        csv.flush();

        // ── 6. Sleep remainder of interval ───────────────────
        auto elapsed_tick = std::chrono::steady_clock::now() - tick_start;
        auto sleep_dur = std::chrono::milliseconds(POLL_MS) - elapsed_tick;
        if (sleep_dur > std::chrono::milliseconds(0))
            std::this_thread::sleep_for(sleep_dur);
    }

    std::cout << "\n[Scheduler v2] Shutting down.\n";
    csv.close();
    redisFree(ctx);
    return 0;
}
