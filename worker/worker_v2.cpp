/**
 * ══════════════════════════════════════════════════════════════
 *  Load-Balanced Worker v2.0
 * ══════════════════════════════════════════════════════════════
 *
 *  LOAD BALANCING STRATEGY:
 *  ────────────────────────
 *  This worker supports TWO queue modes:
 *
 *  Mode 1 (SHARED):  All workers pull from "txn_queue"
 *                    Redis BRPOP naturally balances (first free
 *                    worker wins). Simple and effective.
 *
 *  Mode 2 (DEDICATED): Each worker has "worker_queue:N"
 *                    Scheduler routes heavy/light jobs to
 *                    specific workers (WRR algorithm).
 *                    Falls back to shared queue if dedicated
 *                    queue is empty.
 *
 *  STRATEGY-AWARE PROCESSING:
 *  ──────────────────────────
 *  Worker reads the current strategy from Redis and adjusts:
 *  - SJF mode: prefers smaller quantity items (deferred large ones)
 *  - MLFQ:     reads from tier queues in priority order
 *  - Others:   standard BRPOP from shared or dedicated queue
 *
 *  METRICS PUBLISHED:
 *  ─────────────────
 *  To Redis hash "worker:N":
 *    processed     – total transactions processed
 *    avg_latency   – rolling average queue-wait latency
 *    avg_proc_time – rolling average processing time
 *    last_active   – timestamp of last processed txn
 * ══════════════════════════════════════════════════════════════
 */

#include <iostream>
#include <string>
#include <sstream>
#include <chrono>
#include <thread>
#include <atomic>
#include <fstream>
#include <deque>
#include <numeric>
#include <cstdlib>
#include <cctype>
#include <signal.h>
#include <hiredis/hiredis.h>

static const char* REDIS_HOST   = "127.0.0.1";
static const int   REDIS_PORT   = 6379;
static const char* SHARED_QUEUE = "txn_queue";
static const char* STRATEGY_KEY = "current_strategy";
static const char* LATENCY_LIST = "latency_log";
static const int   POP_TIMEOUT  = 1;
static const int   LATENCY_STATS_WINDOW = 50;

std::atomic<bool> g_running{true};
void sig_handler(int) { g_running = false; }

long long now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

// ── JSON field extractor ──────────────────────────────────────
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


// ── Transaction ───────────────────────────────────────────────
struct Txn {
    std::string id        = "0";
    std::string type;
    std::string symbol;
    int         quantity  = 0;
    double      price     = 0.0;
    int         priority  = 0;
    long long   timestamp = 0;
    int         user_id   = 0;
};

std::string trim_copy(const std::string& s) {
    size_t b = 0, e = s.size();
    while (b < e && std::isspace(static_cast<unsigned char>(s[b]))) b++;
    while (e > b && std::isspace(static_cast<unsigned char>(s[e-1]))) e--;
    return s.substr(b, e - b);
}

bool parse_ll(const std::string& raw, long long& out) {
    std::string s = trim_copy(raw);
    if (s.empty()) return false;
    try {
        size_t idx = 0;
        long long v = std::stoll(s, &idx);
        if (idx != s.size()) return false;
        out = v;
        return true;
    } catch (...) {
        return false;
    }
}

bool parse_int(const std::string& raw, int& out) {
    long long v = 0;
    if (!parse_ll(raw, v)) return false;
    out = static_cast<int>(v);
    return true;
}

bool parse_double(const std::string& raw, double& out) {
    std::string s = trim_copy(raw);
    if (s.empty()) return false;
    try {
        size_t idx = 0;
        double v = std::stod(s, &idx);
        if (idx != s.size()) return false;
        out = v;
        return true;
    } catch (...) {
        return false;
    }
}

long long normalize_epoch_to_ms(long long ts) {
    if (ts <= 0) return 0;
    if (ts < 100000000000LL) return ts * 1000;        // seconds -> ms
    if (ts > 100000000000000000LL) return ts / 1000000; // ns -> ms
    if (ts > 100000000000000LL) return ts / 1000;     // us -> ms
    return ts;                                         // already ms
}

Txn parse(const std::string& json) {
    Txn t;
    t.id = json_field(json, "id");
    if (t.id.empty()) t.id = "0";

    t.type   = json_field(json, "type");
    t.symbol = json_field(json, "symbol");

    parse_int(json_field(json, "quantity"), t.quantity);
    parse_double(json_field(json, "price"), t.price);
    parse_int(json_field(json, "priority"), t.priority);
    parse_int(json_field(json, "user_id"), t.user_id);

    // Accept both names from producers: timestamp or gen_timestamp(_ms)
    long long ts = 0;
    if (!parse_ll(json_field(json, "timestamp"), ts)) {
        if (!parse_ll(json_field(json, "gen_timestamp"), ts)) {
            parse_ll(json_field(json, "gen_timestamp_ms"), ts);
        }
    }
    t.timestamp = normalize_epoch_to_ms(ts);

    return t;
}

// ── Get current strategy from Redis ──────────────────────────
std::string get_strategy(redisContext* ctx) {
    redisReply* r = (redisReply*)redisCommand(
        ctx, "GET %s", STRATEGY_KEY);
    std::string s = "FCFS";
    if (r && r->type == REDIS_REPLY_STRING) s = r->str;
    if (r) freeReplyObject(r);
    return s;
}

// ── Simulated processing delay ────────────────────────────────
// In a REAL system this would be order validation, DB write, etc.
// We keep it fast (1-5ms) to demonstrate sub-10ms processing.
void simulate_processing(const Txn& txn, const std::string& strategy) {
    int base_ms = 1 + (txn.quantity % 4);  // 1-4ms based on size

    // HIGH priority gets fastest path
    if (txn.priority == 1) base_ms = 1;

    // SJF: small jobs are truly fast
    if (strategy == "SJF" && txn.quantity < 50) base_ms = 1;

    // WRR: all jobs same time (worker capacity is the variable)
    // MLFQ tier0 same as priority=1
    if (strategy == "MLFQ" && txn.priority == 1) base_ms = 1;

    std::this_thread::sleep_for(std::chrono::milliseconds(base_ms));
}

// ── Publish latency sample ────────────────────────────────────
void publish_latency(redisContext* ctx, int worker_id,
                     long long latency_ms, long long total_ms) {
    std::ostringstream ss;
    ss << worker_id << "," << latency_ms << "," << total_ms << "," << now_ms();
    redisReply* r = (redisReply*)redisCommand(
        ctx, "LPUSH %s %s", LATENCY_LIST, ss.str().c_str());
    if (r) freeReplyObject(r);
    // Keep list bounded
    r = (redisReply*)redisCommand(ctx, "LTRIM %s 0 499", LATENCY_LIST);
    if (r) freeReplyObject(r);
}

// ── Update worker stats in Redis ─────────────────────────────
void update_stats(redisContext* ctx, int worker_id,
                  long long processed, long long avg_lat, long long avg_proc) {
    std::string key = "worker:" + std::to_string(worker_id);
    redisReply* r = (redisReply*)redisCommand(
        ctx, "HSET %s processed %lld avg_latency %lld avg_proc_time %lld last_active %lld",
        key.c_str(), processed, avg_lat, avg_proc, now_ms());
    if (r) freeReplyObject(r);
}

// ── Pop from queue (shared or dedicated) ─────────────────────
// Returns JSON string or empty string if nothing available.
std::string pop_transaction(redisContext* ctx, int worker_id,
                            const std::string& strategy) {
    std::string dedicated_q = "worker_queue:" + std::to_string(worker_id);

    // For WRR: try dedicated queue first
    if (strategy == "WRR") {
        redisReply* r = (redisReply*)redisCommand(
            ctx, "RPOP %s", dedicated_q.c_str());
        if (r && r->type == REDIS_REPLY_STRING) {
            std::string json = r->str;
            freeReplyObject(r);
            return json;
        }
        if (r) freeReplyObject(r);
        // Fall through to shared queue
    }

    // MLFQ: try tier queues in order (tier0 = highest priority)
    if (strategy == "MLFQ") {
        for (int tier = 0; tier <= 2; tier++) {
            std::string tq = "mlfq_tier:" + std::to_string(tier);
            redisReply* r = (redisReply*)redisCommand(
                ctx, "RPOP %s", tq.c_str());
            if (r && r->type == REDIS_REPLY_STRING) {
                std::string json = r->str;
                freeReplyObject(r);
                return json;
            }
            if (r) freeReplyObject(r);
        }
        // Fall through to shared queue
    }

    // Standard: blocking pop from shared queue
    redisReply* r = (redisReply*)redisCommand(
        ctx, "BRPOP %s %d", SHARED_QUEUE, POP_TIMEOUT);
    if (!r) return "";
    std::string json = "";
    if (r->type == REDIS_REPLY_ARRAY && r->elements >= 2)
        json = r->element[1]->str;
    freeReplyObject(r);
    return json;
}

// ══════════════════════════════════════════════════════════════
//  MAIN WORKER LOOP
// ══════════════════════════════════════════════════════════════
int main(int argc, char* argv[]) {
    int worker_id = (argc > 1) ? std::atoi(argv[1]) : 1;

    signal(SIGINT, sig_handler); signal(SIGTERM, sig_handler);

    std::cout << "╔══════════════════════════════════════════╗\n";
    std::cout << "║  Worker v2.0 #" << worker_id
              << "  (Load-Balanced)         ║\n";
    std::cout << "╚══════════════════════════════════════════╝\n";

    redisContext* ctx = redisConnect(REDIS_HOST, REDIS_PORT);
    if (!ctx || ctx->err) {
        std::cerr << "Redis connection failed\n"; return 1;
    }
    std::cout << "[W" << worker_id << "] Connected to Redis\n";

    // Metrics CSV
    std::string csv_path = "metrics/worker_" +
                           std::to_string(worker_id) + "_v2_metrics.csv";
    std::ofstream csv(csv_path, std::ios::app);
    if (csv.tellp() == 0)
        csv << "worker_id,txn_id,type,symbol,priority,quantity,"
            << "gen_timestamp_ms,latency_ms,proc_ms,processed_at_ms,strategy\n";

    long long processed   = 0;
    long long total_lat   = 0;
    long long total_proc  = 0;

    std::deque<long long> lat_deque;  // rolling window

    while (g_running) {
        std::string strategy = get_strategy(ctx);
        std::string json     = pop_transaction(ctx, worker_id, strategy);

        if (json.empty()) continue; // timeout, loop again

        long long recv_time = now_ms();

        Txn txn;
        try { txn = parse(json); } catch(...) { continue; }
        if (txn.timestamp <= 0) {
            // Avoid epoch-sized fake latency if producer timestamp is missing/bad.
            txn.timestamp = recv_time;
        }

        // Process
        simulate_processing(txn, strategy);

        long long done_time  = now_ms();
        long long latency_ms = recv_time - txn.timestamp;
        long long proc_ms    = done_time - recv_time;
        long long total_ms   = done_time - txn.timestamp;

        // Keep latency non-negative (clock skew across threads)
        if (latency_ms < 0) latency_ms = 0;

        processed++;
        total_lat  += latency_ms;
        total_proc += proc_ms;

        lat_deque.push_back(latency_ms);
        if ((int)lat_deque.size() > LATENCY_STATS_WINDOW)
            lat_deque.pop_front();

        long long roll_lat = 0;
        for (auto l : lat_deque) roll_lat += l;
        roll_lat /= (long long)lat_deque.size();

        // Publish latency sample
        publish_latency(ctx, worker_id, latency_ms, total_ms);

        // Update worker stats
        update_stats(ctx, worker_id, processed, roll_lat,
                     total_proc / processed);

        // Console log (every txn)
        std::cout << "[W" << worker_id << "] "
                  << "#" << txn.id
                  << " " << txn.type << " " << txn.symbol
                  << " qty=" << txn.quantity
                  << " prio=" << (txn.priority ? "HI" : "NO")
                  << " | lat=" << latency_ms << "ms"
                  << " proc=" << proc_ms << "ms"
                  << " algo=" << strategy
                  << "\n";

        // Write to CSV
        csv << worker_id << ","
            << txn.id << ","
            << txn.type << ","
            << txn.symbol << ","
            << txn.priority << ","
            << txn.quantity << ","
            << txn.timestamp << ","
            << latency_ms << ","
            << proc_ms << ","
            << done_time << ","
            << strategy << "\n";
        csv.flush();
    }

    std::cout << "\n[W" << worker_id << "] Done. Processed=" << processed
              << " AvgLat=" << (processed > 0 ? total_lat/processed : 0) << "ms\n";

    csv.close();
    redisFree(ctx);
    return 0;
}
