/**
 * ══════════════════════════════════════════════════════════════
 *  k6 Load Test Script — Financial Transaction Simulator
 * ══════════════════════════════════════════════════════════════
 *
 *  This script simulates realistic trading traffic patterns
 *  pushing directly to a lightweight HTTP Redis bridge.
 *
 *  SETUP (2 steps):
 *  ────────────────
 *  1. Start the Redis bridge:
 *       node redis_bridge.js   (see redis_bridge.js)
 *  2. Run k6:
 *       k6 run k6_load_test.js
 *
 *  WORKLOAD PHASES:
 *  ────────────────
 *  0 -  30s : LOW  LOAD   (  10 VUs  = ~50 txn/sec)
 * 30 -  60s : RAMP UP     (  10→100 VUs)
 * 60 -  90s : SPIKE       ( 100 VUs  = ~500 txn/sec)
 * 90 - 120s : HIGH LOAD   (  50 VUs  = ~250 txn/sec)
 *120 - 150s : COOL DOWN   (  50→5 VUs)
 *
 *  INSTALL k6:
 *  ────────────
 *  Ubuntu: sudo gpg --no-default-keyring --keyring /usr/share/keyrings/k6-archive-keyring.gpg \
 *              --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys C5AD17C747E3415A3642D57D77C6C491D6AC1D69
 *          echo "deb [signed-by=/usr/share/keyrings/k6-archive-keyring.gpg] https://dl.k6.io/deb stable main" \
 *              | sudo tee /etc/apt/sources.list.d/k6.list
 *          sudo apt update && sudo apt install k6
 *
 *  macOS:  brew install k6
 *
 *  Run:    k6 run k6_load_test.js
 *  With summary: k6 run --out csv=k6_results.csv k6_load_test.js
 * ══════════════════════════════════════════════════════════════
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Rate, Trend } from 'k6/metrics';

// ── Custom Metrics ────────────────────────────────────────────
const txnPushed     = new Counter('transactions_pushed');
const txnFailed     = new Counter('transactions_failed');
const pushDuration  = new Trend('push_duration_ms', true);
const successRate   = new Rate('success_rate');

// ── Test Configuration ─────────────────────────────────────────
export const options = {
    scenarios: {
        financial_traffic: {
            executor: 'ramping-vus',
            startVUs: 0,
            stages: [
                // Phase 1: Low load (market open, quiet)
                { duration: '30s', target: 10  },   // ramp to 10 users
                { duration: '30s', target: 10  },   // hold — LOW LOAD

                // Phase 2: Pre-market spike (news event)
                { duration: '10s', target: 100 },   // fast ramp to 100
                { duration: '20s', target: 100 },   // hold — SPIKE

                // Phase 3: High load (peak trading hours)
                { duration: '10s', target: 50  },   // ramp down to 50
                { duration: '30s', target: 50  },   // hold — HIGH LOAD

                // Phase 4: Cool down (after-hours)
                { duration: '20s', target: 5   },   // ramp to 5
                { duration: '10s', target: 0   },   // end
            ],
        },
    },

    thresholds: {
        // SLA targets (adjust to test your system)
        'push_duration_ms': ['p(95)<100', 'p(99)<200'],
        'success_rate':     ['rate>0.99'],
        'http_req_failed':  ['rate<0.01'],
    },
};

// ── Helpers ───────────────────────────────────────────────────
const SYMBOLS  = ['AAPL','GOOGL','MSFT','AMZN','TSLA','NFLX','META','NVDA','RELIANCE','TCS'];
const TXN_TYPES = ['BUY', 'SELL'];
const BRIDGE_URL = 'http://localhost:3000/push';  // Redis bridge endpoint

let txnCounter = 0;

function randomChoice(arr) {
    return arr[Math.floor(Math.random() * arr.length)];
}

function buildTransaction(vuId) {
    txnCounter++;
    return {
        id:        `${Date.now()}_${vuId}_${txnCounter}`,
        type:      randomChoice(TXN_TYPES),
        symbol:    randomChoice(SYMBOLS),
        quantity:  Math.floor(Math.random() * 499) + 1,
        price:     (10 + Math.random() * 1490).toFixed(2),
        priority:  Math.random() < 0.20 ? 1 : 0,   // 20% HIGH priority
        timestamp: Date.now(),
        user_id:   vuId,
    };
}

// ── Main VU function ──────────────────────────────────────────
// Each virtual user (VU) runs this in a loop.
export default function () {
    const txn  = buildTransaction(__VU);
    const body = JSON.stringify(txn);

    const start = Date.now();
    const res   = http.post(BRIDGE_URL, body, {
        headers: { 'Content-Type': 'application/json' },
        timeout: '5s',
    });
    const dur = Date.now() - start;

    // Track metrics
    pushDuration.add(dur);

    const ok = check(res, {
        'status 200':     (r) => r.status === 200,
        'queued success': (r) => r.json('queued') === true,
    });

    if (ok) {
        txnPushed.add(1);
        successRate.add(1);
    } else {
        txnFailed.add(1);
        successRate.add(0);
    }

    // Realistic think time between requests
    // Varies by VU to avoid synchronized bursts
    sleep(0.05 + Math.random() * 0.15);  // 50-200ms between requests
}

// ── Setup: print test config ──────────────────────────────────
export function setup() {
    console.log('╔══════════════════════════════════════════╗');
    console.log('║  k6 Financial Load Test                  ║');
    console.log('║  Target: http://localhost:3000           ║');
    console.log('╚══════════════════════════════════════════╝');
    console.log('Phases: LOW(30s) → RAMP(10s) → SPIKE(20s) → HIGH(30s)');
}

// ── Teardown: print summary ───────────────────────────────────
export function teardown(data) {
    console.log('\n✔ Load test complete. Check k6_results.csv for details.');
}
