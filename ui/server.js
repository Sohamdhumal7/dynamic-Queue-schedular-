/**
 * ═══════════════════════════════════════════════════════════
 *  server.js  —  Enhanced Redis Bridge + UI API Server
 *  Distributed Transaction Scheduling System
 * ═══════════════════════════════════════════════════════════
 *
 *  This single file does THREE jobs:
 *
 *  1. Redis Bridge  — receives transactions from k6 / curl
 *                     and pushes them into Redis queue
 *
 *  2. REST API      — exposes system metrics, control endpoints
 *                     so the UI can read real-time data
 *
 *  3. WebSocket     — pushes live updates to the UI every second
 *                     without the browser needing to poll
 *
 *  4. Static files  — serves the dashboard HTML directly
 *
 *  START:
 *    node server.js
 *
 *  INSTALL:
 *    npm install express ioredis ws cors
 *
 *  ENDPOINTS:
 *    GET  /                    → serves the dashboard HTML
 *    POST /push                → push one transaction
 *    POST /push-batch          → push many transactions
 *    GET  /api/status          → full system snapshot
 *    GET  /api/metrics/history → last 120 snapshots (2 min)
 *    POST /api/control/algo    → force a scheduling algorithm
 *    POST /api/control/flush   → clear the queue
 *    POST /api/workers/generate→ generate N test transactions
 *    WS   /ws                  → live push every 1s
 * ═══════════════════════════════════════════════════════════
 */

const express   = require('express');
const Redis     = require('ioredis');
const WebSocket = require('ws');
const cors      = require('cors');
const http      = require('http');
const path      = require('path');
const fs        = require('fs');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server, path: '/ws' });
const redis  = new Redis({ host: '127.0.0.1', port: 6379, lazyConnect: true });

const PORT       = 3000;
const QUEUE      = 'txn_queue';
const STRAT_KEY  = 'current_strategy';
const MODE_KEY   = 'strategy_mode';
const LAT_LIST   = 'latency_log';

// ── In-memory history (last 120 snapshots = 2 minutes at 1s interval)
const MAX_HISTORY = 120;
const history     = [];  // {timestamp, queueLen, avgLatency, throughput, algorithm, workers}
let   lastTotal   = 0;   // for computing throughput delta

// ── Transaction counter (server-side, used when k6 not running)
let txnCounter = 0;

// ── Stock symbols and types for test generation
const SYMBOLS  = ['AAPL','GOOGL','MSFT','AMZN','TSLA','NFLX','META','NVDA','RELIANCE','TCS','ADANI','WIPRO'];
const TYPES    = ['BUY','SELL'];
const ALGOS    = ['FCFS','PRIORITY','ROUND_ROBIN','SJF','WRR','MLFQ'];

app.use(cors());
app.use(express.json({ limit: '2mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ════════════════════════════════════════════════════════════
//  HELPERS
// ════════════════════════════════════════════════════════════

async function getWorkerStats() {
    const workers = [];
    for (let w = 1; w <= 8; w++) {
        const key   = `worker:${w}`;
        const stats = await redis.hgetall(key);
        if (stats && stats.processed && parseInt(stats.processed) > 0) {
            workers.push({
                id:          w,
                processed:   parseInt(stats.processed)   || 0,
                avgLatency:  parseInt(stats.avg_latency) || 0,
                avgProcTime: parseInt(stats.avg_proc_time) || 0,
                lastActive:  parseInt(stats.last_active) || 0,
                alive:       (Date.now() - (parseInt(stats.last_active) || 0)) < 10000,
            });
        }
    }
    return workers;
}

async function getLatencySamples(n = 20) {
    const raw = await redis.lrange(LAT_LIST, 0, n - 1);
    const samples = [];
    for (const s of raw) {
        const parts = s.split(',');
        if (parts.length >= 3) {
            samples.push({
                workerId:  parseInt(parts[0]),
                latencyMs: parseInt(parts[1]),
                totalMs:   parseInt(parts[2]),
            });
        }
    }
    return samples;
}

async function getSystemSnapshot() {
    const [queueLen, strategy, strategyMode] = await Promise.all([
        redis.llen(QUEUE),
        redis.get(STRAT_KEY),
        redis.get(MODE_KEY),
    ]);

    const workers  = await getWorkerStats();
    const samples  = await getLatencySamples(30);

    const totalProcessed = workers.reduce((s, w) => s + w.processed, 0);
    const throughput      = Math.max(0, totalProcessed - lastTotal);
    lastTotal             = totalProcessed;

    const avgLatency = samples.length > 0
        ? Math.round(samples.reduce((s, x) => s + x.latencyMs, 0) / samples.length)
        : 0;

    const p95Idx    = Math.floor(samples.length * 0.95);
    const sorted    = [...samples].sort((a, b) => a.latencyMs - b.latencyMs);
    const p95       = sorted[p95Idx]?.latencyMs || 0;

    return {
        timestamp:      Date.now(),
        queueLen,
        strategy:       strategy || 'FCFS',
        strategyMode:   strategyMode || 'AUTO',
        avgLatency,
        p95Latency:     p95,
        throughput,
        totalProcessed,
        activeWorkers:  workers.filter(w => w.alive).length,
        workers,
        latencySamples: samples.slice(0, 10),
    };
}

// ════════════════════════════════════════════════════════════
//  REST ROUTES
// ════════════════════════════════════════════════════════════

// Serve dashboard at root
app.get('/', (req, res) => {
    const dashPath = path.join(__dirname, 'public', 'index.html');
    if (fs.existsSync(dashPath)) {
        res.sendFile(dashPath);
    } else {
        res.status(404).send('Dashboard not found. Please place index.html in the public/ folder.');
    }
});

// Health check
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: Date.now() });
});

// ── Full system status (REST, used by UI on initial load)
app.get('/api/status', async (req, res) => {
    try {
        const snap = await getSystemSnapshot();
        res.json(snap);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ── Last N history snapshots
app.get('/api/metrics/history', (req, res) => {
    const n = Math.min(parseInt(req.query.n) || 60, MAX_HISTORY);
    res.json(history.slice(-n));
});

// ── Recent transactions (last 20 from latency log)
app.get('/api/transactions/recent', async (req, res) => {
    try {
        const samples = await getLatencySamples(20);
        res.json(samples);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ── Push one transaction (from k6 or curl)
app.post('/push', async (req, res) => {
    const txn = req.body;
    if (!txn || !txn.type) {
        return res.status(400).json({ error: 'Invalid transaction' });
    }
    if (!txn.timestamp) txn.timestamp = Date.now();
    try {
        await redis.lpush(QUEUE, JSON.stringify(txn));
        res.json({ queued: true, id: txn.id });
    } catch (err) {
        res.status(500).json({ queued: false, error: err.message });
    }
});

// ── Push batch of transactions
app.post('/push-batch', async (req, res) => {
    const txns = req.body;
    if (!Array.isArray(txns)) {
        return res.status(400).json({ error: 'Expected array' });
    }
    try {
        const pl = redis.pipeline();
        for (const t of txns) {
            if (!t.timestamp) t.timestamp = Date.now();
            pl.lpush(QUEUE, JSON.stringify(t));
        }
        await pl.exec();
        res.json({ queued: true, count: txns.length });
    } catch (err) {
        res.status(500).json({ queued: false, error: err.message });
    }
});

// ── Generate N test transactions from the UI
app.post('/api/workers/generate', async (req, res) => {
    const { count = 50, priority = 'mixed' } = req.body;
    const n   = Math.min(parseInt(count), 500);
    const pl  = redis.pipeline();

    for (let i = 0; i < n; i++) {
        txnCounter++;
        const prio = priority === 'high' ? 1
                   : priority === 'low'  ? 0
                   : Math.random() < 0.2 ? 1 : 0;
        const txn = {
            id:        `ui_${Date.now()}_${txnCounter}`,
            type:      TYPES[Math.floor(Math.random() * 2)],
            symbol:    SYMBOLS[Math.floor(Math.random() * SYMBOLS.length)],
            quantity:  Math.floor(Math.random() * 499) + 1,
            price:     parseFloat((10 + Math.random() * 1490).toFixed(2)),
            priority:  prio,
            timestamp: Date.now(),
            user_id:   Math.floor(Math.random() * 10) + 1,
        };
        pl.lpush(QUEUE, JSON.stringify(txn));
    }
    await pl.exec();
    res.json({ generated: n, message: `Pushed ${n} transactions to queue` });
});

// ── Force a scheduling algorithm (UI control panel)
app.post('/api/control/algo', async (req, res) => {
    const algorithm = String(req.body?.algorithm || '').toUpperCase();
    if (!ALGOS.includes(algorithm)) {
        return res.status(400).json({ error: `Unknown algorithm. Use: ${ALGOS.join(', ')}` });
    }
    await redis.set(MODE_KEY, 'MANUAL');
    await redis.set(STRAT_KEY, algorithm);
    res.json({ algorithm, forced: true, mode: 'MANUAL', timestamp: Date.now() });
});

// ── Return scheduler to adaptive mode
app.post('/api/control/auto', async (req, res) => {
    await redis.set(MODE_KEY, 'AUTO');
    res.json({ auto: true, mode: 'AUTO', timestamp: Date.now() });
});

// ── Flush queue (UI control)
app.post('/api/control/flush', async (req, res) => {
    const len = await redis.llen(QUEUE);
    await redis.del(QUEUE);
    res.json({ flushed: true, itemsCleared: len });
});

// ── Get queue peek (first 10 items)
app.get('/api/queue/peek', async (req, res) => {
    try {
        const items = await redis.lrange(QUEUE, 0, 9);
        const parsed = items.map(i => {
            try { return JSON.parse(i); } catch { return { raw: i }; }
        });
        res.json({ items: parsed, total: await redis.llen(QUEUE) });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ════════════════════════════════════════════════════════════
//  WEBSOCKET — push live snapshots every second
// ════════════════════════════════════════════════════════════
wss.on('connection', (ws) => {
    console.log('[WS] Client connected');

    // Send history immediately on connect
    ws.send(JSON.stringify({ type: 'history', data: history.slice(-60) }));

    ws.on('error', () => {});
    ws.on('close', () => console.log('[WS] Client disconnected'));
});

function broadcast(msg) {
    const str = JSON.stringify(msg);
    wss.clients.forEach(c => {
        if (c.readyState === WebSocket.OPEN) c.send(str);
    });
}

// ── Collect snapshot every second and broadcast
setInterval(async () => {
    try {
        const snap = await getSystemSnapshot();

        // Add to history ring buffer
        history.push(snap);
        if (history.length > MAX_HISTORY) history.shift();

        // Broadcast to all connected UI clients
        broadcast({ type: 'snapshot', data: snap });

    } catch (err) {
        // Redis not ready yet, ignore
    }
}, 1000);

// ════════════════════════════════════════════════════════════
//  START
// ════════════════════════════════════════════════════════════
async function start() {
    try {
        await redis.connect();
        console.log('✔ Redis connected');
    } catch (err) {
        console.log('⚠ Redis not ready yet, will retry automatically');
    }

    server.listen(PORT, () => {
        console.log('');
        console.log('╔══════════════════════════════════════════════╗');
        console.log('║  Scheduler UI Server — running on :3000      ║');
        console.log('╚══════════════════════════════════════════════╝');
        console.log(`  Dashboard  → http://localhost:${PORT}`);
        console.log(`  API status → http://localhost:${PORT}/api/status`);
        console.log(`  WebSocket  → ws://localhost:${PORT}/ws`);
        console.log('');
    });
}

start();
