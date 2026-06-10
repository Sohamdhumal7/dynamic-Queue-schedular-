/**
 * ══════════════════════════════════════════════════════════════
 *  Redis Bridge — HTTP → Redis LPUSH
 * ══════════════════════════════════════════════════════════════
 *  k6 speaks HTTP. Redis speaks its own protocol.
 *  This tiny Express server is the bridge between them.
 *
 *  k6 → POST /push {json} → this server → LPUSH txn_queue
 *
 *  INSTALL:
 *    npm install express ioredis
 *
 *  RUN:
 *    node redis_bridge.js
 *
 *  TEST:
 *    curl -X POST http://localhost:3000/push \
 *      -H "Content-Type: application/json" \
 *      -d '{"id":1,"type":"BUY","symbol":"AAPL","quantity":100,"price":150.0,"priority":0,"timestamp":1234567890,"user_id":1}'
 * ══════════════════════════════════════════════════════════════
 */

const express = require('express');
const Redis   = require('ioredis');

const app    = express();
const redis  = new Redis({ host: '127.0.0.1', port: 6379 });
const PORT   = 3000;
const QUEUE  = 'txn_queue';

// Parse JSON bodies
app.use(express.json({ limit: '1mb' }));

// ── Health check ──────────────────────────────────────────────
app.get('/health', (req, res) => {
    res.json({ status: 'ok', queue: QUEUE });
});

// ── Queue length check ────────────────────────────────────────
app.get('/queue-length', async (req, res) => {
    const len = await redis.llen(QUEUE);
    res.json({ queue: QUEUE, length: len });
});

// ── Push transaction to Redis ─────────────────────────────────
app.post('/push', async (req, res) => {
    const txn = req.body;

    // Basic validation
    if (!txn || !txn.id || !txn.type || !txn.symbol) {
        return res.status(400).json({ error: 'Invalid transaction format' });
    }

    // Ensure timestamp is set (k6 sets it, but just in case)
    if (!txn.timestamp) txn.timestamp = Date.now();

    try {
        const json = JSON.stringify(txn);
        await redis.lpush(QUEUE, json);
        res.json({ queued: true, id: txn.id });
    } catch (err) {
        console.error('Redis push error:', err.message);
        res.status(500).json({ queued: false, error: err.message });
    }
});

// ── Batch push endpoint (for high-throughput testing) ─────────
app.post('/push-batch', async (req, res) => {
    const txns = req.body;
    if (!Array.isArray(txns)) {
        return res.status(400).json({ error: 'Expected array of transactions' });
    }

    try {
        const pipeline = redis.pipeline();
        for (const txn of txns) {
            if (!txn.timestamp) txn.timestamp = Date.now();
            pipeline.lpush(QUEUE, JSON.stringify(txn));
        }
        await pipeline.exec();
        res.json({ queued: true, count: txns.length });
    } catch (err) {
        res.status(500).json({ queued: false, error: err.message });
    }
});

// ── Metrics endpoint (for monitoring) ────────────────────────
app.get('/metrics', async (req, res) => {
    const [qlen, strategy] = await Promise.all([
        redis.llen(QUEUE),
        redis.get('current_strategy'),
    ]);

    // Collect worker stats
    const workers = {};
    for (let w = 1; w <= 8; w++) {
        const key = `worker:${w}`;
        const stats = await redis.hgetall(key);
        if (stats && stats.processed) {
            workers[`worker_${w}`] = stats;
        }
    }

    res.json({
        queue_length:     qlen,
        current_strategy: strategy || 'FCFS',
        workers,
        timestamp:        Date.now(),
    });
});

// ── Start server ──────────────────────────────────────────────
app.listen(PORT, () => {
    console.log('╔══════════════════════════════════════════╗');
    console.log('║  Redis Bridge running on port 3000       ║');
    console.log('╚══════════════════════════════════════════╝');
    console.log(`  POST /push           → push one transaction`);
    console.log(`  POST /push-batch     → push array of transactions`);
    console.log(`  GET  /queue-length   → current queue depth`);
    console.log(`  GET  /metrics        → full system metrics`);
    console.log(`  GET  /health         → health check\n`);
    console.log('Ready for k6 load test...');
});
