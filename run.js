/**
 * ╔══════════════════════════════════════════════════════════════╗
 * ║  Experiment 8 — Local Standalone Runner                      ║
 * ║  Runs entirely in-process. No Docker, no Kafka, no MongoDB  ║
 * ║  installation required.                                      ║
 * ║                                                              ║
 * ║  Uses:                                                       ║
 * ║   • mongodb-memory-server  → embedded MongoDB               ║
 * ║   • src/localBroker.js     → in-process Kafka simulation    ║
 * ╚══════════════════════════════════════════════════════════════╝
 *
 * Run:  node run.js
 */

'use strict';

require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const { MongoMemoryServer } = require('mongodb-memory-server');
const { v4: uuidv4 } = require('uuid');
const cron = require('node-cron');

// ── Swap real Kafka with local in-process broker ─────────────────────────────
const { createProducer, createConsumer, createAdmin } = require('./src/localBroker');

// ── Models ───────────────────────────────────────────────────────────────────
const Post = require('./src/models/Post');
const ProcessedEvent = require('./src/models/ProcessedEvent');
const DLQRecord = require('./src/models/DLQRecord');

// ── Utilities ────────────────────────────────────────────────────────────────
const logger = require('./src/utils/logger');
const { withRetry, MAX_RETRY_ATTEMPTS } = require('./src/utils/retry');

// ── Config ───────────────────────────────────────────────────────────────────
const PUBLISH_TOPIC = 'post.publish.requests';
const DLQ_TOPIC     = 'post.publish.dlq';
const GROUP_ID      = 'post-publisher-group';
const DLQ_GROUP_ID  = 'post-dlq-group';

// ─────────────────────────────────────────────────────────────────────────────
// 1. SEED  — create sample posts in MongoDB
// ─────────────────────────────────────────────────────────────────────────────
const seedPosts = async () => {
  const now = new Date();
  const min = 60_000;

  const posts = [
    {
      title: 'Introduction to Apache Kafka',
      content: 'Kafka is a distributed event streaming platform capable of handling trillions of events a day.',
      author: 'Alice Johnson',
      scheduledAt: new Date(now - 3 * min),   // 3 min ago  → due NOW
      tags: ['kafka', 'streaming'],
      status: 'scheduled',
    },
    {
      title: 'Understanding Dead Letter Queues',
      content: 'DLQs store messages that cannot be processed, enabling investigation and reprocessing.',
      author: 'Bob Smith',
      scheduledAt: new Date(now - 1 * min),   // 1 min ago  → due NOW
      tags: ['messaging', 'reliability'],
      status: 'scheduled',
    },
    {
      title: 'Exponential Backoff Explained',
      content: 'Exponential backoff is a standard error-handling strategy for network applications.',
      author: 'Carol Davis',
      scheduledAt: new Date(now - 0.5 * min), // 30s ago    → due NOW
      tags: ['retry', 'resilience'],
      status: 'scheduled',
    },
    {
      title: 'Idempotency in Distributed Systems',
      content: 'An idempotent operation has the same effect whether performed once or many times.',
      author: 'Dave Wilson',
      scheduledAt: new Date(now + 2 * min),   // 2 min future → NOT yet due
      tags: ['idempotency', 'distributed'],
      status: 'scheduled',
    },
  ];

  await Post.deleteMany({});
  const created = await Post.insertMany(posts);
  logger.info(`✅ Seeded ${created.length} posts (3 due now, 1 future)`);
  return created;
};

// ─────────────────────────────────────────────────────────────────────────────
// 2. SCHEDULER — pick up due posts and produce to Kafka
// ─────────────────────────────────────────────────────────────────────────────
const runScheduler = async (producer) => {
  const now = new Date();
  const duePosts = await Post.find({ status: 'scheduled', scheduledAt: { $lte: now } });

  if (!duePosts.length) {
    logger.info('[Scheduler] No posts due at this time');
    return;
  }

  logger.info(`[Scheduler] Found ${duePosts.length} due post(s) — producing to Kafka...`);

  const messages = [];

  for (const post of duePosts) {
    // Atomic: only one scheduler instance can win this update
    const updated = await Post.findOneAndUpdate(
      { _id: post._id, status: 'scheduled' },
      { $set: { status: 'queued', kafkaEventId: uuidv4() } },
      { new: true }
    );
    if (!updated) continue; // Another instance already claimed this post

    messages.push({
      key: updated._id.toString(),
      value: JSON.stringify({
        eventId:     updated.kafkaEventId,
        postId:      updated._id.toString(),
        title:       updated.title,
        content:     updated.content,
        author:      updated.author,
        tags:        updated.tags,
        scheduledAt: updated.scheduledAt,
        producedAt:  new Date().toISOString(),
      }),
      headers: {
        'event-id': updated.kafkaEventId,
        'post-id':  updated._id.toString(),
        source:     'post-scheduler',
      },
    });

    logger.info(`[Scheduler] Queued → "${updated.title}"`, { postId: updated._id });
  }

  if (messages.length) {
    await producer.send({ topic: PUBLISH_TOPIC, messages, acks: -1 });
    logger.info(`[Scheduler] Produced ${messages.length} message(s) to "${PUBLISH_TOPIC}"`);
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// 3. PUBLISHER SERVICE — simulate external platform call
// ─────────────────────────────────────────────────────────────────────────────
const publishToExternalPlatform = async (post, attempt) => {
  // Simulate latency
  await new Promise(r => setTimeout(r, 80 + Math.random() * 120));

  // First attempt always fails for DLQ demo posts, others have 30% failure
  const failureRate = post.title.includes('Dead Letter') ? (attempt < MAX_RETRY_ATTEMPTS ? 1.0 : 0.0) : 0.3;
  if (Math.random() < failureRate) {
    throw new Error(`Transient platform error on attempt ${attempt} (HTTP 503)`);
  }

  return { platformId: `ext-${Date.now()}`, publishedAt: new Date().toISOString() };
};

// ─────────────────────────────────────────────────────────────────────────────
// 4. DLQ SERVICE — route failed messages to DLQ topic + MongoDB
// ─────────────────────────────────────────────────────────────────────────────
const routeToDLQ = async (dlqProducer, { eventId, postId, payload, errorMessage, retryCount, originalTopic, partition, offset }) => {
  const dlqPayload = {
    ...payload,
    _dlq: { originalTopic, partition, offset, errorMessage, retryCount, routedAt: new Date().toISOString() },
  };

  await dlqProducer.send({
    topic: DLQ_TOPIC,
    messages: [{
      key: postId || eventId,
      value: JSON.stringify(dlqPayload),
      headers: {
        'event-id':        eventId,
        'x-original-topic': originalTopic || '',
        'x-retry-count':   String(retryCount),
        'x-error':         (errorMessage || '').substring(0, 200),
      },
    }],
    acks: -1,
  });

  // Persist DLQ record in MongoDB for audit/replay
  await DLQRecord.findOneAndUpdate(
    { eventId },
    { $set: { eventId, postId, originalTopic, partition, offset, payload, errorMessage, retryCount, arrivedAt: new Date() } },
    { upsert: true, new: true }
  );

  logger.warn(`[DLQ] Routed to DLQ after ${retryCount} attempts`, { eventId, postId, errorMessage });
};

// ─────────────────────────────────────────────────────────────────────────────
// 5. CONSUMER — process publish requests with idempotency + retry + DLQ
// ─────────────────────────────────────────────────────────────────────────────
const startConsumer = async (dlqProducer) => {
  const consumer = await createConsumer(GROUP_ID);
  await consumer.subscribe({ topic: PUBLISH_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      let payload;
      try {
        payload = JSON.parse(message.value.toString());
      } catch (e) {
        logger.error('[Consumer] Unparseable message — dropping', { error: e.message });
        return;
      }

      const { eventId, postId, title } = payload;

      logger.info(`[Consumer] Received → "${title}"`, { eventId, postId, offset: message.offset });

      // ── Idempotency check ──────────────────────────────────────────────────
      const alreadyDone = await ProcessedEvent.findOne({ eventId });
      if (alreadyDone) {
        logger.warn(`[Consumer] ⚡ Duplicate event — skipping (idempotent)`, { eventId });
        return;
      }

      // ── Retry with exponential backoff ────────────────────────────────────
      const { success, error, attempts } = await withRetry(
        (attempt) => publishToExternalPlatform(payload, attempt),
        { maxAttempts: MAX_RETRY_ATTEMPTS, label: `publish:${title}`, logger }
      );

      if (success) {
        await Post.findByIdAndUpdate(postId, {
          $set: { status: 'published', publishedAt: new Date(), failureReason: null },
        });
        await ProcessedEvent.findOneAndUpdate(
          { eventId },
          { $setOnInsert: { eventId, postId, processedAt: new Date(), outcome: 'success' } },
          { upsert: true }
        );
        logger.info(`[Consumer] ✅ Published → "${title}" (${attempts} attempt(s))`, { postId });

      } else {
        // ── Route to DLQ ─────────────────────────────────────────────────────
        await routeToDLQ(dlqProducer, {
          eventId, postId, payload,
          errorMessage: error?.message || 'Unknown error',
          retryCount: attempts,
          originalTopic: topic, partition, offset: message.offset,
        });
        await Post.findByIdAndUpdate(postId, {
          $set: { status: 'failed', failureReason: error?.message, retryCount: attempts },
        });
        await ProcessedEvent.findOneAndUpdate(
          { eventId },
          { $setOnInsert: { eventId, postId, processedAt: new Date(), outcome: 'dlq' } },
          { upsert: true }
        );
      }
    },
  });

  logger.info(`[Consumer] Subscribed to "${PUBLISH_TOPIC}" (group: ${GROUP_ID})`);
  return consumer;
};

// ─────────────────────────────────────────────────────────────────────────────
// 6. DLQ CONSUMER — monitor and log DLQ messages
// ─────────────────────────────────────────────────────────────────────────────
const startDLQConsumer = async () => {
  const consumer = await createConsumer(DLQ_GROUP_ID);
  await consumer.subscribe({ topic: DLQ_TOPIC, fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let payload;
      try { payload = JSON.parse(message.value.toString()); }
      catch (e) { return; }

      const { eventId, postId, title, _dlq: meta = {} } = payload;
      logger.error(
        `[DLQ-Consumer] 🚨 ACTION REQUIRED: "${title}" permanently failed after ${meta.retryCount} retries`,
        { eventId, postId, error: meta.errorMessage, routedAt: meta.routedAt }
      );
    },
  });

  logger.info(`[DLQ-Consumer] Subscribed to "${DLQ_TOPIC}" (group: ${DLQ_GROUP_ID})`);
  return consumer;
};

// ─────────────────────────────────────────────────────────────────────────────
// 7. WEB DASHBOARD — Express HTTP server at http://localhost:3000
// ─────────────────────────────────────────────────────────────────────────────
const startWebDashboard = (app) => {
  const PORT = 3000;

  app.get('/', async (req, res) => {
    const [posts, dlqRecords, processedEvents] = await Promise.all([
      Post.find().sort({ scheduledAt: -1 }).lean(),
      DLQRecord.find().sort({ arrivedAt: -1 }).lean(),
      ProcessedEvent.find().sort({ processedAt: -1 }).lean(),
    ]);

    const counts = {
      scheduled: posts.filter(p => p.status === 'scheduled').length,
      queued:    posts.filter(p => p.status === 'queued').length,
      published: posts.filter(p => p.status === 'published').length,
      failed:    posts.filter(p => p.status === 'failed').length,
      dlq:       dlqRecords.length,
    };

    const statusColor = { scheduled:'#3b82f6', queued:'#f59e0b', published:'#22c55e', failed:'#ef4444' };
    const statusIcon  = { scheduled:'🕐', queued:'📤', published:'✅', failed:'❌' };

    const postRows = posts.map(p => `
      <tr>
        <td>${p.title}</td>
        <td>${p.author}</td>
        <td><span class="badge" style="background:${statusColor[p.status]}">${statusIcon[p.status]} ${p.status}</span></td>
        <td>${new Date(p.scheduledAt).toLocaleString()}</td>
        <td>${p.publishedAt ? new Date(p.publishedAt).toLocaleString() : '—'}</td>
        <td>${p.retryCount || 0}</td>
        <td>${p.kafkaEventId ? p.kafkaEventId.substring(0,8)+'…' : '—'}</td>
      </tr>`).join('');

    const dlqRows = dlqRecords.length
      ? dlqRecords.map(d => `
        <tr>
          <td>${d.eventId ? d.eventId.substring(0,8)+'…' : '—'}</td>
          <td>${d.payload?.title || '—'}</td>
          <td>${d.errorMessage || '—'}</td>
          <td>${d.retryCount}</td>
          <td>${new Date(d.arrivedAt).toLocaleString()}</td>
        </tr>`).join('')
      : '<tr><td colspan="5" style="text-align:center;color:#6b7280">No DLQ records — all messages processed successfully ✅</td></tr>';

    const eventRows = processedEvents.slice(0, 10).map(e => `
      <tr>
        <td>${e.eventId ? e.eventId.substring(0,8)+'…' : '—'}</td>
        <td>${e.postId?.toString().substring(0,8)+'…' || '—'}</td>
        <td><span class="badge" style="background:${e.outcome==='success'?'#22c55e':e.outcome==='dlq'?'#ef4444':'#f59e0b'}">${e.outcome}</span></td>
        <td>${new Date(e.processedAt).toLocaleString()}</td>
      </tr>`).join('');

    res.send(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <meta http-equiv="refresh" content="5"/>
  <title>Experiment 8 — Kafka Post Scheduler</title>
  <style>
    *{box-sizing:border-box;margin:0;padding:0}
    body{font-family:'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
    header{background:linear-gradient(135deg,#1e3a5f,#1e40af);padding:24px 32px;display:flex;justify-content:space-between;align-items:center}
    header h1{font-size:1.5rem;font-weight:700;color:#fff}
    header span{font-size:0.8rem;color:#93c5fd;background:#1e3a8a;padding:4px 12px;border-radius:20px}
    .container{max-width:1200px;margin:0 auto;padding:24px}
    .cards{display:grid;grid-template-columns:repeat(5,1fr);gap:16px;margin-bottom:28px}
    .card{background:#1e293b;border-radius:12px;padding:20px;text-align:center;border:1px solid #334155}
    .card .num{font-size:2.5rem;font-weight:800;line-height:1}
    .card .lbl{font-size:0.75rem;color:#94a3b8;margin-top:6px;text-transform:uppercase;letter-spacing:.05em}
    .card.blue .num{color:#60a5fa} .card.yellow .num{color:#fbbf24}
    .card.green .num{color:#4ade80} .card.red .num{color:#f87171} .card.purple .num{color:#c084fc}
    section{margin-bottom:28px}
    h2{font-size:1rem;font-weight:600;color:#94a3b8;margin-bottom:12px;text-transform:uppercase;letter-spacing:.05em;display:flex;align-items:center;gap:8px}
    table{width:100%;border-collapse:collapse;background:#1e293b;border-radius:12px;overflow:hidden;border:1px solid #334155}
    th{background:#0f172a;color:#64748b;font-size:0.7rem;text-transform:uppercase;letter-spacing:.05em;padding:10px 14px;text-align:left}
    td{padding:11px 14px;font-size:0.85rem;border-top:1px solid #334155;color:#cbd5e1}
    tr:hover td{background:#263248}
    .badge{display:inline-block;padding:3px 10px;border-radius:20px;font-size:0.72rem;font-weight:600;color:#fff}
    .refresh{font-size:0.75rem;color:#475569;text-align:right;margin-top:16px}
    .topics{display:flex;gap:12px;margin-bottom:28px}
    .topic{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:14px 20px;flex:1}
    .topic .name{font-size:0.75rem;color:#64748b;text-transform:uppercase;letter-spacing:.05em}
    .topic .val{font-size:1rem;font-weight:600;color:#38bdf8;margin-top:4px}
  </style>
</head>
<body>
<header>
  <h1>🚀 Experiment 8 — Kafka Post Scheduler</h1>
  <span>🔴 LIVE &nbsp;·&nbsp; Auto-refresh every 5s</span>
</header>
<div class="container">

  <div class="cards">
    <div class="card blue"><div class="num">${counts.scheduled}</div><div class="lbl">🕐 Scheduled</div></div>
    <div class="card yellow"><div class="num">${counts.queued}</div><div class="lbl">📤 Queued</div></div>
    <div class="card green"><div class="num">${counts.published}</div><div class="lbl">✅ Published</div></div>
    <div class="card red"><div class="num">${counts.failed}</div><div class="lbl">❌ Failed</div></div>
    <div class="card purple"><div class="num">${counts.dlq}</div><div class="lbl">☠️ DLQ Records</div></div>
  </div>

  <div class="topics">
    <div class="topic"><div class="name">📨 Kafka Topic</div><div class="val">post.publish.requests</div></div>
    <div class="topic"><div class="name">☠️ DLQ Topic</div><div class="val">post.publish.dlq</div></div>
    <div class="topic"><div class="name">👥 Consumer Group</div><div class="val">post-publisher-group</div></div>
    <div class="topic"><div class="name">🔁 Max Retries</div><div class="val">${process.env.MAX_RETRY_ATTEMPTS || 3} attempts (exp. backoff)</div></div>
  </div>

  <section>
    <h2>📋 All Posts</h2>
    <table>
      <thead><tr><th>Title</th><th>Author</th><th>Status</th><th>Scheduled At</th><th>Published At</th><th>Retries</th><th>Event ID</th></tr></thead>
      <tbody>${postRows}</tbody>
    </table>
  </section>

  <section>
    <h2>☠️ Dead Letter Queue Records</h2>
    <table>
      <thead><tr><th>Event ID</th><th>Title</th><th>Error</th><th>Retries</th><th>Arrived At</th></tr></thead>
      <tbody>${dlqRows}</tbody>
    </table>
  </section>

  <section>
    <h2>⚡ Idempotency Log (last 10 events)</h2>
    <table>
      <thead><tr><th>Event ID</th><th>Post ID</th><th>Outcome</th><th>Processed At</th></tr></thead>
      <tbody>${eventRows}</tbody>
    </table>
  </section>

  <div class="refresh">⏱ Page auto-refreshes every 5 seconds &nbsp;·&nbsp; ${new Date().toLocaleString()}</div>
</div>
</body>
</html>`);
  });

  // JSON API endpoint
  app.get('/api/status', async (req, res) => {
    const [posts, dlq, events] = await Promise.all([
      Post.find().lean(),
      DLQRecord.find().lean(),
      ProcessedEvent.find().lean(),
    ]);
    res.json({ posts, dlq, events, timestamp: new Date() });
  });

  app.listen(PORT, () => {
    logger.info(`🌐 Web Dashboard running at → http://localhost:${PORT}`);
  });
};


const printStatus = async () => {
  const [scheduled, queued, published, failed, dlqCount] = await Promise.all([
    Post.countDocuments({ status: 'scheduled' }),
    Post.countDocuments({ status: 'queued' }),
    Post.countDocuments({ status: 'published' }),
    Post.countDocuments({ status: 'failed' }),
    DLQRecord.countDocuments({}),
  ]);

  console.log('\n' + '═'.repeat(55));
  console.log('  📊  POST STATUS DASHBOARD');
  console.log('═'.repeat(55));
  console.log(`  🕐  Scheduled  : ${scheduled}`);
  console.log(`  📤  Queued     : ${queued}`);
  console.log(`  ✅  Published  : ${published}`);
  console.log(`  ❌  Failed     : ${failed}`);
  console.log(`  ☠️   DLQ Records: ${dlqCount}`);
  console.log('═'.repeat(55) + '\n');
};

// ─────────────────────────────────────────────────────────────────────────────
// MAIN — Bootstrap everything
// ─────────────────────────────────────────────────────────────────────────────
const main = async () => {
  console.log('\n' + '█'.repeat(60));
  console.log('  EXPERIMENT 8 — Kafka Post Scheduler (Local Mode)');
  console.log('  No Docker required — fully in-process');
  console.log('█'.repeat(60) + '\n');

  // ── Start embedded MongoDB ─────────────────────────────────────────────────
  logger.info('Starting embedded MongoDB...');
  const mongod = await MongoMemoryServer.create();
  const mongoUri = mongod.getUri();
  await mongoose.connect(mongoUri);
  logger.info(`✅ Embedded MongoDB started at ${mongoUri}`);

  // ── Start Web Dashboard ────────────────────────────────────────────────────
  const app = express();
  startWebDashboard(app);

  // ── Setup Kafka broker topics ──────────────────────────────────────────────
  const admin = await createAdmin();
  await admin.createTopics({
    topics: [
      { topic: PUBLISH_TOPIC, numPartitions: 3, replicationFactor: 1 },
      { topic: DLQ_TOPIC,     numPartitions: 1, replicationFactor: 1 },
    ],
  });
  await admin.disconnect();
  logger.info(`✅ Local Kafka broker ready (topics: ${PUBLISH_TOPIC}, ${DLQ_TOPIC})`);

  // ── Seed sample posts ──────────────────────────────────────────────────────
  await seedPosts();

  // ── Create shared producer + DLQ producer ─────────────────────────────────
  const schedulerProducer = await createProducer();
  const dlqProducer       = await createProducer();

  // ── Start consumers ────────────────────────────────────────────────────────
  const consumer    = await startConsumer(dlqProducer);
  const dlqConsumer = await startDLQConsumer();

  // ── Run scheduler immediately, then every 30s ──────────────────────────────
  logger.info('[Scheduler] Starting — will run every 30 seconds');
  await runScheduler(schedulerProducer);

  cron.schedule('*/30 * * * * *', async () => {
    try { await runScheduler(schedulerProducer); }
    catch (e) { logger.error('[Scheduler] Error', { error: e.message }); }
  });

  // ── Print status every 10 seconds ─────────────────────────────────────────
  setTimeout(printStatus, 5_000);
  setInterval(printStatus, 10_000);

  // ── Graceful shutdown ──────────────────────────────────────────────────────
  const shutdown = async (sig) => {
    logger.info(`\nReceived ${sig} — shutting down...`);
    await consumer.disconnect();
    await dlqConsumer.disconnect();
    await schedulerProducer.disconnect();
    await dlqProducer.disconnect();
    await mongoose.disconnect();
    await mongod.stop();
    logger.info('Shutdown complete. Goodbye!');
    process.exit(0);
  };

  process.on('SIGINT',  () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  logger.info('\n✅ All systems running. Press Ctrl+C to stop.\n');
};

main().catch(err => {
  console.error('Fatal startup error:', err);
  process.exit(1);
});
