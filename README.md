# Experiment 8 — Kafka Post Scheduler

A production-grade post publishing system built with **Node.js**, **Apache Kafka**, and **MongoDB**.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          POST SCHEDULER SYSTEM                          │
│                                                                         │
│  ┌──────────────┐   Cron tick    ┌──────────────────────────────────┐  │
│  │   MongoDB    │◄──────────────►│       postScheduler.js           │  │
│  │  (Posts DB)  │  findOneAndUpd │  (node-cron, every 1 min)        │  │
│  └──────────────┘  status:queued └──────────────┬───────────────────┘  │
│                                                  │ producer.send()      │
│                                                  ▼                      │
│                                    ┌─────────────────────────┐         │
│                                    │  Kafka Topic             │         │
│                                    │  post.publish.requests   │         │
│                                    │  (3 partitions)          │         │
│                                    └────────────┬────────────┘         │
│                                                 │ eachMessage()        │
│                                                 ▼                      │
│                               ┌─────────────────────────────────────┐  │
│                               │        postConsumer.js               │  │
│                               │                                      │  │
│  ┌───────────────────────┐    │  1. Idempotency check (ProcessedEvent│  │
│  │  ProcessedEvent Model │◄───│  2. publishToExternalPlatform()      │  │
│  │  (7-day TTL index)    │    │  3. Retry w/ exponential backoff     │  │
│  └───────────────────────┘    │     attempt 1 → wait 1s             │  │
│                               │     attempt 2 → wait 2s             │  │
│                               │     attempt 3 → wait 4s             │  │
│                               └────────┬──────────────┬─────────────┘  │
│                                        │ success       │ all failed     │
│                                        ▼               ▼               │
│                              status=published    routeToDLQ()          │
│                                                        │               │
│                                                        ▼               │
│                                        ┌───────────────────────────┐   │
│                                        │  Kafka Topic              │   │
│                                        │  post.publish.dlq         │   │
│                                        └──────────────┬────────────┘   │
│                                                       │                │
│                              ┌────────────────────────▼─────────────┐  │
│                              │          dlqConsumer.js               │  │
│                              │  - Logs alert + DLQRecord in MongoDB  │  │
│                              │  - Awaits manual intervention         │  │
│                              └──────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
fsd 8 exp/
├── docker-compose.yml          # Kafka + Zookeeper + Kafka UI + MongoDB
├── .env                        # Environment configuration
├── package.json
└── src/
    ├── index.js                # Startup health checker
    ├── config/
    │   ├── db.js               # MongoDB connection
    │   └── kafka.js            # Kafka client factory (producer / consumer / admin)
    ├── models/
    │   ├── Post.js             # Post schema (scheduled/queued/published/failed)
    │   ├── ProcessedEvent.js   # Idempotency tracking (7-day TTL)
    │   └── DLQRecord.js        # Dead Letter Queue audit log
    ├── scheduler/
    │   └── postScheduler.js    # Cron job — finds due posts, produces to Kafka
    ├── consumer/
    │   ├── postConsumer.js     # Main consumer — publish + retry + DLQ routing
    │   └── dlqConsumer.js      # DLQ monitor consumer
    ├── services/
    │   ├── publisherService.js # External platform publish simulation
    │   └── dlqService.js       # DLQ routing (Kafka + MongoDB)
    └── utils/
        ├── logger.js           # Winston logger
        ├── retry.js            # Exponential backoff retry utility
        └── seedData.js         # DB seeder for sample posts
```

---

## ⚙️ Key Features

| Feature | Implementation |
|---|---|
| **Scheduler** | `node-cron` runs every minute; fetches posts where `status=scheduled` AND `scheduledAt ≤ now` |
| **Idempotent Producer** | KafkaJS `idempotent: true` prevents broker-level duplicates |
| **Atomic Queuing** | `findOneAndUpdate({status:'scheduled'})` ensures only one scheduler wins across instances |
| **Kafka Event ID** | Each message carries a UUID `eventId` in payload + headers |
| **Consumer Idempotency** | `ProcessedEvent` collection checked before processing; `$setOnInsert` on upsert |
| **Retry** | `withRetry()` utility with formula: `delay = baseDelay × 2^(attempt-1) + jitter` |
| **DLQ Routing** | After max retries, `routeToDLQ()` sends to `post.publish.dlq` topic + MongoDB |
| **DLQ Consumer** | Separate consumer group monitors DLQ and logs actionable alerts |
| **TTL Index** | `ProcessedEvent` records auto-expire after 7 days |

---

## 🔁 Retry / Backoff Schedule

With `MAX_RETRY_ATTEMPTS=3` and `RETRY_BASE_DELAY_MS=1000`:

| Attempt | Base Delay | Jitter (0–20%) | Max Delay |
|---------|-----------|----------------|-----------|
| 1       | 1000 ms   | 0–200 ms       | 1200 ms   |
| 2       | 2000 ms   | 0–400 ms       | 2400 ms   |
| 3       | 4000 ms   | 0–800 ms       | 4800 ms   |
| → DLQ  | —         | —              | —         |

---

## 🚀 Getting Started

### 1. Start Infrastructure

```bash
docker-compose up -d
```

Wait ~15 seconds for Kafka to be ready.

### 2. Install Dependencies

```bash
npm install
```

### 3. Configure Environment

Edit `.env` as needed (defaults work with the Docker Compose setup).

### 4. Seed the Database

```bash
npm run seed
```

Creates 6 sample posts — 3 due immediately, 3 in the future.

### 5. Start Components (in separate terminals)

```bash
# Terminal 1 — Scheduler (produces to Kafka every minute)
npm run scheduler

# Terminal 2 — Main Consumer (processes publish requests)
npm run consumer

# Terminal 3 — DLQ Monitor Consumer
npm run dlq-consumer
```

### 6. Monitor via Kafka UI

Open [http://localhost:8080](http://localhost:8080) to inspect topics, messages, and consumer group offsets.

---

## 🗄️ Data Models

### Post

| Field | Type | Description |
|---|---|---|
| `title` | String | Post title |
| `content` | String | Post body |
| `author` | String | Author name |
| `scheduledAt` | Date | When to publish |
| `status` | Enum | `scheduled` → `queued` → `published` \| `failed` |
| `kafkaEventId` | String | UUID assigned when queued |
| `publishedAt` | Date | Set on success |
| `retryCount` | Number | Retry attempts made |
| `failureReason` | String | Last error message |

### ProcessedEvent (Idempotency Store)

| Field | Type | Description |
|---|---|---|
| `eventId` | String | Kafka event UUID (unique index) |
| `postId` | ObjectId | Ref to Post |
| `outcome` | Enum | `success`, `failed`, `dlq` |
| `processedAt` | Date | TTL: auto-deleted after 7 days |

### DLQRecord

| Field | Type | Description |
|---|---|---|
| `eventId` | String | Original event UUID |
| `postId` | ObjectId | Ref to Post |
| `payload` | Mixed | Full original message payload |
| `errorMessage` | String | Final error |
| `retryCount` | Number | Attempts made |
| `isResolved` | Boolean | Manual resolution flag |

---

## 🌊 Message Flow

```
[Post due] → Scheduler picks up → status: scheduled → queued
          → Kafka: post.publish.requests (key=postId, header: event-id)
          → Consumer receives message
          → Check ProcessedEvent(eventId) → if found: SKIP (idempotent)
          → publishToExternalPlatform() — attempt 1
             ✓ success → markPostPublished() + recordProcessedEvent(success)
             ✗ fail    → wait 1s → attempt 2
                          ✗ fail  → wait 2s → attempt 3
                                     ✗ fail  → routeToDLQ()
                                                → Kafka: post.publish.dlq
                                                → MongoDB: DLQRecord
                                                → markPostFailed()
                                                → recordProcessedEvent(dlq)
```
