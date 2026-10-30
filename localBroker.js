/**
 * Local In-Process Kafka Broker Simulation
 * ==========================================
 * Replaces real Kafka with an EventEmitter-based broker for local runs.
 * Implements the same KafkaJS producer/consumer API so no application
 * code needs to change.
 *
 * Features:
 *  - Topic queues with persistent in-memory storage
 *  - Consumer group offset tracking
 *  - Header support
 *  - Idiomatic KafkaJS API (connect/disconnect/send/subscribe/run)
 */

const EventEmitter = require('events');

// ── In-memory topic store ────────────────────────────────────────────────────
const topics = {};         // { topicName: [ {key,value,headers,offset} ] }
const groupOffsets = {};   // { groupId: { topicName: offset } }

const getTopicQueue = (name) => {
  if (!topics[name]) topics[name] = [];
  return topics[name];
};

const getGroupOffset = (groupId, topic) => {
  if (!groupOffsets[groupId]) groupOffsets[groupId] = {};
  if (groupOffsets[groupId][topic] === undefined) groupOffsets[groupId][topic] = 0;
  return groupOffsets[groupId][topic];
};

// Global event bus for new-message notifications
const bus = new EventEmitter();
bus.setMaxListeners(50);

// ── Producer ─────────────────────────────────────────────────────────────────
const createLocalProducer = () => ({
  connect: async () => {},
  disconnect: async () => {},
  send: async ({ topic, messages }) => {
    const queue = getTopicQueue(topic);
    for (const msg of messages) {
      const record = {
        key: msg.key ? Buffer.from(msg.key) : null,
        value: Buffer.from(msg.value),
        headers: msg.headers || {},
        offset: String(queue.length),
        timestamp: String(Date.now()),
      };
      queue.push(record);
      bus.emit(`msg:${topic}`, { topic, record });
    }
  },
});

// ── Consumer ─────────────────────────────────────────────────────────────────
const createLocalConsumer = (groupId) => {
  let subscribedTopic = null;
  let eachMessageHandler = null;
  let running = false;

  // Poll for new messages every 200ms
  const pollInterval = 200;
  let timer = null;

  const poll = async () => {
    if (!running || !subscribedTopic || !eachMessageHandler) return;
    const queue = getTopicQueue(subscribedTopic);
    const offset = getGroupOffset(groupId, subscribedTopic);

    if (offset < queue.length) {
      const record = queue[offset];
      groupOffsets[groupId][subscribedTopic] = offset + 1;
      try {
        await eachMessageHandler({
          topic: subscribedTopic,
          partition: 0,
          message: record,
        });
      } catch (err) {
        // Don't crash the poll loop on handler errors
        console.error('[LocalBroker] eachMessage handler error:', err.message);
      }
    }
  };

  return {
    connect: async () => {},
    disconnect: async () => {
      running = false;
      if (timer) clearInterval(timer);
    },
    subscribe: async ({ topic }) => {
      subscribedTopic = topic;
    },
    run: async ({ eachMessage }) => {
      eachMessageHandler = eachMessage;
      running = true;
      // Drain any existing messages first, then poll
      timer = setInterval(poll, pollInterval);
    },
  };
};

// ── Admin ─────────────────────────────────────────────────────────────────────
const createLocalAdmin = () => ({
  connect: async () => {},
  disconnect: async () => {},
  createTopics: async ({ topics: topicList }) => {
    for (const { topic } of topicList) getTopicQueue(topic);
  },
  listTopics: async () => Object.keys(topics),
});

// ── Exported factory (matches real config/kafka.js API) ─────────────────────
const createProducer = async () => createLocalProducer();
const createConsumer = async (groupId) => createLocalConsumer(groupId);
const createAdmin = async () => createLocalAdmin();

module.exports = { createProducer, createConsumer, createAdmin };
