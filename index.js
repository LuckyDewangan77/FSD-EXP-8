require('dotenv').config();
const connectDB = require('./config/db');
const { createAdmin } = require('./config/kafka');
const logger = require('./utils/logger');

/**
 * Application entry point.
 *
 * This file orchestrates startup checks and prints system status.
 * The scheduler and consumers are started as separate processes
 * (see npm scripts in package.json).
 *
 * For a monolith setup, you could import and call their main() functions here.
 */
const main = async () => {
  logger.info('====================================================');
  logger.info(' Experiment 8 — Kafka Post Scheduler               ');
  logger.info('====================================================');

  // 1. Check MongoDB
  await connectDB();
  logger.info('[✓] MongoDB connection OK');

  // 2. Check Kafka connectivity
  const admin = await createAdmin();
  const topics = await admin.listTopics();
  logger.info('[✓] Kafka broker reachable', { existingTopics: topics });
  await admin.disconnect();

  logger.info('');
  logger.info('System is ready. Start each component separately:');
  logger.info('  npm run seed        — Seed sample posts into MongoDB');
  logger.info('  npm run scheduler   — Start the cron-based post scheduler');
  logger.info('  npm run consumer    — Start the Kafka publish consumer');
  logger.info('  npm run dlq-consumer — Start the DLQ monitor consumer');
  logger.info('');

  process.exit(0);
};

main().catch((err) => {
  logger.error('Startup check failed', { error: err.message });
  process.exit(1);
});
