import dotenv from 'dotenv';
import path from 'path';
import fs from 'fs';

dotenv.config();

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`环境变量 ${name} 未设置，请检查 .env 文件`);
  }
  return value;
}

function parseIntEnv(name: string, defaultValue: number): number {
  const value = process.env[name];
  if (!value) return defaultValue;
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
}

function parseBoolEnv(name: string, defaultValue: boolean): boolean {
  const value = process.env[name];
  if (!value) return defaultValue;
  return value.toLowerCase() === 'true';
}

export interface DatabaseConfig {
  host: string;
  port: number;
  user: string;
  password: string;
  database: string;
  connectionLimit: number;
}

export interface ArchiveConfig {
  cutoffDate: string;
  batchWindowDays: number;
  batchInsertSize: number;
  streamConcurrency: number;
  dryRun: boolean;
  fetchPageSize: number;
  acquireTimeoutMs: number;
  statementTimeoutMs: number;
  lockWaitTimeoutSecs: number;
  ddlTimeoutMs: number;
}

export interface CompensationConfig {
  enabled: boolean;
  maxRetries: number;
  retryDelayMs: number;
  retryBackoffMultiplier: number;
  logDir: string;
}

export interface ScheduleConfig {
  cronExpression: string;
  timezone: string;
}

export interface LogConfig {
  level: string;
  dir: string;
}

export interface AppConfig {
  source: DatabaseConfig;
  target: DatabaseConfig;
  archive: ArchiveConfig;
  compensation: CompensationConfig;
  schedule: ScheduleConfig;
  log: LogConfig;
}

export const config: AppConfig = {
  source: {
    host: requiredEnv('SOURCE_DB_HOST'),
    port: parseIntEnv('SOURCE_DB_PORT', 3306),
    user: requiredEnv('SOURCE_DB_USER'),
    password: requiredEnv('SOURCE_DB_PASSWORD'),
    database: requiredEnv('SOURCE_DB_NAME'),
    connectionLimit: parseIntEnv('SOURCE_DB_POOL_SIZE', 10),
  },
  target: {
    host: requiredEnv('TARGET_DB_HOST'),
    port: parseIntEnv('TARGET_DB_PORT', 3306),
    user: requiredEnv('TARGET_DB_USER'),
    password: requiredEnv('TARGET_DB_PASSWORD'),
    database: requiredEnv('TARGET_DB_NAME'),
    connectionLimit: parseIntEnv('TARGET_DB_POOL_SIZE', 10),
  },
  archive: {
    cutoffDate: requiredEnv('ARCHIVE_CUTOFF_DATE'),
    batchWindowDays: parseIntEnv('BATCH_WINDOW_DAYS', 7),
    batchInsertSize: parseIntEnv('BATCH_INSERT_SIZE', 1000),
    streamConcurrency: parseIntEnv('STREAM_CONCURRENCY', 4),
    dryRun: parseBoolEnv('DRY_RUN', true),
    fetchPageSize: parseIntEnv('FETCH_PAGE_SIZE', 5000),
    acquireTimeoutMs: parseIntEnv('ACQUIRE_TIMEOUT_MS', 30_000),
    statementTimeoutMs: parseIntEnv('STATEMENT_TIMEOUT_MS', 60_000),
    lockWaitTimeoutSecs: parseIntEnv('LOCK_WAIT_TIMEOUT_SECS', 10),
    ddlTimeoutMs: parseIntEnv('DDL_TIMEOUT_MS', 120_000),
  },
  compensation: {
    enabled: parseBoolEnv('COMPENSATION_ENABLED', true),
    maxRetries: parseIntEnv('COMPENSATION_MAX_RETRIES', 5),
    retryDelayMs: parseIntEnv('COMPENSATION_RETRY_DELAY_MS', 60_000),
    retryBackoffMultiplier: parseIntEnv('COMPENSATION_RETRY_BACKOFF_MULTIPLIER', 2),
    logDir: process.env.COMPENSATION_LOG_DIR || path.join(process.cwd(), 'logs', 'compensation'),
  },
  schedule: {
    cronExpression: process.env.CRON_SCHEDULE || '0 0 2 * * *',
    timezone: process.env.CRON_TIMEZONE || 'Asia/Shanghai',
  },
  log: {
    level: process.env.LOG_LEVEL || 'info',
    dir: process.env.LOG_DIR || path.join(process.cwd(), 'logs'),
  },
};

if (!fs.existsSync(config.log.dir)) {
  fs.mkdirSync(config.log.dir, { recursive: true });
}
if (config.compensation.enabled && !fs.existsSync(config.compensation.logDir)) {
  fs.mkdirSync(config.compensation.logDir, { recursive: true });
}

export default config;
