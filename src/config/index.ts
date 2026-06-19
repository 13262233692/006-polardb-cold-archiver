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

function optionalEnv(name: string, defaultValue: string): string {
  const value = process.env[name];
  return value ?? defaultValue;
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

export interface OSSConfig {
  enabled: boolean;
  region: string;
  bucket: string;
  accessKeyId: string;
  accessKeySecret: string;
  endpoint?: string;
  prefix: string;
  shardSizeMb: number;
  concurrency: number;
  maxRetries: number;
  secure: boolean;
}

export interface ParquetConfig {
  enabled: boolean;
  outputDir: string;
  rowGroupSize: number;
  compression: 'UNCOMPRESSED' | 'SNAPPY' | 'GZIP' | 'BROTLI' | 'LZ4' | 'ZSTD';
  compressionLevel: number;
  pageSize: number;
}

export interface GlacierConfig {
  enabled: boolean;
  retentionYears: number;
  scheduleCron: string;
  scanBatchSize: number;
  localTempDir: string;
  keepLocalParquet: boolean;
  stubsTable: string;
  removeSourceAfterSuccess: boolean;
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
  oss: OSSConfig;
  parquet: ParquetConfig;
  glacier: GlacierConfig;
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
  oss: {
    enabled: parseBoolEnv('OSS_ENABLED', false),
    region: optionalEnv('OSS_REGION', 'oss-cn-hangzhou'),
    bucket: optionalEnv('OSS_BUCKET', ''),
    accessKeyId: optionalEnv('OSS_ACCESS_KEY_ID', ''),
    accessKeySecret: optionalEnv('OSS_ACCESS_KEY_SECRET', ''),
    endpoint: process.env.OSS_ENDPOINT,
    prefix: optionalEnv('OSS_PREFIX', 'glacier-archive'),
    shardSizeMb: parseIntEnv('OSS_SHARD_SIZE_MB', 100),
    concurrency: parseIntEnv('OSS_CONCURRENCY', 5),
    maxRetries: parseIntEnv('OSS_MAX_RETRIES', 3),
    secure: parseBoolEnv('OSS_SECURE', true),
  },
  parquet: {
    enabled: parseBoolEnv('PARQUET_ENABLED', true),
    outputDir: optionalEnv('PARQUET_OUTPUT_DIR', path.join(process.cwd(), 'data', 'parquet')),
    rowGroupSize: parseIntEnv('PARQUET_ROW_GROUP_SIZE', 125_000),
    compression: (optionalEnv('PARQUET_COMPRESSION', 'ZSTD') as ParquetConfig['compression']),
    compressionLevel: parseIntEnv('PARQUET_COMPRESSION_LEVEL', 3),
    pageSize: parseIntEnv('PARQUET_PAGE_SIZE', 1024 * 1024),
  },
  glacier: {
    enabled: parseBoolEnv('GLACIER_ENABLED', false),
    retentionYears: parseIntEnv('GLACIER_RETENTION_YEARS', 5),
    scheduleCron: optionalEnv('GLACIER_SCHEDULE_CRON', '0 0 3 * * 0'),
    scanBatchSize: parseIntEnv('GLACIER_SCAN_BATCH_SIZE', 100),
    localTempDir: optionalEnv('GLACIER_TEMP_DIR', path.join(process.cwd(), 'data', 'temp')),
    keepLocalParquet: parseBoolEnv('GLACIER_KEEP_LOCAL_PARQUET', false),
    stubsTable: optionalEnv('GLACIER_STUBS_TABLE', 'glacier_virtual_stubs'),
    removeSourceAfterSuccess: parseBoolEnv('GLACIER_REMOVE_SOURCE', false),
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

function ensureDir(dir: string): void {
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
}

ensureDir(config.log.dir);
if (config.compensation.enabled) ensureDir(config.compensation.logDir);
if (config.parquet.enabled) ensureDir(config.parquet.outputDir);
ensureDir(config.glacier.localTempDir);

export default config;
