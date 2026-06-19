import fs from 'fs';
import path from 'path';
import dayjs from 'dayjs';
import { CompensationAction, CompensationRecord, TimeRange } from '../types';
import { config } from '../config';
import { logger } from '../utils/logger';

export class CompensationLog {
  private logDir: string;
  private maxRetries: number;
  private retryDelayMs: number;
  private backoffMultiplier: number;
  private records: Map<string, CompensationRecord>;

  constructor() {
    this.logDir = config.compensation.logDir;
    this.maxRetries = config.compensation.maxRetries;
    this.retryDelayMs = config.compensation.retryDelayMs;
    this.backoffMultiplier = config.compensation.retryBackoffMultiplier;
    this.records = new Map();
    this.loadFromDisk();
  }

  private get logFilePath(): string {
    return path.join(this.logDir, 'compensation.jsonl');
  }

  private loadFromDisk(): void {
    if (!fs.existsSync(this.logFilePath)) return;
    try {
      const lines = fs.readFileSync(this.logFilePath, 'utf-8').split('\n').filter(Boolean);
      for (const line of lines) {
        try {
          const record: CompensationRecord = JSON.parse(line);
          if (record.status !== 'SUCCEEDED') {
            this.records.set(record.id, record);
          }
        } catch {
          // skip malformed lines
        }
      }
      logger.info(`补偿日志已加载，待处理记录 ${this.records.size} 条`);
    } catch (error) {
      logger.warn(`加载补偿日志失败: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  private appendToDisk(record: CompensationRecord): void {
    try {
      fs.appendFileSync(this.logFilePath, JSON.stringify(record) + '\n', 'utf-8');
    } catch (error) {
      logger.error(`写入补偿日志失败: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  recordFailure(
    tableName: string,
    timeRange: TimeRange,
    action: CompensationAction,
    actionDetail: string,
    errorMessage: string
  ): CompensationRecord {
    const id = `${tableName}_${action}_${dayjs().format('YYYYMMDDHHmmssSSS')}`;
    const now = dayjs().format('YYYY-MM-DD HH:mm:ss');

    const record: CompensationRecord = {
      id,
      tableName,
      timeRange,
      action,
      actionDetail,
      retryCount: 0,
      maxRetries: this.maxRetries,
      status: 'PENDING',
      lastError: errorMessage,
      createdAt: now,
      nextRetryAt: dayjs().add(this.retryDelayMs, 'millisecond').format('YYYY-MM-DD HH:mm:ss'),
      updatedAt: now,
    };

    this.records.set(id, record);
    this.appendToDisk(record);
    logger.warn(
      `[COMPENSATION] 已记录失败操作: ${action} on ${tableName} ` +
      `[${timeRange.start} ~ ${timeRange.end}], 将在 ${record.nextRetryAt} 重试`
    );

    return record;
  }

  markInProgress(id: string): CompensationRecord | null {
    const record = this.records.get(id);
    if (!record) return null;

    record.status = 'IN_PROGRESS';
    record.updatedAt = dayjs().format('YYYY-MM-DD HH:mm:ss');
    this.records.set(id, record);
    this.appendToDisk(record);
    return record;
  }

  markSucceeded(id: string): CompensationRecord | null {
    const record = this.records.get(id);
    if (!record) return null;

    record.status = 'SUCCEEDED';
    record.updatedAt = dayjs().format('YYYY-MM-DD HH:mm:ss');
    delete record.lastError;
    this.records.set(id, record);
    this.appendToDisk(record);
    logger.info(`[COMPENSATION] 操作成功完成: ${record.action} on ${record.tableName}`);
    return record;
  }

  markRetryFailed(id: string, errorMessage: string): CompensationRecord | null {
    const record = this.records.get(id);
    if (!record) return null;

    record.retryCount += 1;
    record.lastError = errorMessage;
    record.updatedAt = dayjs().format('YYYY-MM-DD HH:mm:ss');

    if (record.retryCount >= record.maxRetries) {
      record.status = 'EXHAUSTED';
      logger.error(
        `[COMPENSATION] 操作已耗尽重试次数: ${record.action} on ${record.tableName}, ` +
        `重试 ${record.retryCount} 次均失败`
      );
    } else {
      record.status = 'FAILED';
      const nextDelay = this.retryDelayMs * Math.pow(this.backoffMultiplier, record.retryCount);
      record.nextRetryAt = dayjs().add(nextDelay, 'millisecond').format('YYYY-MM-DD HH:mm:ss');
      logger.warn(
        `[COMPENSATION] 操作重试失败 (${record.retryCount}/${record.maxRetries}): ` +
        `${record.action} on ${record.tableName}, 下次重试: ${record.nextRetryAt}`
      );
    }

    this.records.set(id, record);
    this.appendToDisk(record);
    return record;
  }

  getPendingRecords(): CompensationRecord[] {
    const now = dayjs();
    return Array.from(this.records.values()).filter(r => {
      if (r.status === 'SUCCEEDED' || r.status === 'EXHAUSTED') return false;
      if (r.status === 'PENDING' || r.status === 'FAILED') {
        return now.isAfter(dayjs(r.nextRetryAt)) || now.isSame(dayjs(r.nextRetryAt));
      }
      return false;
    });
  }

  getExhaustedRecords(): CompensationRecord[] {
    return Array.from(this.records.values()).filter(r => r.status === 'EXHAUSTED');
  }

  get stats(): { pending: number; inProgress: number; succeeded: number; failed: number; exhausted: number } {
    let pending = 0;
    let inProgress = 0;
    let succeeded = 0;
    let failed = 0;
    let exhausted = 0;
    for (const r of this.records.values()) {
      switch (r.status) {
        case 'PENDING': pending++; break;
        case 'IN_PROGRESS': inProgress++; break;
        case 'SUCCEEDED': succeeded++; break;
        case 'FAILED': failed++; break;
        case 'EXHAUSTED': exhausted++; break;
      }
    }
    return { pending, inProgress, succeeded, failed, exhausted };
  }
}

export const compensationLog = new CompensationLog();

export default compensationLog;
