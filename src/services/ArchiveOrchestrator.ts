import dayjs from 'dayjs';
import { DataMigrator } from './DataMigrator';
import { PartitionCleaner } from './PartitionCleaner';
import { ARCHIVE_TABLES } from '../config/tables';
import { ArchiveTable, BatchResult, MigrationResult, TimeRange } from '../types';
import { config } from '../config';
import { logger } from '../utils/logger';

export class ArchiveOrchestrator {
  private migrator: DataMigrator;
  private cleaner: PartitionCleaner;

  constructor() {
    this.migrator = new DataMigrator();
    this.cleaner = new PartitionCleaner();
  }

  generateTimeRanges(): TimeRange[] {
    const ranges: TimeRange[] = [];
    const cutoff = dayjs(config.archive.cutoffDate).endOf('day');
    const batchDays = config.archive.batchWindowDays;

    let cursor = dayjs('2000-01-01');

    while (cursor.isBefore(cutoff)) {
      const start = cursor.format('YYYY-MM-DD HH:mm:ss');
      const endCursor = cursor.add(batchDays, 'day');
      const end = endCursor.isAfter(cutoff) ? cutoff.add(1, 'second').format('YYYY-MM-DD HH:mm:ss')
        : endCursor.format('YYYY-MM-DD HH:mm:ss');

      ranges.push({ start, end });
      cursor = endCursor;
    }

    logger.info(`生成 ${ranges.length} 个时间窗口，归档截止日期: ${cutoff.format('YYYY-MM-DD')}`);
    return ranges;
  }

  async runArchive(): Promise<BatchResult> {
    const startTime = new Date();
    logger.info('==============================');
    logger.info('数据归档任务开始执行');
    logger.info(`模式: ${config.archive.dryRun ? 'DRY_RUN（模拟，不实际删除）' : 'PRODUCTION'}`);
    logger.info(`归档截止日期: ${config.archive.cutoffDate}`);
    logger.info(`每批时间窗口: ${config.archive.batchWindowDays} 天`);
    logger.info(`批量插入大小: ${config.archive.batchInsertSize} 行`);
    logger.info('==============================');

    const allResults: MigrationResult[] = [];
    const timeRanges = this.generateTimeRanges();

    for (const table of ARCHIVE_TABLES) {
      logger.info(`------ 开始处理表 ${table.tableName} ------`);
      for (const range of timeRanges) {
        const result = await this.processTableRange(table, range);
        allResults.push(result);
      }
      logger.info(`------ 表 ${table.tableName} 处理完成 ------`);
    }

    const endTime = new Date();
    const summary = this.summarize(allResults, startTime, endTime);
    this.logSummary(summary);

    return summary;
  }

  private async processTableRange(table: ArchiveTable, range: TimeRange): Promise<MigrationResult> {
    const migrationResult = await this.migrator.migrateTable(table, range);

    if (migrationResult.error) {
      logger.error(`表 ${table.tableName} [${range.start} ~ ${range.end}] 迁移失败，跳过清理`);
      return migrationResult;
    }

    if (migrationResult.rowsRead === 0) {
      return migrationResult;
    }

    if (migrationResult.rowsWritten < migrationResult.rowsRead) {
      logger.warn(`表 ${table.tableName} [${range.start} ~ ${range.end}] 写入行数(${migrationResult.rowsWritten}) < 读取行数(${migrationResult.rowsRead})，跳过清理以保证数据安全`);
      migrationResult.error = '数据量不匹配，跳过清理';
      return migrationResult;
    }

    try {
      const verified = await this.migrator.verifyMigration(table, range, migrationResult.rowsWritten);
      if (!verified) {
        migrationResult.error = '目标库数据校验不通过，跳过清理';
        logger.error(migrationResult.error);
        return migrationResult;
      }

      const cleanResult = await this.cleaner.cleanSourceData(table, range, migrationResult.rowsWritten);
      migrationResult.cleaned = cleanResult.rowsDeleted > 0 || cleanResult.partitionDropped;
      logger.info(`清理完成: 删除 ${cleanResult.rowsDeleted} 行, 分区删除=${cleanResult.partitionDropped}`);
    } catch (cleanError) {
      migrationResult.error = `清理失败: ${cleanError instanceof Error ? cleanError.message : String(cleanError)}`;
      logger.error(migrationResult.error);
    }

    return migrationResult;
  }

  private summarize(results: MigrationResult[], startTime: Date, endTime: Date): BatchResult {
    let totalRowsRead = 0;
    let totalRowsWritten = 0;
    let successCount = 0;
    let failCount = 0;

    for (const r of results) {
      totalRowsRead += r.rowsRead;
      totalRowsWritten += r.rowsWritten;
      if (r.error) {
        failCount++;
      } else {
        successCount++;
      }
    }

    return {
      tasks: results,
      totalRowsRead,
      totalRowsWritten,
      successCount,
      failCount,
      startTime,
      endTime,
    };
  }

  private logSummary(summary: BatchResult): void {
    const durationMs = summary.endTime.getTime() - summary.startTime.getTime();
    logger.info('==============================');
    logger.info('数据归档任务执行汇总');
    logger.info(`开始时间: ${summary.startTime.toISOString()}`);
    logger.info(`结束时间: ${summary.endTime.toISOString()}`);
    logger.info(`总耗时: ${(durationMs / 1000).toFixed(2)} 秒`);
    logger.info(`总任务数: ${summary.tasks.length}`);
    logger.info(`成功: ${summary.successCount}`);
    logger.info(`失败/异常: ${summary.failCount}`);
    logger.info(`总读取行数: ${summary.totalRowsRead}`);
    logger.info(`总写入行数: ${summary.totalRowsWritten}`);
    logger.info('==============================');
  }
}

export default ArchiveOrchestrator;
