import dayjs from 'dayjs';
import { DataMigrator } from './DataMigrator';
import { PartitionCleaner } from './PartitionCleaner';
import { compensationLog } from './CompensationLog';
import { dbManager } from '../database';
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
    logger.info('数据归档任务开始执行（两阶段提交模式）');
    logger.info(`模式: ${config.archive.dryRun ? 'DRY_RUN（模拟，不实际删除）' : 'PRODUCTION'}`);
    logger.info(`归档截止日期: ${config.archive.cutoffDate}`);
    logger.info(`每批时间窗口: ${config.archive.batchWindowDays} 天`);
    logger.info(`批量插入大小: ${config.archive.batchInsertSize} 行`);
    logger.info(`分页拉取大小: ${config.archive.fetchPageSize} 行`);
    logger.info(`连接获取超时: ${config.archive.acquireTimeoutMs}ms`);
    logger.info(`语句执行超时: ${config.archive.statementTimeoutMs}ms`);
    logger.info(`DDL超时: ${config.archive.ddlTimeoutMs}ms`);
    logger.info(`补偿机制: ${config.compensation.enabled ? '启用' : '禁用'}`);
    logger.info('==============================');

    dbManager.logAllPoolStats();

    const allResults: MigrationResult[] = [];
    const timeRanges = this.generateTimeRanges();

    logger.info('====== Phase 1: 数据迁移与校验 ======');
    for (const table of ARCHIVE_TABLES) {
      logger.info(`------ 开始处理表 ${table.tableName} ------`);
      for (const range of timeRanges) {
        const result = await this.phase1MigrateAndVerify(table, range);
        allResults.push(result);

        dbManager.logAllPoolStats();
      }
      logger.info(`------ 表 ${table.tableName} 迁移阶段完成 ------`);
    }

    logger.info('====== Phase 2: 源库数据清理 ======');
    for (const table of ARCHIVE_TABLES) {
      logger.info(`------ 开始清理表 ${table.tableName} ------`);
      for (const range of timeRanges) {
        const result = await this.phase2Cleanup(table, range, allResults);
        if (result) {
          const existing = allResults.find(
            r => r.tableName === table.tableName && r.timeRange.start === range.start && r.timeRange.end === range.end
          );
          if (existing) {
            existing.cleaned = result.cleaned;
            if (result.error) existing.error = result.error;
          }
        }
      }
      logger.info(`------ 表 ${table.tableName} 清理阶段完成 ------`);
    }

    if (config.compensation.enabled) {
      logger.info('====== Phase 3: 补偿重试 ======');
      await this.cleaner.retryCompensations();

      const compStats = compensationLog.stats;
      logger.info(
        `补偿统计: 待处理=${compStats.pending}, 进行中=${compStats.inProgress}, ` +
        `已成功=${compStats.succeeded}, 已失败=${compStats.failed}, 已耗尽=${compStats.exhausted}`
      );
    }

    const endTime = new Date();
    const summary = this.summarize(allResults, startTime, endTime);
    this.logSummary(summary);

    return summary;
  }

  private async phase1MigrateAndVerify(table: ArchiveTable, range: TimeRange): Promise<MigrationResult> {
    const migrationResult = await this.migrator.migrateTable(table, range);

    if (migrationResult.error) {
      logger.error(`[Phase1] 表 ${table.tableName} [${range.start} ~ ${range.end}] 迁移失败，跳过清理`);
      return migrationResult;
    }

    if (migrationResult.rowsRead === 0) {
      return migrationResult;
    }

    if (migrationResult.rowsWritten < migrationResult.rowsRead) {
      logger.warn(
        `[Phase1] 表 ${table.tableName} [${range.start} ~ ${range.end}] ` +
        `写入行数(${migrationResult.rowsWritten}) < 读取行数(${migrationResult.rowsRead})，跳过清理以保证数据安全`
      );
      migrationResult.error = '数据量不匹配，跳过清理';
      return migrationResult;
    }

    return migrationResult;
  }

  private async phase2Cleanup(
    table: ArchiveTable,
    range: TimeRange,
    allResults: MigrationResult[]
  ): Promise<MigrationResult | null> {
    const migrationResult = allResults.find(
      r => r.tableName === table.tableName && r.timeRange.start === range.start && r.timeRange.end === range.end
    );

    if (!migrationResult || migrationResult.error || migrationResult.rowsRead === 0) {
      return null;
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

      if (cleanResult.compensated) {
        migrationResult.cleaned = false;
        migrationResult.error = '清理已降级为补偿模式';
      }

      logger.info(
        `[Phase2] 清理完成: 删除 ${cleanResult.rowsDeleted} 行, ` +
        `分区删除=${cleanResult.partitionDropped}, 补偿=${cleanResult.compensated}`
      );
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
    let compensatedCount = 0;

    for (const r of results) {
      totalRowsRead += r.rowsRead;
      totalRowsWritten += r.rowsWritten;
      if (r.error) {
        failCount++;
        if (r.error.includes('补偿')) {
          compensatedCount++;
        }
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
      compensatedCount,
      startTime,
      endTime,
    };
  }

  private logSummary(summary: BatchResult): void {
    const durationMs = summary.endTime.getTime() - summary.startTime.getTime();
    logger.info('==============================');
    logger.info('数据归档任务执行汇总（两阶段提交模式）');
    logger.info(`开始时间: ${summary.startTime.toISOString()}`);
    logger.info(`结束时间: ${summary.endTime.toISOString()}`);
    logger.info(`总耗时: ${(durationMs / 1000).toFixed(2)} 秒`);
    logger.info(`总任务数: ${summary.tasks.length}`);
    logger.info(`成功: ${summary.successCount}`);
    logger.info(`失败/异常: ${summary.failCount}`);
    logger.info(`降级为补偿: ${summary.compensatedCount}`);
    logger.info(`总读取行数: ${summary.totalRowsRead}`);
    logger.info(`总写入行数: ${summary.totalRowsWritten}`);

    if (config.compensation.enabled) {
      const compStats = compensationLog.stats;
      logger.info('--- 补偿日志统计 ---');
      logger.info(`待处理: ${compStats.pending}`);
      logger.info(`已成功: ${compStats.succeeded}`);
      logger.info(`已耗尽: ${compStats.exhausted}`);
    }

    logger.info('==============================');
  }
}

export default ArchiveOrchestrator;
