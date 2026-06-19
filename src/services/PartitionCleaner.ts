import { PoolConnection, RowDataPacket } from 'mysql2/promise';
import { dbManager } from '../database';
import { DatabaseClient } from '../database/DatabaseClient';
import { ARCHIVE_TABLES } from '../config/tables';
import { ArchiveTable, CleanResult, TimeRange } from '../types';
import { compensationLog } from './CompensationLog';
import { config } from '../config';
import { logger } from '../utils/logger';
import dayjs from 'dayjs';

export class PartitionCleaner {
  private source: DatabaseClient;
  private dryRun: boolean;

  constructor() {
    this.source = dbManager.source;
    this.dryRun = config.archive.dryRun;
  }

  async cleanSourceData(table: ArchiveTable, timeRange: TimeRange, verifiedRows: number): Promise<CleanResult> {
    logger.info(
      `[CLEAN] 准备清理表 ${table.tableName}，` +
      `时间范围: ${timeRange.start} ~ ${timeRange.end}, DRY_RUN=${this.dryRun}`
    );

    if (this.dryRun) {
      logger.info(`[CLEAN][DRY_RUN] 模拟模式，不实际执行清理操作`);
      return {
        tableName: table.tableName,
        timeRange,
        rowsDeleted: verifiedRows,
        partitionDropped: false,
        compensated: false,
      };
    }

    try {
      const rowsToDelete = await this.countRowsToDelete(table, timeRange);

      if (rowsToDelete === 0) {
        logger.info(`[CLEAN] 表 ${table.tableName} 在该时间范围无数据需要清理`);
        return { tableName: table.tableName, timeRange, rowsDeleted: 0, partitionDropped: false, compensated: false };
      }

      logger.info(`[CLEAN] 待清理数据量: ${rowsToDelete} 行`);

      const partitionInfo = await this.getTimePartitions(table.tableName);
      const matchingPartition = await this.findMatchingPartition(partitionInfo, timeRange);

      if (matchingPartition) {
        const dropResult = await this.tryDropPartitionWithTimeout(table, matchingPartition);
        if (dropResult.success) {
          return {
            tableName: table.tableName,
            timeRange,
            rowsDeleted: rowsToDelete,
            partitionDropped: true,
            compensated: false,
          };
        }

        logger.warn(
          `[CLEAN] DROP PARTITION 失败 (MDL冲突或超时)，已记录补偿日志，降级为 DELETE: ${dropResult.error}`
        );
        compensationLog.recordFailure(
          table.tableName,
          timeRange,
          'DROP_PARTITION',
          `ALTER TABLE \`${table.tableName}\` DROP PARTITION \`${matchingPartition}\``,
          dropResult.error ?? 'DROP PARTITION failed with unknown error'
        );
      }

      const deleteResult = await this.deleteByTimeRangeWithTimeout(table, timeRange);
      return {
        tableName: table.tableName,
        timeRange,
        rowsDeleted: deleteResult.rowsDeleted,
        partitionDropped: false,
        compensated: deleteResult.compensated,
      };
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);
      logger.error(`[CLEAN] 清理失败，记录补偿日志: ${errMsg}`);

      compensationLog.recordFailure(
        table.tableName,
        timeRange,
        'DELETE_RANGE',
        `DELETE FROM \`${table.tableName}\` WHERE \`${table.partitionColumn}\` >= '${timeRange.start}' AND \`${table.partitionColumn}\` < '${timeRange.end}'`,
        errMsg
      );

      return {
        tableName: table.tableName,
        timeRange,
        rowsDeleted: 0,
        partitionDropped: false,
        compensated: true,
      };
    }
  }

  async retryCompensations(): Promise<number> {
    if (!config.compensation.enabled) {
      logger.info('[COMPENSATION] 补偿机制未启用，跳过重试');
      return 0;
    }

    const pending = compensationLog.getPendingRecords();
    if (pending.length === 0) {
      logger.info('[COMPENSATION] 无待处理的补偿记录');
      return 0;
    }

    logger.info(`[COMPENSATION] 开始处理 ${pending.length} 条待重试补偿记录`);
    let succeeded = 0;

    for (const record of pending) {
      compensationLog.markInProgress(record.id);

      try {
        if (record.action === 'DROP_PARTITION') {
          const partitionName = this.extractPartitionName(record.actionDetail);
          if (partitionName) {
            const result = await this.tryDropPartitionWithTimeout(
              { tableName: record.tableName },
              partitionName
            );
            if (result.success) {
              compensationLog.markSucceeded(record.id);
              succeeded++;
              continue;
            }
            throw new Error(result.error);
          }
        } else if (record.action === 'DELETE_RANGE') {
          const tableDef = ARCHIVE_TABLES.find(t => t.tableName === record.tableName);
          if (!tableDef) {
            compensationLog.markRetryFailed(record.id, `未找到表 ${record.tableName} 的定义`);
            continue;
          }
          const deleteResult = await this.deleteByTimeRangeWithTimeout(tableDef, record.timeRange);
          if (deleteResult.rowsDeleted > 0) {
            compensationLog.markSucceeded(record.id);
            succeeded++;
            continue;
          }
        }
      } catch (error) {
        compensationLog.markRetryFailed(record.id, error instanceof Error ? error.message : String(error));
      }
    }

    logger.info(`[COMPENSATION] 重试完成，成功 ${succeeded} 条`);
    return succeeded;
  }

  private extractPartitionName(sql: string): string | null {
    const match = sql.match(/DROP PARTITION\s+`(\w+)`/i);
    return match ? match[1] : null;
  }

  private async countRowsToDelete(table: ArchiveTable, timeRange: TimeRange): Promise<number> {
    const countSql = `SELECT COUNT(*) as cnt FROM \`${table.tableName}\` 
                     WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?`;
    const countResult = await this.source.query(
      countSql,
      [timeRange.start, timeRange.end],
      { statementTimeoutMs: 30_000, lockWaitTimeoutSecs: 5 }
    ) as unknown as Array<{ cnt: number }>;
    return countResult[0]?.cnt || 0;
  }

  private async getTimePartitions(tableName: string): Promise<Array<{ partitionName: string; partitionDescription: string }>> {
    try {
      const sql = `SELECT PARTITION_NAME, PARTITION_DESCRIPTION 
                   FROM information_schema.PARTITIONS 
                   WHERE TABLE_SCHEMA = DATABASE() 
                   AND TABLE_NAME = ? 
                   AND PARTITION_NAME IS NOT NULL
                   ORDER BY PARTITION_ORDINAL_POSITION`;
      const rows = await this.source.query(sql, [tableName]) as RowDataPacket[];
      return rows as unknown as Array<{ partitionName: string; partitionDescription: string }>;
    } catch (error) {
      logger.debug(`表 ${tableName} 未找到分区信息或非分区表`);
      return [];
    }
  }

  private async findMatchingPartition(
    partitions: Array<{ partitionName: string; partitionDescription: string }>,
    timeRange: TimeRange
  ): Promise<string | null> {
    if (partitions.length === 0) return null;

    const end = dayjs(timeRange.end);

    for (const p of partitions) {
      if (!p.partitionDescription || p.partitionDescription === 'MAXVALUE') continue;

      const desc = p.partitionDescription.replace(/'/g, '');
      const partitionBoundary = dayjs(desc);

      if (!partitionBoundary.isValid()) continue;

      if (end.isBefore(partitionBoundary) || end.isSame(partitionBoundary)) {
        return p.partitionName;
      }
    }

    return null;
  }

  private async tryDropPartitionWithTimeout(
    table: Pick<ArchiveTable, 'tableName'>,
    partitionName: string
  ): Promise<{ success: boolean; error?: string }> {
    try {
      const sql = `ALTER TABLE \`${table.tableName}\` DROP PARTITION \`${partitionName}\``;
      await this.source.executeDdl(sql, config.archive.ddlTimeoutMs);
      logger.info(`[CLEAN] 已通过 DROP PARTITION ${partitionName} 删除分区`);
      return { success: true };
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);
      return { success: false, error: errMsg };
    }
  }

  private async deleteByTimeRangeWithTimeout(
    table: ArchiveTable,
    timeRange: TimeRange
  ): Promise<{ rowsDeleted: number; compensated: boolean }> {
    let totalDeleted = 0;
    let compensated = false;
    const deleteBatchSize = 1000;

    try {
      while (true) {
        const deleteSql = `DELETE FROM \`${table.tableName}\` 
                          WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?
                          LIMIT ?`;

        const batchDeleted = await this.source.withTransaction(
          async (conn) => {
            const [res] = await conn.execute(deleteSql, [
              timeRange.start, timeRange.end, deleteBatchSize,
            ]) as unknown as [import('mysql2/promise').ResultSetHeader, unknown];
            return res.affectedRows;
          },
          {
            statementTimeoutMs: 30_000,
            lockWaitTimeoutSecs: 5,
          }
        );

        totalDeleted += batchDeleted;
        logger.debug(`已分批删除 ${batchDeleted} 行，累计 ${totalDeleted}`);

        if (batchDeleted < deleteBatchSize) break;
      }

      logger.info(`[CLEAN] DELETE 清理完成，共删除 ${totalDeleted} 行`);
    } catch (error) {
      const errMsg = error instanceof Error ? error.message : String(error);
      logger.warn(`[CLEAN] DELETE 操作超时或锁等待失败，已删除 ${totalDeleted} 行，剩余部分记录补偿日志: ${errMsg}`);

      compensationLog.recordFailure(
        table.tableName,
        timeRange,
        'DELETE_RANGE',
        `DELETE FROM \`${table.tableName}\` WHERE \`${table.partitionColumn}\` >= '${timeRange.start}' AND \`${table.partitionColumn}\` < '${timeRange.end}'`,
        errMsg
      );
      compensated = true;
    }

    return { rowsDeleted: totalDeleted, compensated };
  }
}

export default PartitionCleaner;
