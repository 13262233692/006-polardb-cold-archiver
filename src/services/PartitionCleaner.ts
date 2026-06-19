import { PoolConnection, ResultSetHeader, RowDataPacket } from 'mysql2/promise';
import { dbManager } from '../database';
import { DatabaseClient } from '../database/DatabaseClient';
import { ArchiveTable, CleanResult, TimeRange } from '../types';
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
    logger.info(`[CLEAN] 准备清理表 ${table.tableName}，时间范围: ${timeRange.start} ~ ${timeRange.end}, DRY_RUN=${this.dryRun}`);

    if (this.dryRun) {
      logger.info(`[CLEAN][DRY_RUN] 模拟模式，不实际执行清理操作`);
      return {
        tableName: table.tableName,
        timeRange,
        rowsDeleted: verifiedRows,
        partitionDropped: false,
      };
    }

    const countSql = `SELECT COUNT(*) as cnt FROM \`${table.tableName}\` 
                     WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?`;
    const countResult = await this.source.query(countSql, [timeRange.start, timeRange.end]) as unknown as Array<{ cnt: number }>;
    const rowsToDelete = countResult[0]?.cnt || 0;

    if (rowsToDelete === 0) {
      logger.info(`[CLEAN] 表 ${table.tableName} 在该时间范围无数据需要清理`);
      return {
        tableName: table.tableName,
        timeRange,
        rowsDeleted: 0,
        partitionDropped: false,
      };
    }

    logger.info(`[CLEAN] 待清理数据量: ${rowsToDelete} 行`);

    let rowsDeleted = 0;
    let partitionDropped = false;

    try {
      const partitionInfo = await this.getTimePartitions(table.tableName);
      const matchingPartition = await this.findMatchingPartition(await partitionInfo, timeRange);

      if (matchingPartition) {
        partitionDropped = await this.tryDropPartition(table.tableName, matchingPartition);
        if (partitionDropped) {
          rowsDeleted = rowsToDelete;
          logger.info(`[CLEAN] 已通过 DROP PARTITION ${matchingPartition} 分区`);
        }
      }

      if (!partitionDropped) {
        rowsDeleted = await this.deleteByTimeRange(table, timeRange);
      }

      logger.info(`[CLEAN] 清理完成，共删除 ${rowsDeleted} 行，分区删除=${partitionDropped}`);
    } catch (error) {
      logger.error(`[CLEAN] 清理失败: ${error instanceof Error ? error.message : String(error)}`);
      throw error;
    }

    return {
      tableName: table.tableName,
      timeRange,
      rowsDeleted,
      partitionDropped,
    };
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

    const start = dayjs(timeRange.start);
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

  private async tryDropPartition(tableName: string, partitionName: string): Promise<boolean> {
    try {
      const sql = `ALTER TABLE \`${tableName}\` DROP PARTITION \`${partitionName}\``;
      await this.source.query(sql);
      return true;
    } catch (error) {
      logger.warn(`DROP PARTITION 失败，将使用 DELETE 方式: ${error instanceof Error ? error.message : String(error)}`);
      return false;
    }
  }

  private async deleteByTimeRange(table: ArchiveTable, timeRange: TimeRange): Promise<number> {
    let totalDeleted = 0;
    const deleteBatchSize = 1000;

    while (true) {
      const deleteSql = `DELETE FROM \`${table.tableName}\` 
                        WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?
                        LIMIT ?`;

      const result = await this.source.withTransaction(async () => {
        const conn = await this.source.getConnection();
        try {
          const [res] = await conn.execute(deleteSql, [timeRange.start, timeRange.end, deleteBatchSize]) as unknown as [ResultSetHeader, unknown];
          return res.affectedRows;
        } finally {
          conn.release();
        }
      });

      totalDeleted += result;
      logger.debug(`已分批删除 ${result} 行，累计 ${totalDeleted}`);

      if (result < deleteBatchSize) break;
    }

    return totalDeleted;
  }
}

export default PartitionCleaner;
