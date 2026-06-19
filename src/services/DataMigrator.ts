import { dbManager } from '../database';
import { DatabaseClient, DbRow } from '../database/DatabaseClient';
import { DataCleaner } from './DataCleaner';
import { ArchiveTable, MigrationResult, TimeRange } from '../types';
import { config } from '../config';
import { logger } from '../utils/logger';

function generateTaskId(tableName: string, timeRange: TimeRange): string {
  return `${tableName}_${timeRange.start}_${timeRange.end}`;
}

export class DataMigrator {
  private source: DatabaseClient;
  private target: DatabaseClient;
  private cleaner: DataCleaner;
  private batchSize: number;
  private fetchPageSize: number;

  constructor() {
    this.source = dbManager.source;
    this.target = dbManager.target;
    this.cleaner = new DataCleaner();
    this.batchSize = config.archive.batchInsertSize;
    this.fetchPageSize = config.archive.fetchPageSize;
  }

  async migrateTable(table: ArchiveTable, timeRange: TimeRange): Promise<MigrationResult> {
    const startTime = Date.now();
    const taskId = generateTaskId(table.tableName, timeRange);
    logger.info(`[${taskId}] 开始迁移表 ${table.tableName}，时间范围: ${timeRange.start} ~ ${timeRange.end}`);

    let rowsRead = 0;
    let rowsWritten = 0;

    try {
      const targetExists = await this.target.tableExists(table.tableName);
      if (!targetExists) {
        logger.info(`[${taskId}] 目标表 ${table.tableName} 不存在，跳过`);
        return {
          taskId,
          tableName: table.tableName,
          timeRange,
          rowsRead: 0,
          rowsWritten: 0,
          cleaned: false,
          durationMs: Date.now() - startTime,
          error: '目标表不存在',
        };
      }

      const countSql = `SELECT COUNT(*) as cnt FROM \`${table.tableName}\` 
                        WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?`;
      const countResult = await this.source.query(
        countSql,
        [timeRange.start, timeRange.end],
        { statementTimeoutMs: 30_000, lockWaitTimeoutSecs: 5 }
      ) as unknown as Array<{ cnt: number }>;
      const totalRows = countResult[0]?.cnt || 0;
      logger.info(`[${taskId}] 源表待迁移数据量: ${totalRows} 行`);

      if (totalRows === 0) {
        logger.info(`[${taskId}] 源表无数据，跳过迁移`);
        return {
          taskId,
          tableName: table.tableName,
          timeRange,
          rowsRead: 0,
          rowsWritten: 0,
          cleaned: false,
          durationMs: Date.now() - startTime,
        };
      }

      const fetchSql = `SELECT ${table.columns.map(c => `\`${c}\``).join(', ')} FROM \`${table.tableName}\` 
                        WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?
                        ORDER BY \`${table.partitionColumn}\` ASC`;

      rowsRead = await this.source.paginatedQuery(
        fetchSql,
        [timeRange.start, timeRange.end],
        this.fetchPageSize,
        async (pageRows) => {
          const cleanedRows = this.cleaner.processBatch(pageRows as DbRow[], table.columns);
          await this.writeBatchToTarget(taskId, table, cleanedRows, rowsWritten);
          rowsWritten += cleanedRows.length;
        },
        { statementTimeoutMs: 60_000, lockWaitTimeoutSecs: 10 }
      );

      logger.info(`[${taskId}] 迁移完成，读取 ${rowsRead} 行，写入 ${rowsWritten} 行，耗时 ${Date.now() - startTime}ms`);

      return {
        taskId,
        tableName: table.tableName,
        timeRange,
        rowsRead,
        rowsWritten,
        cleaned: false,
        durationMs: Date.now() - startTime,
      };
    } catch (error) {
      logger.error(`[${taskId}] 迁移失败: ${error instanceof Error ? error.message : String(error)}`);

      this.source.logPoolStats();
      this.target.logPoolStats();

      return {
        taskId,
        tableName: table.tableName,
        timeRange,
        rowsRead,
        rowsWritten,
        cleaned: false,
        durationMs: Date.now() - startTime,
        error: error instanceof Error ? error.message : String(error),
      };
    }
  }

  private async writeBatchToTarget(taskId: string, table: ArchiveTable, cleanedRows: DbRow[], alreadyWritten: number): Promise<void> {
    await this.target.withTransaction(
      async (targetConn) => {
        let written = 0;
        for (let i = 0; i < cleanedRows.length; i += this.batchSize) {
          const batch = cleanedRows.slice(i, i + this.batchSize);
          const batchCount = await this.target.bulkInsert(targetConn, table.tableName, table.columns, batch);
          written += batchCount;
          logger.debug(
            `[${taskId}] 已写入批次 ${Math.floor(i / this.batchSize) + 1}，` +
            `本批 ${batchCount} 行，累计 ${alreadyWritten + written} 行`
          );
        }
      },
      {
        statementTimeoutMs: 60_000,
        lockWaitTimeoutSecs: 10,
      }
    );
  }

  async verifyMigration(table: ArchiveTable, timeRange: TimeRange, expectedCount: number): Promise<boolean> {
    const verifySql = `SELECT COUNT(*) as cnt FROM \`${table.tableName}\` 
                       WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?`;
    const targetResult = await this.target.query(
      verifySql,
      [timeRange.start, timeRange.end],
      { statementTimeoutMs: 30_000, lockWaitTimeoutSecs: 5 }
    ) as unknown as Array<{ cnt: number }>;
    const actualCount = targetResult[0]?.cnt || 0;
    const match = actualCount >= expectedCount;
    logger.info(`迁移校验 [${table.tableName}]: 期望>=${expectedCount}, 实际=${actualCount}, 结果=${match ? '通过' : '失败'}`);
    return match;
  }
}

export default DataMigrator;
