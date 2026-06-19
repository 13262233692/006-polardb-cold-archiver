import dayjs from 'dayjs';
import { dbManager } from '../database';
import { DatabaseClient } from '../database/DatabaseClient';
import { ARCHIVE_TABLES } from '../config/tables';
import { ArchiveTable, GlacierCandidate, TimeRange } from '../types';
import { config } from '../config';
import { logger } from '../utils/logger';

export class GlacierDetector {
  private target: DatabaseClient;
  private retentionYears: number;
  private scanBatchSize: number;
  private stubsTable: string;

  constructor() {
    this.target = dbManager.target;
    this.retentionYears = config.glacier.retentionYears;
    this.scanBatchSize = config.glacier.scanBatchSize;
    this.stubsTable = config.glacier.stubsTable;
  }

  async detectCandidates(): Promise<GlacierCandidate[]> {
    const cutoff = dayjs().subtract(this.retentionYears, 'year');
    logger.info(
      `[GlacierDetector] 开始扫描极寒归档候选数据，保留期=${this.retentionYears}年，` +
      `截止时间=${cutoff.format('YYYY-MM-DD')}`
    );

    await this.ensureStubsTable();

    const candidates: GlacierCandidate[] = [];
    const alreadyArchived = await this.getAlreadyArchivedRanges();

    for (const table of ARCHIVE_TABLES) {
      const tableCandidates = await this.detectForTable(table, cutoff, alreadyArchived);
      candidates.push(...tableCandidates);
    }

    logger.info(`[GlacierDetector] 扫描完成，共发现 ${candidates.length} 个候选分区`);
    return candidates;
  }

  private async ensureStubsTable(): Promise<void> {
    const exists = await this.target.tableExists(this.stubsTable);
    if (exists) return;

    logger.info(`[GlacierDetector] 存根表 ${this.stubsTable} 不存在，正在创建...`);

    const ddl = `
      CREATE TABLE IF NOT EXISTS \`${this.stubsTable}\` (
        id VARCHAR(128) NOT NULL PRIMARY KEY,
        table_name VARCHAR(128) NOT NULL,
        partition_column VARCHAR(64) NOT NULL,
        range_start DATETIME NOT NULL,
        range_end DATETIME NOT NULL,
        parquet_object_name VARCHAR(512) NOT NULL,
        parquet_file_size_bytes BIGINT NOT NULL,
        row_count BIGINT NOT NULL,
        column_schema JSON NOT NULL,
        oss_bucket VARCHAR(128) NOT NULL,
        oss_region VARCHAR(64) NOT NULL,
        oss_prefix VARCHAR(256) NOT NULL,
        local_parquet_path VARCHAR(512),
        status VARCHAR(32) NOT NULL DEFAULT 'ARCHIVED',
        checksum VARCHAR(128) NOT NULL,
        archived_at DATETIME NOT NULL,
        expiry_at DATETIME NULL,
        restored_at DATETIME NULL,
        restore_expiry_at DATETIME NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        INDEX idx_table_range (table_name, range_start, range_end),
        INDEX idx_status (status)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
    `;

    await this.target.executeDdl(ddl, 120_000);
    logger.info(`[GlacierDetector] 存根表 ${this.stubsTable} 创建成功`);
  }

  private async getAlreadyArchivedRanges(): Promise<Set<string>> {
    const result = new Set<string>();
    try {
      const sql = `SELECT table_name, range_start, range_end FROM \`${this.stubsTable}\` WHERE status != 'INVALID'`;
      const rows = await this.target.query(sql) as unknown as Array<{
        table_name: string;
        range_start: Date | string;
        range_end: Date | string;
      }>;
      for (const row of rows) {
        const key = `${row.table_name}_${dayjs(row.range_start).format('YYYY-MM-DD')}_${dayjs(row.range_end).format('YYYY-MM-DD')}`;
        result.add(key);
      }
      logger.debug(`[GlacierDetector] 已归档范围记录数: ${result.size}`);
    } catch (error) {
      logger.warn(`[GlacierDetector] 查询已归档范围失败: ${error instanceof Error ? error.message : String(error)}`);
    }
    return result;
  }

  private async detectForTable(
    table: ArchiveTable,
    cutoff: dayjs.Dayjs,
    alreadyArchived: Set<string>
  ): Promise<GlacierCandidate[]> {
    const candidates: GlacierCandidate[] = [];

    const tableExists = await this.target.tableExists(table.tableName);
    if (!tableExists) {
      logger.debug(`[GlacierDetector] 表 ${table.tableName} 不存在于历史库，跳过`);
      return candidates;
    }

    const partitions = await this.getTimePartitions(table.tableName);

    if (partitions.length > 0) {
      for (const p of partitions) {
        if (!p.partitionDescription || p.partitionDescription === 'MAXVALUE') continue;
        const desc = p.partitionDescription.replace(/'/g, '');
        const boundary = dayjs(desc);
        if (!boundary.isValid()) continue;

        if (boundary.isAfter(cutoff) || boundary.isSame(cutoff)) continue;

        const start = dayjs(partitions[partitions.indexOf(p) - 1]?.partitionDescription?.replace(/'/g, '') || '2000-01-01');
        const end = boundary;
        const key = `${table.tableName}_${start.format('YYYY-MM-DD')}_${end.format('YYYY-MM-DD')}`;
        if (alreadyArchived.has(key)) continue;

        const timeRange: TimeRange = {
          start: start.format('YYYY-MM-DD HH:mm:ss'),
          end: end.format('YYYY-MM-DD HH:mm:ss'),
        };

        const stats = await this.getPartitionStats(table, timeRange);
        if (stats.rowCount === 0) continue;

        candidates.push({
          tableName: table.tableName,
          timeRange,
          partitionName: p.partitionName,
          rowCount: stats.rowCount,
          estimatedSizeBytes: stats.estimatedBytes,
        });
      }
    } else {
      const earliest = await this.getEarliestRecordTime(table);
      if (!earliest || earliest.isAfter(cutoff)) {
        return candidates;
      }

      const rangeSize = 30;
      let cursor = earliest;
      while (cursor.isBefore(cutoff)) {
        const start = cursor;
        const end = cursor.add(rangeSize, 'day').isAfter(cutoff) ? cutoff : cursor.add(rangeSize, 'day');

        const key = `${table.tableName}_${start.format('YYYY-MM-DD')}_${end.format('YYYY-MM-DD')}`;
        if (alreadyArchived.has(key)) {
          cursor = end;
          continue;
        }

        const timeRange: TimeRange = {
          start: start.format('YYYY-MM-DD HH:mm:ss'),
          end: end.format('YYYY-MM-DD HH:mm:ss'),
        };

        const stats = await this.getPartitionStats(table, timeRange);
        if (stats.rowCount > 0) {
          candidates.push({
            tableName: table.tableName,
            timeRange,
            rowCount: stats.rowCount,
            estimatedSizeBytes: stats.estimatedBytes,
          });
        }
        cursor = end;
      }
    }

    logger.info(`[GlacierDetector] 表 ${table.tableName} 发现 ${candidates.length} 个候选分区`);
    return candidates;
  }

  private async getTimePartitions(
    tableName: string
  ): Promise<Array<{ partitionName: string; partitionDescription: string }>> {
    try {
      const sql = `SELECT PARTITION_NAME, PARTITION_DESCRIPTION
                   FROM information_schema.PARTITIONS
                   WHERE TABLE_SCHEMA = DATABASE()
                   AND TABLE_NAME = ?
                   AND PARTITION_NAME IS NOT NULL
                   ORDER BY PARTITION_ORDINAL_POSITION`;
      const rows = await this.target.query(sql, [tableName]) as unknown as Array<{
        PARTITION_NAME: string;
        PARTITION_DESCRIPTION: string;
      }>;
      return rows.map(r => ({ partitionName: r.PARTITION_NAME, partitionDescription: r.PARTITION_DESCRIPTION }));
    } catch {
      return [];
    }
  }

  private async getEarliestRecordTime(table: ArchiveTable): Promise<dayjs.Dayjs | null> {
    try {
      const sql = `SELECT MIN(\`${table.partitionColumn}\`) as earliest FROM \`${table.tableName}\``;
      const rows = await this.target.query(sql) as unknown as Array<{ earliest: Date | string | null }>;
      const earliest = rows[0]?.earliest;
      if (!earliest) return null;
      return dayjs(earliest);
    } catch {
      return null;
    }
  }

  private async getPartitionStats(
    table: ArchiveTable,
    timeRange: TimeRange
  ): Promise<{ rowCount: number; estimatedBytes: number }> {
    const countSql = `SELECT COUNT(*) as cnt FROM \`${table.tableName}\`
                      WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?`;
    const countRows = await this.target.query(
      countSql,
      [timeRange.start, timeRange.end],
      { statementTimeoutMs: 30_000, lockWaitTimeoutSecs: 5 }
    ) as unknown as Array<{ cnt: number }>;
    const rowCount = countRows[0]?.cnt || 0;

    const avgRowSizeSql = `SELECT AVG_ROW_LENGTH as avg_len FROM information_schema.TABLES
                           WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`;
    const avgRows = await this.target.query(avgRowSizeSql, [table.tableName]) as unknown as Array<{ avg_len: number }>;
    const avgRowSize = avgRows[0]?.avg_len || 200;

    return {
      rowCount,
      estimatedBytes: rowCount * avgRowSize,
    };
  }
}

export default GlacierDetector;
