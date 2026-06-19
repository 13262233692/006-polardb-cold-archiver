import dayjs from 'dayjs';
import { dbManager } from '../database';
import { DatabaseClient } from '../database/DatabaseClient';
import { ArchiveTable, ParquetColumnSchema, OSSShardResult, ParquetExportResult, VirtualStub, TimeRange } from '../types';
import { config } from '../config';
import { logger } from '../utils/logger';

export class VirtualStubService {
  private target: DatabaseClient;
  private stubsTable: string;

  constructor() {
    this.target = dbManager.target;
    this.stubsTable = config.glacier.stubsTable;
  }

  async createStub(
    table: ArchiveTable,
    timeRange: TimeRange,
    parquetResult: ParquetExportResult,
    ossResult: OSSShardResult,
    checksum: string
  ): Promise<VirtualStub> {
    const id = this.generateStubId(table.tableName, timeRange);
    const now = dayjs().format('YYYY-MM-DD HH:mm:ss');

    const stub: VirtualStub = {
      id,
      tableName: table.tableName,
      partitionColumn: table.partitionColumn,
      timeRange,
      parquetObjectName: ossResult.objectKey,
      parquetFileSizeBytes: parquetResult.fileSizeBytes,
      rowCount: parquetResult.rowCount,
      columnSchema: parquetResult.schema,
      ossBucket: ossResult.bucket,
      ossRegion: config.oss.region,
      ossPrefix: config.oss.prefix,
      localParquetPath: config.glacier.keepLocalParquet ? parquetResult.filePath : undefined,
      status: 'ARCHIVED',
      checksum,
      archivedAt: now,
      createdAt: now,
      updatedAt: now,
    };

    await this.upsertStub(stub);
    logger.info(
      `[VirtualStub] 存根已创建: ${id}, 表=${table.tableName}, ` +
      `范围=${timeRange.start} ~ ${timeRange.end}, rows=${stub.rowCount}, size=${parquetResult.fileSizeBytes}`
    );
    return stub;
  }

  async upsertStub(stub: VirtualStub): Promise<void> {
    const sql = `
      INSERT INTO \`${this.stubsTable}\` (
        id, table_name, partition_column, range_start, range_end,
        parquet_object_name, parquet_file_size_bytes, row_count, column_schema,
        oss_bucket, oss_region, oss_prefix, local_parquet_path, status, checksum,
        archived_at, expiry_at, restored_at, restore_expiry_at, created_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON DUPLICATE KEY UPDATE
        parquet_object_name = VALUES(parquet_object_name),
        parquet_file_size_bytes = VALUES(parquet_file_size_bytes),
        row_count = VALUES(row_count),
        column_schema = VALUES(column_schema),
        oss_bucket = VALUES(oss_bucket),
        oss_region = VALUES(oss_region),
        oss_prefix = VALUES(oss_prefix),
        local_parquet_path = VALUES(local_parquet_path),
        status = VALUES(status),
        checksum = VALUES(checksum),
        archived_at = VALUES(archived_at),
        expiry_at = VALUES(expiry_at),
        restored_at = VALUES(restored_at),
        restore_expiry_at = VALUES(restore_expiry_at),
        updated_at = VALUES(updated_at)
    `;

    const params = [
      stub.id,
      stub.tableName,
      stub.partitionColumn,
      stub.timeRange.start,
      stub.timeRange.end,
      stub.parquetObjectName,
      stub.parquetFileSizeBytes,
      stub.rowCount,
      JSON.stringify(stub.columnSchema),
      stub.ossBucket,
      stub.ossRegion,
      stub.ossPrefix,
      stub.localParquetPath ?? null,
      stub.status,
      stub.checksum,
      stub.archivedAt,
      stub.expiryAt ?? null,
      stub.restoredAt ?? null,
      stub.restoreExpiryAt ?? null,
      stub.createdAt,
      stub.updatedAt,
    ];

    await this.target.query(sql, params, { statementTimeoutMs: 15_000, lockWaitTimeoutSecs: 5 });
  }

  async findStub(tableName: string, timeRange: TimeRange): Promise<VirtualStub | null> {
    const sql = `SELECT * FROM \`${this.stubsTable}\`
                 WHERE table_name = ? AND range_start = ? AND range_end = ?
                 AND status != 'INVALID'
                 LIMIT 1`;
    const rows = await this.target.query(sql, [tableName, timeRange.start, timeRange.end]) as unknown as Array<{
      id: string;
      table_name: string;
      partition_column: string;
      range_start: Date | string;
      range_end: Date | string;
      parquet_object_name: string;
      parquet_file_size_bytes: number;
      row_count: number;
      column_schema: string;
      oss_bucket: string;
      oss_region: string;
      oss_prefix: string;
      local_parquet_path?: string | null;
      status: VirtualStub['status'];
      checksum: string;
      archived_at: Date | string;
      expiry_at?: Date | string | null;
      restored_at?: Date | string | null;
      restore_expiry_at?: Date | string | null;
      created_at: Date | string;
      updated_at: Date | string;
    }>;

    if (!rows || rows.length === 0) return null;

    const r = rows[0];
    return {
      id: r.id,
      tableName: r.table_name,
      partitionColumn: r.partition_column,
      timeRange: { start: dayjs(r.range_start).format('YYYY-MM-DD HH:mm:ss'), end: dayjs(r.range_end).format('YYYY-MM-DD HH:mm:ss') },
      parquetObjectName: r.parquet_object_name,
      parquetFileSizeBytes: r.parquet_file_size_bytes,
      rowCount: r.row_count,
      columnSchema: JSON.parse(r.column_schema) as ParquetColumnSchema[],
      ossBucket: r.oss_bucket,
      ossRegion: r.oss_region,
      ossPrefix: r.oss_prefix,
      localParquetPath: r.local_parquet_path ?? undefined,
      status: r.status,
      checksum: r.checksum,
      archivedAt: dayjs(r.archived_at).format('YYYY-MM-DD HH:mm:ss'),
      expiryAt: r.expiry_at ? dayjs(r.expiry_at).format('YYYY-MM-DD HH:mm:ss') : undefined,
      restoredAt: r.restored_at ? dayjs(r.restored_at).format('YYYY-MM-DD HH:mm:ss') : undefined,
      restoreExpiryAt: r.restore_expiry_at ? dayjs(r.restore_expiry_at).format('YYYY-MM-DD HH:mm:ss') : undefined,
      createdAt: dayjs(r.created_at).format('YYYY-MM-DD HH:mm:ss'),
      updatedAt: dayjs(r.updated_at).format('YYYY-MM-DD HH:mm:ss'),
    };
  }

  async listStubsByTable(tableName: string, limit = 100): Promise<VirtualStub[]> {
    const sql = `SELECT * FROM \`${this.stubsTable}\`
                 WHERE table_name = ? AND status != 'INVALID'
                 ORDER BY range_start DESC LIMIT ?`;
    const rows = await this.target.query(sql, [tableName, limit]) as unknown as Array<Record<string, unknown>>;
    const stubs: VirtualStub[] = [];
    for (const r of rows) {
      stubs.push({
        id: r.id as string,
        tableName: r.table_name as string,
        partitionColumn: r.partition_column as string,
        timeRange: {
          start: dayjs(r.range_start as Date).format('YYYY-MM-DD HH:mm:ss'),
          end: dayjs(r.range_end as Date).format('YYYY-MM-DD HH:mm:ss'),
        },
        parquetObjectName: r.parquet_object_name as string,
        parquetFileSizeBytes: r.parquet_file_size_bytes as number,
        rowCount: r.row_count as number,
        columnSchema: JSON.parse(r.column_schema as string) as ParquetColumnSchema[],
        ossBucket: r.oss_bucket as string,
        ossRegion: r.oss_region as string,
        ossPrefix: r.oss_prefix as string,
        localParquetPath: r.local_parquet_path as string | undefined,
        status: r.status as VirtualStub['status'],
        checksum: r.checksum as string,
        archivedAt: dayjs(r.archived_at as Date).format('YYYY-MM-DD HH:mm:ss'),
        createdAt: dayjs(r.created_at as Date).format('YYYY-MM-DD HH:mm:ss'),
        updatedAt: dayjs(r.updated_at as Date).format('YYYY-MM-DD HH:mm:ss'),
      });
    }
    return stubs;
  }

  private generateStubId(tableName: string, timeRange: TimeRange): string {
    const start = timeRange.start.replace(/[:\s-]/g, '').slice(0, 14);
    const end = timeRange.end.replace(/[:\s-]/g, '').slice(0, 14);
    return `stub_${tableName}_${start}_${end}`;
  }
}

export default VirtualStubService;
