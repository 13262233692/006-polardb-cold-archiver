import fs from 'fs';
import path from 'path';
import zlib from 'zlib';
import { Buffer } from 'buffer';
import { dbManager } from '../database';
import { DatabaseClient } from '../database/DatabaseClient';
import { ArchiveTable, ParquetColumnSchema, ParquetExportResult, TimeRange, DbRow } from '../types';
import { config } from '../config';
import { logger } from '../utils/logger';

type ParquetType =
  | 'BOOLEAN'
  | 'INT32'
  | 'INT64'
  | 'FLOAT'
  | 'DOUBLE'
  | 'DECIMAL'
  | 'UTF8'
  | 'BINARY'
  | 'TIMESTAMP_MILLIS'
  | 'TIMESTAMP_MICROS'
  | 'DATE';

interface CompressionCodec {
  readonly code: number;
  readonly name: string;
}

const PARQUET_CODECS: Record<string, CompressionCodec> = {
  UNCOMPRESSED: { code: 0, name: 'UNCOMPRESSED' },
  SNAPPY: { code: 1, name: 'SNAPPY' },
  GZIP: { code: 2, name: 'GZIP' },
  LZO: { code: 3, name: 'LZO' },
  BROTLI: { code: 4, name: 'BROTLI' },
  LZ4: { code: 5, name: 'LZ4' },
  ZSTD: { code: 6, name: 'ZSTD' },
};

interface InternalField {
  name: string;
  type: ParquetType;
  nullable: boolean;
}

function mysqlTypeToParquet(mysqlType: string, nullable: boolean): ParquetType {
  const lower = mysqlType.toLowerCase();
  if (lower.includes('bigint')) return 'INT64';
  if (lower.includes('int') || lower.includes('mediumint') || lower.includes('smallint') || lower.includes('tinyint')) return 'INT32';
  if (lower.includes('decimal') || lower.includes('double')) return 'DOUBLE';
  if (lower.includes('float')) return 'FLOAT';
  if (lower.includes('datetime') || lower.includes('timestamp')) return 'TIMESTAMP_MILLIS';
  if (lower.includes('date')) return 'DATE';
  if (lower.includes('blob') || lower.includes('binary')) return 'BINARY';
  return 'UTF8';
}

export class ParquetExporter {
  private target: DatabaseClient;

  constructor() {
    this.target = dbManager.target;
  }

  async exportTable(table: ArchiveTable, timeRange: TimeRange): Promise<ParquetExportResult> {
    const startTime = Date.now();
    logger.info(`[ParquetExporter] 开始导出 ${table.tableName} [${timeRange.start} ~ ${timeRange.end}]`);

    const schema = await this.buildSchema(table);
    const rows: DbRow[] = [];

    const fetchSql = `SELECT ${table.columns.map(c => `\`${c}\``).join(', ')} FROM \`${table.tableName}\`
                      WHERE \`${table.partitionColumn}\` >= ? AND \`${table.partitionColumn}\` < ?
                      ORDER BY \`${table.partitionColumn}\` ASC`;

    await this.target.paginatedQuery(
      fetchSql,
      [timeRange.start, timeRange.end],
      config.archive.fetchPageSize,
      async (pageRows) => {
        rows.push(...pageRows);
        logger.debug(`[ParquetExporter] 已读取 ${rows.length} 行`);
      },
      { statementTimeoutMs: 60_000, lockWaitTimeoutSecs: 10 }
    );

    logger.info(`[ParquetExporter] 读取完成，共 ${rows.length} 行，开始写 Parquet...`);

    const { filePath, fileName } = this.generateFilePath(table, timeRange);
    await this.writeParquetFile(filePath, schema, rows);
    const fileSize = fs.statSync(filePath).size;

    const avgRowSize = schema.reduce((s, f) => s + (f.type === 'UTF8' ? 32 : 8), 0);
    const uncompressedEstimate = rows.length * avgRowSize;
    const compressionRatio = uncompressedEstimate > 0 ? uncompressedEstimate / fileSize : 1;

    logger.info(
      `[ParquetExporter] 导出完成: ${fileName}, ${rows.length} rows, ${(fileSize / 1024 / 1024).toFixed(2)} MB, 压缩比=${compressionRatio.toFixed(2)}x`
    );

    return {
      filePath,
      fileName,
      rowCount: rows.length,
      fileSizeBytes: fileSize,
      compressionRatio,
      durationMs: Date.now() - startTime,
      schema,
    };
  }

  private async buildSchema(table: ArchiveTable): Promise<ParquetColumnSchema[]> {
    const descRows = await this.target.getTableColumns(table.tableName);
    const schema: ParquetColumnSchema[] = [];
    for (const col of table.columns) {
      const desc = descRows.find(d => d.Field === col);
      if (desc) {
        schema.push({
          name: col,
          type: mysqlTypeToParquet(desc.Type, desc.Null === 'YES'),
          nullable: desc.Null === 'YES',
        });
      } else {
        schema.push({ name: col, type: 'UTF8', nullable: true });
      }
    }
    return schema;
  }

  private generateFilePath(table: ArchiveTable, timeRange: TimeRange): { filePath: string; fileName: string } {
    const safeStart = timeRange.start.replace(/[:\s]/g, '-').slice(0, 19);
    const safeEnd = timeRange.end.replace(/[:\s]/g, '-').slice(0, 19);
    const fileName = `${table.tableName}_${safeStart}_to_${safeEnd}.parquet`;
    const filePath = path.join(config.parquet.outputDir, fileName);
    return { filePath, fileName };
  }

  private async writeParquetFile(
    filePath: string,
    schema: ParquetColumnSchema[],
    rows: DbRow[]
  ): Promise<void> {
    const PARQUET_MAGIC = Buffer.from('PAR1');
    const codec = PARQUET_CODECS[config.parquet.compression] || PARQUET_CODECS.UNCOMPRESSED;
    const fields: InternalField[] = schema.map(s => ({ name: s.name, type: s.type, nullable: s.nullable }));
    const rowGroupSize = config.parquet.rowGroupSize;

    const rowGroups: Array<{
      num_rows: number;
      columns: unknown[];
      total_byte_size: number;
    }> = [];

    let offset = 4;
    const fd = fs.openSync(filePath, 'w');
    fs.writeSync(fd, PARQUET_MAGIC);

    for (let rgStart = 0; rgStart < rows.length; rgStart += rowGroupSize) {
      const rgRows = rows.slice(rgStart, rgStart + rowGroupSize);
      const rowGroupColumns: unknown[] = [];
      let rgTotalBytes = 0;

      for (const field of fields) {
        const values = rgRows.map(r => this.serializeValue(r[field.name] ?? null, field));
        const rawBuf = this.encodeColumnChunk(values, field);
        let compressed: Buffer;
        if (codec.code === 2) {
          compressed = zlib.gzipSync(rawBuf, { level: config.parquet.compressionLevel });
        } else {
          compressed = rawBuf;
        }

        fs.writeSync(fd, compressed);
        rgTotalBytes += compressed.length;
        rowGroupColumns.push({
          file_path: null,
          file_offset: offset,
          meta_data: {
            type: field.type,
            encodings: [0],
            path_in_schema: [field.name],
            codec: codec.code,
            num_values: values.length,
            total_uncompressed_size: rawBuf.length,
            total_compressed_size: compressed.length,
            data_page_offset: offset,
          },
        });
        offset += compressed.length;
      }

      rowGroups.push({
        num_rows: rgRows.length,
        columns: rowGroupColumns,
        total_byte_size: rgTotalBytes,
      });
    }

    const footerObj: Record<string, unknown> = {
      version: 1,
      schema: [
        { name: 'schema', num_children: fields.length },
        ...fields.map(f => ({ name: f.name, type: f.type, repetition_type: f.nullable ? 1 : 0 })),
      ],
      num_rows: rows.length,
      row_groups: rowGroups,
      key_value_metadata: [
        { key: 'created_by', value: 'polardb-cold-archiver' },
      ],
    };

    const footerBuf = Buffer.from(JSON.stringify(footerObj), 'utf8');
    fs.writeSync(fd, footerBuf);

    const footerLenBuf = Buffer.alloc(4);
    footerLenBuf.writeUInt32LE(footerBuf.length, 0);
    fs.writeSync(fd, footerLenBuf);
    fs.writeSync(fd, PARQUET_MAGIC);
    fs.closeSync(fd);
  }

  private serializeValue(value: unknown, field: InternalField): unknown {
    if (value === null || value === undefined) return null;
    switch (field.type) {
      case 'INT32':
      case 'INT64':
      case 'FLOAT':
      case 'DOUBLE':
        return typeof value === 'number' ? value : Number(value);
      case 'BOOLEAN':
        return !!value;
      case 'DATE':
        if (value instanceof Date) return Math.floor(value.getTime() / 86400000);
        if (typeof value === 'string') return Math.floor(new Date(value).getTime() / 86400000);
        return Number(value);
      case 'TIMESTAMP_MILLIS':
      case 'TIMESTAMP_MICROS':
        if (value instanceof Date) return value.getTime();
        if (typeof value === 'string') return new Date(value).getTime();
        return Number(value);
      case 'BINARY':
        if (Buffer.isBuffer(value)) return value;
        return Buffer.from(String(value), 'utf8');
      case 'UTF8':
      case 'DECIMAL':
      default:
        return String(value);
    }
  }

  private encodeColumnChunk(values: unknown[], field: InternalField): Buffer {
    const parts: Buffer[] = [];
    for (const v of values) {
      if (v === null) {
        parts.push(Buffer.from([0]));
      } else if (field.type === 'UTF8' || field.type === 'BINARY' || field.type === 'DECIMAL') {
        const buf = Buffer.isBuffer(v) ? v : Buffer.from(String(v), 'utf8');
        const len = Buffer.alloc(4);
        len.writeUInt32LE(buf.length, 0);
        parts.push(Buffer.from([1]));
        parts.push(len);
        parts.push(buf);
      } else if (field.type === 'INT32' || field.type === 'DATE') {
        const b = Buffer.alloc(4);
        b.writeInt32LE(Number(v), 0);
        parts.push(Buffer.from([1]));
        parts.push(b);
      } else if (field.type === 'INT64' || field.type === 'TIMESTAMP_MILLIS' || field.type === 'TIMESTAMP_MICROS') {
        const b = Buffer.alloc(8);
        b.writeBigInt64LE(BigInt(Number(v)), 0);
        parts.push(Buffer.from([1]));
        parts.push(b);
      } else if (field.type === 'FLOAT') {
        const b = Buffer.alloc(4);
        b.writeFloatLE(Number(v), 0);
        parts.push(Buffer.from([1]));
        parts.push(b);
      } else if (field.type === 'DOUBLE') {
        const b = Buffer.alloc(8);
        b.writeDoubleLE(Number(v), 0);
        parts.push(Buffer.from([1]));
        parts.push(b);
      } else if (field.type === 'BOOLEAN') {
        parts.push(Buffer.from([1, v ? 1 : 0]));
      } else {
        const buf = Buffer.from(String(v), 'utf8');
        const len = Buffer.alloc(4);
        len.writeUInt32LE(buf.length, 0);
        parts.push(Buffer.from([1]));
        parts.push(len);
        parts.push(buf);
      }
    }
    return Buffer.concat(parts);
  }
}

export default ParquetExporter;
