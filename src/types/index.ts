export interface ArchiveTable {
  tableName: string;
  partitionColumn: string;
  columns: string[];
}

export interface TimeRange {
  start: string;
  end: string;
}

export interface MigrationTask {
  table: ArchiveTable;
  timeRange: TimeRange;
  taskId: string;
}

export interface MigrationResult {
  taskId: string;
  tableName: string;
  timeRange: TimeRange;
  rowsRead: number;
  rowsWritten: number;
  cleaned: boolean;
  durationMs: number;
  error?: string;
}

export interface CleanResult {
  tableName: string;
  timeRange: TimeRange;
  rowsDeleted: number;
  partitionDropped: boolean;
  compensated: boolean;
}

export interface BatchResult {
  tasks: MigrationResult[];
  totalRowsRead: number;
  totalRowsWritten: number;
  successCount: number;
  failCount: number;
  compensatedCount: number;
  startTime: Date;
  endTime: Date;
}

export type DbValue = string | number | boolean | null | Date | Buffer;
export type DbRow = Record<string, DbValue | undefined>;

export type CompensationAction = 'DROP_PARTITION' | 'DELETE_RANGE';

export interface CompensationRecord {
  id: string;
  tableName: string;
  timeRange: TimeRange;
  action: CompensationAction;
  actionDetail: string;
  retryCount: number;
  maxRetries: number;
  status: 'PENDING' | 'IN_PROGRESS' | 'SUCCEEDED' | 'FAILED' | 'EXHAUSTED';
  lastError?: string;
  createdAt: string;
  nextRetryAt: string;
  updatedAt: string;
}

export interface ParquetColumnSchema {
  name: string;
  type: 'BOOLEAN' | 'INT32' | 'INT64' | 'FLOAT' | 'DOUBLE' | 'DECIMAL' | 'UTF8' | 'BINARY' | 'TIMESTAMP_MILLIS' | 'TIMESTAMP_MICROS' | 'DATE';
  nullable: boolean;
  comment?: string;
}

export interface ParquetExportResult {
  filePath: string;
  fileName: string;
  rowCount: number;
  fileSizeBytes: number;
  compressionRatio: number;
  durationMs: number;
  schema: ParquetColumnSchema[];
}

export interface OSSShardResult {
  uploadId: string;
  objectKey: string;
  bucket: string;
  eTag: string;
  fileSizeBytes: number;
  partCount: number;
  shardSizeMb: number;
  durationMs: number;
  etagParts: Array<{ partNumber: number; eTag: string }>;
}

export interface GlacierCandidate {
  tableName: string;
  timeRange: TimeRange;
  partitionName?: string;
  rowCount: number;
  lastAccessedAt?: string;
  estimatedSizeBytes: number;
}

export type StubStatus = 'ARCHIVED' | 'RESTORING' | 'RESTORED' | 'INVALID';

export interface VirtualStub {
  id: string;
  tableName: string;
  partitionColumn: string;
  timeRange: TimeRange;
  parquetObjectName: string;
  parquetFileSizeBytes: number;
  rowCount: number;
  columnSchema: ParquetColumnSchema[];
  ossBucket: string;
  ossRegion: string;
  ossPrefix: string;
  localParquetPath?: string;
  status: StubStatus;
  checksum: string;
  archivedAt: string;
  expiryAt?: string;
  restoredAt?: string;
  restoreExpiryAt?: string;
  createdAt: string;
  updatedAt: string;
}

export interface GlacierPipelineResult {
  processedCount: number;
  succeededCount: number;
  failedCount: number;
  skippedCount: number;
  totalRowsArchived: number;
  totalBytesUploaded: number;
  stubsCreated: number;
  candidates: GlacierCandidate[];
  stubs: VirtualStub[];
  errors: Array<{ tableName: string; timeRange: TimeRange; error: string }>;
  startTime: Date;
  endTime: Date;
}
