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
