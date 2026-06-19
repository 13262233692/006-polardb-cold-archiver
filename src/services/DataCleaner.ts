import { DbRow, DbValue } from '../types';
import { logger } from '../utils/logger';

function trimString(value: DbValue): DbValue {
  if (typeof value === 'string') {
    return value.trim();
  }
  return value;
}

function normalizeNull(value: DbValue | undefined): DbValue {
  if (value === undefined || value === '') {
    return null;
  }
  if (typeof value === 'string' && value.toLowerCase() === 'null') {
    return null;
  }
  return value;
}

function sanitizeText(value: DbValue): DbValue {
  if (typeof value === 'string') {
    return value
      .replace(/\u0000/g, '')
      .replace(/\r\n/g, '\n')
      .replace(/\r/g, '\n');
  }
  return value;
}

function cleanField(key: string, value: DbValue | undefined): DbValue {
  let result: DbValue = normalizeNull(value);
  if (result === null) return result;

  if (typeof result === 'string') {
    result = trimString(result) as DbValue;
    if (key.toLowerCase().includes('content') ||
        key.toLowerCase().includes('text') ||
        key.toLowerCase().includes('remark') ||
        key.toLowerCase().includes('comment')) {
      result = sanitizeText(result) as DbValue;
    }
  }

  return result;
}

export function cleanRow(row: DbRow): DbRow {
  const cleaned: DbRow = {};
  for (const [key, value] of Object.entries(row)) {
    cleaned[key] = cleanField(key, value);
  }
  return cleaned;
}

export function cleanRows(rows: DbRow[]): DbRow[] {
  return rows.map(cleanRow);
}

export function filterColumns(row: DbRow, columns: string[]): DbRow {
  const filtered: DbRow = {};
  for (const col of columns) {
    filtered[col] = row[col] ?? null;
  }
  return filtered;
}

export class DataCleaner {
  cleanRow(row: DbRow): DbRow {
    return cleanRow(row);
  }

  cleanRows(rows: DbRow[]): DbRow[] {
    const result = cleanRows(rows);
    logger.debug(`数据清洗完成，共清洗 ${rows.length} 行`);
    return result;
  }

  filterColumns(row: DbRow, columns: string[]): DbRow {
    return filterColumns(row, columns);
  }

  processBatch(rows: DbRow[], columns: string[]): DbRow[] {
    return rows
      .map(row => this.filterColumns(this.cleanRow(row), columns));
  }
}

export default DataCleaner;
