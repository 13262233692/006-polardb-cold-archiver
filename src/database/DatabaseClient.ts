import mysql, { Pool, PoolConnection, PoolOptions, RowDataPacket, OkPacket, ResultSetHeader, ExecuteValues } from 'mysql2/promise';
import { DatabaseConfig } from '../config';
import { logger } from '../utils/logger';
import { DbRow, DbValue } from '../types';

export type QueryResult = RowDataPacket[] | OkPacket | ResultSetHeader | RowDataPacket[][];

export { DbRow, DbValue };

export interface SessionOptions {
  acquireTimeoutMs: number;
  statementTimeoutMs: number;
  lockWaitTimeoutSecs: number;
}

const DEFAULT_SESSION_OPTIONS: SessionOptions = {
  acquireTimeoutMs: 30_000,
  statementTimeoutMs: 60_000,
  lockWaitTimeoutSecs: 10,
};

export class DatabaseClient {
  private pool: Pool;
  private ddlPool: Pool;
  private name: string;

  constructor(config: DatabaseConfig, name: string) {
    this.name = name;

    const baseOptions: PoolOptions = {
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
      database: config.database,
      waitForConnections: true,
      queueLimit: 0,
      enableKeepAlive: true,
      keepAliveInitialDelay: 0,
      charset: 'utf8mb4',
      timezone: '+08:00',
      supportBigNumbers: true,
      bigNumberStrings: false,
      dateStrings: false,
      connectTimeout: 10_000,
    };

    this.pool = mysql.createPool({
      ...baseOptions,
      connectionLimit: config.connectionLimit || 10,
    });

    this.ddlPool = mysql.createPool({
      ...baseOptions,
      connectionLimit: 2,
    });

    logger.info(
      `[${this.name}] 数据库连接池已创建，host=${config.host}, port=${config.port}, ` +
      `db=${config.database}, dmlPool=${config.connectionLimit || 10}, ddlPool=2`
    );
  }

  async getConnection(timeoutMs?: number): Promise<PoolConnection> {
    const timeout = timeoutMs ?? 30_000;
    const conn = await Promise.race([
      this.pool.getConnection(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`[${this.name}] 获取连接超时 (${timeout}ms)，连接池可能已耗尽`)), timeout)
      ),
    ]);
    return conn;
  }

  async getDdlConnection(timeoutMs?: number): Promise<PoolConnection> {
    const timeout = timeoutMs ?? 30_000;
    const conn = await Promise.race([
      this.ddlPool.getConnection(),
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`[${this.name}] 获取DDL连接超时 (${timeout}ms)`)), timeout)
      ),
    ]);
    return conn;
  }

  private async applySessionTimeouts(conn: PoolConnection, options: Partial<SessionOptions>): Promise<void> {
    const opts = { ...DEFAULT_SESSION_OPTIONS, ...options };
    await conn.execute(`SET SESSION max_execution_time = ${opts.statementTimeoutMs}`);
    await conn.execute(`SET SESSION innodb_lock_wait_timeout = ${opts.lockWaitTimeoutSecs}`);
  }

  async query<T extends QueryResult = RowDataPacket[]>(
    sql: string,
    params?: DbValue[],
    sessionOptions?: Partial<SessionOptions>
  ): Promise<T> {
    const conn = await this.getConnection();
    try {
      await this.applySessionTimeouts(conn, sessionOptions ?? {});
      logger.debug(`[${this.name}] 执行SQL: ${sql}${params ? `, params=${JSON.stringify(params)}` : ''}`);
      const [rows] = await conn.execute(sql, params as ExecuteValues) as unknown as [T, unknown];
      return rows;
    } finally {
      conn.release();
    }
  }

  async executeDdl(sql: string, timeoutMs?: number): Promise<void> {
    const conn = await this.getDdlConnection();
    try {
      const timeout = timeoutMs ?? 120_000;
      await conn.execute(`SET SESSION max_execution_time = ${timeout}`);
      await conn.execute(`SET SESSION lock_wait_timeout = 10`);
      logger.info(`[${this.name}][DDL] 执行: ${sql}`);
      await conn.execute(sql);
      logger.info(`[${this.name}][DDL] 执行成功`);
    } catch (error) {
      logger.error(`[${this.name}][DDL] 执行失败: ${error instanceof Error ? error.message : String(error)}`);
      throw error;
    } finally {
      conn.release();
    }
  }

  async paginatedQuery(
    sql: string,
    params: DbValue[],
    pageSize: number,
    onBatch: (rows: DbRow[]) => Promise<void>,
    sessionOptions?: Partial<SessionOptions>
  ): Promise<number> {
    const conn = await this.getConnection();
    let totalProcessed = 0;
    let offset = 0;

    try {
      await this.applySessionTimeouts(conn, sessionOptions ?? {});

      while (true) {
        const pagedSql = `${sql} LIMIT ? OFFSET ?`;
        const pagedParams = [...params, pageSize, offset];
        logger.debug(`[${this.name}] 分页查询 offset=${offset}, limit=${pageSize}`);

        const [rows] = await conn.execute(pagedSql, pagedParams as ExecuteValues) as unknown as [RowDataPacket[], unknown];
        const batch = rows as unknown as DbRow[];

        if (batch.length === 0) break;

        await onBatch(batch);
        totalProcessed += batch.length;
        offset += pageSize;

        if (batch.length < pageSize) break;
      }

      return totalProcessed;
    } finally {
      conn.release();
    }
  }

  async withTransaction<T>(
    fn: (conn: PoolConnection) => Promise<T>,
    sessionOptions?: Partial<SessionOptions>
  ): Promise<T> {
    const conn = await this.getConnection();
    try {
      await this.applySessionTimeouts(conn, sessionOptions ?? {});
      await conn.beginTransaction();
      const result = await fn(conn);
      await conn.commit();
      logger.debug(`[${this.name}] 事务已提交`);
      return result;
    } catch (error) {
      try {
        await conn.rollback();
      } catch (rollbackError) {
        logger.error(`[${this.name}] 事务回滚失败: ${rollbackError instanceof Error ? rollbackError.message : String(rollbackError)}`);
      }
      logger.warn(`[${this.name}] 事务已回滚，错误: ${error instanceof Error ? error.message : String(error)}`);
      throw error;
    } finally {
      conn.release();
    }
  }

  async bulkInsert(conn: PoolConnection, table: string, columns: string[], rows: DbRow[]): Promise<number> {
    if (rows.length === 0) return 0;
    const placeholders = rows.map(() => `(${columns.map(() => '?').join(', ')})`).join(', ');
    const values: DbValue[] = [];
    for (const row of rows) {
      for (const col of columns) {
        values.push((row[col] ?? null) as DbValue);
      }
    }
    const sql = `INSERT INTO ${table} (${columns.map(c => `\`${c}\``).join(', ')}) VALUES ${placeholders}`;
    const [result] = await conn.execute(sql, values as ExecuteValues) as unknown as [ResultSetHeader, unknown];
    return result.affectedRows;
  }

  async tableExists(table: string): Promise<boolean> {
    const rows = await this.query<RowDataPacket[]>(
      `SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
      [table]
    );
    return (rows[0] as unknown as { cnt: number }).cnt > 0;
  }

  async getTableColumns(table: string): Promise<Array<{ Field: string; Type: string; Null: string; Key: string; Default: unknown; Extra: string }>> {
    const rows = await this.query<RowDataPacket[]>(`DESCRIBE \`${table}\``);
    return rows as unknown as Array<{ Field: string; Type: string; Null: string; Key: string; Default: unknown; Extra: string }>;
  }

  get poolStats(): { totalConnections: number; freeConnections: number; queuedRequests: number } {
    const poolAny = this.pool as unknown as {
      _allConnections?: { length: number };
      _freeConnections?: { length: number };
      _connectionQueue?: { length: number };
    };
    return {
      totalConnections: poolAny._allConnections?.length ?? -1,
      freeConnections: poolAny._freeConnections?.length ?? -1,
      queuedRequests: poolAny._connectionQueue?.length ?? -1,
    };
  }

  logPoolStats(): void {
    const stats = this.poolStats;
    logger.info(
      `[${this.name}] 连接池状态: 总连接=${stats.totalConnections}, ` +
      `空闲=${stats.freeConnections}, 排队请求=${stats.queuedRequests}`
    );
  }

  async close(): Promise<void> {
    await Promise.all([this.pool.end(), this.ddlPool.end()]);
    logger.info(`[${this.name}] 数据库连接池已关闭`);
  }
}
