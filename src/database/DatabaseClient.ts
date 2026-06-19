import mysql, { Pool, PoolConnection, PoolOptions, RowDataPacket, OkPacket, ResultSetHeader, ExecuteValues } from 'mysql2/promise';
import { DatabaseConfig } from '../config';
import { logger } from '../utils/logger';
import { DbRow, DbValue } from '../types';

export type QueryResult = RowDataPacket[] | OkPacket | ResultSetHeader | RowDataPacket[][];

export { DbRow, DbValue };

export class DatabaseClient {
  private pool: Pool;
  private name: string;

  constructor(config: DatabaseConfig, name: string) {
    this.name = name;
    const poolOptions: PoolOptions = {
      host: config.host,
      port: config.port,
      user: config.user,
      password: config.password,
      database: config.database,
      connectionLimit: config.connectionLimit || 10,
      waitForConnections: true,
      queueLimit: 0,
      enableKeepAlive: true,
      keepAliveInitialDelay: 0,
      charset: 'utf8mb4',
      timezone: '+08:00',
      supportBigNumbers: true,
      bigNumberStrings: false,
      dateStrings: false,
    };
    this.pool = mysql.createPool(poolOptions);
    logger.info(`[${this.name}] 数据库连接池已创建，host=${config.host}, port=${config.port}, db=${config.database}`);
  }

  async query<T extends QueryResult = RowDataPacket[]>(sql: string, params?: DbValue[]): Promise<T> {
    logger.debug(`[${this.name}] 执行SQL: ${sql}${params ? `, params=${JSON.stringify(params)}` : ''}`);
    const [rows] = await this.pool.execute(sql, params as ExecuteValues) as unknown as [T, unknown];
    return rows;
  }

  async streamQuery(sql: string, params?: DbValue[]): Promise<RowDataPacket[]> {
    const connection = await this.pool.getConnection();
    try {
      const [rows] = await connection.execute(sql, params as ExecuteValues) as unknown as [RowDataPacket[], unknown];
      return rows;
    } finally {
      connection.release();
    }
  }

  async getConnection(): Promise<PoolConnection> {
    return this.pool.getConnection();
  }

  async withTransaction<T>(fn: (conn: PoolConnection) => Promise<T>): Promise<T> {
    const conn = await this.pool.getConnection();
    try {
      await conn.beginTransaction();
      const result = await fn(conn);
      await conn.commit();
      logger.debug(`[${this.name}] 事务已提交`);
      return result;
    } catch (error) {
      await conn.rollback();
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

  async getTableColumns(table: string): Promise<Array<{ Field: string; Type: string; Null: string; Key: string; Default: unknown; Extra: string }>> {
    const rows = await this.query<RowDataPacket[]>(`DESCRIBE \`${table}\``);
    return rows as unknown as Array<{ Field: string; Type: string; Null: string; Key: string; Default: unknown; Extra: string }>;
  }

  async tableExists(table: string): Promise<boolean> {
    const rows = await this.query<RowDataPacket[]>(
      `SELECT COUNT(*) as cnt FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
      [table]
    );
    return (rows[0] as unknown as { cnt: number }).cnt > 0;
  }

  async close(): Promise<void> {
    await this.pool.end();
    logger.info(`[${this.name}] 数据库连接池已关闭`);
  }
}
