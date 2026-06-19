import { DatabaseClient } from './DatabaseClient';
import { config } from '../config';
import { logger } from '../utils/logger';

class DatabaseManager {
  private _source: DatabaseClient | null = null;
  private _target: DatabaseClient | null = null;

  get source(): DatabaseClient {
    if (!this._source) {
      this._source = new DatabaseClient(config.source, 'SOURCE');
    }
    return this._source;
  }

  get target(): DatabaseClient {
    if (!this._target) {
      this._target = new DatabaseClient(config.target, 'TARGET');
    }
    return this._target;
  }

  async closeAll(): Promise<void> {
    logger.info('正在关闭所有数据库连接池...');
    const tasks: Promise<void>[] = [];
    if (this._source) tasks.push(this._source.close());
    if (this._target) tasks.push(this._target.close());
    await Promise.all(tasks);
  }
}

export const dbManager = new DatabaseManager();

export default dbManager;
