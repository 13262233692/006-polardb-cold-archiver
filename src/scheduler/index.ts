import cron from 'node-cron';
import { ArchiveOrchestrator } from '../services/ArchiveOrchestrator';
import { config } from '../config';
import { logger } from '../utils/logger';

export class Scheduler {
  private orchestrator: ArchiveOrchestrator;
  private task: cron.ScheduledTask | null = null;
  private running: boolean = false;

  constructor() {
    this.orchestrator = new ArchiveOrchestrator();
  }

  start(): void {
    const cronExpr = config.schedule.cronExpression;
    const tz = config.schedule.timezone;

    logger.info(`调度器启动中，Cron 表达式: ${cronExpr}，时区: ${tz}`);

    this.task = cron.schedule(
      cronExpr,
      async () => {
        await this.execute();
      },
      {
        scheduled: true,
        timezone: tz,
      }
    );

    logger.info('调度器已启动，等待下一次执行时机');
  }

  async execute(): Promise<void> {
    if (this.running) {
      logger.warn('检测到归档任务正在运行，跳过本次触发以避免并发');
      return;
    }

    this.running = true;
    try {
      await this.orchestrator.runArchive();
    } catch (error) {
      logger.error(`归档任务执行异常: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      this.running = false;
    }
  }

  stop(): void {
    if (this.task) {
      this.task.stop();
      this.task = null;
      logger.info('调度器已停止');
    }
  }

  isRunning(): boolean {
    return this.running;
  }
}

export default Scheduler;
