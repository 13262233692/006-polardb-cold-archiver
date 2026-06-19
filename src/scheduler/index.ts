import cron from 'node-cron';
import { ArchiveOrchestrator } from '../services/ArchiveOrchestrator';
import { GlacierOrchestrator } from '../services/GlacierOrchestrator';
import { config } from '../config';
import { logger } from '../utils/logger';

export class Scheduler {
  private orchestrator: ArchiveOrchestrator;
  private glacier: GlacierOrchestrator;
  private archiveTask: cron.ScheduledTask | null = null;
  private glacierTask: cron.ScheduledTask | null = null;
  private archiveRunning: boolean = false;
  private glacierRunning: boolean = false;

  constructor() {
    this.orchestrator = new ArchiveOrchestrator();
    this.glacier = new GlacierOrchestrator();
  }

  start(): void {
    this.startArchiveTask();
    this.startGlacierTask();
    logger.info('所有调度任务已启动，等待下一次执行时机');
  }

  private startArchiveTask(): void {
    const cronExpr = config.schedule.cronExpression;
    const tz = config.schedule.timezone;
    logger.info(`归档调度器启动，Cron: ${cronExpr}，时区: ${tz}`);

    this.archiveTask = cron.schedule(
      cronExpr,
      async () => {
        await this.executeArchive();
      },
      { scheduled: true, timezone: tz }
    );
  }

  private startGlacierTask(): void {
    if (!config.glacier.enabled || !config.parquet.enabled || !config.oss.enabled) {
      logger.info('极寒归档未完全启用（GLACIER / PARQUET / OSS），不启动调度');
      return;
    }
    const cronExpr = config.glacier.scheduleCron;
    const tz = config.schedule.timezone;
    logger.info(`极寒归档调度器启动，Cron: ${cronExpr}，时区: ${tz}`);

    this.glacierTask = cron.schedule(
      cronExpr,
      async () => {
        await this.executeGlacier();
      },
      { scheduled: true, timezone: tz }
    );
  }

  async executeArchive(): Promise<void> {
    if (this.archiveRunning) {
      logger.warn('检测到归档任务正在运行，跳过本次触发以避免并发');
      return;
    }
    this.archiveRunning = true;
    try {
      await this.orchestrator.runArchive();
    } catch (error) {
      logger.error(`归档任务执行异常: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      this.archiveRunning = false;
    }
  }

  async executeGlacier(): Promise<void> {
    if (this.glacierRunning) {
      logger.warn('检测到极寒归档任务正在运行，跳过本次触发');
      return;
    }
    if (this.archiveRunning) {
      logger.warn('检测到主归档任务正在运行，极寒归档延迟到下次执行');
      return;
    }
    this.glacierRunning = true;
    try {
      await this.glacier.runPipeline();
    } catch (error) {
      logger.error(`极寒归档任务执行异常: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      this.glacierRunning = false;
    }
  }

  stop(): void {
    if (this.archiveTask) {
      this.archiveTask.stop();
      this.archiveTask = null;
    }
    if (this.glacierTask) {
      this.glacierTask.stop();
      this.glacierTask = null;
    }
    logger.info('调度器已停止');
  }

  isRunning(): boolean {
    return this.archiveRunning || this.glacierRunning;
  }
}

export default Scheduler;
