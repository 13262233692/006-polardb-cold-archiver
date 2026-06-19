import { Scheduler } from './scheduler';
import { ArchiveOrchestrator } from './services/ArchiveOrchestrator';
import { GlacierOrchestrator } from './services/GlacierOrchestrator';
import { dbManager } from './database';
import { logger } from './utils/logger';

async function runOnce(): Promise<void> {
  logger.info('运行模式: 立即执行一次归档任务');
  const orchestrator = new ArchiveOrchestrator();
  try {
    await orchestrator.runArchive();
  } catch (error) {
    logger.error(`归档任务执行失败: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}

async function runGlacier(): Promise<void> {
  logger.info('运行模式: 立即执行一次极寒归档管道');
  const orchestrator = new GlacierOrchestrator();
  try {
    await orchestrator.runPipeline();
  } catch (error) {
    logger.error(`极寒归档执行失败: ${error instanceof Error ? error.message : String(error)}`);
    process.exitCode = 1;
  }
}

function runScheduled(): Scheduler {
  logger.info('运行模式: 定时调度');
  const scheduler = new Scheduler();
  scheduler.start();
  return scheduler;
}

async function shutdown(scheduler?: Scheduler): Promise<void> {
  logger.info('收到关闭信号，开始优雅停机...');
  if (scheduler) {
    scheduler.stop();
  }
  await dbManager.closeAll();
  logger.info('服务已完全停止');
  process.exit(0);
}

async function main(): Promise<void> {
  logger.info('========================================');
  logger.info('  PolarDB 冷备份与归档服务启动中');
  logger.info('========================================');

  const args = process.argv.slice(2);
  const runOnceMode = args.includes('--once') || args.includes('-o');
  const runGlacierMode = args.includes('--glacier') || args.includes('-g');

  process.on('uncaughtException', (err) => {
    logger.error(`未捕获的异常: ${err.message}`);
    logger.error(err.stack || '');
  });

  process.on('unhandledRejection', (reason) => {
    logger.error(`未处理的 Promise 拒绝: ${reason instanceof Error ? reason.message : String(reason)}`);
  });

  if (runGlacierMode) {
    await runGlacier();
    await dbManager.closeAll();
    logger.info('极寒归档任务执行完毕，进程退出');
    return;
  }

  if (runOnceMode) {
    await runOnce();
    await dbManager.closeAll();
    logger.info('单次归档任务执行完毕，进程退出');
    return;
  }

  const scheduler = runScheduled();

  process.on('SIGINT', async () => shutdown(scheduler));
  process.on('SIGTERM', async () => shutdown(scheduler));
}

main().catch((err) => {
  logger.error(`服务启动失败: ${err instanceof Error ? err.message : String(err)}`);
  process.exit(1);
});
