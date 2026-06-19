import fs from 'fs';
import { GlacierDetector } from './GlacierDetector';
import { ParquetExporter } from './ParquetExporter';
import { OSSUploader } from './OSSUploader';
import { VirtualStubService } from './VirtualStubService';
import { PartitionCleaner } from './PartitionCleaner';
import { ARCHIVE_TABLES } from '../config/tables';
import {
  ArchiveTable,
  GlacierCandidate,
  GlacierPipelineResult,
  ParquetExportResult,
  TimeRange,
  VirtualStub,
} from '../types';
import { config } from '../config';
import { logger } from '../utils/logger';

export class GlacierOrchestrator {
  private detector: GlacierDetector;
  private parquetExporter: ParquetExporter;
  private ossUploader: OSSUploader;
  private stubService: VirtualStubService;
  private partitionCleaner: PartitionCleaner;

  constructor() {
    this.detector = new GlacierDetector();
    this.parquetExporter = new ParquetExporter();
    this.ossUploader = new OSSUploader();
    this.stubService = new VirtualStubService();
    this.partitionCleaner = new PartitionCleaner();
  }

  async runPipeline(): Promise<GlacierPipelineResult> {
    const startTime = new Date();

    logger.info('==============================');
    logger.info('极寒归档降级管道启动');
    logger.info(`启用状态: GLACIER=${config.glacier.enabled}, PARQUET=${config.parquet.enabled}, OSS=${config.oss.enabled}`);
    logger.info(`数据保留期: ${config.glacier.retentionYears} 年`);
    logger.info('==============================');

    if (!config.glacier.enabled || !config.parquet.enabled || !config.oss.enabled) {
      logger.warn('极寒归档管道未完全启用（GLACIER / PARQUET / OSS 任一关闭），跳过执行');
      return this.emptyResult(startTime);
    }

    const candidates = await this.detector.detectCandidates();
    if (candidates.length === 0) {
      logger.info('未发现符合条件的极寒归档候选，任务结束');
      return { ...this.emptyResult(startTime), candidates };
    }

    logger.info(`发现 ${candidates.length} 个候选分区，开始处理...`);

    const stubs: VirtualStub[] = [];
    const errors: GlacierPipelineResult['errors'] = [];
    let succeeded = 0;
    let failed = 0;
    let skipped = 0;
    let totalRows = 0;
    let totalBytes = 0;

    for (const candidate of candidates) {
      try {
        const tableDef = ARCHIVE_TABLES.find(t => t.tableName === candidate.tableName);
        if (!tableDef) {
          skipped++;
          logger.warn(`表 ${candidate.tableName} 未在 ARCHIVE_TABLES 中定义，跳过`);
          continue;
        }

        const result = await this.processCandidate(tableDef, candidate);
        if (result.stub) {
          stubs.push(result.stub);
          succeeded++;
          totalRows += result.rows;
          totalBytes += result.bytes;
        } else if (result.skipped) {
          skipped++;
        } else {
          failed++;
          if (result.error) {
            errors.push({ tableName: candidate.tableName, timeRange: candidate.timeRange, error: result.error });
          }
        }
      } catch (err) {
        failed++;
        const msg = err instanceof Error ? err.message : String(err);
        logger.error(`处理候选分区异常: ${candidate.tableName} [${candidate.timeRange.start} ~ ${candidate.timeRange.end}]: ${msg}`);
        errors.push({ tableName: candidate.tableName, timeRange: candidate.timeRange, error: msg });
      }
    }

    const endTime = new Date();
    const result: GlacierPipelineResult = {
      processedCount: candidates.length,
      succeededCount: succeeded,
      failedCount: failed,
      skippedCount: skipped,
      totalRowsArchived: totalRows,
      totalBytesUploaded: totalBytes,
      stubsCreated: stubs.length,
      candidates,
      stubs,
      errors,
      startTime,
      endTime,
    };

    this.logSummary(result);
    return result;
  }

  private async processCandidate(
    table: ArchiveTable,
    candidate: GlacierCandidate
  ): Promise<{ stub?: VirtualStub; rows: number; bytes: number; skipped?: boolean; error?: string }> {
    const { tableName, timeRange } = candidate;
    logger.info(`------ 处理 ${tableName} [${timeRange.start} ~ ${timeRange.end}] ------`);

    const existing = await this.stubService.findStub(tableName, timeRange);
    if (existing) {
      logger.info(`该分区已存在存根 (${existing.id})，跳过`);
      return { rows: 0, bytes: 0, skipped: true };
    }

    let parquetResult: ParquetExportResult | null = null;
    let parquetFilePath = '';

    try {
      logger.info(`Step 1/4: 导出 Parquet 文件`);
      parquetResult = await this.parquetExporter.exportTable(table, timeRange);
      parquetFilePath = parquetResult.filePath;

      if (parquetResult.rowCount === 0) {
        logger.info('Parquet 文件为空，跳过后续步骤');
        this.cleanupLocalFile(parquetFilePath);
        return { rows: 0, bytes: 0, skipped: true };
      }

      logger.info(`Step 2/4: 分片上传到 OSS`);
      const objectKey = this.ossUploader.generateObjectKey(parquetResult.fileName);
      const ossResult = await this.ossUploader.uploadFile(parquetFilePath, objectKey);

      logger.info(`Step 3/4: 生成并保存虚拟路由存根`);
      const checksum = this.ossUploader.computeFileChecksum(parquetFilePath);
      const stub = await this.stubService.createStub(table, timeRange, parquetResult, ossResult, checksum);

      logger.info(`Step 4/4: 清理本地与历史库源数据`);
      if (!config.glacier.keepLocalParquet) {
        this.cleanupLocalFile(parquetFilePath);
      }

      if (config.glacier.removeSourceAfterSuccess) {
        try {
          await this.partitionCleaner.cleanSourceData(table, timeRange, parquetResult.rowCount);
          logger.info('历史库源数据清理完成');
        } catch (cleanErr) {
          logger.warn(`历史库源数据清理失败（已降级为补偿重试）: ${cleanErr instanceof Error ? cleanErr.message : String(cleanErr)}`);
        }
      }

      return { stub, rows: parquetResult.rowCount, bytes: ossResult.fileSizeBytes };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error(`${tableName} [${timeRange.start} ~ ${timeRange.end}] 处理失败: ${msg}`);
      if (parquetFilePath) {
        this.cleanupLocalFile(parquetFilePath);
      }
      return { rows: parquetResult?.rowCount ?? 0, bytes: 0, error: msg };
    }
  }

  private cleanupLocalFile(filePath: string): void {
    try {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
        logger.debug(`已清理本地 Parquet 文件: ${filePath}`);
      }
    } catch (err) {
      logger.warn(`清理本地 Parquet 文件失败: ${filePath}, err=${err instanceof Error ? err.message : String(err)}`);
    }
  }

  private emptyResult(startTime: Date): GlacierPipelineResult {
    return {
      processedCount: 0,
      succeededCount: 0,
      failedCount: 0,
      skippedCount: 0,
      totalRowsArchived: 0,
      totalBytesUploaded: 0,
      stubsCreated: 0,
      candidates: [],
      stubs: [],
      errors: [],
      startTime,
      endTime: new Date(),
    };
  }

  private logSummary(r: GlacierPipelineResult): void {
    const dur = (r.endTime.getTime() - r.startTime.getTime()) / 1000;
    logger.info('==============================');
    logger.info('极寒归档管道执行汇总');
    logger.info(`开始时间: ${r.startTime.toISOString()}`);
    logger.info(`结束时间: ${r.endTime.toISOString()}`);
    logger.info(`总耗时: ${dur.toFixed(2)} 秒`);
    logger.info(`候选分区: ${r.processedCount}`);
    logger.info(`成功: ${r.succeededCount}`);
    logger.info(`失败: ${r.failedCount}`);
    logger.info(`跳过: ${r.skippedCount}`);
    logger.info(`存根创建: ${r.stubsCreated}`);
    logger.info(`总行数: ${r.totalRowsArchived}`);
    logger.info(`总上传字节: ${(r.totalBytesUploaded / 1024 / 1024).toFixed(2)} MB`);
    if (r.errors.length > 0) {
      logger.info(`错误数: ${r.errors.length}`);
    }
    logger.info('==============================');
  }
}

export default GlacierOrchestrator;
