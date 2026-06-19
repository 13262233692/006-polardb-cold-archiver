import fs from 'fs';
import path from 'path';
import crypto from 'crypto';
import { config, OSSConfig } from '../config';
import { OSSShardResult } from '../types';
import { logger } from '../utils/logger';

interface OSSClientLike {
  initMultipartUpload(name: string, options?: unknown): Promise<{ uploadId: string; name: string }>;
  uploadPart(name: string, uploadId: string, partNo: number, data: unknown, options?: unknown): Promise<{ eTag: string; partNumber?: number }>;
  completeMultipartUpload(name: string, uploadId: string, parts: Array<{ eTag: string; partNumber: number }>, options?: unknown): Promise<{ eTag: string; bucket: string; name: string }>;
  abortMultipartUpload(name: string, uploadId: string, options?: unknown): Promise<unknown>;
}

interface PartEtag {
  partNumber: number;
  eTag: string;
}

export class OSSUploader {
  private ossConfig: OSSConfig;
  private client: OSSClientLike | null = null;

  constructor() {
    this.ossConfig = config.oss;
  }

  private async getClient(): Promise<OSSClientLike> {
    if (this.client) return this.client;
    if (!this.ossConfig.enabled || !this.ossConfig.accessKeyId || !this.ossConfig.accessKeySecret || !this.ossConfig.bucket) {
      throw new Error('OSS 未正确配置，请检查 .env 中的 OSS_* 变量');
    }
    const OSS = require('ali-oss').Wrapper || require('ali-oss');
    this.client = new OSS({
      region: this.ossConfig.region,
      accessKeyId: this.ossConfig.accessKeyId,
      accessKeySecret: this.ossConfig.accessKeySecret,
      bucket: this.ossConfig.bucket,
      endpoint: this.ossConfig.endpoint,
      secure: this.ossConfig.secure,
    }) as OSSClientLike;
    logger.info(`[OSSUploader] OSS 客户端已初始化，region=${this.ossConfig.region}, bucket=${this.ossConfig.bucket}`);
    return this.client;
  }

  generateObjectKey(fileName: string): string {
    const date = new Date();
    const ymd = `${date.getFullYear()}/${String(date.getMonth() + 1).padStart(2, '0')}/${String(date.getDate()).padStart(2, '0')}`;
    return `${this.ossConfig.prefix}/${ymd}/${fileName}`;
  }

  async uploadFile(filePath: string, objectKey?: string): Promise<OSSShardResult> {
    const startTime = Date.now();
    const stats = fs.statSync(filePath);
    const fileName = path.basename(filePath);
    const finalKey = objectKey || this.generateObjectKey(fileName);

    logger.info(
      `[OSSUploader] 开始分片上传: ${filePath} -> ${finalKey}, size=${(stats.size / 1024 / 1024).toFixed(2)} MB`
    );

    const fileSize = stats.size;
    const shardSizeBytes = this.ossConfig.shardSizeMb * 1024 * 1024;
    const partCount = Math.max(1, Math.ceil(fileSize / shardSizeBytes));
    const client = await this.getClient();

    const initResult = await client.initMultipartUpload(finalKey);
    const uploadId = initResult.uploadId;
    logger.debug(`[OSSUploader] 初始化分片上传成功: uploadId=${uploadId}, partCount=${partCount}`);

    const etagParts: PartEtag[] = [];
    const concurrency = this.ossConfig.concurrency;
    let cursor = 0;
    const errors: Error[] = [];

    try {
      while (cursor < partCount && errors.length === 0) {
        const batch: Array<{ partNo: number; start: number; end: number }> = [];
        for (let i = 0; i < concurrency && cursor < partCount; i++) {
          const start = cursor * shardSizeBytes;
          const end = Math.min(start + shardSizeBytes, fileSize);
          batch.push({ partNo: cursor + 1, start, end });
          cursor++;
        }

        await Promise.all(batch.map(async ({ partNo, start, end }) => {
          let retries = 0;
          let lastErr: Error | null = null;
          while (retries < this.ossConfig.maxRetries) {
            try {
              const buf = this.readFileSlice(filePath, start, end);
              const partResult = await client.uploadPart(finalKey, uploadId, partNo, buf);
              etagParts.push({ partNumber: partResult.partNumber ?? partNo, eTag: partResult.eTag });
              logger.debug(`[OSSUploader] Part ${partNo} 上传成功, size=${end - start} bytes`);
              return;
            } catch (err) {
              lastErr = err as Error;
              retries++;
              logger.warn(
                `[OSSUploader] Part ${partNo} 上传失败 (${retries}/${this.ossConfig.maxRetries}): ${lastErr.message}`
              );
              await this.sleep(Math.pow(2, retries) * 1000);
            }
          }
          if (lastErr) errors.push(lastErr);
        }));
      }

      if (errors.length > 0) {
        throw new Error(`分片上传失败: ${errors.map(e => e.message).join('; ')}`);
      }

      etagParts.sort((a, b) => a.partNumber - b.partNumber);
      const complete = await client.completeMultipartUpload(finalKey, uploadId, etagParts);
      logger.info(
        `[OSSUploader] 上传完成: ${finalKey}, etag=${complete.eTag}, parts=${etagParts.length}, ` +
        `耗时=${Date.now() - startTime}ms`
      );

      return {
        uploadId,
        objectKey: finalKey,
        bucket: this.ossConfig.bucket,
        eTag: complete.eTag,
        fileSizeBytes: fileSize,
        partCount: etagParts.length,
        shardSizeMb: this.ossConfig.shardSizeMb,
        durationMs: Date.now() - startTime,
        etagParts,
      };
    } catch (err) {
      logger.error(`[OSSUploader] 上传失败，中止分片上传...: ${err instanceof Error ? err.message : String(err)}`);
      try {
        await client.abortMultipartUpload(finalKey, uploadId);
      } catch (abortErr) {
        logger.warn(`[OSSUploader] 中止分片上传失败: ${abortErr instanceof Error ? abortErr.message : String(abortErr)}`);
      }
      throw err;
    }
  }

  computeFileChecksum(filePath: string): string {
    const hash = crypto.createHash('md5');
    const fd = fs.openSync(filePath, 'r');
    const buf = Buffer.alloc(64 * 1024);
    let read: number;
    while ((read = fs.readSync(fd, buf, 0, buf.length, null)) > 0) {
      hash.update(buf.slice(0, read));
    }
    fs.closeSync(fd);
    return hash.digest('hex');
  }

  private readFileSlice(filePath: string, start: number, end: number): Buffer {
    const len = end - start;
    const buf = Buffer.alloc(len);
    const fd = fs.openSync(filePath, 'r');
    fs.readSync(fd, buf, 0, len, start);
    fs.closeSync(fd);
    return buf;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

export default OSSUploader;
