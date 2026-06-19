import winston from 'winston';
import path from 'path';
import { config } from '../config';

const logFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
  winston.format.errors({ stack: true }),
  winston.format.printf(({ timestamp, level, message, stack }) => {
    if (stack) {
      return `[${timestamp}] [${level.toUpperCase()}] ${message}\n${stack}`;
    }
    return `[${timestamp}] [${level.toUpperCase()}] ${message}`;
  })
);

export const logger = winston.createLogger({
  level: config.log.level,
  format: logFormat,
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(winston.format.colorize(), logFormat),
    }),
    new winston.transports.File({
      filename: path.join(config.log.dir, 'error.log'),
      level: 'error',
      maxsize: 50 * 1024 * 1024,
      maxFiles: 5,
    }),
    new winston.transports.File({
      filename: path.join(config.log.dir, 'combined.log'),
      maxsize: 50 * 1024 * 1024,
      maxFiles: 10,
    }),
  ],
});

export default logger;
