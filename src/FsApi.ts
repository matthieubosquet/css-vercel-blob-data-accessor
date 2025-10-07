import { createReadStream as fsCreateReadStream, createWriteStream as fsCreateWriteStream, ensureDir as fsEnsureDir, lstat as fsLstat, opendir as fsOpendir, remove as fsRemove, stat as fsStat } from "fs-extra";
import type { Readable } from 'stream';
import type { Writable } from 'stream';
import type { Dir, StatsBase} from 'fs';

import { getLoggerFor } from 'global-logger-factory';


const logger = getLoggerFor("fs API");

export function createReadStream(path: string): Readable {
    logger.info("createReadStream");

    return fsCreateReadStream(path);
};

export function createWriteStream(path: string): Writable {
    logger.info("createWriteStream");

    return fsCreateWriteStream(path);
}

export function ensureDir(path: string): Promise<void> {
    logger.info("ensureDir");

    return fsEnsureDir(path);
}

export function lstat(path: string): Promise<StatsBase<number>> {
    logger.info("lstat");

    return fsLstat(path);
}

export function opendir(path: string): Promise<Dir> {
    logger.info("opendir");

    return fsOpendir(path);
}

export function remove(dir: string): Promise<void> {
    logger.info("remove");

    return fsRemove(dir);
}

export function stat(path: string): Promise<StatsBase<number>> {
    logger.info("stat");

    return fsStat(path);
}
