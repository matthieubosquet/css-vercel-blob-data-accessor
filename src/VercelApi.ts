import { head, put } from '@vercel/blob';

import { createWriteStream as fsCreateWriteStream, ensureDir as fsEnsureDir, lstat as fsLstat, opendir as fsOpendir, remove as fsRemove, stat as fsStat } from "fs-extra";
import  { Readable } from 'stream';
import type { Writable } from 'stream';
import type { Dir, StatsBase} from 'fs';

import { getLoggerFor } from 'global-logger-factory';


const logger = getLoggerFor("Vercel API");

export async function createReadStream(path: string): Promise<Readable> {
    logger.info("createReadStream");
    logger.info(path);
    try {
        var x = await head(path, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
    } catch(e) {
        return new Readable()
    }
    var response = await fetch(x.url);

    return Readable.fromWeb(response.body as any);;
};

export async function createWriteStream(path: string, data: Readable): Promise<void> {
    logger.info("createWriteStream");
    var a = Readable.toWeb(data);
    await put(path, a, { access: "public", token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" });
}

export function ensureDir(path: string): Promise<void> {
    logger.info("ensureDir");
    return Promise.resolve();
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

    return Promise.resolve();
}

export async function stat(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
    logger.info("stat");
    try {
        var x = await head(path, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
        return {
            isFile: () => !x.pathname.endsWith("/"),
            isDirectory: () => x.pathname.endsWith("/"),
            mtime: x.uploadedAt,
            size: x.size
        }
    } catch(e) {
        return {
            isFile: () => false,
            isDirectory: () => false,
            mtime: new Date(),
            size: 0
        }
    }
}
