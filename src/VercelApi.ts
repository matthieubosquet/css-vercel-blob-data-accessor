import { head, put, del } from '@vercel/blob';

import { createWriteStream as fsCreateWriteStream, ensureDir as fsEnsureDir, lstat as fsLstat, opendir as fsOpendir, remove as fsRemove, stat as fsStat } from "fs-extra";
import { Readable } from 'stream';
import type { Dir } from 'fs';
import { getLoggerFor } from 'global-logger-factory';


const logger = getLoggerFor("Vercel API");

// Thrown errors must get throught checks like `if (isSystemError(error) && error.code === 'ENOENT')`
export async function createReadStream(path: string): Promise<Readable> {
    logger.info("createReadStream " + path);
    try {
        var headResponse = await head(path, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
        var response = await fetch(headResponse.url);
        return Readable.fromWeb(response.body as any);;
    } catch (e) {
        logger.info("error " + e);
        
        // Almost certain we should throw in this case
        // like throw new Error("ENOENT")
        return new Readable()
    }
};

export async function createWriteStream(path: string, data: Readable): Promise<void> {
    logger.info("createWriteStream");
    var a = Readable.toWeb(data);
    await put(path, a, { allowOverwrite: true, access: "public", token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" });
}

export async function ensureDir(path: string): Promise<void> {
    logger.info("ensureDir " + path);

    const statResponse = await stat(path)
    if (!statResponse.isDirectory()) {
        logger.info("ensureDir not a directory yet, writing folder marker file " + path);

        await put(path + ".FOLDER_MARKER_META_FILE", "nothing here", { allowOverwrite: true, access: "public", token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" });
    }
}

// I think there's no difference between stat and lstat. lstat is something with symlinks
export function lstat(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
    logger.info("lstat " + path);

    return stat(path);
}

export function opendir(path: string): Promise<Dir> {
    logger.info("opendir");

    return fsOpendir(path);
}

export async function remove(dir: string): Promise<void> {
    logger.info("remove " + dir);

    await del(dir, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
}

export async function stat(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
    logger.info("stat " + path);

    if (path.endsWith("/")) {
        logger.info("folder");
        try {
            var headResponse = await head(path + ".FOLDER_MARKER_META_FILE", { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
            return {
                isFile: () => false,
                isDirectory: () => true,
                mtime: headResponse.uploadedAt,
                size: 0
            }
        } catch (e) {
            logger.info("error " + e);
            throw new Error("ENOENT")
        }
    } else {
        logger.info("file");
        try {
            var headResponse = await head(path, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
            return {
                isFile: () => !headResponse.pathname.endsWith("/"),
                isDirectory: () => headResponse.pathname.endsWith("/"),
                mtime: headResponse.uploadedAt,
                size: headResponse.size
            }
        } catch (e) {
            logger.info("error " + e);
            throw new Error("ENOENT")
        }
    }
}
