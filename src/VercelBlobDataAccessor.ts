import { head, put, del, list } from '@vercel/blob';
import { Readable } from 'stream';
import type { Dir } from 'fs';

import { DataAccessor, FileIdentifierMapper, ResourceLink, Guarded, Representation, RepresentationMetadata, ResourceIdentifier, UnsupportedMediaTypeHttpError, NotFoundHttpError, isContainerIdentifier, isContainerPath, joinFilePath, CONTENT_TYPE_TERM, DC, IANA, LDP, POSIX, RDF, SOLID_META, XSD, addResourceMetadata, updateModifiedDate, guardStream, parseQuads, serializeQuads, parseContentType, toLiteral, toNamedTerm } from '@solid/community-server';
import { getLoggerFor } from 'global-logger-factory';

import type { BlobIdentifierMapper } from './BlobIdentifierMapper.js';

export class VercelBlobDataAccessor implements DataAccessor {
    protected readonly logger = getLoggerFor(this);

    protected readonly resourceMapper: FileIdentifierMapper;

    public constructor(resourceMapper: BlobIdentifierMapper) {
        this.resourceMapper = resourceMapper;
    }

    public async canHandle(representation: Representation): Promise<void> {
        this.logger.info("canHandle");
        if (!representation.binary) {
            throw new UnsupportedMediaTypeHttpError('Only binary data is supported.');
        }
    }

    public async getData(identifier: ResourceIdentifier): Promise<Guarded<Readable>> {
        this.logger.info("getData: " + identifier.path);

        if (!isContainerIdentifier(identifier)) {
            var headResponse = await head(await this.getBlobIdentifier(identifier), { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" });
            var response = await fetch(headResponse.url);

            return guardStream(Readable.from(response.body as any));
        }

        this.logger.info("getData: Not Found");
        throw new NotFoundHttpError();
    }

    public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
        this.logger.info("getMetadata " + identifier.path);
        const metadata = new RepresentationMetadata(identifier);

        try {
            var headResponse = await head(await this.getBlobIdentifier(identifier), { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" });

            // Make sure the last modified date is the max of data and metadata modified date
            const modified = new Date(metadata.get(DC.terms.modified)?.value ?? 0);
            if (modified < headResponse.uploadedAt) {
                updateModifiedDate(metadata, headResponse.uploadedAt);
            }

            metadata.add(
                POSIX.terms.mtime,
                toLiteral(Math.floor(headResponse.uploadedAt.getTime() / 1000), XSD.terms.integer),
                SOLID_META.terms.ResponseMetadata,
            );

            if (!isContainerIdentifier(identifier)) {
                metadata.add(POSIX.terms.size, toLiteral(headResponse.size, XSD.terms.integer), SOLID_META.terms.ResponseMetadata);
            }

            metadata.set(CONTENT_TYPE_TERM, headResponse.contentType);

            return metadata;
        } catch(e) {
            this.logger.info("getMetadata: error "+ e)
            this.logger.info("getMetadata: is container identifier" + isContainerIdentifier(identifier) + identifier.path)
            throw new NotFoundHttpError();
        }
    }

    public async* getChildren(identifier: ResourceIdentifier): AsyncIterableIterator<RepresentationMetadata> {
        this.logger.info("getChildren " + identifier.path);

        const prefix = await this.getBlobIdentifier(identifier);

        const listResponse = await list({ prefix, token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })

        // Map Blobs to direct children
        const children = listResponse.blobs.map(blob => {
            const relativePath = blob.pathname.slice(prefix.length);
            const slashIndex = relativePath.indexOf('/');
            
            // Files
            if (slashIndex === -1) {
                return relativePath;
            }
            // Subfolders
            return relativePath.slice(0, slashIndex + 1);
        }).map(path => new RepresentationMetadata({path}));

        yield* children;
    }

    public async writeDocument(identifier: ResourceIdentifier, data: Guarded<Readable>, metadata: RepresentationMetadata): Promise<void> {
        this.logger.info("writeDocument " + identifier.path)

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false, metadata.contentType);

        // Check if we already have a corresponding file with a different extension
        await this.verifyExistingExtension(link);

        const wroteMetadata = await this.writeMetadataFile(link, metadata);

        try {
            await this.writeDataFile(link.filePath, data);
        } catch (error: unknown) {
            this.logger.info("error " + error)
            // Delete the metadata if there was an error writing the file
            if (wroteMetadata) {
                this.logger.info("writeMetadata")
                const metaLink = await this.resourceMapper.mapUrlToFilePath(identifier, true);
                await this.remove(metaLink.filePath);
            }
            throw error;
        }
    }

    public async writeContainer(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
        this.logger.info("writeContainer " + identifier.path);

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false);
        await this.ensureDir(link.filePath);

        await this.writeMetadataFile(link, metadata);
    }

    public async writeMetadata(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
        this.logger.info("writeMetadata " + identifier.path);

        const metadataLink = await this.resourceMapper.mapUrlToFilePath(identifier, true);
        await this.writeMetadataFile(metadataLink, metadata);
    }

    public async deleteResource(identifier: ResourceIdentifier): Promise<void> {
        this.logger.info("deleteResource" + identifier.path);

        const metaLink = await this.resourceMapper.mapUrlToFilePath(identifier, true);
        await this.remove(metaLink.filePath);

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false);
        const stats = await this.getStats(link.filePath);

        if (!isContainerIdentifier(identifier) && stats.isFile()) {
            await this.remove(link.filePath);
        } else if (isContainerIdentifier(identifier) && stats.isDirectory()) {
            await this.remove(link.filePath);
        } else {
            throw new NotFoundHttpError();
        }
    }

    private async getBlobIdentifier(identifier: ResourceIdentifier): Promise<string> {
        return (await this.resourceMapper.mapUrlToFilePath(identifier, false)).filePath.substring(1);
    }

    protected async getStats(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
        this.logger.info("getStats " + path);

        try {
            return await this.stat(path);
        } catch (error: unknown) {
            this.logger.info("getStats - Catch " + error);
            if (error instanceof Error && error.message == "ENOENT") {
                throw new NotFoundHttpError('', { cause: error });
            }
            this.logger.info("getStats - not system error");
            throw error;
        }
    }

    protected async verifyExistingExtension(link: ResourceLink): Promise<void> {
        this.logger.info("verifyExistingExtension " + link.identifier.path);

        // Delete the old file with the (now) wrong extension
        const oldLink = await this.resourceMapper.mapUrlToFilePath(link.identifier, false);
        if (oldLink.filePath !== link.filePath) {
            this.logger.info("oldLink.filePath !== link.filePath");
            await this.remove(oldLink.filePath);
        }
    }

    protected async writeDataFile(path: string, data: Readable): Promise<void> {
        this.logger.info("writeDataFile " + path);

        await this.createWriteStream(path, data);
    }

    protected async writeMetadataFile(link: ResourceLink, metadata: RepresentationMetadata): Promise<boolean> {
        this.logger.info("writeMetadataFile " + link.identifier.path);

        // These are stored by file system conventions
        metadata.remove(RDF.terms.type, LDP.terms.Resource);
        metadata.remove(RDF.terms.type, LDP.terms.Container);
        metadata.remove(RDF.terms.type, LDP.terms.BasicContainer);
        metadata.removeAll(DC.terms.modified);
        // When writing metadata for a document, only remove the content-type when dealing with a supported media type.
        // A media type is supported if the FileIdentifierMapper can correctly store it.
        // This allows restoring the appropriate content-type on data read (see getFileMetadata).
        // The FileIdentifierMapper defaults to octet stream in case it can't determine the content-type,
        // so we have to check if the link content-type is the same as the one in the metadata.
        if (isContainerPath(link.filePath) || link.contentType === metadata.contentType) {
            metadata.removeAll(CONTENT_TYPE_TERM);
        }
        const quads = metadata.quads();
        const metadataLink = await this.resourceMapper.mapUrlToFilePath(link.identifier, true);
        let wroteMetadata: boolean;

        // Write metadata to file if there are quads remaining
        if (quads.length > 0) {
            // Determine required content-type based on mapper
            const serializedMetadata = serializeQuads(quads, metadataLink.contentType);
            await this.writeDataFile(metadataLink.filePath, serializedMetadata);
            wroteMetadata = true;

            // Delete (potentially) existing metadata file if no metadata needs to be stored
        } else {
            await this.remove(metadataLink.filePath);
            wroteMetadata = false;
        }
        return wroteMetadata;
    }

    private async createWriteStream(path: string, data: Readable): Promise<void> {
        this.logger.info("createWriteStream " + path);

        await put(path, data, { allowOverwrite: true, access: "public", token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" });
    }

    private async ensureDir(path: string): Promise<void> {
        this.logger.info("ensureDir " + path);

        try {
            await this.stat(path)
        } catch (error: unknown) {
            if (error instanceof Error && error.message == "ENOENT") {
                this.logger.info("ensureDir not a directory yet, writing folder marker file " + path);
                await put(path + ".FOLDER_MARKER_META_FILE", "nothing here", { allowOverwrite: true, access: "public", token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" });
            } else {
                throw error
            }
        }
    }
    
    private async remove(dir: string): Promise<void> {
        this.logger.info("remove " + dir);

        await del(dir, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
    }

    private async stat(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
        this.logger.info("stat " + path);

        if (path.endsWith("/")) {
            this.logger.info("is folder " + path);
            try {
                //var headResponse = await head(path + ".FOLDER_MARKER_META_FILE", { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
                var listResponse = await list({ prefix: path, token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
                return {
                    isFile: () => false,
                    isDirectory: () => true,
                    mtime: new Date,
                    size: 0
                }
            } catch (e) {
                this.logger.info("error " + e);
                throw new Error("ENOENT")
            }
        } else {
            this.logger.info("is file " + path.substring(1));
            try {
                var headResponse = await head(path.substring(1), { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
                return {
                    isFile: () => !headResponse.pathname.endsWith("/"),
                    isDirectory: () => headResponse.pathname.endsWith("/"),
                    mtime: headResponse.uploadedAt,
                    size: headResponse.size
                }
            } catch (e) {
                this.logger.info("error " + e);
                throw new Error("ENOENT")
            }
        }
    }
}

