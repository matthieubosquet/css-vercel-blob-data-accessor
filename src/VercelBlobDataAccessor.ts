import { head, put, del } from '@vercel/blob';
import { Readable } from 'stream';
import type { Dir } from 'fs';

import { DataAccessor, FileIdentifierMapper, ResourceLink, Guarded, Representation, RepresentationMetadata, ResourceIdentifier, UnsupportedMediaTypeHttpError, NotFoundHttpError, isContainerIdentifier, isContainerPath, joinFilePath, CONTENT_TYPE_TERM, DC, IANA, LDP, POSIX, RDF, SOLID_META, XSD, addResourceMetadata, updateModifiedDate, guardStream, parseQuads, serializeQuads, parseContentType, toLiteral, toNamedTerm } from '@solid/community-server';
import { getLoggerFor } from 'global-logger-factory';
import { fim } from './fim';

export class VercelBlobDataAccessor implements DataAccessor {
    protected readonly logger = getLoggerFor(this);

    protected readonly resourceMapper: FileIdentifierMapper;

    // TODO: Configure instead of hardcoding
    public constructor(resourceMapper: FileIdentifierMapper) {
        //this.resourceMapper = resourceMapper;
        this.resourceMapper = new fim()
    }

    public async canHandle(representation: Representation): Promise<void> {
        this.logger.info("canHandle");
        if (!representation.binary) {
            throw new UnsupportedMediaTypeHttpError('Only binary data is supported.');
        }
    }

    public async getData(identifier: ResourceIdentifier): Promise<Guarded<Readable>> {
        this.logger.info("getData " + identifier.path);

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false);
        const stats = await this.getStats(link.filePath);

        if (stats.isFile()) {
            this.logger.info("is file");
            return guardStream(await this.createReadStream(link.filePath));
        }

        this.logger.info("not found");
        throw new NotFoundHttpError();
    }

    public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
        this.logger.info("getMetadata " + identifier.path);

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false);
        const stats = await this.getStats(link.filePath);

        if (!isContainerIdentifier(identifier) && stats.isFile()) {
            return this.getFileMetadata(link, stats);
        }

        if (isContainerIdentifier(identifier) && stats.isDirectory()) {
            return this.getDirectoryMetadata(link, stats);
        }

        this.logger.info("not found");
        throw new NotFoundHttpError();
    }

    public async* getChildren(identifier: ResourceIdentifier): AsyncIterableIterator<RepresentationMetadata> {
        this.logger.info("getChildren " + identifier.path);

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false);
        yield* this.getChildMetadata(link);
    }

    public async writeDocument(identifier: ResourceIdentifier, data: Guarded<Readable>, metadata: RepresentationMetadata):
        Promise<void> {
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

    private async getFileMetadata(link: ResourceLink, stats: { isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }): Promise<RepresentationMetadata> {
        this.logger.info("getFileMetadata " + link.identifier.path);

        const metadata = await this.getBaseMetadata(link, stats, false);
        // If the resource is using an unsupported contentType, the original contentType was written to the metadata file.
        // As a result, we should only set the contentType derived from the file path,
        // when no previous metadata entry for contentType is present.
        if (typeof metadata.contentType === 'undefined') {
            metadata.set(CONTENT_TYPE_TERM, link.contentType);
        }
        return metadata;
    }

    private async getBaseMetadata(link: ResourceLink, stats: { isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }, isContainer: boolean):
        Promise<RepresentationMetadata> {
        this.logger.info("getBaseMetadata " + link.identifier.path);

        const metadata = await this.getRawMetadata(link.identifier);
        addResourceMetadata(metadata, isContainer);
        this.addPosixMetadata(metadata, stats);
        return metadata;
    }
    private async getRawMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
        this.logger.info("getRawMetadata " + identifier.path);

        try {
            const metadataLink = await this.resourceMapper.mapUrlToFilePath(identifier, true);

            // Check if the metadata file exists first
            const stats = await this.lstat(metadataLink.filePath);

            const readMetadataStream = guardStream(await this.createReadStream(metadataLink.filePath));
            const quads = await parseQuads(
                readMetadataStream,
                { format: metadataLink.contentType, baseIRI: identifier.path },
            );
            const metadata = new RepresentationMetadata(identifier).addQuads(quads);

            // Already add modified date of metadata.
            // Final modified date should be max of data and metadata.
            updateModifiedDate(metadata, stats.mtime);

            return metadata;
        } catch (error: unknown) {
            this.logger.info("error " + error);

            // Metadata file doesn't exist so return empty metadata.
            if (!(error instanceof Error && error.message == "ENOENT")) {
                throw error;
            }
            return new RepresentationMetadata(identifier);
        }
    }
    private async* getChildMetadata(link: ResourceLink): AsyncIterableIterator<RepresentationMetadata> {
        this.logger.info("getChildMetadata " + link.identifier.path);

        const dir = await this.opendir(link.filePath);

        // For every child in the container we want to generate specific metadata
        for await (const entry of dir) {
            // Obtain details of the entry, resolving any symbolic links
            const childPath = joinFilePath(link.filePath, entry.name);
            let childStats;
            try {
                childStats = await this.getStats(childPath);
            } catch {
                // Skip this entry if details could not be retrieved (e.g., bad symbolic link)
                continue;
            }

            // Ignore non-file/directory entries in the folder
            if (!childStats.isFile() && !childStats.isDirectory()) {
                continue;
            }

            // Generate the URI corresponding to the child resource
            const childLink = await this.resourceMapper.mapFilePathToUrl(childPath, childStats.isDirectory());

            // Hide metadata files
            if (childLink.isMetadata) {
                continue;
            }

            // Generate metadata of this specific child as described in
            // https://solidproject.org/TR/2021/protocol-20211217#contained-resource-metadata
            const metadata = new RepresentationMetadata(childLink.identifier);
            addResourceMetadata(metadata, childStats.isDirectory());
            this.addPosixMetadata(metadata, childStats);
            // Containers will not have a content-type
            const { contentType, identifier } = childLink;
            if (contentType) {
                // Make sure we don't generate invalid URIs
                try {
                    const { value } = parseContentType(contentType);
                    metadata.add(RDF.terms.type, toNamedTerm(`${IANA.namespace}${value}#Resource`));
                } catch {
                    this.logger.warn(`Detected an invalid content-type "${contentType}" for ${identifier.path}`);
                }
            }

            yield metadata;
        }
    }

    private addPosixMetadata(metadata: RepresentationMetadata, stats: { isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }): void {
        this.logger.info("addPosixMetadata");

        // Make sure the last modified date is the max of data and metadata modified date
        const modified = new Date(metadata.get(DC.terms.modified)?.value ?? 0);
        if (modified < stats.mtime) {
            updateModifiedDate(metadata, stats.mtime);
        }
        metadata.add(
            POSIX.terms.mtime,
            toLiteral(Math.floor(stats.mtime.getTime() / 1000), XSD.terms.integer),
            SOLID_META.terms.ResponseMetadata,
        );
        if (!stats.isDirectory()) {
            metadata.add(POSIX.terms.size, toLiteral(stats.size, XSD.terms.integer), SOLID_META.terms.ResponseMetadata);
        }
    }
    private async getDirectoryMetadata(link: ResourceLink, stats: { isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }):
        Promise<RepresentationMetadata> {
        this.logger.info("getDirectoryMetadata " + link.identifier.path);

        return this.getBaseMetadata(link, stats, true);
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

    private async createReadStream(path: string): Promise<Readable> {
        this.logger.info("createReadStream " + path);

        var headResponse = await head(path, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
        var response = await fetch(headResponse.url);
        return Readable.fromWeb(response.body as any);;
    };

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

    private lstat(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
        this.logger.info("lstat " + path);

        return this.stat(path);
    }

    private opendir(path: string): Promise<Dir> {
        this.logger.info("opendir");

        throw new Error("Not implemented")
    }

    private async remove(dir: string): Promise<void> {
        this.logger.info("remove " + dir);

        await del(dir, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
    }

    private async stat(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
        this.logger.info("stat " + path);

        if (path.endsWith("/")) {
            this.logger.info("is folder");
            try {
                var headResponse = await head(path + ".FOLDER_MARKER_META_FILE", { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
                return {
                    isFile: () => false,
                    isDirectory: () => true,
                    mtime: headResponse.uploadedAt,
                    size: 0
                }
            } catch (e) {
                this.logger.info("error " + e);
                throw new Error("ENOENT")
            }
        } else {
            this.logger.info("is file");
            try {
                var headResponse = await head(path, { token: "vercel_blob_rw_M7axDeklTQ426rLR_RGYECRm0P4vN8MQYOZ2edlpw031Wsv" })
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

