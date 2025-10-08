import type { StatsBase } from 'fs';
import { Readable } from 'stream';

import { createReadStream, createWriteStream, ensureDir, lstat, opendir, remove, stat } from './VercelApi';

import { DataAccessor, FileIdentifierMapper, ResourceLink, Guarded, Representation, RepresentationMetadata, ResourceIdentifier, UnsupportedMediaTypeHttpError, isSystemError, NotFoundHttpError, isContainerIdentifier, isContainerPath, joinFilePath, CONTENT_TYPE_TERM, DC, IANA, LDP, POSIX, RDF, SOLID_META, XSD, addResourceMetadata, updateModifiedDate, guardStream, parseQuads, serializeQuads, parseContentType, toLiteral, toNamedTerm } from '@solid/community-server';
import { getLoggerFor } from 'global-logger-factory';

export class VercelBlobDataAccessor implements DataAccessor {
    protected readonly logger = getLoggerFor(this);

    protected readonly resourceMapper: FileIdentifierMapper;

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
            return guardStream(await createReadStream(link.filePath));
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
                await remove(metaLink.filePath);
            }
            throw error;
        }
    }

    public async writeContainer(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
        this.logger.info("writeContainer " + identifier.path);

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false);
        await ensureDir(link.filePath);

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
        await remove(metaLink.filePath);

        const link = await this.resourceMapper.mapUrlToFilePath(identifier, false);
        const stats = await this.getStats(link.filePath);

        if (!isContainerIdentifier(identifier) && stats.isFile()) {
            await remove(link.filePath);
        } else if (isContainerIdentifier(identifier) && stats.isDirectory()) {
            await remove(link.filePath);
        } else {
            throw new NotFoundHttpError();
        }
    }

    protected async getStats(path: string): Promise<{ isFile: () => boolean, isDirectory: () => boolean, mtime: Date, size: number }> {
        this.logger.info("getStats " + path);

        try {
            return await stat(path);
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
            const stats = await lstat(metadataLink.filePath);

            const readMetadataStream = guardStream(await createReadStream(metadataLink.filePath));
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

        const dir = await opendir(link.filePath);

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
            await remove(oldLink.filePath);
        }
    }

    protected async writeDataFile(path: string, data: Readable): Promise<void> {
        this.logger.info("writeDataFile " + path);


        await createWriteStream(path, data);
        // return new Promise((resolve, reject): void => {
        //   const writeStream = createWriteStream(path);
        //   data.pipe(writeStream);
        //   data.on('error', (error): void => {
        //     reject(error);
        //     writeStream.end();
        //   });

        //   writeStream.on('error', reject);
        //   writeStream.on('finish', resolve);
        // });
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
            await remove(metadataLink.filePath);
            wroteMetadata = false;
        }
        return wroteMetadata;
    }
}

class fim implements FileIdentifierMapper {
    public mapFilePathToUrl(filePath: string, isContainer: boolean): Promise<ResourceLink> {
        throw new Error("fim mapFilePathToUrl not implemented")
    }
    public mapUrlToFilePath(identifier: ResourceIdentifier, isMetadata: boolean, contentType?: string): Promise<ResourceLink> {
        const a = identifier.path
        const b = new URL(a)
        const c = b.pathname
        const d = c + (isMetadata ? ".meta" : "")
        const e = "x" + d

        return Promise.resolve({
            identifier: identifier,
            filePath: e,
            isMetadata: isMetadata
        })
    }
}
