import type { FileIdentifierMapper, ResourceLink, ResourceIdentifier } from '@solid/community-server';

import { BadRequestHttpError, decodeUriPathComponents, NotFoundHttpError, trimTrailingSlashes } from '@solid/community-server';
import { getLoggerFor } from 'global-logger-factory';

export class BlobIdentifierMapper implements FileIdentifierMapper {
    private readonly metadataSuffix = '.meta';

    protected readonly logger = getLoggerFor(this);
    protected readonly baseRequestURI: string;

    public constructor(base: string) {
        this.baseRequestURI = trimTrailingSlashes(base);
    }

    public mapFilePathToUrl(filePath: string, isContainer: boolean): Promise<ResourceLink> {
        throw new Error("fim mapFilePathToUrl not implemented");
    }

    public async mapUrlToFilePath(identifier: ResourceIdentifier, isMetadata: boolean, contentType?: string): Promise<ResourceLink> {
        let path = this.getRelativePath(identifier);

        if (isMetadata) {
            path += this.metadataSuffix;
        }

        this.validateRelativePath(path, identifier);

        this.logger.debug(`The blob resource for ${identifier.path} is ${path}`);
        return { identifier, filePath: path, contentType, isMetadata: path.endsWith(this.metadataSuffix) };
    }

    private getRelativePath(identifier: ResourceIdentifier): string {
        if (!identifier.path.startsWith(this.baseRequestURI)) {
            this.logger.warn(`The URL ${identifier.path} is outside of the scope ${this.baseRequestURI}`);
            throw new NotFoundHttpError();
        }
        return decodeUriPathComponents(identifier.path.slice(this.baseRequestURI.length));
    }

    private validateRelativePath(path: string, identifier: ResourceIdentifier): void {
        if (!path.startsWith('/')) {
            this.logger.warn(`URL ${identifier.path} needs a / after the base`);
            throw new BadRequestHttpError('URL needs a / after the base');
        }

        if (path.includes('/../') || path.endsWith('/..')) {
            this.logger.warn(`Disallowed /../ segment in URL ${identifier.path}.`);
            throw new BadRequestHttpError('Disallowed /../ segment in URL');
        }
    }
}
