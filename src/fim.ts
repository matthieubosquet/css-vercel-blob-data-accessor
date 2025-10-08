import { FileIdentifierMapper, ResourceLink, ResourceIdentifier } from '@solid/community-server';

export class fim implements FileIdentifierMapper {
    public mapFilePathToUrl(filePath: string, isContainer: boolean): Promise<ResourceLink> {
        throw new Error("fim mapFilePathToUrl not implemented");
    }
    public mapUrlToFilePath(identifier: ResourceIdentifier, isMetadata: boolean, contentType?: string): Promise<ResourceLink> {
        const a = identifier.path;
        const b = new URL(a);
        const c = b.pathname;
        const d = c + (isMetadata ? ".meta" : "");
        const e = "x" + d;

        return Promise.resolve({
            identifier: identifier,
            filePath: e,
            isMetadata: isMetadata
        });
    }
}
