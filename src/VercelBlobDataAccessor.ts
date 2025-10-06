import { DataAccessor, Guarded, Representation, RepresentationMetadata, ResourceIdentifier } from '@solid/community-server';
import { getLoggerFor } from 'global-logger-factory';
import { Readable } from 'stream';

export class VercelBlobDataAccessor implements DataAccessor {
  protected readonly logger = getLoggerFor(this);

  constructor(resourceMapper: any) {
    this.logger.info("bla")
  }
  
  public async canHandle(representation: Representation): Promise<void> {
    throw new Error("CAN HANDLE");
  }
  
  public async getData(identifier: ResourceIdentifier): Promise<Guarded<Readable>> {
    throw new Error("GET DATA");
  }

  public async getMetadata(identifier: ResourceIdentifier): Promise<RepresentationMetadata> {
    throw new Error("GET METADATA".concat(identifier.path));
  }
  
  public async* getChildren(identifier: ResourceIdentifier): AsyncIterableIterator<RepresentationMetadata> {
    throw new Error("GET CHILDREN");
  }

  public async writeDocument(identifier: ResourceIdentifier, data: Guarded<Readable>, metadata: RepresentationMetadata):
  Promise<void> {
    throw new Error("WRITE DOCUMENT");
  }

  public async writeContainer(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
    throw new Error("WRITE CONTAINER");
  }

  public async writeMetadata(identifier: ResourceIdentifier, metadata: RepresentationMetadata): Promise<void> {
    throw new Error("WRITE METADATA");
  }

  public async deleteResource(identifier: ResourceIdentifier): Promise<void> {
    throw new Error("DELETE RESOURCE");
  }
}
