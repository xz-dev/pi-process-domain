import type { Router } from "zeromq";
import type { ProcessDomainTransport } from "./types.js";
export interface BoundEndpoint {
    readonly transport: ProcessDomainTransport;
    readonly endpoint: string;
}
export declare function preferredTransport(): ProcessDomainTransport;
export declare function wildcardEndpoint(transport?: ProcessDomainTransport): string;
export declare function bindTemporaryEndpoint(socket: Router, transport?: ProcessDomainTransport): Promise<BoundEndpoint>;
