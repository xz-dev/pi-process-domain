import { type OpenProcessDomainOptions, type PiLifecycleExtensionApi, type ProcessDomainNode } from "./types.js";
export * from "./types.js";
export { ENV_NAMES } from "./protocol.js";
export { preferredTransport, wildcardEndpoint } from "./endpoint.js";
export declare function openProcessDomain(options?: OpenProcessDomainOptions): Promise<ProcessDomainNode>;
export declare function attachPiLifecycle(node: ProcessDomainNode, pi: PiLifecycleExtensionApi, sessionId?: string): {
    close(): void;
};
