/**
 * Internal RPC/transport layer.
 *
 * A socket (Unix domain socket or Windows named pipe) carries length-prefixed
 * canonical-JSON frames. The handshake is performed at the raw frame level;
 * once authenticated, the same socket is upgraded to a birpc channel for
 * request/response dispatch. birpc is hidden behind this adapter and never part
 * of the public interface.
 */
import type { Socket } from "node:net";
import { type BirpcReturn } from "../vendor/birpc/main.js";
import { type CanonicalObject } from "./framing.js";
export type LocalFunctions = Record<string, (...args: any[]) => unknown>;
export type RemoteFunctions = Record<string, (...args: any[]) => unknown>;
export type FrameHandler = (frame: CanonicalObject) => void;
/**
 * Raw framed channel over a socket. Any number of frame handlers may register;
 * each decoded frame is dispatched to all current handlers. Used for the
 * handshake exchange, then a birpc message handler is registered on upgrade.
 */
export declare class RawChannel {
    private handlers;
    private destroyedFlag;
    private socket;
    private onCloseCallback;
    private closeDelivered;
    private pendingBytes;
    private readonly maxPendingBytes;
    constructor(socket: Socket, onProtocolError: (error: Error) => void, onClose?: (error?: Error) => void);
    addHandler(fn: FrameHandler): void;
    removeHandler(fn: FrameHandler): void;
    onClose(fn: (error?: Error) => void): void;
    send(obj: CanonicalObject): boolean;
    destroy(error?: Error): void;
    get destroyed(): boolean;
    private handleClose;
    private deliverClose;
}
export interface RpcPeer<R extends RemoteFunctions = RemoteFunctions, L extends LocalFunctions = LocalFunctions> {
    rpc: BirpcReturn<R, L, true>;
    close(error?: Error): void;
    readonly closed: boolean;
}
/**
 * Create a birpc peer over an already-authenticated raw channel.
 * birpc's message handler is registered as a frame handler on the channel.
 */
export declare function createRpcPeer<R extends RemoteFunctions, L extends LocalFunctions>(raw: RawChannel, localFunctions: L): RpcPeer<R, L>;
