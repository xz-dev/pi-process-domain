/**
 * Vendored from antfu-collective/birpc at reviewed commit 6b891740de348a82c69dbd38310d1ec822c7640b (v4.0.0).
 * MIT License, Copyright (c) 2021 Anthony Fu <https://github.com/antfu>.
 * Distributed under the MIT License. See THIRD_PARTY_NOTICES.md at repository root.
 * This file matches that commit except for one mechanical change: relative import paths
 * were given explicit .js extensions for NodeNext module resolution. It is
 * imported only as an internal RPC dispatcher; it is not part of the public interface.
 */
import type { RpcRequest } from './messages.js';
import type { ArgumentsType, ReturnType, Thenable } from './utils.js';
export type PromisifyFn<T> = ReturnType<T> extends Promise<any> ? T : (...args: ArgumentsType<T>) => Promise<Awaited<ReturnType<T>>>;
export type BirpcResolver<This> = (this: This, name: string, resolved: (...args: unknown[]) => unknown) => Thenable<((...args: any[]) => any) | undefined>;
export interface ChannelOptions {
    /**
     * Function to post raw message
     */
    post: (data: any, ...extras: any[]) => Thenable<any>;
    /**
     * Listener to receive raw message
     */
    on: (fn: (data: any, ...extras: any[]) => void) => Thenable<any>;
    /**
     * Clear the listener when `$close` is called
     */
    off?: (fn: (data: any, ...extras: any[]) => void) => Thenable<any>;
    /**
     * Custom function to serialize data
     *
     * by default it passes the data as-is
     */
    serialize?: (data: any) => any;
    /**
     * Custom function to deserialize data
     *
     * by default it passes the data as-is
     */
    deserialize?: (data: any) => any;
    /**
     * Call the methods with the RPC context or the original functions object
     */
    bind?: 'rpc' | 'functions';
    /**
     * Custom meta data to attached to the RPC instance's `$meta` property
     */
    meta?: any;
}
export interface EventOptions<RemoteFunctions extends object = Record<string, unknown>, LocalFunctions extends object = Record<string, unknown>, Proxify extends boolean = true> {
    /**
     * Names of remote functions that do not need response.
     */
    eventNames?: (keyof RemoteFunctions)[];
    /**
     * Maximum timeout for waiting for response, in milliseconds.
     *
     * @default 60_000
     */
    timeout?: number;
    /**
     * Whether to proxy the remote functions.
     *
     * When `proxify` is false, calling the remote function
     * with `rpc.$call('method', ...args)` instead of `rpc.method(...args)`
     * explicitly is required.
     *
     * @default true
     */
    proxify?: Proxify;
    /**
     * Custom resolver to resolve function to be called
     *
     * For advanced use cases only
     */
    resolver?: BirpcResolver<BirpcReturn<RemoteFunctions, LocalFunctions, Proxify>>;
    /**
     * Hook triggered before an event is sent to the remote
     *
     * @param req - Request parameters
     * @param next - Function to continue the request
     * @param resolve - Function to resolve the response directly
     */
    onRequest?: (this: BirpcReturn<RemoteFunctions, LocalFunctions, Proxify>, req: RpcRequest, next: (req?: RpcRequest) => Promise<any>, resolve: (res: any) => void) => void | Promise<void>;
    /**
     * Custom error handler for errors occurred in local functions being called
     *
     * @returns `true` to prevent the error from being thrown
     */
    onFunctionError?: (this: BirpcReturn<RemoteFunctions, LocalFunctions, Proxify>, error: Error, functionName: string, args: any[]) => boolean | void;
    /**
     * Custom error handler for errors occurred during serialization or messsaging
     *
     * @returns `true` to prevent the error from being thrown
     */
    onGeneralError?: (this: BirpcReturn<RemoteFunctions, LocalFunctions, Proxify>, error: Error, functionName?: string, args?: any[]) => boolean | void;
    /**
     * Custom error handler for timeouts
     *
     * @returns `true` to prevent the error from being thrown
     */
    onTimeoutError?: (this: BirpcReturn<RemoteFunctions, LocalFunctions, Proxify>, functionName: string, args: any[]) => boolean | void;
}
export type BirpcOptions<RemoteFunctions extends object = Record<string, unknown>, LocalFunctions extends object = Record<string, unknown>, Proxify extends boolean = true> = EventOptions<RemoteFunctions, LocalFunctions, Proxify> & ChannelOptions;
export type BirpcFn<T> = PromisifyFn<T> & {
    /**
     * Send event without asking for response
     */
    asEvent: (...args: ArgumentsType<T>) => Promise<void>;
};
export interface BirpcReturnBuiltin<RemoteFunctions, LocalFunctions = Record<string, unknown>> {
    /**
     * Raw functions object
     */
    $functions: LocalFunctions;
    /**
     * Whether the RPC is closed
     */
    readonly $closed: boolean;
    /**
     * Custom meta data attached to the RPC instance
     */
    readonly $meta: any;
    /**
     * Close the RPC connection
     */
    $close: (error?: Error) => void;
    /**
     * Reject pending calls
     */
    $rejectPendingCalls: (handler?: PendingCallHandler) => Promise<void>[];
    /**
     * Call the remote function and wait for the result.
     * An alternative to directly calling the function
     */
    $call: <K extends keyof RemoteFunctions>(method: K, ...args: ArgumentsType<RemoteFunctions[K]>) => Promise<Awaited<ReturnType<RemoteFunctions[K]>>>;
    /**
     * Same as `$call`, but returns `undefined` if the function is not defined on the remote side.
     */
    $callOptional: <K extends keyof RemoteFunctions>(method: K, ...args: ArgumentsType<RemoteFunctions[K]>) => Promise<Awaited<ReturnType<RemoteFunctions[K]> | undefined>>;
    /**
     * Send event without asking for response
     */
    $callEvent: <K extends keyof RemoteFunctions>(method: K, ...args: ArgumentsType<RemoteFunctions[K]>) => Promise<void>;
    /**
     * Call the remote function with the raw options.
     */
    $callRaw: (options: {
        method: string;
        args: unknown[];
        event?: boolean;
        optional?: boolean;
    }) => Promise<Awaited<ReturnType<any>>[]>;
}
export type ProxifiedRemoteFunctions<RemoteFunctions extends object = Record<string, unknown>> = {
    [K in keyof RemoteFunctions]: BirpcFn<RemoteFunctions[K]>;
};
export type BirpcReturn<RemoteFunctions extends object = Record<string, unknown>, LocalFunctions extends object = Record<string, unknown>, Proxify extends boolean = true> = Proxify extends true ? ProxifiedRemoteFunctions<RemoteFunctions> & BirpcReturnBuiltin<RemoteFunctions, LocalFunctions> : BirpcReturnBuiltin<RemoteFunctions, LocalFunctions>;
export interface CallRawOptions {
    method: string;
    args: unknown[];
    event?: boolean;
    optional?: boolean;
}
export type PendingCallHandler = (options: Pick<PromiseEntry, 'method' | 'reject'>) => void | Promise<void>;
interface PromiseEntry {
    resolve: (arg: any) => void;
    reject: (error: any) => void;
    method: string;
    timeoutId?: ReturnType<typeof setTimeout>;
}
declare const setTimeout: typeof globalThis.setTimeout;
export declare function createBirpc<RemoteFunctions extends object = Record<string, unknown>, LocalFunctions extends object = Record<string, unknown>, Proxify extends boolean = true>($functions: LocalFunctions, options: BirpcOptions<RemoteFunctions, LocalFunctions, Proxify>): BirpcReturn<RemoteFunctions, LocalFunctions, Proxify>;
export {};
