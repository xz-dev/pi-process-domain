/**
 * Vendored from antfu-collective/birpc at reviewed commit 6b891740de348a82c69dbd38310d1ec822c7640b (v4.0.0).
 * MIT License, Copyright (c) 2021 Anthony Fu <https://github.com/antfu>.
 * Distributed under the MIT License. See THIRD_PARTY_NOTICES.md at repository root.
 * This file matches that commit except for one mechanical change: relative import paths
 * were given explicit .js extensions for NodeNext module resolution. It is
 * imported only as an internal RPC dispatcher; it is not part of the public interface.
 */
export type ArgumentsType<T> = T extends (...args: infer A) => any ? A : never;
export type ReturnType<T> = T extends (...args: any) => infer R ? R : never;
export type Thenable<T> = T | PromiseLike<T>;
export declare function createPromiseWithResolvers<T>(): {
    promise: Promise<T>;
    resolve: (value: T | PromiseLike<T>) => void;
    reject: (reason?: any) => void;
};
export declare function cachedMap<T, R>(items: T[], fn: ((i: T) => R)): R[];
export declare function nanoid(size?: number): string;
