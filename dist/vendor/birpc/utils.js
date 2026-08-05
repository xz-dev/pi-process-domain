/**
 * Vendored from antfu-collective/birpc at reviewed commit 6b891740de348a82c69dbd38310d1ec822c7640b (v4.0.0).
 * MIT License, Copyright (c) 2021 Anthony Fu <https://github.com/antfu>.
 * Distributed under the MIT License. See THIRD_PARTY_NOTICES.md at repository root.
 * This file matches that commit except for one mechanical change: relative import paths
 * were given explicit .js extensions for NodeNext module resolution. It is
 * imported only as an internal RPC dispatcher; it is not part of the public interface.
 */
export function createPromiseWithResolvers() {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => {
        resolve = res;
        reject = rej;
    });
    return { promise, resolve: resolve, reject: reject };
}
const _cacheMap = new WeakMap();
export function cachedMap(items, fn) {
    return items.map((i) => {
        let r = _cacheMap.get(i);
        if (!r) {
            r = fn(i);
            _cacheMap.set(i, r);
        }
        return r;
    });
}
const random = Math.random.bind(Math);
// port from nanoid
// https://github.com/ai/nanoid
const urlAlphabet = 'useandom-26T198340PX75pxJACKVERYMINDBUSHWOLF_GQZbfghjklqvwyzrict';
export function nanoid(size = 21) {
    let id = '';
    let i = size;
    while (i--)
        id += urlAlphabet[(random() * 64) | 0];
    return id;
}
