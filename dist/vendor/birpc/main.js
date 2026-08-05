/**
 * Vendored from antfu-collective/birpc at reviewed commit 6b891740de348a82c69dbd38310d1ec822c7640b (v4.0.0).
 * MIT License, Copyright (c) 2021 Anthony Fu <https://github.com/antfu>.
 * Distributed under the MIT License. See THIRD_PARTY_NOTICES.md at repository root.
 * This file matches that commit except for one mechanical change: relative import paths
 * were given explicit .js extensions for NodeNext module resolution. It is
 * imported only as an internal RPC dispatcher; it is not part of the public interface.
 */
import { TYPE_REQUEST, TYPE_RESPONSE } from './messages.js';
import { createPromiseWithResolvers, nanoid } from './utils.js';
const DEFAULT_TIMEOUT = 60_000; // 1 minute
const defaultSerialize = (i) => i;
const defaultDeserialize = defaultSerialize;
// Store public APIs locally in case they are overridden later
const { clearTimeout, setTimeout } = globalThis;
export function createBirpc($functions, options) {
    const { post, on, off = () => { }, eventNames = [], serialize = defaultSerialize, deserialize = defaultDeserialize, resolver, bind = 'rpc', timeout = DEFAULT_TIMEOUT, proxify = true, } = options;
    let $closed = false;
    const _rpcPromiseMap = new Map();
    let _promiseInit;
    let rpc;
    async function _call(method, args, event, optional) {
        if ($closed)
            throw new Error(`[birpc] rpc is closed, cannot call "${method}"`);
        const req = { m: method, a: args, t: TYPE_REQUEST };
        if (optional)
            req.o = true;
        const send = async (_req) => post(serialize(_req));
        if (event) {
            await send(req);
            return;
        }
        if (_promiseInit) {
            // Wait if `on` is promise
            try {
                await _promiseInit;
            }
            finally {
                // don't keep resolved promise hanging
                _promiseInit = undefined;
            }
        }
        // eslint-disable-next-line prefer-const
        let { promise, resolve, reject } = createPromiseWithResolvers();
        const id = nanoid();
        req.i = id;
        let timeoutId;
        async function handler(newReq = req) {
            if (timeout >= 0) {
                timeoutId = setTimeout(() => {
                    try {
                        // Custom onTimeoutError handler can throw its own error too
                        const handleResult = options.onTimeoutError?.call(rpc, method, args);
                        if (handleResult !== true)
                            throw new Error(`[birpc] timeout on calling "${method}"`);
                    }
                    catch (e) {
                        reject(e);
                    }
                    _rpcPromiseMap.delete(id);
                }, timeout);
                // For node.js, `unref` is not available in browser-like environments
                if (typeof timeoutId === 'object')
                    timeoutId = timeoutId.unref?.();
            }
            _rpcPromiseMap.set(id, { resolve, reject, timeoutId, method });
            await send(newReq);
            return promise;
        }
        try {
            if (options.onRequest)
                await options.onRequest.call(rpc, req, handler, resolve);
            else
                await handler();
        }
        catch (e) {
            if (options.onGeneralError?.call(rpc, e) !== true)
                throw e;
            return;
        }
        finally {
            clearTimeout(timeoutId);
            _rpcPromiseMap.delete(id);
        }
        return promise;
    }
    const builtinMethods = {
        $call: (method, ...args) => _call(method, args, false),
        $callOptional: (method, ...args) => _call(method, args, false, true),
        $callEvent: (method, ...args) => _call(method, args, true),
        $callRaw: (options) => _call(options.method, options.args, options.event, options.optional),
        $rejectPendingCalls,
        get $closed() {
            return $closed;
        },
        get $meta() {
            return options.meta;
        },
        $close,
        $functions,
    };
    if (proxify) {
        rpc = new Proxy({}, {
            get(_, method) {
                if (Object.prototype.hasOwnProperty.call(builtinMethods, method))
                    return builtinMethods[method];
                // catch if "createBirpc" is returned from async function
                if (method === 'then' && !eventNames.includes('then') && !('then' in $functions))
                    return undefined;
                const sendEvent = (...args) => _call(method, args, true);
                if (eventNames.includes(method)) {
                    sendEvent.asEvent = sendEvent;
                    return sendEvent;
                }
                const sendCall = (...args) => _call(method, args, false);
                sendCall.asEvent = sendEvent;
                return sendCall;
            },
        });
    }
    else {
        rpc = builtinMethods;
    }
    function $close(customError) {
        $closed = true;
        _rpcPromiseMap.forEach(({ reject, method }) => {
            const error = new Error(`[birpc] rpc is closed, cannot call "${method}"`);
            if (customError) {
                customError.cause ??= error;
                return reject(customError);
            }
            reject(error);
        });
        _rpcPromiseMap.clear();
        off(onMessage);
    }
    function $rejectPendingCalls(handler) {
        const entries = Array.from(_rpcPromiseMap.values());
        const handlerResults = entries.map(({ method, reject }) => {
            if (!handler) {
                return reject(new Error(`[birpc]: rejected pending call "${method}".`));
            }
            return handler({ method, reject });
        });
        _rpcPromiseMap.clear();
        return handlerResults;
    }
    async function onMessage(data, ...extra) {
        let msg;
        try {
            msg = deserialize(data);
        }
        catch (e) {
            if (options.onGeneralError?.call(rpc, e) !== true)
                throw e;
            return;
        }
        if (msg.t === TYPE_REQUEST) {
            const { m: method, a: args, o: optional } = msg;
            let result, error;
            let fn = await (resolver
                ? resolver.call(rpc, method, $functions[method])
                : $functions[method]);
            if (optional)
                fn ||= () => undefined;
            if (!fn) {
                error = new Error(`[birpc] function "${method}" not found`);
            }
            else {
                try {
                    result = await fn.apply(bind === 'rpc' ? rpc : $functions, args);
                }
                catch (e) {
                    error = e;
                }
            }
            if (msg.i) {
                if (error && options.onFunctionError) {
                    if (options.onFunctionError.call(rpc, error, method, args) === true)
                        return;
                }
                // Send data
                if (!error) {
                    try {
                        await post(serialize({ t: TYPE_RESPONSE, i: msg.i, r: result }), ...extra);
                        return;
                    }
                    catch (e) {
                        error = e;
                        if (options.onGeneralError?.call(rpc, e, method, args) !== true)
                            throw e;
                    }
                }
                // Try to send error if serialization failed
                try {
                    await post(serialize({ t: TYPE_RESPONSE, i: msg.i, e: error }), ...extra);
                }
                catch (e) {
                    if (options.onGeneralError?.call(rpc, e, method, args) !== true)
                        throw e;
                }
            }
        }
        else {
            const { i: ack, r: result, e: error } = msg;
            const promise = _rpcPromiseMap.get(ack);
            if (promise) {
                clearTimeout(promise.timeoutId);
                if (error)
                    promise.reject(error);
                else
                    promise.resolve(result);
            }
            _rpcPromiseMap.delete(ack);
        }
    }
    _promiseInit = on(onMessage);
    return rpc;
}
