import { capability } from "zeromq";
export function preferredTransport() {
    return capability.ipc === true ? "ipc" : "tcp-loopback";
}
export function wildcardEndpoint(transport = preferredTransport()) {
    return transport === "ipc" ? "ipc://*" : "tcp://127.0.0.1:*";
}
export async function bindTemporaryEndpoint(socket, transport = preferredTransport()) {
    await socket.bind(wildcardEndpoint(transport));
    const endpoint = socket.lastEndpoint;
    if (endpoint === null ||
        (transport === "ipc" && !endpoint.startsWith("ipc://")) ||
        (transport === "tcp-loopback" && !endpoint.startsWith("tcp://127.0.0.1:"))) {
        socket.close();
        throw new Error("ZeroMQ did not publish a valid temporary endpoint");
    }
    return { transport, endpoint };
}
