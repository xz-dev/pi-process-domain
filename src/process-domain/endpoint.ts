import type { Router } from "zeromq";
import { capability } from "zeromq";
import type { ProcessDomainTransport } from "./types.js";

export interface BoundEndpoint {
  readonly transport: ProcessDomainTransport;
  readonly endpoint: string;
}

export function preferredTransport(): ProcessDomainTransport {
  return capability.ipc === true ? "ipc" : "tcp-loopback";
}

export function wildcardEndpoint(transport = preferredTransport()): string {
  return transport === "ipc" ? "ipc://*" : "tcp://127.0.0.1:*";
}

export async function bindTemporaryEndpoint(
  socket: Router,
  transport = preferredTransport(),
): Promise<BoundEndpoint> {
  await socket.bind(wildcardEndpoint(transport));
  const endpoint = socket.lastEndpoint;
  if (
    endpoint === null ||
    (transport === "ipc" && !endpoint.startsWith("ipc://")) ||
    (transport === "tcp-loopback" && !endpoint.startsWith("tcp://127.0.0.1:"))
  ) {
    socket.close();
    throw new Error("ZeroMQ did not publish a valid temporary endpoint");
  }
  return { transport, endpoint };
}
