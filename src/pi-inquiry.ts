import { randomBytes } from "node:crypto";
import { neutralizeAssistantMessage, nonThinkingText } from "./xml.js";

export const INQUIRY_PROTOCOL_VERSION = 1 as const;
export const MAX_INQUIRY_CONTENT_CODE_POINTS = 65_536;

export interface InquiryCorrelation {
  readonly version: typeof INQUIRY_PROTOCOL_VERSION;
  readonly namespace: string;
  readonly inquiryId: string;
  readonly attempt: number;
}

export interface InquiryReplacement {
  readonly customType: string;
  readonly content: string;
  readonly details?: unknown;
}

export interface InquiryMessage {
  readonly customType: string;
  readonly content: string;
  readonly display: false;
  readonly details: InquiryCorrelation;
}

export interface InquiryFoldMessage {
  readonly customType: string;
  readonly content: string;
  readonly display: false;
  readonly details: InquiryCorrelation & {
    readonly outcome: "remove" | "replace";
    readonly replacement?: InquiryReplacement;
  };
}

export interface InquirySendApi {
  sendMessage(
    message: InquiryMessage,
    options: { readonly triggerTurn: true; readonly deliverAs: "steer" },
  ): void;
}

export interface InquiryContextApi {
  on(
    event: "context",
    handler: (event: { messages: object[] }) => { messages?: object[] },
  ): void;
}

export interface InquiryRuntime {
  readonly inquiryId: string;
  readonly namespace: string;
  correlation(attempt: number): InquiryCorrelation;
  prompt(content: string, attempt: number): InquiryMessage;
  send(pi: InquirySendApi, content: string, attempt: number): InquiryMessage;
  fold(attempt: number, replacement?: InquiryReplacement): InquiryFoldMessage;
  capture(message: unknown): string | null;
  neutralize<T>(message: T, attempt: number): T;
}

type ParsedPluginMessage =
  | { readonly type: "unrelated" }
  | { readonly type: "invalid"; readonly inquiryId?: string }
  | { readonly type: "prompt"; readonly correlation: InquiryCorrelation }
  | {
      readonly type: "fold";
      readonly correlation: InquiryCorrelation;
      readonly replacement?: InquiryReplacement;
      readonly timestamp: number;
    };

type Segment<T extends object> =
  | {
      readonly kind: "folded";
      readonly endIndex: number;
      readonly replacement: readonly T[];
    }
  | { readonly kind: "aborted"; readonly endIndex: number }
  | { readonly kind: "incomplete" };

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validNamespace(value: string): boolean {
  return /^[A-Za-z0-9_.:-]{1,128}$/.test(value);
}

function validInquiryId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 128 &&
    /^[A-Za-z0-9_-]+$/.test(value)
  );
}

function validAttempt(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value > 0;
}

function validContent(value: unknown, allowEmpty = false): value is string {
  return (
    typeof value === "string" &&
    (allowEmpty || value.trim().length > 0) &&
    Array.from(value).length <= MAX_INQUIRY_CONTENT_CODE_POINTS
  );
}

function correlationOf(value: unknown): InquiryCorrelation | null {
  if (!isObject(value)) return null;
  return value.version === INQUIRY_PROTOCOL_VERSION &&
    typeof value.namespace === "string" &&
    validNamespace(value.namespace) &&
    validInquiryId(value.inquiryId) &&
    validAttempt(value.attempt)
      ? {
          version: INQUIRY_PROTOCOL_VERSION,
          namespace: value.namespace,
          inquiryId: value.inquiryId,
          attempt: value.attempt,
        }
      : null;
}

function customTypeOf(value: unknown): string | null {
  return isObject(value) && typeof value.customType === "string"
    ? value.customType
    : null;
}

function roleOf(value: unknown): string | null {
  return isObject(value) && typeof value.role === "string" ? value.role : null;
}

function contentText(value: unknown): string | null {
  if (!isObject(value)) return null;
  if (typeof value.content === "string") return value.content;
  if (!Array.isArray(value.content) || value.content.length !== 1) return null;
  const block = value.content[0];
  return isObject(block) && block.type === "text" && typeof block.text === "string"
    ? block.text
    : null;
}

function replacementOf(value: unknown): InquiryReplacement | null {
  if (!isObject(value)) return null;
  if (
    typeof value.customType !== "string" ||
    !validNamespace(value.customType) ||
    !validContent(value.content)
  ) {
    return null;
  }
  return {
    customType: value.customType,
    content: value.content,
    ...(Object.hasOwn(value, "details") ? { details: value.details } : {}),
  };
}

function serializable(value: unknown): boolean {
  try {
    return JSON.stringify(value) !== undefined;
  } catch {
    return false;
  }
}

function parsePluginMessage(value: unknown, namespace: string): ParsedPluginMessage {
  if (!isObject(value) || value.role !== "custom") return { type: "unrelated" };
  const customType = customTypeOf(value);
  const promptType = `${namespace}:inquiry`;
  const foldType = `${namespace}:inquiry-fold`;
  if (customType !== promptType && customType !== foldType) return { type: "unrelated" };

  const rawDetails = isObject(value.details) ? value.details : undefined;
  const inquiryId = validInquiryId(rawDetails?.inquiryId)
    ? rawDetails.inquiryId
    : undefined;
  const correlation = correlationOf(value.details);
  const content = contentText(value);
  if (
    correlation === null ||
    correlation.namespace !== namespace ||
    typeof value.timestamp !== "number" ||
    !Number.isFinite(value.timestamp) ||
    content === null
  ) {
    return { type: "invalid", inquiryId };
  }

  if (customType === promptType) {
    return validContent(content)
      ? { type: "prompt", correlation }
      : { type: "invalid", inquiryId: correlation.inquiryId };
  }

  const outcome = rawDetails?.outcome;
  if (outcome === "remove") {
    return content.length === 0
      ? { type: "fold", correlation, timestamp: value.timestamp }
      : { type: "invalid", inquiryId: correlation.inquiryId };
  }
  const replacement = replacementOf(rawDetails?.replacement);
  if (
    outcome !== "replace" ||
    replacement === null ||
    content !== replacement.content ||
    (Object.hasOwn(replacement, "details") && !serializable(replacement.details))
  ) {
    return { type: "invalid", inquiryId: correlation.inquiryId };
  }
  return {
    type: "fold",
    correlation,
    replacement,
    timestamp: value.timestamp,
  };
}

function isAbortedAssistant(value: unknown): boolean {
  return isObject(value) && value.role === "assistant" && value.stopReason === "aborted";
}

function isNeutralizedAssistant(
  value: unknown,
  namespace: string,
  inquiryId: string,
  attempt: number,
): boolean {
  if (
    !isObject(value) ||
    value.role !== "assistant" ||
    !Array.isArray(value.content) ||
    value.content.length !== 0 ||
    !isObject(value.details) ||
    !isObject(value.details.piInquiry)
  ) {
    return false;
  }
  const correlation = correlationOf(value.details.piInquiry);
  return (
    correlation?.namespace === namespace &&
    correlation.inquiryId === inquiryId &&
    correlation.attempt === attempt
  );
}

function replacementMessage<T extends object>(
  replacement: InquiryReplacement,
  correlation: InquiryCorrelation,
  timestamp: number,
): T {
  return {
    role: "custom",
    customType: replacement.customType,
    content: [{ type: "text", text: replacement.content }],
    display: false,
    details: {
      ...(isObject(replacement.details) ? replacement.details : {}),
      piInquiry: correlation,
    },
    timestamp,
  } as T;
}

function findSegment<T extends object>(
  messages: readonly T[],
  startIndex: number,
  start: InquiryCorrelation,
  namespace: string,
): Segment<T> {
  if (start.attempt !== 1) return { kind: "incomplete" };
  let attempt = start.attempt;
  const preserved: T[] = [];

  for (let index = startIndex + 1; index < messages.length; index += 1) {
    const message = messages[index];
    if (message === undefined) return { kind: "incomplete" };
    const plugin = parsePluginMessage(message, namespace);
    if (plugin.type === "invalid") return { kind: "incomplete" };
    if (plugin.type === "prompt") {
      if (
        plugin.correlation.inquiryId !== start.inquiryId ||
        plugin.correlation.attempt !== attempt + 1
      ) {
        return { kind: "incomplete" };
      }
      attempt = plugin.correlation.attempt;
      continue;
    }
    if (plugin.type === "fold") {
      if (
        plugin.correlation.inquiryId !== start.inquiryId ||
        plugin.correlation.attempt !== attempt
      ) {
        return { kind: "incomplete" };
      }
      let endIndex = index;
      for (let tail = index + 1; tail < messages.length; tail += 1) {
        const trailing = messages[tail];
        if (trailing === undefined) break;
        if (
          isNeutralizedAssistant(
            trailing,
            namespace,
            start.inquiryId,
            attempt,
          )
        ) {
          endIndex = tail;
          break;
        }
        const trailingPlugin = parsePluginMessage(trailing, namespace);
        if (trailingPlugin.type !== "unrelated" || roleOf(trailing) !== "custom") break;
        preserved.push(trailing);
        endIndex = tail;
      }
      return {
        kind: "folded",
        endIndex,
        replacement: [
          ...preserved,
          ...(plugin.replacement === undefined
            ? []
            : [
                replacementMessage<T>(
                  plugin.replacement,
                  plugin.correlation,
                  plugin.timestamp,
                ),
              ]),
        ],
      };
    }

    const role = roleOf(message);
    if (role === "custom") {
      preserved.push(message);
      continue;
    }
    if (role === "assistant") {
      if (isAbortedAssistant(message)) return { kind: "aborted", endIndex: index };
      continue;
    }
    if (role === "toolResult") continue;
    return { kind: "incomplete" };
  }
  return { kind: "incomplete" };
}

export function createInquiryRuntime(
  namespace: string,
  options: { readonly inquiryId?: string } = {},
): InquiryRuntime {
  if (!validNamespace(namespace)) throw new TypeError("invalid inquiry namespace");
  const inquiryId = options.inquiryId ?? randomBytes(16).toString("base64url");
  if (!validInquiryId(inquiryId)) throw new TypeError("invalid inquiry id");

  const correlation = (attempt: number): InquiryCorrelation => {
    if (!validAttempt(attempt)) throw new TypeError("invalid inquiry attempt");
    return {
      version: INQUIRY_PROTOCOL_VERSION,
      namespace,
      inquiryId,
      attempt,
    };
  };

  const prompt = (content: string, attempt: number): InquiryMessage => {
    if (!validContent(content)) throw new TypeError("invalid inquiry prompt");
    return {
      customType: `${namespace}:inquiry`,
      content,
      display: false,
      details: correlation(attempt),
    };
  };

  return {
    inquiryId,
    namespace,
    correlation,
    prompt,
    send(pi, content, attempt) {
      const message = prompt(content, attempt);
      pi.sendMessage(message, { triggerTurn: true, deliverAs: "steer" });
      return message;
    },
    fold(attempt, replacement) {
      if (
        replacement !== undefined &&
        (replacementOf(replacement) === null ||
          (Object.hasOwn(replacement, "details") && !serializable(replacement.details)))
      ) {
        throw new TypeError("invalid inquiry replacement");
      }
      return {
        customType: `${namespace}:inquiry-fold`,
        content: replacement?.content ?? "",
        display: false,
        details: {
          ...correlation(attempt),
          outcome: replacement === undefined ? "remove" : "replace",
          ...(replacement === undefined ? {} : { replacement }),
        },
      };
    },
    capture(message) {
      return roleOf(message) === "assistant" ? nonThinkingText(message) : null;
    },
    neutralize(message, attempt) {
      return neutralizeInquiryAssistant(message, correlation(attempt));
    },
  };
}

export function foldInquiryContext<T extends object>(
  messages: T[],
  namespace: string,
): T[] {
  if (!Array.isArray(messages) || !validNamespace(namespace)) return messages;
  const result: T[] = [];
  const poisoned = new Set<string>();
  let changed = false;

  for (let index = 0; index < messages.length; index += 1) {
    const message = messages[index];
    if (message === undefined) continue;
    const plugin = parsePluginMessage(message, namespace);
    if (plugin.type === "invalid") {
      if (plugin.inquiryId !== undefined) poisoned.add(plugin.inquiryId);
      result.push(message);
      continue;
    }
    if (plugin.type !== "prompt" || plugin.correlation.attempt !== 1) {
      result.push(message);
      continue;
    }
    if (poisoned.has(plugin.correlation.inquiryId)) {
      result.push(message);
      continue;
    }
    const segment = findSegment(messages, index, plugin.correlation, namespace);
    if (segment.kind === "folded") {
      result.push(...segment.replacement);
      index = segment.endIndex;
      changed = true;
      continue;
    }
    if (segment.kind === "aborted") {
      index = segment.endIndex;
      changed = true;
      continue;
    }
    poisoned.add(plugin.correlation.inquiryId);
    result.push(message);
  }
  return changed ? result : messages;
}

export function registerInquiryContextFolding(
  pi: InquiryContextApi,
  namespace: string,
): void {
  if (!validNamespace(namespace)) throw new TypeError("invalid inquiry namespace");
  pi.on("context", (event) => ({
    messages: foldInquiryContext(event.messages, namespace),
  }));
}

export function neutralizeInquiryAssistant<T>(
  message: T,
  correlation: InquiryCorrelation,
  options: { readonly stopReason?: "stop" | "aborted" } = {},
): T {
  if (roleOf(message) !== "assistant") return message;
  const neutralized = neutralizeAssistantMessage(message) as T &
    Record<string, unknown>;
  const currentDetails = isObject(neutralized.details)
    ? neutralized.details
    : {};
  return {
    ...neutralized,
    stopReason: options.stopReason ?? neutralized.stopReason,
    details: {
      ...currentDetails,
      piInquiry: correlation,
    },
  } as T;
}
