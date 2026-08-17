import { describe, expect, it } from "vitest";
import {
  createInquiryRuntime,
  foldInquiryContext,
  neutralizeInquiryAssistant,
  registerInquiryContextFolding,
} from "../src/pi-inquiry.js";

type Message = Record<string, unknown>;
const namespace = "test.plugin";
const inquiryId = "inquiry-1";

function persisted(message: { customType: string; content: string; display: false; details: unknown }, timestamp: number): Message {
  return {
    role: "custom",
    customType: message.customType,
    content: [{ type: "text", text: message.content }],
    display: message.display,
    details: message.details,
    timestamp,
  };
}

function assistant(text: string, timestamp: number, stopReason = "stop"): Message {
  return {
    role: "assistant",
    content: [{ type: "text", text }],
    stopReason,
    timestamp,
  };
}

function toolResult(timestamp: number): Message {
  return {
    role: "toolResult",
    content: [{ type: "text", text: "blocked" }],
    timestamp,
  };
}

describe("Pi inquiry lifecycle", () => {
  it("sends a correlated hidden inquiry and captures non-thinking text", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    let sent: unknown;
    const message = inquiry.send(
      {
        sendMessage(value, options) {
          sent = { value, options };
        },
      },
      "decide",
      1,
    );
    expect(sent).toEqual({
      value: message,
      options: { triggerTurn: true, deliverAs: "steer" },
    });
    expect(inquiry.capture({ role: "assistant", content: [
      { type: "thinking", thinking: "hidden" },
      { type: "text", text: "answer" },
    ] })).toBe("answer");
  });

  it("neutralizes only assistants and persists exact correlation metadata", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    const message = assistant("secret", 3);
    expect(inquiry.neutralize(message, 2)).toEqual({
      ...message,
      content: [],
      details: { piInquiry: inquiry.correlation(2) },
    });
    const user = { role: "user", content: "keep" };
    expect(neutralizeInquiryAssistant(user, inquiry.correlation(1))).toBe(user);
  });

  it("folds contiguous retries, assistants, and tool results as one exchange", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    const other = {
      role: "custom",
      customType: "other.plugin:entry",
      content: "keep",
      timestamp: 4,
    };
    const messages = [
      { role: "user", content: "task", timestamp: 1 },
      persisted(inquiry.prompt("first", 1), 2),
      assistant("invalid", 3),
      other,
      persisted(inquiry.prompt("second", 2), 5),
      assistant("tool", 6),
      toolResult(7),
      persisted(inquiry.prompt("third", 3), 8),
      inquiry.neutralize(assistant("valid", 9), 3),
      persisted(inquiry.fold(3, {
        customType: "test.plugin:continuation",
        content: "continue compactly",
        details: { reason: "valid" },
      }), 10),
      { role: "user", content: "later", timestamp: 11 },
    ];

    expect(foldInquiryContext(messages, namespace)).toEqual([
      messages[0],
      other,
      {
        role: "custom",
        customType: "test.plugin:continuation",
        content: [{ type: "text", text: "continue compactly" }],
        display: false,
        details: {
          reason: "valid",
          piInquiry: inquiry.correlation(3),
        },
        timestamp: 10,
      },
      messages[10],
    ]);
  });

  it("accepts and folds a replacement without optional details", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    const replacement = { customType: "test.plugin:continuation", content: "continue" };
    const messages = [
      persisted(inquiry.prompt("first", 1), 1),
      inquiry.neutralize(assistant("valid", 2), 1),
      persisted(inquiry.fold(1, replacement), 3),
    ];
    expect(foldInquiryContext(messages, namespace)).toEqual([{
      role: "custom",
      customType: replacement.customType,
      content: [{ type: "text", text: replacement.content }],
      display: false,
      details: { piInquiry: inquiry.correlation(1) },
      timestamp: 3,
    }]);
  });

  it("removes a complete exchange without replacement", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    const messages = [
      persisted(inquiry.prompt("first", 1), 1),
      assistant("invalid", 2),
      persisted(inquiry.prompt("second", 2), 3),
      inquiry.neutralize(assistant("valid", 4), 2),
      persisted(inquiry.fold(2), 5),
    ];
    expect(foldInquiryContext(messages, namespace)).toEqual([]);
  });

  it("removes a canonical aborted inquiry without a fold marker", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    const before = { role: "user", content: "before", timestamp: 1 };
    const after = { role: "user", content: "after", timestamp: 4 };
    const messages = [
      before,
      persisted(inquiry.prompt("first", 1), 2),
      assistant("partial", 3, "aborted"),
      after,
    ];
    expect(foldInquiryContext(messages, namespace)).toEqual([before, after]);
  });

  it("fails closed on skipped attempts, interleaved user input, and malformed records", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    const skipped = [
      persisted(inquiry.prompt("one", 1), 1),
      persisted(inquiry.prompt("three", 3), 2),
      persisted(inquiry.fold(3), 3),
    ];
    expect(foldInquiryContext(skipped, namespace)).toBe(skipped);

    const interleaved = [
      persisted(inquiry.prompt("one", 1), 1),
      { role: "user", content: "take over", timestamp: 2 },
      persisted(inquiry.fold(1), 3),
    ];
    expect(foldInquiryContext(interleaved, namespace)).toBe(interleaved);

    const malformed = {
      ...persisted(inquiry.prompt("one", 1), 1),
      content: [],
    };
    const later = [
      malformed,
      persisted(inquiry.prompt("two", 2), 2),
      persisted(inquiry.fold(2), 3),
    ];
    expect(foldInquiryContext(later, namespace)).toBe(later);
  });

  it("folds later independent inquiries after an incomplete exchange", () => {
    const incomplete = createInquiryRuntime(namespace, { inquiryId: "incomplete" });
    const complete = createInquiryRuntime(namespace, { inquiryId: "complete" });
    const rawPrompt = persisted(incomplete.prompt("raw", 1), 1);
    const rawAssistant = assistant("no fold", 2);
    const separator = { role: "user", content: "separator", timestamp: 3 };
    const messages = [
      rawPrompt,
      rawAssistant,
      separator,
      persisted(complete.prompt("done", 1), 4),
      complete.neutralize(assistant("answer", 5), 1),
      persisted(complete.fold(1), 6),
    ];
    expect(foldInquiryContext(messages, namespace)).toEqual([
      rawPrompt,
      rawAssistant,
      separator,
    ]);
  });

  it("registers a context transform and validates builder inputs", () => {
    const inquiry = createInquiryRuntime(namespace, { inquiryId });
    const messages = [
      persisted(inquiry.prompt("done", 1), 1),
      inquiry.neutralize(assistant("answer", 2), 1),
      persisted(inquiry.fold(1), 3),
    ];
    let transformed: unknown;
    registerInquiryContextFolding(
      {
        on(event, handler) {
          expect(event).toBe("context");
          transformed = handler({ messages });
        },
      },
      namespace,
    );
    expect(transformed).toEqual({ messages: [] });
    expect(() => inquiry.prompt("", 1)).toThrow();
    expect(() => inquiry.prompt("x", 0)).toThrow();
    expect(() => inquiry.fold(1, { customType: "bad space", content: "x" })).toThrow();
  });
});
