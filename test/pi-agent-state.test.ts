import { describe, expect, it, vi } from "vitest";

import { probePiAgentState } from "../src/pi-agent-state.js";

describe("probePiAgentState", () => {
	it("returns one fresh official idle and pending-message observation", () => {
		const source = {
			isIdle: vi.fn(() => false),
			hasPendingMessages: vi.fn(() => true),
		};

		expect(probePiAgentState(source)).toEqual({
			idle: false,
			busy: true,
			pendingMessages: true,
		});
		expect(source.isIdle).toHaveBeenCalledTimes(1);
		expect(source.hasPendingMessages).toHaveBeenCalledTimes(1);
	});

	it("does not cache state between public Pi event probes", () => {
		let idle = true;
		const source = { isIdle: () => idle };

		expect(probePiAgentState(source)).toEqual({
			idle: true,
			busy: false,
			pendingMessages: false,
		});
		idle = false;
		expect(probePiAgentState(source)).toEqual({
			idle: false,
			busy: true,
			pendingMessages: false,
		});
	});

	it("preserves the context receiver for official state methods", () => {
		const source = {
			idle: false,
			pending: true,
			isIdle() {
				return this.idle;
			},
			hasPendingMessages() {
				return this.pending;
			},
		};

		expect(probePiAgentState(source)).toEqual({
			idle: false,
			busy: true,
			pendingMessages: true,
		});
	});
});
