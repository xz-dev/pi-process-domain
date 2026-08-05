/**
 * Public domain snapshot types. These are the immutable, broker-committed view
 * of one authenticated domain at one exact broker revision.
 */
/** Compute the immutable aggregate snapshot from committed state counters. */
export function computeSnapshot(input) {
    const certain = input.certain;
    const allIdle = certain &&
        input.participants > 0 &&
        input.busyParticipants === 0 &&
        input.pendingSpawns === 0;
    return {
        domainId: input.domainId,
        brokerEpoch: input.brokerEpoch,
        revision: input.revision,
        activityGeneration: input.activityGeneration,
        participants: input.participants,
        busyParticipants: input.busyParticipants,
        pendingSpawns: input.pendingSpawns,
        allIdle,
        certain,
        fence: { brokerEpoch: input.brokerEpoch, activityGeneration: input.activityGeneration },
    };
}
