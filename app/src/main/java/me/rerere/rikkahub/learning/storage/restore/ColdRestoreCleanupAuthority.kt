package me.rerere.rikkahub.learning.storage.restore

import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import me.rerere.rikkahub.learning.model.LearningPreferencesV1

internal fun persistedLearningIsDisabled(settings: LearningPreferencesV1?): Boolean =
    settings != null && settings.failClosed() == settings &&
        !settings.handoff && !settings.capture && !settings.jobs &&
        !settings.reflectionShadow && !settings.policyCandidate && !settings.policyRetrievalShadow &&
        !settings.policyInjection && !settings.workflowCandidate && !settings.workflowPromotion &&
        !settings.curatorUpdate && !settings.curatorMerge && !settings.curatorSplit && !settings.curatorSupersede

/** Three distinct heads: immutable archive, selected bootstrap snapshot, current installed stream.
 * Coverage alone, or an unrelated stream with a larger sequence, is never cleanup authority. */
internal fun provesColdRestoreRebuild(
    archive: BackupAuthorityStreamV1,
    installed: BackupAuthorityStreamV1,
    checkpointStreamId: String,
    bootstrapHeadSeq: Long,
    lastContiguousSeq: Long,
): Boolean = archive.streamId == installed.streamId && checkpointStreamId == installed.streamId &&
    bootstrapHeadSeq >= archive.headSeq && bootstrapHeadSeq <= installed.headSeq &&
    lastContiguousSeq >= bootstrapHeadSeq && lastContiguousSeq <= installed.headSeq
