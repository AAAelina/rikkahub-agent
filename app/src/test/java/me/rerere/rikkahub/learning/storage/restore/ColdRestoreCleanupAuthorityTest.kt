package me.rerere.rikkahub.learning.storage.restore

import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import me.rerere.rikkahub.learning.model.LearningPreferencesV1
import org.junit.Assert.*
import org.junit.Test

class ColdRestoreCleanupAuthorityTest {
    private val stream = "00000000-0000-4000-8000-000000000001"
    private val archive = BackupAuthorityStreamV1(stream, 7L)

    @Test fun appendBeforeBootstrapMaySelectNewHead() {
        assertTrue(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(stream, 12L), stream, 10L, 10L))
    }

    @Test fun uncoveredSnapshotAndRewindCannotAuthorize() {
        assertFalse(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(stream, 12L), stream, 10L, 9L))
        assertFalse(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(stream, 6L), stream, 7L, 7L))
        assertFalse(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(stream, 12L), stream, 6L, 12L))
        assertFalse(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(stream, 12L), stream, 13L, 13L))
        assertFalse(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(stream, 12L), stream, 10L, 13L))
    }

    @Test fun largerUnrelatedStreamIsNotProof() {
        val other = "00000000-0000-4000-8000-000000000002"
        assertFalse(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(other, 12L), other, 12L, 12L))
        assertFalse(provesColdRestoreRebuild(archive, BackupAuthorityStreamV1(stream, 12L), other, 12L, 12L))
    }

    @Test fun missingOrInvalidSettingsDoNotBecomeDisabledConsent() {
        assertFalse(persistedLearningIsDisabled(null))
        assertFalse(persistedLearningIsDisabled(LearningPreferencesV1(schemaVersion = 99)))
        assertFalse(persistedLearningIsDisabled(LearningPreferencesV1(policyInjection = true)))
        assertFalse(persistedLearningIsDisabled(LearningPreferencesV1(handoff = true)))
        assertTrue(persistedLearningIsDisabled(LearningPreferencesV1()))
    }
}
