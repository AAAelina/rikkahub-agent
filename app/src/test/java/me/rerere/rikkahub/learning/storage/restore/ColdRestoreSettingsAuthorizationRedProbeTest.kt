package me.rerere.rikkahub.learning.storage.restore

import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import me.rerere.rikkahub.learning.model.LearningPreferencesV1
import org.junit.Assert.*
import org.junit.Test

/** Regression for the original R3 RED probes; always participates in normal tests. */
class ColdRestoreSettingsAuthorizationRedProbeTest {
    @Test
    fun delayedSettingsMustNotAuthorizeDestructiveCleanup() = checkCurrentPreDiCall(null)

    @Test
    fun databaseOnlyRestoreWithPersistedLearningEnabledMustRetainQuarantine() =
        checkCurrentPreDiCall(LearningPreferencesV1(handoff = true, capture = true, jobs = true))

    @Test
    fun persistedDisabledControlCanCompleteAfterAuthorityValidation() =
        checkCurrentPreDiCall(LearningPreferencesV1())

    private fun checkCurrentPreDiCall(persisted: LearningPreferencesV1?) {
        var cleanupCalled = false
        var authorityValidated = false
        // Production refuses cleanup until strict persisted consent is available.
        val result = ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
            journalRead = ColdRestoreJournalReadResult.Valid(journal()),
            persistedSettings = persisted,
            validateInstalled = { _, _ -> authorityValidated = true },
            complete = { _, _ ->
                assertTrue(authorityValidated)
                cleanupCalled = true
                true
            },
        )
        val mayDiscardDerivedState = persisted != null &&
            !persisted.handoff && !persisted.capture && !persisted.jobs
        assertEquals("Cleanup requires loaded disabled settings; current pre-DI call lacks proof",
            mayDiscardDerivedState, cleanupCalled)
        assertEquals(mayDiscardDerivedState, result)
    }

    private fun journal() = ColdRestoreJournalV1.staged(
        requestId = "0123456789abcdef0123456789abcdef",
        components = listOf(BackupArchiveComponent.DATABASE),
        archiveSize = 2048L, archiveSha256 = "a".repeat(64),
        mainDatabaseSize = 1024L, mainDatabaseSha256 = "b".repeat(64),
        mainStream = BackupAuthorityStreamV1("00000000-0000-0000-0000-000000000001", 7L),
        createdAtMs = 100L,
    ).copy(
        phase = ColdRestorePhase.REBUILD_REQUIRED,
        stateVersion = ColdRestorePhase.REBUILD_REQUIRED.ordinal.toLong(),
        preparedDatabaseSize = 1024L, preparedDatabaseSha256 = "c".repeat(64),
        updatedAtMs = 120L, learningQuarantineId = "0011223344556677",
        mainQuarantineId = "8899aabbccddeeff",
    ).also { check(ColdRestoreJournalCodec.validate(it) == null) }
}
