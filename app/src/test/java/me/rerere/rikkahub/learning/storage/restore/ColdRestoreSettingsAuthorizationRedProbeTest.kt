package me.rerere.rikkahub.learning.storage.restore

import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import me.rerere.rikkahub.learning.model.LearningPreferencesV1
import org.junit.Assert.*
import org.junit.Assume.assumeTrue
import org.junit.Before
import org.junit.Test

/**
 * Opt-in, intentionally RED probes for the still-unfixed pre-DI caller contract.
 * Run with RIKKAHUB_R3_RED_PROBE=1. Normal test runs skip this known-defect probe.
 * This invokes the production decision seam, not Android Application/DataStore startup.
 * The current seam cannot accept loaded settings; routing/proof must be added before these
 * become ordinary regression tests. Do not authorize cleanup merely to make this probe green.
 */
class ColdRestoreSettingsAuthorizationRedProbeTest {
    @Before
    fun optIn() = assumeTrue(System.getenv("RIKKAHUB_R3_RED_PROBE") == "1")

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
        // Exactly the current pre-settings decision seam: there is no persisted-settings input.
        val result = ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
            journalRead = ColdRestoreJournalReadResult.Valid(journal()),
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
