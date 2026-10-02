package me.rerere.rikkahub.data.sync.backup.restore

import java.io.File
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import org.junit.Assert.assertEquals
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class ColdRestoreJournalTest {
    @get:Rule
    val temporaryFolder = TemporaryFolder()

    @Test
    fun journalCreateReadAndStateVersionCasAreDeterministic() {
        val pending = File(temporaryFolder.newFolder("journal"), "pending_restore.json")
        val store = ColdRestoreJournalStore(pending.toPath())
        val staged = stagedJournal()

        assertEquals(ColdRestoreJournalWriteResult.Written, store.create(staged))
        assertEquals(ColdRestoreJournalReadResult.Valid(staged), store.read())

        val next = staged.copy(
            stateVersion = 1L,
            phase = ColdRestorePhase.READY_TO_SWAP,
            updatedAtMs = 101L,
            preparedDatabaseSize = 640L,
            preparedDatabaseSha256 = "c".repeat(64),
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Written,
            store.transition(staged.requestId, staged.stateVersion, next),
        )
        val quarantineStarted = next.copy(
            stateVersion = 2L,
            phase = ColdRestorePhase.LEARNING_QUARANTINE_STARTED,
            updatedAtMs = 102L,
            learningQuarantineId = "0011223344556677",
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Written,
            store.transition(next.requestId, next.stateVersion, quarantineStarted),
        )
        assertEquals(ColdRestoreJournalReadResult.Valid(quarantineStarted), store.read())
        assertEquals(
            ColdRestoreJournalWriteResult.Conflict,
            store.transition(staged.requestId, staged.stateVersion, next),
        )
    }

    @Test
    fun transitionCannotMutateFrozenArchiveIdentity() {
        val pending = File(temporaryFolder.newFolder("immutable"), "pending_restore.json")
        val store = ColdRestoreJournalStore(pending.toPath())
        val staged = stagedJournal()
        assertEquals(ColdRestoreJournalWriteResult.Written, store.create(staged))

        val forged = staged.copy(
            stateVersion = 1L,
            phase = ColdRestorePhase.READY_TO_SWAP,
            updatedAtMs = 101L,
            archiveSha256 = "f".repeat(64),
            preparedDatabaseSize = 640L,
            preparedDatabaseSha256 = "c".repeat(64),
        )

        assertEquals(
            ColdRestoreJournalWriteResult.Conflict,
            store.transition(staged.requestId, staged.stateVersion, forged),
        )
        assertEquals(ColdRestoreJournalReadResult.Valid(staged), store.read())

        val ready = forged.copy(archiveSha256 = staged.archiveSha256)
        assertEquals(
            ColdRestoreJournalWriteResult.Written,
            store.transition(staged.requestId, staged.stateVersion, ready),
        )
        val changedPreparedIdentity = ready.copy(
            stateVersion = 2L,
            phase = ColdRestorePhase.LEARNING_QUARANTINE_STARTED,
            updatedAtMs = 102L,
            learningQuarantineId = "0011223344556677",
            preparedDatabaseSha256 = "e".repeat(64),
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Conflict,
            store.transition(ready.requestId, ready.stateVersion, changedPreparedIdentity),
        )
    }

    @Test
    fun directTerminalCreateAndQuarantineIdReplacementFailClosed() {
        val pending = File(temporaryFolder.newFolder("phase"), "pending_restore.json")
        val store = ColdRestoreJournalStore(pending.toPath())
        val staged = stagedJournal()
        val terminal = staged.copy(
            stateVersion = ColdRestorePhase.COMPLETE.ordinal.toLong(),
            phase = ColdRestorePhase.COMPLETE,
            updatedAtMs = 109L,
            learningQuarantineId = "0011223344556677",
            mainQuarantineId = "8899aabbccddeeff",
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Rejected(
                ColdRestoreJournalValidationFailure.PHASE_FIELDS_INVALID,
            ),
            store.create(terminal),
        )

        assertEquals(ColdRestoreJournalWriteResult.Written, store.create(staged))
        val skippedReady = staged.copy(
            stateVersion = 1L,
            phase = ColdRestorePhase.LEARNING_QUARANTINE_STARTED,
            updatedAtMs = 101L,
            learningQuarantineId = "0011223344556677",
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Conflict,
            store.transition(staged.requestId, staged.stateVersion, skippedReady),
        )
        val ready = staged.copy(
            stateVersion = 1L,
            phase = ColdRestorePhase.READY_TO_SWAP,
            updatedAtMs = 101L,
            preparedDatabaseSize = 640L,
            preparedDatabaseSha256 = "c".repeat(64),
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Written,
            store.transition(staged.requestId, staged.stateVersion, ready),
        )
        val started = ready.copy(
            stateVersion = 2L,
            phase = ColdRestorePhase.LEARNING_QUARANTINE_STARTED,
            updatedAtMs = 102L,
            learningQuarantineId = "0011223344556677",
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Written,
            store.transition(ready.requestId, ready.stateVersion, started),
        )
        val changedId = started.copy(
            stateVersion = 3L,
            phase = ColdRestorePhase.LEARNING_QUARANTINED,
            updatedAtMs = 103L,
            learningQuarantineId = "8899aabbccddeeff",
        )
        assertEquals(
            ColdRestoreJournalWriteResult.Conflict,
            store.transition(started.requestId, started.stateVersion, changedId),
        )
    }

    @Test
    fun malformedOrOversizedJournalIsNeverPartiallyDecoded() {
        val directory = temporaryFolder.newFolder("bounded")
        val pending = File(directory, "pending_restore.json")
        val store = ColdRestoreJournalStore(pending.toPath())

        pending.writeBytes(ByteArray(65 * 1_024) { 'x'.code.toByte() })
        val oversized = store.read()
        assertTrue(oversized is ColdRestoreJournalReadResult.Invalid)
        oversized as ColdRestoreJournalReadResult.Invalid
        assertEquals(ColdRestoreJournalReadFailure.TOO_LARGE, oversized.readFailure)

        pending.writeText("{\"journalVersion\":1,\"unknown\":true}")
        val malformed = store.read()
        assertTrue(malformed is ColdRestoreJournalReadResult.Invalid)
        malformed as ColdRestoreJournalReadResult.Invalid
        assertEquals(ColdRestoreJournalReadFailure.MALFORMED_JSON, malformed.readFailure)
    }

    @Test
    fun failedPhaseRequiresAnAllowlistedFailureCode() {
        val invalid = stagedJournal().copy(
            phase = ColdRestorePhase.FAILED_RESTART_REQUIRED,
            stateVersion = 1L,
            updatedAtMs = 101L,
            failureCode = null,
        )

        assertEquals(
            ColdRestoreJournalValidationFailure.PHASE_FIELDS_INVALID,
            ColdRestoreJournalCodec.validate(invalid),
        )
    }

    private fun stagedJournal() = ColdRestoreJournalV1.staged(
        requestId = "0123456789abcdef0123456789abcdef",
        components = listOf(
            BackupArchiveComponent.DATABASE,
            BackupArchiveComponent.SETTINGS,
        ),
        archiveSize = 2_048L,
        archiveSha256 = "a".repeat(64),
        mainDatabaseSize = 512L,
        mainDatabaseSha256 = "b".repeat(64),
        mainStream = BackupAuthorityStreamV1(
            streamId = "00000000-0000-0000-0000-000000000001",
            headSeq = 5L,
        ),
        createdAtMs = 100L,
    )
}
