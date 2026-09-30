package me.rerere.rikkahub.data.sync.backup.restore

import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ColdRestoreStartupCoordinatorTest {
    @Test
    fun `missing journal is already finalized without touching the installed database`() {
        var callbackCount = 0

        assertTrue(
            ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
                journalRead = ColdRestoreJournalReadResult.Missing,
                validateInstalled = { _, _ -> callbackCount += 1 },
                complete = { _, _ ->
                    callbackCount += 1
                    true
                },
            ),
        )
        assertEquals(0, callbackCount)
    }

    @Test
    fun `invalid or precommit journal is refused without cleanup`() {
        var callbackCount = 0
        val validate: (String, Long) -> Unit = { _, _ -> callbackCount += 1 }
        val complete: (String, Long) -> Boolean = { _, _ ->
            callbackCount += 1
            true
        }

        assertFalse(
            ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
                journalRead = ColdRestoreJournalReadResult.Invalid(
                    readFailure = ColdRestoreJournalReadFailure.MALFORMED_JSON,
                ),
                validateInstalled = validate,
                complete = complete,
            ),
        )
        assertFalse(
            ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
                journalRead = ColdRestoreJournalReadResult.Valid(
                    terminalJournal(ColdRestorePhase.REBUILD_REQUIRED).copy(
                        phase = ColdRestorePhase.SWAP_COMMITTED,
                    ),
                ),
                validateInstalled = validate,
                complete = complete,
            ),
        )
        assertEquals(0, callbackCount)
    }

    @Test
    fun `rebuild required validates exact authority before flags-off cleanup`() {
        val calls = mutableListOf<String>()
        val journal = terminalJournal(ColdRestorePhase.REBUILD_REQUIRED)

        assertTrue(
            ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
                journalRead = ColdRestoreJournalReadResult.Valid(journal),
                validateInstalled = { streamId, headSeq ->
                    calls += "validate:$streamId:$headSeq"
                },
                complete = { streamId, headSeq ->
                    calls += "complete:$streamId:$headSeq"
                    true
                },
            ),
        )
        assertEquals(
            listOf(
                "validate:${journal.mainStream.streamId}:${journal.mainStream.headSeq}",
                "complete:${journal.mainStream.streamId}:${journal.mainStream.headSeq}",
            ),
            calls,
        )
    }

    @Test
    fun `complete journal is durable authorization without revalidating mutable live head`() {
        val calls = mutableListOf<String>()
        val journal = terminalJournal(ColdRestorePhase.COMPLETE)

        assertTrue(
            ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
                journalRead = ColdRestoreJournalReadResult.Valid(journal),
                validateInstalled = { _, _ -> calls += "validate" },
                complete = { _, _ ->
                    calls += "complete"
                    true
                },
            ),
        )
        assertEquals(listOf("complete"), calls)
    }

    @Test
    fun `installed validation failure is fail closed and never invokes cleanup`() {
        var cleanupCalled = false

        assertFalse(
            ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
                journalRead = ColdRestoreJournalReadResult.Valid(
                    terminalJournal(ColdRestorePhase.REBUILD_REQUIRED),
                ),
                validateInstalled = { _, _ -> error("installed authority mismatch") },
                complete = { _, _ ->
                    cleanupCalled = true
                    true
                },
            ),
        )
        assertFalse(cleanupCalled)
    }

    @Test
    fun `cleanup refusal is propagated so the journal remains retryable`() {
        assertFalse(
            ColdRestoreStartupCoordinator.finalizeDisabledDerivedState(
                journalRead = ColdRestoreJournalReadResult.Valid(
                    terminalJournal(ColdRestorePhase.REBUILD_REQUIRED),
                ),
                validateInstalled = { _, _ -> Unit },
                complete = { _, _ -> false },
            ),
        )
    }

    private fun terminalJournal(phase: ColdRestorePhase): ColdRestoreJournalV1 {
        require(phase == ColdRestorePhase.REBUILD_REQUIRED || phase == ColdRestorePhase.COMPLETE)
        return ColdRestoreJournalV1.staged(
            requestId = "0123456789abcdef0123456789abcdef",
            components = listOf(BackupArchiveComponent.DATABASE),
            archiveSize = 2_048L,
            archiveSha256 = "a".repeat(64),
            mainDatabaseSize = 1_024L,
            mainDatabaseSha256 = "b".repeat(64),
            mainStream = BackupAuthorityStreamV1(
                streamId = "00000000-0000-0000-0000-000000000001",
                headSeq = 7L,
            ),
            createdAtMs = 100L,
        ).copy(
            stateVersion = phase.ordinal.toLong(),
            phase = phase,
            preparedDatabaseSize = 1_024L,
            preparedDatabaseSha256 = "c".repeat(64),
            updatedAtMs = 100L + phase.ordinal,
            learningQuarantineId = "0011223344556677",
            mainQuarantineId = "8899aabbccddeeff",
        ).also { journal ->
            check(ColdRestoreJournalCodec.validate(journal) == null)
        }
    }
}
