package me.rerere.rikkahub.data.sync.backup.restore

import java.io.File
import java.security.MessageDigest
import java.util.UUID
import kotlin.io.path.createTempDirectory
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class ColdRestoreSwapExecutorTest {
    @Test
    fun `every injected process kill resumes without exposing old Learning timeline`() {
        for (point in ColdRestoreCrashPoint.entries) {
            val fixture = fixture()
            try {
                var killed = false
                val first = executor(fixture, ColdRestoreCrashInjector { observed ->
                    if (!killed && observed == point) {
                        killed = true
                        throw SimulatedProcessKill()
                    }
                })
                try {
                    first.execute()
                } catch (_: SimulatedProcessKill) {
                    // The journal/files are intentionally left exactly at the kill point.
                }
                assertTrue("crash point was not reached: $point", killed)

                val resumed = executor(fixture).execute()
                assertTrue(resumed is ColdRestoreSwapResult.RebuildRequired)
                assertArrayEquals(fixture.newMain, fixture.main.readBytes())
                assertFalse(fixture.learning.exists())
                assertTrue(fixture.unrelated.readText() == "keep")
            } finally {
                fixture.root.deleteRecursively()
            }
        }
    }

    @Test
    fun `post-boundary validation failure stays degraded and never restores old Learning`() {
        val fixture = fixture()
        try {
            var calls = 0
            val result = ColdRestoreSwapExecutor(
                stagingPaths = fixture.staging,
                bootstrapPaths = fixture.bootstrap,
                learningPaths = fixture.learningPaths,
                validator = ColdRestorePreparedDatabaseValidator { _, _ ->
                    calls += 1
                    if (calls == 2) error("installed validation failed")
                },
                clockMs = { 20L },
            ).execute()

            assertTrue(result is ColdRestoreSwapResult.DegradedRestartRequired)
            assertFalse(fixture.learning.exists())
            val journal = ColdRestoreJournalStore(fixture.pending).read()
            assertTrue(journal is ColdRestoreJournalReadResult.Valid)
            assertTrue((journal as ColdRestoreJournalReadResult.Valid).journal.phase ==
                ColdRestorePhase.FAILED_RESTART_REQUIRED)
        } finally {
            fixture.root.deleteRecursively()
        }
    }

    private fun executor(
        fixture: Fixture,
        crashInjector: ColdRestoreCrashInjector = ColdRestoreCrashInjector { },
    ) = ColdRestoreSwapExecutor(
        stagingPaths = fixture.staging,
        bootstrapPaths = fixture.bootstrap,
        learningPaths = fixture.learningPaths,
        validator = ColdRestorePreparedDatabaseValidator { file, _ ->
            check(file.readBytes().contentEquals(fixture.newMain))
        },
        clockMs = { 20L },
        crashInjector = crashInjector,
    )

    private fun fixture(): Fixture {
        val root = createTempDirectory("cold-swap-").toFile()
        val databases = root.resolve("databases").apply { mkdir() }
        val noBackup = root.resolve("no_backup").apply { mkdir() }
        val main = databases.resolve("rikka_hub").apply { writeBytes(ByteArray(512) { 1 }) }
        databases.resolve("rikka_hub-wal").writeBytes(byteArrayOf(2))
        databases.resolve("rikka_hub-shm").writeBytes(byteArrayOf(3))
        val learning = databases.resolve(LEARNING_DATABASE_FILE_NAME).apply {
            writeBytes(byteArrayOf(4))
        }
        databases.resolve("${LEARNING_DATABASE_FILE_NAME}-wal").writeBytes(byteArrayOf(5))
        databases.resolve("${LEARNING_DATABASE_FILE_NAME}-shm").writeBytes(byteArrayOf(6))
        val unrelated = databases.resolve("unrelated.txt").apply { writeText("keep") }

        val staging = ColdRestoreStagingPaths.verify(root, noBackup)
        val validStaging = staging as ColdRestoreStagingPathValidation.Valid
        validStaging.paths.rootDirectory.toFile().mkdir()
        validStaging.paths.lockFile.toFile().createNewFile()
        val bootstrap = ColdRestoreBootstrapPaths.verify(root, main)
        val validBootstrap = bootstrap as ColdRestoreBootstrapPathValidation.Valid
        val learningPaths = LearningOwnedDatabasePaths.verify(root, learning)
        val requestId = "0123456789abcdef0123456789abcdef"
        val newMain = ByteArray(512) { 9 }
        val prepared = validBootstrap.paths.preparedDatabase(requestId).toFile().apply {
            writeBytes(newMain)
        }
        val stream = BackupAuthorityStreamV1(UUID.randomUUID().toString(), 1L)
        val staged = ColdRestoreJournalV1.staged(
            requestId = requestId,
            components = listOf(BackupArchiveComponent.DATABASE),
            archiveSize = 1024L,
            archiveSha256 = "a".repeat(64),
            mainDatabaseSize = newMain.size.toLong(),
            mainDatabaseSha256 = sha256(newMain),
            mainStream = stream,
            createdAtMs = 10L,
        )
        val store = ColdRestoreJournalStore(validStaging.paths.pendingJournal)
        check(store.create(staged) == ColdRestoreJournalWriteResult.Written)
        val ready = staged.copy(
            stateVersion = 1L,
            phase = ColdRestorePhase.READY_TO_SWAP,
            updatedAtMs = 11L,
            preparedDatabaseSize = newMain.size.toLong(),
            preparedDatabaseSha256 = sha256(newMain),
        )
        check(store.transition(requestId, 0L, ready) == ColdRestoreJournalWriteResult.Written)
        return Fixture(
            root = root,
            main = main,
            learning = learning,
            unrelated = unrelated,
            pending = validStaging.paths.pendingJournal,
            staging = staging,
            bootstrap = bootstrap,
            learningPaths = learningPaths,
            newMain = newMain,
        )
    }

    private data class Fixture(
        val root: File,
        val main: File,
        val learning: File,
        val unrelated: File,
        val pending: java.nio.file.Path,
        val staging: ColdRestoreStagingPathValidation,
        val bootstrap: ColdRestoreBootstrapPathValidation,
        val learningPaths: LearningOwnedDatabasePathValidation,
        val newMain: ByteArray,
    )

    private class SimulatedProcessKill : Error()
}

private fun sha256(bytes: ByteArray): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { byte ->
        (byte.toInt() and 0xff).toString(16).padStart(2, '0')
    }
