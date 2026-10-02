package me.rerere.rikkahub.data.sync.backup.restore

import java.io.File
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_FORMAT_VERSION
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_MANIFEST_ENTRY
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupArchiveEntryV1
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestCodec
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestV1
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import org.junit.Assert.assertArrayEquals
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class ColdRestoreBootstrapTest {
    @get:Rule
    val temporaryFolder = TemporaryFolder()

    @Test
    fun prepareCommitsReadyWithoutTouchingAnyLiveDatabaseFile() {
        val fixture = fixture("ready")
        val reconcileCalls = AtomicInteger()
        val validateCalls = AtomicInteger()
        val bootstrap = bootstrap(
            fixture = fixture,
            reconciler = ColdRestorePreparedDatabaseReconciler { candidate, stream ->
                assertLiveFilesUnchanged(fixture)
                assertArrayEquals(fixture.databaseBytes, candidate.readBytes())
                assertEquals(fixture.mainStream, stream)
                reconcileCalls.incrementAndGet()
            },
            validator = ColdRestorePreparedDatabaseValidator { candidate, stream ->
                assertArrayEquals(fixture.databaseBytes, candidate.readBytes())
                assertEquals(fixture.mainStream, stream)
                validateCalls.incrementAndGet()
            },
        )

        val result = bootstrap.prepare()

        assertTrue(result is ColdRestoreBootstrapResult.ReadyToSwap)
        result as ColdRestoreBootstrapResult.ReadyToSwap
        assertEquals(REQUEST_ID, result.requestId)
        assertArrayEquals(fixture.databaseBytes, result.preparedDatabase.readBytes())
        assertLiveFilesUnchanged(fixture)
        assertEquals(1, reconcileCalls.get())
        assertEquals(1, validateCalls.get())
        val journal = readJournal(fixture)
        assertEquals(ColdRestorePhase.READY_TO_SWAP, journal.phase)
        assertEquals(1L, journal.stateVersion)
        assertEquals(result.preparedDatabase.length(), requireNotNull(journal.preparedDatabaseSize))
        assertEquals(sha256(result.preparedDatabase.readBytes()), journal.preparedDatabaseSha256)
        assertNull(journal.learningQuarantineId)
        assertNull(journal.mainQuarantineId)
    }

    @Test
    fun readyJournalResumesIdempotentlyAfterProcessCrash() {
        val fixture = fixture("resume")
        val calls = AtomicInteger()
        val first = bootstrap(
            fixture,
            ColdRestorePreparedDatabaseReconciler { _, _ -> calls.incrementAndGet() },
            ColdRestorePreparedDatabaseValidator { _, _ -> Unit },
        ).prepare()
        assertTrue(first is ColdRestoreBootstrapResult.ReadyToSwap)

        val resumed = bootstrap(
            fixture,
            ColdRestorePreparedDatabaseReconciler { _, _ -> calls.incrementAndGet() },
            ColdRestorePreparedDatabaseValidator { candidate, _ ->
                assertArrayEquals(fixture.databaseBytes, candidate.readBytes())
            },
        ).prepare()

        assertTrue(resumed is ColdRestoreBootstrapResult.ReadyToSwap)
        assertEquals(2, calls.get())
        assertEquals(ColdRestorePhase.READY_TO_SWAP, readJournal(fixture).phase)
        assertLiveFilesUnchanged(fixture)
    }

    @Test
    fun readyCandidateDigestMismatchStopsResumeWithoutTouchingLiveFiles() {
        val fixture = fixture("ready-tamper")
        val first = bootstrap(
            fixture,
            ColdRestorePreparedDatabaseReconciler { _, _ -> Unit },
            ColdRestorePreparedDatabaseValidator { _, _ -> Unit },
        ).prepare()
        assertTrue(first is ColdRestoreBootstrapResult.ReadyToSwap)
        first as ColdRestoreBootstrapResult.ReadyToSwap
        val changed = first.preparedDatabase.readBytes().apply {
            this[lastIndex] = (this[lastIndex].toInt() xor 1).toByte()
        }
        first.preparedDatabase.writeBytes(changed)

        val resumed = bootstrap(
            fixture,
            ColdRestorePreparedDatabaseReconciler { _, _ -> error("must not reconcile") },
            ColdRestorePreparedDatabaseValidator { _, _ -> error("must not validate") },
        ).prepare()

        assertEquals(
            ColdRestoreBootstrapResult.Failed(
                ColdRestoreBootstrapFailure.DATABASE_VALIDATION_FAILED,
            ),
            resumed,
        )
        assertEquals(ColdRestorePhase.READY_TO_SWAP, readJournal(fixture).phase)
        assertTrue(first.preparedDatabase.isFile)
        assertLiveFilesUnchanged(fixture)
    }

    @Test
    fun staleUncommittedPreparedFileIsExactlyRebuilt() {
        val fixture = fixture("stale")
        val prepared = fixture.bootstrapPaths.preparedDatabase(REQUEST_ID).toFile()
        prepared.writeText("crash-before-journal-commit")

        val result = bootstrap(
            fixture,
            ColdRestorePreparedDatabaseReconciler { _, _ -> Unit },
            ColdRestorePreparedDatabaseValidator { candidate, _ ->
                assertArrayEquals(fixture.databaseBytes, candidate.readBytes())
            },
        ).prepare()

        assertTrue(result is ColdRestoreBootstrapResult.ReadyToSwap)
        assertArrayEquals(fixture.databaseBytes, prepared.readBytes())
        assertLiveFilesUnchanged(fixture)
    }

    @Test
    fun stagedArchiveTamperFailsBeforeCandidateOrLiveMutation() {
        val fixture = fixture("tamper")
        fixture.stagedArchive.appendBytes(byteArrayOf(1, 2, 3))

        val result = bootstrap(
            fixture,
            ColdRestorePreparedDatabaseReconciler { _, _ -> error("must not reconcile") },
            ColdRestorePreparedDatabaseValidator { _, _ -> error("must not validate") },
        ).prepare()

        assertEquals(
            ColdRestoreBootstrapResult.Failed(
                ColdRestoreBootstrapFailure.STAGED_ARCHIVE_CHANGED,
            ),
            result,
        )
        assertEquals(ColdRestorePhase.STAGED, readJournal(fixture).phase)
        assertFalse(fixture.bootstrapPaths.preparedDatabase(REQUEST_ID).toFile().exists())
        assertLiveFilesUnchanged(fixture)
    }

    @Test
    fun reconciliationFailureDeletesOnlyPreparedArtifactsAndKeepsStagedRecoveryState() {
        val fixture = fixture("reconcile-failure")

        val result = bootstrap(
            fixture,
            ColdRestorePreparedDatabaseReconciler { _, _ ->
                throw IllegalStateException("refused")
            },
            ColdRestorePreparedDatabaseValidator { _, _ -> error("must not validate") },
        ).prepare()

        assertEquals(
            ColdRestoreBootstrapResult.Failed(
                ColdRestoreBootstrapFailure.DATABASE_RECONCILE_FAILED,
            ),
            result,
        )
        assertEquals(ColdRestorePhase.STAGED, readJournal(fixture).phase)
        assertFalse(fixture.bootstrapPaths.preparedDatabase(REQUEST_ID).toFile().exists())
        assertTrue(fixture.stagedArchive.isFile)
        assertLiveFilesUnchanged(fixture)
    }

    @Test
    fun absentStagingRootIsAConstantTimeNoOp() {
        val appData = temporaryFolder.newFolder("no-pending")
        val noBackup = File(appData, "no_backup").apply { mkdir() }
        val databases = File(appData, "databases").apply { mkdir() }
        val staging = ColdRestoreStagingPaths.verify(appData.absoluteFile, noBackup.absoluteFile)
        val main = File(databases, "rikka_hub")
        val paths = ColdRestoreBootstrapPaths.verify(appData.absoluteFile, main.absoluteFile)

        val result = ColdRestoreBootstrap(
            stagingPaths = staging,
            bootstrapPaths = paths,
            reconciler = ColdRestorePreparedDatabaseReconciler { _, _ -> error("must not run") },
            validator = ColdRestorePreparedDatabaseValidator { _, _ -> error("must not run") },
        ).prepare()

        assertEquals(ColdRestoreBootstrapResult.NoPendingRestore, result)
    }

    @Test
    fun bootstrapDatabasePathMustBeTheExactAppOwnedMainFile() {
        val appData = temporaryFolder.newFolder("wrong-main")
        val databases = File(appData, "databases").apply { mkdir() }

        val validation = ColdRestoreBootstrapPaths.verify(
            applicationDataDirectory = appData.absoluteFile,
            mainDatabaseFile = File(databases, "other.db").absoluteFile,
        )

        assertEquals(
            ColdRestoreBootstrapPathValidation.Invalid(
                ColdRestoreBootstrapPathFailure.MAIN_DATABASE_PATH_NOT_EXACT,
            ),
            validation,
        )
    }

    private fun fixture(name: String): Fixture {
        val appData = temporaryFolder.newFolder("app-$name")
        val noBackup = File(appData, "no_backup").apply { mkdir() }
        val databases = File(appData, "databases").apply { mkdir() }
        val liveFiles = listOf(
            "rikka_hub",
            "rikka_hub-wal",
            "rikka_hub-shm",
            "learning_runtime.db",
            "learning_runtime.db-wal",
            "learning_runtime.db-shm",
        ).associateWith { fileName ->
            File(databases, fileName).apply { writeBytes("live:$fileName".toByteArray()) }
        }
        val liveSnapshots = liveFiles.mapValues { (_, file) -> file.readBytes() }
        val databaseBytes = sqlitePage()
        val manifest = manifest(databaseBytes)
        val sourceArchive = temporaryFolder.newFile("$name.zip")
        writeArchive(sourceArchive, manifest, databaseBytes)

        val stagingValidation = ColdRestoreStagingPaths.verify(
            appData.absoluteFile,
            noBackup.absoluteFile,
        )
        assertTrue(stagingValidation is ColdRestoreStagingPathValidation.Valid)
        val verified = VerifiedColdRestoreArchive.verify(
            archiveFile = sourceArchive.absoluteFile,
            archiveSize = sourceArchive.length(),
            archiveSha256 = sha256(sourceArchive.readBytes()),
            manifest = manifest,
        )
        assertTrue(verified is VerifiedColdRestoreArchiveResult.Verified)
        val staged = ColdRestoreArchiveStager(
            pathValidation = stagingValidation,
            requestIdSource = ColdRestoreRequestIdSource { REQUEST_ID },
            clockMs = { 10L },
        ).stage((verified as VerifiedColdRestoreArchiveResult.Verified).archive)
        assertTrue(staged is ColdRestoreStageResult.Staged)

        val bootstrapValidation = ColdRestoreBootstrapPaths.verify(
            applicationDataDirectory = appData.absoluteFile,
            mainDatabaseFile = requireNotNull(liveFiles["rikka_hub"]).absoluteFile,
        )
        assertTrue(bootstrapValidation is ColdRestoreBootstrapPathValidation.Valid)
        val stagingPaths =
            (stagingValidation as ColdRestoreStagingPathValidation.Valid).paths
        return Fixture(
            stagingValidation = stagingValidation,
            bootstrapValidation = bootstrapValidation,
            bootstrapPaths =
                (bootstrapValidation as ColdRestoreBootstrapPathValidation.Valid).paths,
            stagedArchive = stagingPaths.stagedArchive(REQUEST_ID).toFile(),
            pendingJournal = stagingPaths.pendingJournal.toFile(),
            databaseBytes = databaseBytes,
            mainStream = requireNotNull(manifest.mainStream),
            liveFiles = liveFiles,
            liveSnapshots = liveSnapshots,
        )
    }

    private fun bootstrap(
        fixture: Fixture,
        reconciler: ColdRestorePreparedDatabaseReconciler,
        validator: ColdRestorePreparedDatabaseValidator,
    ) = ColdRestoreBootstrap(
        stagingPaths = fixture.stagingValidation,
        bootstrapPaths = fixture.bootstrapValidation,
        reconciler = reconciler,
        validator = validator,
        clockMs = { 20L },
    )

    private fun readJournal(fixture: Fixture): ColdRestoreJournalV1 {
        val result = ColdRestoreJournalStore(fixture.pendingJournal.toPath()).read()
        assertTrue(result is ColdRestoreJournalReadResult.Valid)
        return (result as ColdRestoreJournalReadResult.Valid).journal
    }

    private fun assertLiveFilesUnchanged(fixture: Fixture) {
        fixture.liveFiles.forEach { (name, file) ->
            assertArrayEquals(name, requireNotNull(fixture.liveSnapshots[name]), file.readBytes())
        }
    }

    private fun manifest(database: ByteArray) = BackupArchiveManifestV1(
        formatVersion = BACKUP_ARCHIVE_FORMAT_VERSION,
        learningDbExcluded = true,
        components = listOf(BackupArchiveComponent.DATABASE),
        mainStream = BackupAuthorityStreamV1(
            streamId = "00000000-0000-0000-0000-000000000001",
            headSeq = 1L,
        ),
        entries = mapOf(
            BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY to BackupArchiveEntryV1(
                size = database.size.toLong(),
                sha256 = sha256(database),
            ),
        ),
    )

    private fun writeArchive(
        file: File,
        manifest: BackupArchiveManifestV1,
        database: ByteArray,
    ) {
        ZipOutputStream(file.outputStream().buffered()).use { zip ->
            zip.putNextEntry(ZipEntry(BACKUP_ARCHIVE_MANIFEST_ENTRY))
            zip.write(BackupArchiveManifestCodec.encode(manifest))
            zip.closeEntry()
            zip.putNextEntry(ZipEntry(BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY))
            zip.write(database)
            zip.closeEntry()
        }
    }

    private fun sqlitePage(): ByteArray = ByteArray(512).apply {
        "SQLite format 3\u0000".toByteArray(Charsets.US_ASCII).copyInto(this)
        this[16] = 2
        this[17] = 0
        this[18] = 1
        this[19] = 1
    }

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { byte ->
            (byte.toInt() and 0xff).toString(16).padStart(2, '0')
        }

    private data class Fixture(
        val stagingValidation: ColdRestoreStagingPathValidation,
        val bootstrapValidation: ColdRestoreBootstrapPathValidation,
        val bootstrapPaths: ColdRestoreBootstrapPaths,
        val stagedArchive: File,
        val pendingJournal: File,
        val databaseBytes: ByteArray,
        val mainStream: BackupAuthorityStreamV1,
        val liveFiles: Map<String, File>,
        val liveSnapshots: Map<String, ByteArray>,
    )

    private companion object {
        const val REQUEST_ID = "0123456789abcdef0123456789abcdef"
    }
}
