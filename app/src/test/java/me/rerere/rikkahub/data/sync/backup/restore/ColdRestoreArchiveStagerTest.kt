package me.rerere.rikkahub.data.sync.backup.restore

import java.io.File
import java.security.MessageDigest
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_FORMAT_VERSION
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupArchiveEntryV1
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestV1
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class ColdRestoreArchiveStagerTest {
    @get:Rule
    val temporaryFolder = TemporaryFolder()

    @Test
    fun verifiedArchiveIsPrivatelyCopiedBeforePendingMarkerCommits() {
        val fixture = fixture("success")
        val databaseSentinel = File(fixture.databases, "must_not_change").apply {
            writeText("live")
        }
        val stager = ColdRestoreArchiveStager(
            pathValidation = fixture.pathValidation,
            requestIdSource = ColdRestoreRequestIdSource {
                "0123456789abcdef0123456789abcdef"
            },
            clockMs = { 123L },
        )

        val result = stager.stage(fixture.verifiedArchive)

        assertTrue(result is ColdRestoreStageResult.Staged)
        val paths = (fixture.pathValidation as ColdRestoreStagingPathValidation.Valid).paths
        val staged = paths.stagedArchive("0123456789abcdef0123456789abcdef")
        assertTrue(staged.toFile().isFile)
        assertTrue(fixture.source.readBytes().contentEquals(staged.toFile().readBytes()))
        assertEquals("live", databaseSentinel.readText())

        val journal = ColdRestoreJournalStore(paths.pendingJournal).read()
        assertTrue(journal is ColdRestoreJournalReadResult.Valid)
        journal as ColdRestoreJournalReadResult.Valid
        assertEquals(ColdRestorePhase.STAGED, journal.journal.phase)
        assertEquals(0L, journal.journal.stateVersion)
        assertEquals(fixture.source.length(), journal.journal.archiveSize)
        assertEquals(123L, journal.journal.createdAtMs)
        assertFalse(paths.pendingJournal.toFile().readText().contains(fixture.source.absolutePath))
    }

    @Test
    fun sourceDigestChangeLeavesNoPendingMarkerOrPartialRequest() {
        val fixture = fixture("digest")
        val forged = verifiedArchive(
            source = fixture.source,
            archiveSha256 = "f".repeat(64),
        )
        val stager = ColdRestoreArchiveStager(
            pathValidation = fixture.pathValidation,
            requestIdSource = ColdRestoreRequestIdSource {
                "11111111111111111111111111111111"
            },
            clockMs = { 1L },
        )

        val result = stager.stage(forged)

        assertEquals(
            ColdRestoreStageResult.Rejected(
                ColdRestoreStageFailure.ARCHIVE_CHANGED_AFTER_PREFLIGHT,
            ),
            result,
        )
        val paths = (fixture.pathValidation as ColdRestoreStagingPathValidation.Valid).paths
        assertFalse(paths.pendingJournal.toFile().exists())
        assertFalse(paths.requestDirectory("11111111111111111111111111111111").toFile().exists())
    }

    @Test
    fun anExistingPendingRestoreCannotBeOverwritten() {
        val fixture = fixture("pending")
        val ids = listOf(
            "22222222222222222222222222222222",
            "33333333333333333333333333333333",
        ).iterator()
        val stager = ColdRestoreArchiveStager(
            pathValidation = fixture.pathValidation,
            requestIdSource = ColdRestoreRequestIdSource { ids.next() },
            clockMs = { 2L },
        )

        assertTrue(stager.stage(fixture.verifiedArchive) is ColdRestoreStageResult.Staged)
        assertEquals(ColdRestoreStageResult.PendingRestoreExists, stager.stage(fixture.verifiedArchive))

        val paths = (fixture.pathValidation as ColdRestoreStagingPathValidation.Valid).paths
        assertFalse(paths.requestDirectory("33333333333333333333333333333333").toFile().exists())
    }

    @Test
    fun stagingRootMustBeAnExactChildOfAppOwnedNoBackupDirectory() {
        val appData = temporaryFolder.newFolder("app-escaped")
        val escaped = temporaryFolder.newFolder("not-app-owned")

        val validation = ColdRestoreStagingPaths.verify(appData.absoluteFile, escaped.absoluteFile)

        assertEquals(
            ColdRestoreStagingPathValidation.Invalid(
                ColdRestoreStagingPathFailure.NO_BACKUP_PATH_NOT_APP_OWNED,
            ),
            validation,
        )
    }

    @Test
    fun invalidRequestIdCannotCreateAnyRequestDirectory() {
        val fixture = fixture("request-id")
        val stager = ColdRestoreArchiveStager(
            pathValidation = fixture.pathValidation,
            requestIdSource = ColdRestoreRequestIdSource { "../escape" },
            clockMs = { 3L },
        )

        assertEquals(
            ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.REQUEST_ID_INVALID),
            stager.stage(fixture.verifiedArchive),
        )
        val paths = (fixture.pathValidation as ColdRestoreStagingPathValidation.Valid).paths
        assertFalse(paths.pendingJournal.toFile().exists())
        assertEquals(
            emptyList<File>(),
            paths.rootDirectory.toFile().listFiles().orEmpty()
                .filter { it.name.startsWith("request_") },
        )
    }

    private fun fixture(name: String): Fixture {
        val appData = temporaryFolder.newFolder("app-$name")
        val noBackup = File(appData, "no_backup").apply { mkdir() }
        val databases = File(appData, "databases").apply { mkdir() }
        val source = temporaryFolder.newFile("$name.zip").apply {
            writeBytes(ByteArray(4_096) { index -> (index % 251).toByte() })
        }
        val pathValidation = ColdRestoreStagingPaths.verify(
            appData.absoluteFile,
            noBackup.absoluteFile,
        )
        assertTrue(pathValidation is ColdRestoreStagingPathValidation.Valid)
        return Fixture(
            source = source,
            databases = databases,
            pathValidation = pathValidation,
            verifiedArchive = verifiedArchive(source),
        )
    }

    private fun verifiedArchive(
        source: File,
        archiveSha256: String = sha256(source.readBytes()),
    ): VerifiedColdRestoreArchive {
        val manifest = BackupArchiveManifestV1(
            formatVersion = BACKUP_ARCHIVE_FORMAT_VERSION,
            learningDbExcluded = true,
            components = listOf(BackupArchiveComponent.DATABASE),
            mainStream = BackupAuthorityStreamV1(
                streamId = "00000000-0000-0000-0000-000000000001",
                headSeq = 1L,
            ),
            entries = mapOf(
                BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY to BackupArchiveEntryV1(
                    size = 512L,
                    sha256 = "d".repeat(64),
                ),
            ),
        )
        val result = VerifiedColdRestoreArchive.verify(
            archiveFile = source.absoluteFile,
            archiveSize = source.length(),
            archiveSha256 = archiveSha256,
            manifest = manifest,
        )
        assertTrue(result is VerifiedColdRestoreArchiveResult.Verified)
        return (result as VerifiedColdRestoreArchiveResult.Verified).archive
    }

    private data class Fixture(
        val source: File,
        val databases: File,
        val pathValidation: ColdRestoreStagingPathValidation,
        val verifiedArchive: VerifiedColdRestoreArchive,
    )

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { byte ->
            (byte.toInt() and 0xff).toString(16).padStart(2, '0')
        }
}
