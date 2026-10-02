package me.rerere.rikkahub.data.sync.backup.restore

import java.io.File
import java.nio.file.Files
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeNoException
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class LearningRestoreQuarantineTest {
    @get:Rule
    val temporaryFolder = TemporaryFolder()

    @Test
    fun traversalLikeDatabasePathIsRejected() {
        val appData = temporaryFolder.newFolder("app-traversal")
        File(appData, "databases").mkdir()
        val escaped = File(appData, "databases/../outside/${LEARNING_DATABASE_FILE_NAME}").absoluteFile

        val result = LearningOwnedDatabasePaths.verify(appData.absoluteFile, escaped)

        assertEquals(
            LearningOwnedDatabasePathValidation.Invalid(
                LearningOwnedDatabasePathFailure.DATABASE_PATH_NOT_EXACT,
            ),
            result,
        )
    }

    @Test
    fun symbolicLinkAtExactDatabaseNameIsRejectedWhenPlatformSupportsLinks() {
        val appData = temporaryFolder.newFolder("app-link")
        val databases = File(appData, "databases").apply { mkdir() }
        val outside = temporaryFolder.newFile("outside.db").apply { writeBytes(byteArrayOf(1)) }
        val link = File(databases, LEARNING_DATABASE_FILE_NAME).toPath()
        try {
            Files.createSymbolicLink(link, outside.toPath())
        } catch (error: Exception) {
            assumeNoException("Symbolic links unavailable on this test host", error)
        }

        val result = LearningOwnedDatabasePaths.verify(appData.absoluteFile, link.toFile().absoluteFile)

        assertEquals(
            LearningOwnedDatabasePathValidation.Invalid(
                LearningOwnedDatabasePathFailure.DATABASE_FILE_SYMBOLIC_LINK,
            ),
            result,
        )
    }

    @Test
    fun exactMainWalAndShmAreAtomicallyRenamedIntoOneUniqueBatch() {
        val fixture = ownedPathsFixture("app-three-files")
        val contents = mapOf(
            LEARNING_DATABASE_FILE_NAME to byteArrayOf(1),
            "${LEARNING_DATABASE_FILE_NAME}-wal" to byteArrayOf(2),
            "${LEARNING_DATABASE_FILE_NAME}-shm" to byteArrayOf(3),
        )
        contents.forEach { (name, bytes) -> File(fixture.databases, name).writeBytes(bytes) }
        val quarantine = LearningRestoreQuarantine(
            paths = fixture.paths,
            idSource = LearningQuarantineIdSource { "0123456789abcdef" },
        )

        val batch = quarantine.quarantineExactFiles()

        assertEquals(3, batch.fileCount)
        contents.forEach { (name, bytes) ->
            assertFalse(File(fixture.databases, name).exists())
            val moved = batch.quarantinedFiles.single { it.fileName.toString() == name }
            assertTrue(Files.exists(moved))
            assertTrue(bytes.contentEquals(Files.readAllBytes(moved)))
        }
        assertFalse(batch.toString().contains(fixture.appData.absolutePath))

        val cleanup = quarantine.cleanupAfterNewTimelineBootstrapSucceeded()
        assertEquals(LearningQuarantineCleanupSummary(1, 0, 3), cleanup)
        assertFalse(batch.directory!!.toFile().exists())
    }

    @Test
    fun cleanupRetainsBatchWithAnyUnexpectedFile() {
        val fixture = ownedPathsFixture("app-unexpected")
        File(fixture.databases, LEARNING_DATABASE_FILE_NAME).writeBytes(byteArrayOf(1))
        val quarantine = LearningRestoreQuarantine(
            paths = fixture.paths,
            idSource = LearningQuarantineIdSource { "fedcba9876543210" },
        )
        val batch = quarantine.quarantineExactFiles()
        File(batch.directory!!.toFile(), "not-owned.txt").writeText("do not delete")

        val cleanup = quarantine.cleanupAfterNewTimelineBootstrapSucceeded()

        assertEquals(LearningQuarantineCleanupSummary(0, 1, 0), cleanup)
        assertTrue(batch.quarantinedFiles.single().toFile().exists())
        assertTrue(File(batch.directory!!.toFile(), "not-owned.txt").exists())
    }

    private fun ownedPathsFixture(name: String): OwnedPathsFixture {
        val appData = temporaryFolder.newFolder(name)
        val databases = File(appData, "databases").apply { mkdir() }
        val validation = LearningOwnedDatabasePaths.verify(
            appData.absoluteFile,
            File(databases, LEARNING_DATABASE_FILE_NAME).absoluteFile,
        )
        assertTrue(validation is LearningOwnedDatabasePathValidation.Valid)
        return OwnedPathsFixture(
            appData = appData,
            databases = databases,
            paths = (validation as LearningOwnedDatabasePathValidation.Valid).paths,
        )
    }

    private data class OwnedPathsFixture(
        val appData: File,
        val databases: File,
        val paths: LearningOwnedDatabasePaths,
    )
}
