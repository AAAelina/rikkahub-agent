package me.rerere.rikkahub.data.sync.backup.restore

import android.content.Context
import java.io.File
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponentRestorer
import me.rerere.rikkahub.data.sync.backup.BackupArchiveV1FileIO
import me.rerere.rikkahub.utils.JsonInstant

/** Cold-start only: no application reader can see a partially replayed component set.
 * A crash before the receipt replays the same frozen archive; after it, settings are never reset.
 * The phase ordinals and immutable archive authority are deliberately unchanged. */
internal object ColdRestoreComponents {
    const val RECEIPT = "components.applied"
    const val PART = "components.applied.part"

    fun settingsStore(context: Context): SettingsStore =
        SettingsStore(context, AppScope().apply { cancel() })

    fun applyBeforeGraph(context: Context, settings: () -> SettingsStore): Boolean {
        val paths = (ColdRestoreStagingPaths.verify(
            File(context.applicationInfo.dataDir), context.noBackupFilesDir,
        ) as? ColdRestoreStagingPathValidation.Valid)?.paths ?: return false
        return runCatching {
            FileChannel.open(paths.lockFile, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS).use { channel ->
                val lock = try { channel.tryLock() } catch (_: OverlappingFileLockException) { null }
                    ?: return false
                lock.use {
                    val journal = (ColdRestoreJournalStore(paths.pendingJournal).read()
                        as? ColdRestoreJournalReadResult.Valid)?.journal ?: return false
                    if (!journal.deferredComponents || journal.phase == ColdRestorePhase.COMPLETE) return true
                    if (journal.phase != ColdRestorePhase.REBUILD_REQUIRED) return false
                    if (applied(paths, journal)) return true
                    val directory = paths.requestDirectory(journal.requestId)
                    check(!Files.isSymbolicLink(directory) && directory.toFile().canonicalFile.toPath() == directory)
                    val archive = BackupArchiveV1FileIO.inspectForRestore(paths.stagedArchive(journal.requestId).toFile())
                    check(archive.archiveSize == journal.archiveSize && archive.archiveSha256 == journal.archiveSha256)
                    check(archive.manifest.mainStream == journal.mainStream)
                    check(archive.manifest.components.sortedBy { it.ordinal } == journal.components)
                    val restorer = BackupArchiveComponentRestorer(settings(), JsonInstant, context)
                    runBlocking(Dispatchers.IO) {
                        if (journal.restoreFiles) restorer.restoreFiles(archive)
                        restorer.restoreSettingsIfPresent(archive)
                    }
                    val partial = directory.resolve(PART)
                    check(!Files.isSymbolicLink(partial))
                    FileChannel.open(partial, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                        StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS).use {
                        val bytes = java.nio.ByteBuffer.wrap(receipt(journal))
                        while (bytes.hasRemaining()) it.write(bytes)
                        it.force(true)
                    }
                    Files.move(partial, directory.resolve(RECEIPT), StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING)
                    applied(paths, journal)
                }
            }
        }.getOrDefault(false)
    }

    fun applied(paths: ColdRestoreStagingPaths, journal: ColdRestoreJournalV1): Boolean {
        if (!journal.deferredComponents) return true
        val directory = paths.requestDirectory(journal.requestId)
        if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) ||
            directory.toFile().canonicalFile.toPath() != directory) return false
        val marker = directory.resolve(RECEIPT)
        val expected = receipt(journal)
        return !Files.isSymbolicLink(marker) && Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS) &&
            Files.size(marker) == expected.size.toLong() && Files.readAllBytes(marker).contentEquals(expected)
    }

    private fun receipt(journal: ColdRestoreJournalV1) =
        "${journal.requestId}:${journal.archiveSha256}:${journal.restoreFiles}".toByteArray(Charsets.UTF_8)
}
