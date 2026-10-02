package me.rerere.rikkahub.data.sync.backup.restore

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.security.MessageDigest
import java.security.SecureRandom
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestCodec
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestV1
import me.rerere.rikkahub.data.sync.backup.MAX_BACKUP_ARCHIVE_BYTES
import me.rerere.rikkahub.data.sync.backup.isCanonicalBackupSha256

enum class ColdRestoreStagingPathFailure {
    APP_DATA_PATH_NOT_ABSOLUTE,
    APP_DATA_PATH_NOT_DIRECTORY,
    APP_DATA_PATH_SYMBOLIC_LINK,
    NO_BACKUP_PATH_NOT_ABSOLUTE,
    NO_BACKUP_PATH_NOT_DIRECTORY,
    NO_BACKUP_PATH_SYMBOLIC_LINK,
    NO_BACKUP_PATH_NOT_APP_OWNED,
    STAGING_ROOT_NOT_DIRECTORY,
    STAGING_ROOT_SYMBOLIC_LINK,
    PATH_CANONICALIZATION_FAILED,
}

sealed interface ColdRestoreStagingPathValidation {
    class Valid internal constructor(val paths: ColdRestoreStagingPaths) :
        ColdRestoreStagingPathValidation

    data class Invalid(val failure: ColdRestoreStagingPathFailure) :
        ColdRestoreStagingPathValidation
}

/** Exact app-private root used by staging and cold-start bootstrap. */
class ColdRestoreStagingPaths private constructor(
    internal val rootDirectory: Path,
    internal val pendingJournal: Path,
    internal val lockFile: Path,
) {
    internal fun requestDirectory(requestId: String): Path =
        rootDirectory.resolve("$REQUEST_DIRECTORY_PREFIX$requestId").normalize()

    internal fun stagedArchive(requestId: String): Path =
        requestDirectory(requestId).resolve(STAGED_ARCHIVE_NAME).normalize()

    override fun toString(): String = "ColdRestoreStagingPaths(paths=<redacted>)"

    companion object {
        fun verify(
            applicationDataDirectory: File,
            noBackupFilesDirectory: File,
        ): ColdRestoreStagingPathValidation {
            if (!applicationDataDirectory.isAbsolute) {
                return invalidPath(ColdRestoreStagingPathFailure.APP_DATA_PATH_NOT_ABSOLUTE)
            }
            if (!noBackupFilesDirectory.isAbsolute) {
                return invalidPath(ColdRestoreStagingPathFailure.NO_BACKUP_PATH_NOT_ABSOLUTE)
            }
            return try {
                val appData = applicationDataDirectory.toPath().toAbsolutePath().normalize()
                if (Files.isSymbolicLink(appData)) {
                    return invalidPath(ColdRestoreStagingPathFailure.APP_DATA_PATH_SYMBOLIC_LINK)
                }
                if (!Files.isDirectory(appData, LinkOption.NOFOLLOW_LINKS)) {
                    return invalidPath(ColdRestoreStagingPathFailure.APP_DATA_PATH_NOT_DIRECTORY)
                }
                if (appData.toFile().canonicalFile.toPath().normalize() != appData) {
                    return invalidPath(ColdRestoreStagingPathFailure.APP_DATA_PATH_SYMBOLIC_LINK)
                }

                val noBackup = noBackupFilesDirectory.toPath().toAbsolutePath().normalize()
                if (Files.isSymbolicLink(noBackup)) {
                    return invalidPath(ColdRestoreStagingPathFailure.NO_BACKUP_PATH_SYMBOLIC_LINK)
                }
                if (!Files.isDirectory(noBackup, LinkOption.NOFOLLOW_LINKS)) {
                    return invalidPath(ColdRestoreStagingPathFailure.NO_BACKUP_PATH_NOT_DIRECTORY)
                }
                if (noBackup.parent != appData) {
                    return invalidPath(ColdRestoreStagingPathFailure.NO_BACKUP_PATH_NOT_APP_OWNED)
                }
                if (noBackup.toFile().canonicalFile.toPath().normalize() != noBackup) {
                    return invalidPath(ColdRestoreStagingPathFailure.NO_BACKUP_PATH_SYMBOLIC_LINK)
                }

                val root = noBackup.resolve(STAGING_ROOT_NAME).normalize()
                if (root.parent != noBackup) {
                    return invalidPath(ColdRestoreStagingPathFailure.NO_BACKUP_PATH_NOT_APP_OWNED)
                }
                if (Files.exists(root, LinkOption.NOFOLLOW_LINKS)) {
                    if (Files.isSymbolicLink(root)) {
                        return invalidPath(ColdRestoreStagingPathFailure.STAGING_ROOT_SYMBOLIC_LINK)
                    }
                    if (!Files.isDirectory(root, LinkOption.NOFOLLOW_LINKS)) {
                        return invalidPath(ColdRestoreStagingPathFailure.STAGING_ROOT_NOT_DIRECTORY)
                    }
                    if (root.toFile().canonicalFile.toPath().normalize() != root) {
                        return invalidPath(ColdRestoreStagingPathFailure.STAGING_ROOT_SYMBOLIC_LINK)
                    }
                }
                ColdRestoreStagingPathValidation.Valid(
                    ColdRestoreStagingPaths(
                        rootDirectory = root,
                        pendingJournal = root.resolve(PENDING_JOURNAL_NAME),
                        lockFile = root.resolve(LOCK_FILE_NAME),
                    ),
                )
            } catch (_: Exception) {
                invalidPath(ColdRestoreStagingPathFailure.PATH_CANONICALIZATION_FAILED)
            }
        }
    }
}

enum class VerifiedColdRestoreArchiveFailure {
    ARCHIVE_PATH_NOT_ABSOLUTE,
    ARCHIVE_PATH_SYMBOLIC_LINK,
    ARCHIVE_NOT_REGULAR_FILE,
    ARCHIVE_UNREADABLE,
    ARCHIVE_SIZE_INVALID,
    ARCHIVE_CHECKSUM_INVALID,
    MANIFEST_INVALID,
    DATABASE_NOT_SELECTED,
}

sealed interface VerifiedColdRestoreArchiveResult {
    data class Verified(val archive: VerifiedColdRestoreArchive) :
        VerifiedColdRestoreArchiveResult

    data class Rejected(val failure: VerifiedColdRestoreArchiveFailure) :
        VerifiedColdRestoreArchiveResult
}

/**
 * A preflight-owned archive identity. Construction validates metadata and the current source path;
 * [ColdRestoreArchiveStager] independently rechecks every byte while taking private ownership.
 */
class VerifiedColdRestoreArchive private constructor(
    internal val archivePath: Path,
    val archiveSize: Long,
    val archiveSha256: String,
    val manifest: BackupArchiveManifestV1,
) {
    override fun toString(): String =
        "VerifiedColdRestoreArchive(bytes=$archiveSize, archive=<redacted>, digests=<redacted>)"

    companion object {
        fun verify(
            archiveFile: File,
            archiveSize: Long,
            archiveSha256: String,
            manifest: BackupArchiveManifestV1,
        ): VerifiedColdRestoreArchiveResult {
            if (!archiveFile.isAbsolute || archiveFile.path.isBlank()) {
                return rejectedArchive(
                    VerifiedColdRestoreArchiveFailure.ARCHIVE_PATH_NOT_ABSOLUTE,
                )
            }
            val path = archiveFile.toPath().toAbsolutePath().normalize()
            val canonicalMatches = runCatching {
                path.toFile().canonicalFile.toPath().normalize() == path
            }.getOrDefault(false)
            if (Files.isSymbolicLink(path) || !canonicalMatches) {
                return rejectedArchive(
                    VerifiedColdRestoreArchiveFailure.ARCHIVE_PATH_SYMBOLIC_LINK,
                )
            }
            if (!Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
                return rejectedArchive(VerifiedColdRestoreArchiveFailure.ARCHIVE_NOT_REGULAR_FILE)
            }
            if (!Files.isReadable(path)) {
                return rejectedArchive(VerifiedColdRestoreArchiveFailure.ARCHIVE_UNREADABLE)
            }
            if (archiveSize <= 0L || archiveSize > MAX_BACKUP_ARCHIVE_BYTES ||
                runCatching { Files.size(path) }.getOrDefault(-1L) != archiveSize
            ) {
                return rejectedArchive(VerifiedColdRestoreArchiveFailure.ARCHIVE_SIZE_INVALID)
            }
            if (!isCanonicalBackupSha256(archiveSha256)) {
                return rejectedArchive(VerifiedColdRestoreArchiveFailure.ARCHIVE_CHECKSUM_INVALID)
            }
            if (BackupArchiveManifestCodec.validate(manifest) != null) {
                return rejectedArchive(VerifiedColdRestoreArchiveFailure.MANIFEST_INVALID)
            }
            if (BackupArchiveComponent.DATABASE !in manifest.components) {
                return rejectedArchive(VerifiedColdRestoreArchiveFailure.DATABASE_NOT_SELECTED)
            }
            return VerifiedColdRestoreArchiveResult.Verified(
                VerifiedColdRestoreArchive(path, archiveSize, archiveSha256, manifest),
            )
        }
    }
}

enum class ColdRestoreStageFailure {
    STAGING_PATH_UNSAFE,
    STAGING_PATH_CHANGED,
    SOURCE_INSIDE_STAGING,
    REQUEST_ID_INVALID,
    REQUEST_DIRECTORY_COLLISION,
    ARCHIVE_CHANGED_AFTER_PREFLIGHT,
    ARCHIVE_TOO_LARGE,
    ATOMIC_MOVE_UNSUPPORTED,
    JOURNAL_REJECTED,
    JOURNAL_DURABILITY_FAILED,
    STORAGE_IO_FAILED,
}

sealed interface ColdRestoreStageResult {
    data class Staged(val ticket: ColdRestoreTicket) : ColdRestoreStageResult

    data object PendingRestoreExists : ColdRestoreStageResult

    data object Busy : ColdRestoreStageResult

    data class Rejected(val failure: ColdRestoreStageFailure) : ColdRestoreStageResult
}

class ColdRestoreTicket internal constructor(val requestId: String) {
    override fun toString(): String = "ColdRestoreTicket(request=<redacted>)"
}

fun interface ColdRestoreRequestIdSource {
    fun nextId(): String
}

/**
 * Takes durable ownership of an already verified archive without touching either Room graph.
 * The pending journal is the final commit point and is never written before the private archive
 * has passed an independent byte-count and SHA-256 check.
 */
class ColdRestoreArchiveStager(
    private val pathValidation: ColdRestoreStagingPathValidation,
    private val requestIdSource: ColdRestoreRequestIdSource =
        ColdRestoreRequestIdSource { newRequestId() },
    private val clockMs: () -> Long = System::currentTimeMillis,
) {
    fun stage(
        archive: VerifiedColdRestoreArchive,
        deferComponents: Boolean = false,
        restoreFiles: Boolean = false,
    ): ColdRestoreStageResult {
        val paths = when (pathValidation) {
            is ColdRestoreStagingPathValidation.Invalid -> {
                return ColdRestoreStageResult.Rejected(
                    ColdRestoreStageFailure.STAGING_PATH_UNSAFE,
                )
            }
            is ColdRestoreStagingPathValidation.Valid -> pathValidation.paths
        }
        if (!ensureRoot(paths)) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STAGING_PATH_CHANGED)
        }
        if (archive.archivePath.startsWith(paths.rootDirectory)) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.SOURCE_INSIDE_STAGING)
        }
        if (!ensureRegularLockFile(paths.lockFile)) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STAGING_PATH_CHANGED)
        }

        return try {
            FileChannel.open(paths.lockFile, StandardOpenOption.WRITE).use { channel ->
                val lock = try {
                    channel.tryLock()
                } catch (_: OverlappingFileLockException) {
                    null
                }
                if (lock == null) {
                    ColdRestoreStageResult.Busy
                } else {
                    lock.use { stageWhileLocked(paths, archive, deferComponents, restoreFiles) }
                }
            }
        } catch (_: Exception) {
            ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STORAGE_IO_FAILED)
        }
    }

    private fun stageWhileLocked(
        paths: ColdRestoreStagingPaths,
        archive: VerifiedColdRestoreArchive,
        deferComponents: Boolean,
        restoreFiles: Boolean,
    ): ColdRestoreStageResult {
        if (Files.exists(paths.pendingJournal, LinkOption.NOFOLLOW_LINKS)) {
            return if (Files.isSymbolicLink(paths.pendingJournal) ||
                !Files.isRegularFile(paths.pendingJournal, LinkOption.NOFOLLOW_LINKS)
            ) {
                ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STAGING_PATH_CHANGED)
            } else {
                ColdRestoreStageResult.PendingRestoreExists
            }
        }
        val requestId = try {
            requestIdSource.nextId()
        } catch (_: Exception) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.REQUEST_ID_INVALID)
        }
        if (!REQUEST_ID.matches(requestId)) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.REQUEST_ID_INVALID)
        }
        val createdAtMs = try {
            clockMs()
        } catch (_: Exception) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STORAGE_IO_FAILED)
        }
        if (createdAtMs < 0L) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STORAGE_IO_FAILED)
        }

        val requestDirectory = paths.requestDirectory(requestId)
        if (requestDirectory.parent != paths.rootDirectory ||
            Files.exists(requestDirectory, LinkOption.NOFOLLOW_LINKS)
        ) {
            return ColdRestoreStageResult.Rejected(
                ColdRestoreStageFailure.REQUEST_DIRECTORY_COLLISION,
            )
        }
        try {
            Files.createDirectory(requestDirectory)
        } catch (_: Exception) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STORAGE_IO_FAILED)
        }

        val partialArchive = requestDirectory.resolve(STAGED_ARCHIVE_PART_NAME)
        val stagedArchive = paths.stagedArchive(requestId)
        var journalCommitted = false
        try {
            copyAndVerify(archive, partialArchive)
            try {
                Files.move(partialArchive, stagedArchive, StandardCopyOption.ATOMIC_MOVE)
            } catch (_: AtomicMoveNotSupportedException) {
                return ColdRestoreStageResult.Rejected(
                    ColdRestoreStageFailure.ATOMIC_MOVE_UNSUPPORTED,
                )
            }
            if (Files.isSymbolicLink(stagedArchive) ||
                !Files.isRegularFile(stagedArchive, LinkOption.NOFOLLOW_LINKS) ||
                Files.size(stagedArchive) != archive.archiveSize
            ) {
                return ColdRestoreStageResult.Rejected(
                    ColdRestoreStageFailure.STAGING_PATH_CHANGED,
                )
            }

            val database = requireNotNull(
                archive.manifest.entries[BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY],
            )
            val stream = requireNotNull(archive.manifest.mainStream)
            val journal = ColdRestoreJournalV1.staged(
                requestId = requestId,
                components = archive.manifest.components,
                archiveSize = archive.archiveSize,
                archiveSha256 = archive.archiveSha256,
                mainDatabaseSize = database.size,
                mainDatabaseSha256 = database.sha256,
                mainStream = stream,
                createdAtMs = createdAtMs,
            ).copy(deferredComponents = deferComponents, restoreFiles = restoreFiles)
            val writeResult = ColdRestoreJournalStore(paths.pendingJournal).create(journal)
            return when (writeResult) {
                ColdRestoreJournalWriteResult.Written -> {
                    journalCommitted = true
                    ColdRestoreStageResult.Staged(ColdRestoreTicket(requestId))
                }
                ColdRestoreJournalWriteResult.AlreadyExists ->
                    ColdRestoreStageResult.PendingRestoreExists
                ColdRestoreJournalWriteResult.AtomicMoveUnsupported ->
                    ColdRestoreStageResult.Rejected(
                        ColdRestoreStageFailure.ATOMIC_MOVE_UNSUPPORTED,
                    )
                is ColdRestoreJournalWriteResult.Rejected ->
                    ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.JOURNAL_REJECTED)
                ColdRestoreJournalWriteResult.Conflict,
                ColdRestoreJournalWriteResult.CurrentJournalInvalid,
                ColdRestoreJournalWriteResult.IoFailed -> ColdRestoreStageResult.Rejected(
                    ColdRestoreStageFailure.JOURNAL_DURABILITY_FAILED,
                )
            }
        } catch (failure: StageCopyException) {
            return ColdRestoreStageResult.Rejected(failure.failure)
        } catch (_: Exception) {
            return ColdRestoreStageResult.Rejected(ColdRestoreStageFailure.STORAGE_IO_FAILED)
        } finally {
            runCatching { Files.deleteIfExists(partialArchive) }
            if (!journalCommitted) {
                runCatching { Files.deleteIfExists(stagedArchive) }
                // Exact, non-recursive cleanup only. A non-empty/changed directory is retained.
                runCatching { Files.deleteIfExists(requestDirectory) }
            }
        }
    }

    companion object {
        /** The same cross-process lane rejects any second restore before live component writes. */
        internal suspend fun <T> withRestoreAdmission(
            context: android.content.Context,
            operation: suspend () -> T,
        ): T {
            val paths = (ColdRestoreStagingPaths.verify(
                File(context.applicationInfo.dataDir), context.noBackupFilesDir,
            ) as? ColdRestoreStagingPathValidation.Valid)?.paths
                ?: throw me.rerere.rikkahub.data.sync.backup.BackupArchiveServiceException(
                    me.rerere.rikkahub.data.sync.backup.BackupArchiveServiceFailure.COLD_STAGING_PATH_UNSAFE,
                )
            check(ensureRoot(paths) && ensureRegularLockFile(paths.lockFile))
            return FileChannel.open(paths.lockFile, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS).use { channel ->
                val lock = try { channel.tryLock() } catch (_: OverlappingFileLockException) { null }
                    ?: throw me.rerere.rikkahub.data.sync.backup.BackupArchiveServiceException(
                        me.rerere.rikkahub.data.sync.backup.BackupArchiveServiceFailure.COLD_STAGING_BUSY,
                    )
                lock.use {
                    if (Files.exists(paths.pendingJournal, LinkOption.NOFOLLOW_LINKS)) {
                        throw me.rerere.rikkahub.data.sync.backup.BackupArchiveServiceException(
                            me.rerere.rikkahub.data.sync.backup.BackupArchiveServiceFailure.COLD_RESTORE_ALREADY_PENDING,
                        )
                    }
                    operation()
                }
            }
        }
    }

    private fun copyAndVerify(
        archive: VerifiedColdRestoreArchive,
        destination: Path,
    ) {
        if (Files.isSymbolicLink(archive.archivePath) ||
            !Files.isRegularFile(archive.archivePath, LinkOption.NOFOLLOW_LINKS)
        ) {
            throw StageCopyException(ColdRestoreStageFailure.ARCHIVE_CHANGED_AFTER_PREFLIGHT)
        }
        val digest = MessageDigest.getInstance("SHA-256")
        var total = 0L
        FileInputStream(archive.archivePath.toFile()).buffered().use { input ->
            FileOutputStream(destination.toFile()).buffered().use { output ->
                val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
                while (true) {
                    val count = input.read(buffer)
                    if (count < 0) break
                    if (count == 0) continue
                    total = try {
                        Math.addExact(total, count.toLong())
                    } catch (_: ArithmeticException) {
                        throw StageCopyException(ColdRestoreStageFailure.ARCHIVE_TOO_LARGE)
                    }
                    if (total > archive.archiveSize || total > MAX_BACKUP_ARCHIVE_BYTES) {
                        throw StageCopyException(ColdRestoreStageFailure.ARCHIVE_TOO_LARGE)
                    }
                    digest.update(buffer, 0, count)
                    output.write(buffer, 0, count)
                }
                output.flush()
            }
        }
        FileChannel.open(destination, StandardOpenOption.WRITE).use { it.force(true) }
        if (total != archive.archiveSize ||
            !constantTimeDigestEquals(archive.archiveSha256, digest.digest())
        ) {
            throw StageCopyException(ColdRestoreStageFailure.ARCHIVE_CHANGED_AFTER_PREFLIGHT)
        }
    }
}

private fun ensureRoot(paths: ColdRestoreStagingPaths): Boolean = try {
    if (!Files.exists(paths.rootDirectory, LinkOption.NOFOLLOW_LINKS)) {
        Files.createDirectory(paths.rootDirectory)
    }
    !Files.isSymbolicLink(paths.rootDirectory) &&
        Files.isDirectory(paths.rootDirectory, LinkOption.NOFOLLOW_LINKS) &&
        paths.rootDirectory.toFile().canonicalFile.toPath().normalize() == paths.rootDirectory
} catch (_: Exception) {
    false
}

private fun ensureRegularLockFile(lockFile: Path): Boolean = try {
    if (!Files.exists(lockFile, LinkOption.NOFOLLOW_LINKS)) {
        try {
            Files.createFile(lockFile)
        } catch (_: java.nio.file.FileAlreadyExistsException) {
            // A concurrent process created it; the checks below still decide whether it is safe.
        }
    }
    !Files.isSymbolicLink(lockFile) &&
        Files.isRegularFile(lockFile, LinkOption.NOFOLLOW_LINKS)
} catch (_: Exception) {
    false
}

private fun constantTimeDigestEquals(expectedHex: String, actual: ByteArray): Boolean {
    if (!isCanonicalBackupSha256(expectedHex) || actual.size != 32) return false
    val expected = ByteArray(32) { index ->
        expectedHex.substring(index * 2, index * 2 + 2).toInt(16).toByte()
    }
    return MessageDigest.isEqual(expected, actual)
}

private fun newRequestId(): String = buildString(32) {
    ByteArray(16).also(secureRandom::nextBytes).forEach { byte ->
        append((byte.toInt() and 0xff).toString(16).padStart(2, '0'))
    }
}

private class StageCopyException(val failure: ColdRestoreStageFailure) : RuntimeException()

private fun invalidPath(failure: ColdRestoreStagingPathFailure) =
    ColdRestoreStagingPathValidation.Invalid(failure)

private fun rejectedArchive(failure: VerifiedColdRestoreArchiveFailure) =
    VerifiedColdRestoreArchiveResult.Rejected(failure)

private const val STAGING_ROOT_NAME = "agent_learning_restore_v1"
private const val PENDING_JOURNAL_NAME = "pending_restore.json"
private const val LOCK_FILE_NAME = ".restore.lock"
private const val REQUEST_DIRECTORY_PREFIX = "request_"
private const val STAGED_ARCHIVE_NAME = "archive.zip"
private const val STAGED_ARCHIVE_PART_NAME = "archive.zip.part"
private val REQUEST_ID = Regex("[0-9a-f]{32}")
private val secureRandom = SecureRandom()
