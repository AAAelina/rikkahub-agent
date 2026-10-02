package me.rerere.rikkahub.data.sync.backup.restore

import java.io.File
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.UUID

enum class LearningOwnedDatabasePathFailure {
    APP_DATA_PATH_NOT_ABSOLUTE,
    APP_DATA_PATH_NOT_DIRECTORY,
    APP_DATA_PATH_SYMBOLIC_LINK,
    DATABASE_DIRECTORY_NOT_OWNED,
    DATABASE_DIRECTORY_NOT_DIRECTORY,
    DATABASE_DIRECTORY_SYMBOLIC_LINK,
    DATABASE_PATH_NOT_ABSOLUTE,
    DATABASE_PATH_NOT_EXACT,
    DATABASE_FILE_SYMBOLIC_LINK,
    DATABASE_FILE_NOT_REGULAR,
    PATH_CANONICALIZATION_FAILED,
}

sealed interface LearningOwnedDatabasePathValidation {
    class Valid internal constructor(val paths: LearningOwnedDatabasePaths) :
        LearningOwnedDatabasePathValidation

    data class Invalid(val failure: LearningOwnedDatabasePathFailure) :
        LearningOwnedDatabasePathValidation
}

/**
 * Exact app-private files that may be quarantined during a main database restore.
 *
 * Creation requires the standard `<application dataDir>/databases/learning_runtime.db` location.
 * No caller-supplied child name, glob, recursive traversal, or alternate directory is accepted.
 */
class LearningOwnedDatabasePaths private constructor(
    internal val databaseDirectory: Path,
    internal val databaseFile: Path,
    internal val walFile: Path,
    internal val shmFile: Path,
) {
    internal val exactFiles: List<Path> = listOf(databaseFile, walFile, shmFile)

    override fun toString(): String = "LearningOwnedDatabasePaths(files=3, paths=<redacted>)"

    companion object {
        fun verify(
            applicationDataDirectory: File,
            learningDatabaseFile: File,
        ): LearningOwnedDatabasePathValidation {
            if (!applicationDataDirectory.isAbsolute) {
                return invalid(LearningOwnedDatabasePathFailure.APP_DATA_PATH_NOT_ABSOLUTE)
            }
            if (!learningDatabaseFile.isAbsolute) {
                return invalid(LearningOwnedDatabasePathFailure.DATABASE_PATH_NOT_ABSOLUTE)
            }

            return try {
                val appData = applicationDataDirectory.toPath().toAbsolutePath().normalize()
                if (Files.isSymbolicLink(appData)) {
                    return invalid(LearningOwnedDatabasePathFailure.APP_DATA_PATH_SYMBOLIC_LINK)
                }
                if (!Files.isDirectory(appData, LinkOption.NOFOLLOW_LINKS)) {
                    return invalid(LearningOwnedDatabasePathFailure.APP_DATA_PATH_NOT_DIRECTORY)
                }
                if (appData.toFile().canonicalFile.toPath().normalize() != appData) {
                    return invalid(LearningOwnedDatabasePathFailure.APP_DATA_PATH_SYMBOLIC_LINK)
                }

                val databaseDirectory = appData.resolve(DATABASES_DIRECTORY).normalize()
                if (databaseDirectory.parent != appData) {
                    return invalid(LearningOwnedDatabasePathFailure.DATABASE_DIRECTORY_NOT_OWNED)
                }
                if (Files.isSymbolicLink(databaseDirectory)) {
                    return invalid(LearningOwnedDatabasePathFailure.DATABASE_DIRECTORY_SYMBOLIC_LINK)
                }
                if (!Files.isDirectory(databaseDirectory, LinkOption.NOFOLLOW_LINKS)) {
                    return invalid(LearningOwnedDatabasePathFailure.DATABASE_DIRECTORY_NOT_DIRECTORY)
                }
                if (databaseDirectory.toFile().canonicalFile.toPath().normalize() != databaseDirectory) {
                    return invalid(LearningOwnedDatabasePathFailure.DATABASE_DIRECTORY_SYMBOLIC_LINK)
                }

                val expectedDatabase = databaseDirectory.resolve(LEARNING_DATABASE_FILE_NAME).normalize()
                val actualDatabase = learningDatabaseFile.toPath().toAbsolutePath().normalize()
                if (actualDatabase != expectedDatabase) {
                    return invalid(LearningOwnedDatabasePathFailure.DATABASE_PATH_NOT_EXACT)
                }

                val wal = databaseDirectory.resolve("${LEARNING_DATABASE_FILE_NAME}-wal")
                val shm = databaseDirectory.resolve("${LEARNING_DATABASE_FILE_NAME}-shm")
                for (candidate in listOf(expectedDatabase, wal, shm)) {
                    if (Files.isSymbolicLink(candidate)) {
                        return invalid(LearningOwnedDatabasePathFailure.DATABASE_FILE_SYMBOLIC_LINK)
                    }
                    if (Files.exists(candidate, LinkOption.NOFOLLOW_LINKS) &&
                        !Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS)
                    ) {
                        return invalid(LearningOwnedDatabasePathFailure.DATABASE_FILE_NOT_REGULAR)
                    }
                }

                LearningOwnedDatabasePathValidation.Valid(
                    LearningOwnedDatabasePaths(
                        databaseDirectory = databaseDirectory,
                        databaseFile = expectedDatabase,
                        walFile = wal,
                        shmFile = shm,
                    ),
                )
            } catch (_: Exception) {
                invalid(LearningOwnedDatabasePathFailure.PATH_CANONICALIZATION_FAILED)
            }
        }

        private const val DATABASES_DIRECTORY = "databases"
    }
}

enum class LearningQuarantineFailure {
    SOURCE_PATH_CHANGED,
    QUARANTINE_ID_INVALID,
    QUARANTINE_DIRECTORY_CREATE_FAILED,
    ATOMIC_RENAME_UNSUPPORTED,
    ATOMIC_RENAME_FAILED,
}

class LearningQuarantineException internal constructor(
    val failure: LearningQuarantineFailure,
    val partialBatch: LearningQuarantineBatch?,
) : Exception(failure.name) {
    override fun toString(): String =
        "LearningQuarantineException(failure=$failure, partialFiles=${partialBatch?.fileCount ?: 0})"
}

/** A process-random, app-private batch. Paths are intentionally not rendered by [toString]. */
class LearningQuarantineBatch internal constructor(
    val opaqueId: String,
    internal val directory: Path?,
    internal val quarantinedFiles: List<Path>,
) {
    val fileCount: Int get() = quarantinedFiles.size

    fun containsFileName(fileName: String): Boolean =
        quarantinedFiles.any { it.fileName.toString() == fileName }

    override fun toString(): String =
        "LearningQuarantineBatch(id=<opaque>, files=$fileCount, paths=<redacted>)"
}

fun interface LearningQuarantineIdSource {
    fun nextId(): String
}

data class LearningQuarantineCleanupSummary(
    val deletedBatches: Int,
    val retainedBatches: Int,
    val deletedFiles: Int,
)

/**
 * Moves only the three exact Learning Room files into a unique direct child of the app databases
 * directory. Every move requests filesystem-level ATOMIC_MOVE and never falls back to copy/delete.
 */
class LearningRestoreQuarantine(
    private val paths: LearningOwnedDatabasePaths,
    private val idSource: LearningQuarantineIdSource =
        LearningQuarantineIdSource { UUID.randomUUID().toString().replace("-", "") },
) {
    @Throws(LearningQuarantineException::class)
    fun quarantineExactFiles(): LearningQuarantineBatch {
        validateDatabaseDirectoryUnchanged()
        validateSourcesUnchanged()
        val existing = paths.exactFiles.filter { Files.exists(it, LinkOption.NOFOLLOW_LINKS) }
        if (existing.isEmpty()) {
            return LearningQuarantineBatch(opaqueId = EMPTY_BATCH_ID, directory = null, quarantinedFiles = emptyList())
        }

        val batchId = try {
            idSource.nextId()
        } catch (_: Exception) {
            throw LearningQuarantineException(LearningQuarantineFailure.QUARANTINE_ID_INVALID, null)
        }
        if (!QUARANTINE_ID.matches(batchId)) {
            throw LearningQuarantineException(LearningQuarantineFailure.QUARANTINE_ID_INVALID, null)
        }
        val directory = paths.databaseDirectory.resolve("$QUARANTINE_PREFIX$batchId").normalize()
        if (directory.parent != paths.databaseDirectory || Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
            throw LearningQuarantineException(
                LearningQuarantineFailure.QUARANTINE_DIRECTORY_CREATE_FAILED,
                null,
            )
        }
        try {
            Files.createDirectory(directory)
        } catch (_: Exception) {
            throw LearningQuarantineException(
                LearningQuarantineFailure.QUARANTINE_DIRECTORY_CREATE_FAILED,
                null,
            )
        }

        val moved = mutableListOf<Path>()
        for (source in existing) {
            val target = directory.resolve(source.fileName.toString()).normalize()
            if (target.parent != directory || target.fileName.toString() !in EXACT_FILE_NAMES) {
                throw LearningQuarantineException(
                    LearningQuarantineFailure.SOURCE_PATH_CHANGED,
                    LearningQuarantineBatch(batchId, directory, moved.toList()),
                )
            }
            try {
                Files.move(source, target, StandardCopyOption.ATOMIC_MOVE)
                moved.add(target)
            } catch (_: AtomicMoveNotSupportedException) {
                throw LearningQuarantineException(
                    LearningQuarantineFailure.ATOMIC_RENAME_UNSUPPORTED,
                    LearningQuarantineBatch(batchId, directory, moved.toList()),
                )
            } catch (_: Exception) {
                throw LearningQuarantineException(
                    LearningQuarantineFailure.ATOMIC_RENAME_FAILED,
                    LearningQuarantineBatch(batchId, directory, moved.toList()),
                )
            }
        }
        return LearningQuarantineBatch(batchId, directory, moved.toList())
    }

    /**
     * The only production deletion API. It must be called by the new-process bootstrap integration
     * only after the new main timeline has been validated and its initial outbox replay completed.
     * Unknown entries/symlinks retain the entire batch for manual diagnosis.
     */
    fun cleanupAfterNewTimelineBootstrapSucceeded(): LearningQuarantineCleanupSummary {
        validateDatabaseDirectoryUnchanged()
        var deletedBatches = 0
        var retainedBatches = 0
        var deletedFiles = 0
        Files.newDirectoryStream(paths.databaseDirectory).use { children ->
            for (child in children) {
                val name = child.fileName.toString()
                if (!QUARANTINE_DIRECTORY.matches(name)) continue
                if (Files.isSymbolicLink(child) || !Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                    retainedBatches += 1
                    continue
                }
                val entries = Files.newDirectoryStream(child).use { it.toList() }
                val isExactBatch = entries.all { entry ->
                    entry.parent == child &&
                        entry.fileName.toString() in EXACT_FILE_NAMES &&
                        !Files.isSymbolicLink(entry) &&
                        Files.isRegularFile(entry, LinkOption.NOFOLLOW_LINKS)
                }
                if (!isExactBatch) {
                    retainedBatches += 1
                    continue
                }

                var batchSucceeded = true
                for (entry in entries) {
                    if (runCatching { Files.deleteIfExists(entry) }.getOrDefault(false)) {
                        deletedFiles += 1
                    } else {
                        batchSucceeded = false
                    }
                }
                if (batchSucceeded && runCatching { Files.deleteIfExists(child) }.getOrDefault(false)) {
                    deletedBatches += 1
                } else {
                    retainedBatches += 1
                }
            }
        }
        return LearningQuarantineCleanupSummary(deletedBatches, retainedBatches, deletedFiles)
    }

    private fun validateSourcesUnchanged() {
        for (candidate in paths.exactFiles) {
            if (candidate.parent != paths.databaseDirectory ||
                candidate.fileName.toString() !in EXACT_FILE_NAMES ||
                Files.isSymbolicLink(candidate) ||
                (Files.exists(candidate, LinkOption.NOFOLLOW_LINKS) &&
                    !Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS))
            ) {
                throw LearningQuarantineException(LearningQuarantineFailure.SOURCE_PATH_CHANGED, null)
            }
        }
    }

    private fun validateDatabaseDirectoryUnchanged() {
        val directory = paths.databaseDirectory
        val canonicalMatches = runCatching {
            directory.toFile().canonicalFile.toPath().normalize() == directory
        }.getOrDefault(false)
        if (Files.isSymbolicLink(directory) ||
            !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) ||
            !canonicalMatches
        ) {
            throw LearningQuarantineException(LearningQuarantineFailure.SOURCE_PATH_CHANGED, null)
        }
    }

    private companion object {
        const val QUARANTINE_PREFIX = ".learning_runtime_quarantine_"
        const val EMPTY_BATCH_ID = "none"
        val QUARANTINE_ID = Regex("[a-zA-Z0-9_-]{16,64}")
        val QUARANTINE_DIRECTORY = Regex("\\.learning_runtime_quarantine_[a-zA-Z0-9_-]{16,64}")
        val EXACT_FILE_NAMES = setOf(
            LEARNING_DATABASE_FILE_NAME,
            "${LEARNING_DATABASE_FILE_NAME}-wal",
            "${LEARNING_DATABASE_FILE_NAME}-shm",
        )
    }
}

private fun invalid(failure: LearningOwnedDatabasePathFailure) =
    LearningOwnedDatabasePathValidation.Invalid(failure)

/** File name of the removed Learning database; restore still quarantines stale copies. */
internal const val LEARNING_DATABASE_FILE_NAME = "learning_runtime.db"
