package me.rerere.rikkahub.data.sync.backup.restore

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import java.security.MessageDigest

enum class ColdRestoreSwapFailure {
    STAGING_PATH_UNSAFE,
    MAIN_DATABASE_PATH_UNSAFE,
    LEARNING_DATABASE_PATH_UNSAFE,
    PENDING_JOURNAL_INVALID,
    PREPARED_DATABASE_NOT_READY,
    PREPARED_DATABASE_CHANGED,
    PREPARED_DATABASE_VALIDATION_FAILED,
    QUARANTINE_PATH_UNSAFE,
    LEARNING_QUARANTINE_FAILED,
    MAIN_QUARANTINE_FAILED,
    MAIN_INSTALL_FAILED,
    INSTALLED_DATABASE_CHANGED,
    INSTALLED_DATABASE_VALIDATION_FAILED,
    JOURNAL_UPDATE_FAILED,
    STORAGE_IO_FAILED,
}

sealed interface ColdRestoreSwapResult {
    data object NoPendingRestore : ColdRestoreSwapResult

    data object Busy : ColdRestoreSwapResult

    /** The irreversible boundary was not crossed; the existing live Room files are untouched. */
    data class LiveDatabaseUnchanged(val failure: ColdRestoreSwapFailure) : ColdRestoreSwapResult

    /** New main timeline is committed; old Learning remains quarantined until bootstrap succeeds. */
    data object RebuildRequired : ColdRestoreSwapResult

    data object Complete : ColdRestoreSwapResult

    /** Room/Koin must not start in this process. */
    data class DegradedRestartRequired(val failure: ColdRestoreSwapFailure) :
        ColdRestoreSwapResult
}

enum class ColdRestoreCrashPoint {
    AFTER_LEARNING_QUARANTINE_INTENT,
    AFTER_LEARNING_FILE_MOVE,
    AFTER_LEARNING_QUARANTINED,
    AFTER_MAIN_QUARANTINE_INTENT,
    AFTER_MAIN_FILE_MOVE,
    AFTER_MAIN_QUARANTINED,
    AFTER_MAIN_INSTALL_INTENT,
    AFTER_MAIN_INSTALL,
    AFTER_MAIN_INSTALLED_JOURNAL,
    AFTER_INSTALLED_VALIDATION,
    AFTER_SWAP_COMMITTED,
    AFTER_REBUILD_REQUIRED,
}

/** Test seam: an Error thrown here models process death and is intentionally not caught. */
fun interface ColdRestoreCrashInjector {
    fun after(point: ColdRestoreCrashPoint)
}

/**
 * Resumable same-filesystem swap executor. It only moves the exact Room main/Learning DB, WAL and
 * SHM names. Every multi-file mutation is preceded by a durable journal intent, and each move is
 * idempotently reconstructed from the exact source/target pair after a process kill.
 */
class ColdRestoreSwapExecutor(
    private val stagingPaths: ColdRestoreStagingPathValidation,
    private val bootstrapPaths: ColdRestoreBootstrapPathValidation,
    private val learningPaths: LearningOwnedDatabasePathValidation,
    private val validator: ColdRestorePreparedDatabaseValidator,
    private val clockMs: () -> Long = System::currentTimeMillis,
    private val crashInjector: ColdRestoreCrashInjector = ColdRestoreCrashInjector { },
) {
    fun execute(): ColdRestoreSwapResult {
        val staging = when (stagingPaths) {
            is ColdRestoreStagingPathValidation.Invalid -> {
                return unchanged(ColdRestoreSwapFailure.STAGING_PATH_UNSAFE)
            }
            is ColdRestoreStagingPathValidation.Valid -> stagingPaths.paths
        }
        if (!Files.exists(staging.pendingJournal, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreSwapResult.NoPendingRestore
        }
        if (!safeLockFile(staging.lockFile)) {
            return degraded(ColdRestoreSwapFailure.STAGING_PATH_UNSAFE)
        }
        return try {
            FileChannel.open(
                staging.lockFile,
                StandardOpenOption.WRITE,
                LinkOption.NOFOLLOW_LINKS,
            ).use { channel ->
                val lock = try {
                    channel.tryLock()
                } catch (_: OverlappingFileLockException) {
                    null
                }
                if (lock == null) {
                    ColdRestoreSwapResult.Busy
                } else {
                    lock.use { executeWhileLocked(staging) }
                }
            }
        } catch (_: Exception) {
            degraded(ColdRestoreSwapFailure.STORAGE_IO_FAILED)
        }
    }

    private fun executeWhileLocked(staging: ColdRestoreStagingPaths): ColdRestoreSwapResult {
        var journal = when (val read = ColdRestoreJournalStore(staging.pendingJournal).read()) {
            ColdRestoreJournalReadResult.Missing -> return ColdRestoreSwapResult.NoPendingRestore
            is ColdRestoreJournalReadResult.Invalid -> {
                return degraded(ColdRestoreSwapFailure.PENDING_JOURNAL_INVALID)
            }
            is ColdRestoreJournalReadResult.Valid -> read.journal
        }
        if (journal.phase == ColdRestorePhase.STAGED) {
            return unchanged(ColdRestoreSwapFailure.PREPARED_DATABASE_NOT_READY)
        }
        if (journal.phase == ColdRestorePhase.FAILED_RESTART_REQUIRED) {
            return degraded(ColdRestoreSwapFailure.PENDING_JOURNAL_INVALID)
        }
        if (journal.phase == ColdRestorePhase.COMPLETE) return ColdRestoreSwapResult.Complete

        val bootstrap = when (bootstrapPaths) {
            is ColdRestoreBootstrapPathValidation.Invalid -> {
                return resultForCurrentPhase(
                    journal.phase,
                    ColdRestoreSwapFailure.MAIN_DATABASE_PATH_UNSAFE,
                )
            }
            is ColdRestoreBootstrapPathValidation.Valid -> bootstrapPaths.paths
        }
        val learning = when (learningPaths) {
            is LearningOwnedDatabasePathValidation.Invalid -> {
                return resultForCurrentPhase(
                    journal.phase,
                    ColdRestoreSwapFailure.LEARNING_DATABASE_PATH_UNSAFE,
                )
            }
            is LearningOwnedDatabasePathValidation.Valid -> learningPaths.paths
        }
        if (!safeDatabaseDirectory(bootstrap.databaseDirectory) ||
            bootstrap.databaseDirectory != learning.databaseDirectory
        ) {
            return resultForCurrentPhase(
                journal.phase,
                ColdRestoreSwapFailure.MAIN_DATABASE_PATH_UNSAFE,
            )
        }

        try {
            if (journal.phase == ColdRestorePhase.READY_TO_SWAP) {
                validatePrepared(journal, bootstrap)
            }
            while (true) {
                journal = when (journal.phase) {
                    ColdRestorePhase.READY_TO_SWAP -> {
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.LEARNING_QUARANTINE_STARTED,
                            learningQuarantineId = "${journal.requestId}l",
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_LEARNING_QUARANTINE_INTENT)
                        next
                    }
                    ColdRestorePhase.LEARNING_QUARANTINE_STARTED -> {
                        quarantineLearning(journal, learning)
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.LEARNING_QUARANTINED,
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_LEARNING_QUARANTINED)
                        next
                    }
                    ColdRestorePhase.LEARNING_QUARANTINED -> {
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.OLD_MAIN_QUARANTINE_STARTED,
                            mainQuarantineId = "${journal.requestId}m",
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_MAIN_QUARANTINE_INTENT)
                        next
                    }
                    ColdRestorePhase.OLD_MAIN_QUARANTINE_STARTED -> {
                        quarantineMain(journal, bootstrap)
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.OLD_MAIN_QUARANTINED,
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_MAIN_QUARANTINED)
                        next
                    }
                    ColdRestorePhase.OLD_MAIN_QUARANTINED -> {
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.MAIN_INSTALL_STARTED,
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_MAIN_INSTALL_INTENT)
                        next
                    }
                    ColdRestorePhase.MAIN_INSTALL_STARTED -> {
                        installPrepared(journal, bootstrap)
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_MAIN_INSTALL)
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.MAIN_INSTALLED,
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_MAIN_INSTALLED_JOURNAL)
                        next
                    }
                    ColdRestorePhase.MAIN_INSTALLED -> {
                        validateInstalled(journal, bootstrap)
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_INSTALLED_VALIDATION)
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.SWAP_COMMITTED,
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_SWAP_COMMITTED)
                        next
                    }
                    ColdRestorePhase.SWAP_COMMITTED -> {
                        val next = transition(
                            staging,
                            journal,
                            ColdRestorePhase.REBUILD_REQUIRED,
                        )
                        crashInjector.after(ColdRestoreCrashPoint.AFTER_REBUILD_REQUIRED)
                        next
                    }
                    ColdRestorePhase.REBUILD_REQUIRED ->
                        return ColdRestoreSwapResult.RebuildRequired
                    ColdRestorePhase.COMPLETE -> return ColdRestoreSwapResult.Complete
                    ColdRestorePhase.STAGED ->
                        return unchanged(ColdRestoreSwapFailure.PREPARED_DATABASE_NOT_READY)
                    ColdRestorePhase.FAILED_RESTART_REQUIRED ->
                        return degraded(ColdRestoreSwapFailure.PENDING_JOURNAL_INVALID)
                }
            }
        } catch (error: ColdRestoreSwapException) {
            if (crossedIrreversibleBoundary(journal.phase)) {
                markFailedBestEffort(staging, journal, error.failureCode)
                return degraded(error.failure)
            }
            return unchanged(error.failure)
        } catch (_: Exception) {
            if (crossedIrreversibleBoundary(journal.phase)) {
                markFailedBestEffort(
                    staging,
                    journal,
                    ColdRestoreFailureCode.JOURNAL_DURABILITY_FAILED,
                )
                return degraded(ColdRestoreSwapFailure.STORAGE_IO_FAILED)
            }
            return unchanged(ColdRestoreSwapFailure.STORAGE_IO_FAILED)
        }
    }

    private fun validatePrepared(
        journal: ColdRestoreJournalV1,
        bootstrap: ColdRestoreBootstrapPaths,
    ) {
        val prepared = bootstrap.preparedDatabase(journal.requestId)
        if (!exactRegularFile(prepared, bootstrap.databaseDirectory) ||
            !identityMatches(
                prepared,
                requireNotNull(journal.preparedDatabaseSize),
                requireNotNull(journal.preparedDatabaseSha256),
            ) || preparedSidecars(prepared).any { Files.exists(it, LinkOption.NOFOLLOW_LINKS) }
        ) {
            throw swapFailure(
                ColdRestoreSwapFailure.PREPARED_DATABASE_CHANGED,
                ColdRestoreFailureCode.STAGED_ARCHIVE_CHANGED,
            )
        }
        try {
            validator.validate(prepared.toFile(), journal.mainStream)
        } catch (error: Exception) {
            throw swapFailure(
                ColdRestoreSwapFailure.PREPARED_DATABASE_VALIDATION_FAILED,
                ColdRestoreFailureCode.MAIN_VALIDATION_FAILED,
                error,
            )
        }
        if (!identityMatches(
                prepared,
                requireNotNull(journal.preparedDatabaseSize),
                requireNotNull(journal.preparedDatabaseSha256),
            )
        ) {
            throw swapFailure(
                ColdRestoreSwapFailure.PREPARED_DATABASE_CHANGED,
                ColdRestoreFailureCode.MAIN_VALIDATION_FAILED,
            )
        }
    }

    private fun quarantineLearning(
        journal: ColdRestoreJournalV1,
        learning: LearningOwnedDatabasePaths,
    ) {
        val id = requireNotNull(journal.learningQuarantineId)
        val directory = exactQuarantineDirectory(
            parent = learning.databaseDirectory,
            prefix = LEARNING_QUARANTINE_PREFIX,
            id = id,
        )
        moveExactSetResumably(
            sources = learning.exactFiles,
            targetDirectory = directory,
            exactNames = LEARNING_FILE_NAMES,
            crashPoint = ColdRestoreCrashPoint.AFTER_LEARNING_FILE_MOVE,
            failure = ColdRestoreSwapFailure.LEARNING_QUARANTINE_FAILED,
            failureCode = ColdRestoreFailureCode.LEARNING_QUARANTINE_FAILED,
        )
    }

    private fun quarantineMain(
        journal: ColdRestoreJournalV1,
        bootstrap: ColdRestoreBootstrapPaths,
    ) {
        val id = requireNotNull(journal.mainQuarantineId)
        val directory = exactQuarantineDirectory(
            parent = bootstrap.databaseDirectory,
            prefix = MAIN_QUARANTINE_PREFIX,
            id = id,
        )
        moveExactSetResumably(
            sources = listOf(bootstrap.mainDatabase, bootstrap.mainWal, bootstrap.mainShm),
            targetDirectory = directory,
            exactNames = MAIN_FILE_NAMES,
            crashPoint = ColdRestoreCrashPoint.AFTER_MAIN_FILE_MOVE,
            failure = ColdRestoreSwapFailure.MAIN_QUARANTINE_FAILED,
            failureCode = ColdRestoreFailureCode.MAIN_QUARANTINE_FAILED,
        )
    }

    private fun moveExactSetResumably(
        sources: List<Path>,
        targetDirectory: Path,
        exactNames: Set<String>,
        crashPoint: ColdRestoreCrashPoint,
        failure: ColdRestoreSwapFailure,
        failureCode: ColdRestoreFailureCode,
    ) {
        for (source in sources) {
            if (source.fileName.toString() !in exactNames || source.parent != targetDirectory.parent) {
                throw swapFailure(failure, failureCode)
            }
            val target = targetDirectory.resolve(source.fileName.toString()).normalize()
            if (target.parent != targetDirectory) throw swapFailure(failure, failureCode)
            val sourceExists = Files.exists(source, LinkOption.NOFOLLOW_LINKS)
            val targetExists = Files.exists(target, LinkOption.NOFOLLOW_LINKS)
            if (sourceExists && targetExists) throw swapFailure(failure, failureCode)
            if (targetExists && !exactRegularFile(target, targetDirectory)) {
                throw swapFailure(failure, failureCode)
            }
            if (sourceExists) {
                if (!exactRegularFile(source, source.parent)) throw swapFailure(failure, failureCode)
                try {
                    Files.move(source, target, StandardCopyOption.ATOMIC_MOVE)
                } catch (error: AtomicMoveNotSupportedException) {
                    throw swapFailure(failure, failureCode, error)
                } catch (error: Exception) {
                    throw swapFailure(failure, failureCode, error)
                }
                crashInjector.after(crashPoint)
            }
        }
    }

    private fun installPrepared(
        journal: ColdRestoreJournalV1,
        bootstrap: ColdRestoreBootstrapPaths,
    ) {
        val prepared = bootstrap.preparedDatabase(journal.requestId)
        val live = bootstrap.mainDatabase
        if (Files.exists(bootstrap.mainWal, LinkOption.NOFOLLOW_LINKS) ||
            Files.exists(bootstrap.mainShm, LinkOption.NOFOLLOW_LINKS)
        ) {
            throw swapFailure(
                ColdRestoreSwapFailure.MAIN_INSTALL_FAILED,
                ColdRestoreFailureCode.MAIN_INSTALL_FAILED,
            )
        }
        val preparedExists = Files.exists(prepared, LinkOption.NOFOLLOW_LINKS)
        val liveExists = Files.exists(live, LinkOption.NOFOLLOW_LINKS)
        if (preparedExists && liveExists || !preparedExists && !liveExists) {
            throw swapFailure(
                ColdRestoreSwapFailure.MAIN_INSTALL_FAILED,
                ColdRestoreFailureCode.MAIN_INSTALL_FAILED,
            )
        }
        if (preparedExists) {
            if (!identityMatches(
                    prepared,
                    requireNotNull(journal.preparedDatabaseSize),
                    requireNotNull(journal.preparedDatabaseSha256),
                )
            ) {
                throw swapFailure(
                    ColdRestoreSwapFailure.PREPARED_DATABASE_CHANGED,
                    ColdRestoreFailureCode.MAIN_INSTALL_FAILED,
                )
            }
            try {
                Files.move(prepared, live, StandardCopyOption.ATOMIC_MOVE)
            } catch (error: AtomicMoveNotSupportedException) {
                throw swapFailure(
                    ColdRestoreSwapFailure.MAIN_INSTALL_FAILED,
                    ColdRestoreFailureCode.MAIN_INSTALL_FAILED,
                    error,
                )
            }
        } else if (!identityMatches(
                live,
                requireNotNull(journal.preparedDatabaseSize),
                requireNotNull(journal.preparedDatabaseSha256),
            )
        ) {
            throw swapFailure(
                ColdRestoreSwapFailure.INSTALLED_DATABASE_CHANGED,
                ColdRestoreFailureCode.MAIN_INSTALL_FAILED,
            )
        }
    }

    private fun validateInstalled(
        journal: ColdRestoreJournalV1,
        bootstrap: ColdRestoreBootstrapPaths,
    ) {
        if (!identityMatches(
                bootstrap.mainDatabase,
                requireNotNull(journal.preparedDatabaseSize),
                requireNotNull(journal.preparedDatabaseSha256),
            ) || Files.exists(bootstrap.mainWal, LinkOption.NOFOLLOW_LINKS) ||
            Files.exists(bootstrap.mainShm, LinkOption.NOFOLLOW_LINKS)
        ) {
            throw swapFailure(
                ColdRestoreSwapFailure.INSTALLED_DATABASE_CHANGED,
                ColdRestoreFailureCode.MAIN_VALIDATION_FAILED,
            )
        }
        try {
            validator.validate(bootstrap.mainDatabase.toFile(), journal.mainStream)
        } catch (error: Exception) {
            throw swapFailure(
                ColdRestoreSwapFailure.INSTALLED_DATABASE_VALIDATION_FAILED,
                ColdRestoreFailureCode.MAIN_VALIDATION_FAILED,
                error,
            )
        }
        if (!identityMatches(
                bootstrap.mainDatabase,
                requireNotNull(journal.preparedDatabaseSize),
                requireNotNull(journal.preparedDatabaseSha256),
            )
        ) {
            throw swapFailure(
                ColdRestoreSwapFailure.INSTALLED_DATABASE_CHANGED,
                ColdRestoreFailureCode.MAIN_VALIDATION_FAILED,
            )
        }
    }

    private fun transition(
        staging: ColdRestoreStagingPaths,
        current: ColdRestoreJournalV1,
        phase: ColdRestorePhase,
        learningQuarantineId: String? = current.learningQuarantineId,
        mainQuarantineId: String? = current.mainQuarantineId,
    ): ColdRestoreJournalV1 {
        val now = clockMs()
        if (now < current.updatedAtMs) {
            throw swapFailure(
                ColdRestoreSwapFailure.JOURNAL_UPDATE_FAILED,
                ColdRestoreFailureCode.JOURNAL_DURABILITY_FAILED,
            )
        }
        val next = current.copy(
            stateVersion = Math.addExact(current.stateVersion, 1L),
            phase = phase,
            updatedAtMs = now,
            learningQuarantineId = learningQuarantineId,
            mainQuarantineId = mainQuarantineId,
            failureCode = null,
        )
        val store = ColdRestoreJournalStore(staging.pendingJournal)
        val result = store.transition(current.requestId, current.stateVersion, next)
        if (result == ColdRestoreJournalWriteResult.Written) return next
        val reread = store.read()
        if (reread is ColdRestoreJournalReadResult.Valid && reread.journal == next) return next
        throw swapFailure(
            ColdRestoreSwapFailure.JOURNAL_UPDATE_FAILED,
            ColdRestoreFailureCode.JOURNAL_DURABILITY_FAILED,
        )
    }

    private fun markFailedBestEffort(
        staging: ColdRestoreStagingPaths,
        current: ColdRestoreJournalV1,
        code: ColdRestoreFailureCode,
    ) {
        val now = runCatching { clockMs() }.getOrDefault(current.updatedAtMs)
            .coerceAtLeast(current.updatedAtMs)
        val failed = current.copy(
            stateVersion = runCatching { Math.addExact(current.stateVersion, 1L) }
                .getOrDefault(current.stateVersion),
            phase = ColdRestorePhase.FAILED_RESTART_REQUIRED,
            updatedAtMs = now,
            failureCode = code,
        )
        runCatching {
            ColdRestoreJournalStore(staging.pendingJournal).transition(
                current.requestId,
                current.stateVersion,
                failed,
            )
        }
    }
}

private class ColdRestoreSwapException(
    val failure: ColdRestoreSwapFailure,
    val failureCode: ColdRestoreFailureCode,
    cause: Throwable? = null,
) : Exception(failure.name, cause)

private fun exactQuarantineDirectory(parent: Path, prefix: String, id: String): Path {
    if (!QUARANTINE_ID.matches(id) || !safeDatabaseDirectory(parent)) {
        throw swapFailure(
            ColdRestoreSwapFailure.QUARANTINE_PATH_UNSAFE,
            ColdRestoreFailureCode.LEARNING_PATH_UNSAFE,
        )
    }
    val directory = parent.resolve("$prefix$id").normalize()
    if (directory.parent != parent) {
        throw swapFailure(
            ColdRestoreSwapFailure.QUARANTINE_PATH_UNSAFE,
            ColdRestoreFailureCode.LEARNING_PATH_UNSAFE,
        )
    }
    if (!Files.exists(directory, LinkOption.NOFOLLOW_LINKS)) {
        try {
            Files.createDirectory(directory)
        } catch (error: Exception) {
            throw swapFailure(
                ColdRestoreSwapFailure.QUARANTINE_PATH_UNSAFE,
                ColdRestoreFailureCode.LEARNING_PATH_UNSAFE,
                error,
            )
        }
    }
    val canonical = runCatching { directory.toFile().canonicalFile.toPath().normalize() }.getOrNull()
    if (canonical != directory || Files.isSymbolicLink(directory) ||
        !Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS)
    ) {
        throw swapFailure(
            ColdRestoreSwapFailure.QUARANTINE_PATH_UNSAFE,
            ColdRestoreFailureCode.LEARNING_PATH_UNSAFE,
        )
    }
    return directory
}

private fun identityMatches(path: Path, expectedSize: Long, expectedSha256: String): Boolean =
    runCatching {
        exactRegularFile(path, path.parent) && Files.size(path) == expectedSize &&
            constantTimeHexEquals(expectedSha256, sha256Swap(path))
    }.getOrDefault(false)

private fun exactRegularFile(path: Path, parent: Path): Boolean =
    path.parent == parent && !Files.isSymbolicLink(path) &&
        Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)

private fun safeDatabaseDirectory(path: Path): Boolean = runCatching {
    !Files.isSymbolicLink(path) && Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
        path.toFile().canonicalFile.toPath().normalize() == path
}.getOrDefault(false)

private fun safeLockFile(path: Path): Boolean =
    !Files.isSymbolicLink(path) && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)

private fun preparedSidecars(prepared: Path): List<Path> = listOf(
    prepared.resolveSibling("${prepared.fileName}-wal"),
    prepared.resolveSibling("${prepared.fileName}-shm"),
    prepared.resolveSibling("${prepared.fileName}-journal"),
)

private fun sha256Swap(path: Path): String {
    val digest = MessageDigest.getInstance("SHA-256")
    FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS).use { channel ->
        val buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE)
        while (true) {
            val count = channel.read(buffer)
            if (count < 0) break
            if (count == 0) continue
            digest.update(buffer.array(), 0, count)
            buffer.clear()
        }
    }
    return digest.digest().joinToString("") { byte ->
        (byte.toInt() and 0xff).toString(16).padStart(2, '0')
    }
}

private fun constantTimeHexEquals(expected: String, actual: String): Boolean {
    if (!HEX_SHA256.matches(expected) || !HEX_SHA256.matches(actual)) return false
    return MessageDigest.isEqual(expected.hexBytes(), actual.hexBytes())
}

private fun String.hexBytes(): ByteArray = ByteArray(length / 2) { index ->
    substring(index * 2, index * 2 + 2).toInt(16).toByte()
}

private fun crossedIrreversibleBoundary(phase: ColdRestorePhase): Boolean =
    phase.ordinal >= ColdRestorePhase.LEARNING_QUARANTINE_STARTED.ordinal &&
        phase != ColdRestorePhase.FAILED_RESTART_REQUIRED

private fun resultForCurrentPhase(
    phase: ColdRestorePhase,
    failure: ColdRestoreSwapFailure,
): ColdRestoreSwapResult = if (crossedIrreversibleBoundary(phase)) {
    degraded(failure)
} else {
    unchanged(failure)
}

private fun unchanged(failure: ColdRestoreSwapFailure) =
    ColdRestoreSwapResult.LiveDatabaseUnchanged(failure)

private fun degraded(failure: ColdRestoreSwapFailure) =
    ColdRestoreSwapResult.DegradedRestartRequired(failure)

private fun swapFailure(
    failure: ColdRestoreSwapFailure,
    failureCode: ColdRestoreFailureCode,
    cause: Throwable? = null,
) = ColdRestoreSwapException(failure, failureCode, cause)

private const val LEARNING_QUARANTINE_PREFIX = ".learning_runtime_quarantine_"
private const val MAIN_QUARANTINE_PREFIX = ".rikka_hub_quarantine_"
private val QUARANTINE_ID = Regex("[a-zA-Z0-9_-]{16,64}")
private val HEX_SHA256 = Regex("[0-9a-f]{64}")
private val LEARNING_FILE_NAMES = setOf(
    "learning_runtime.db",
    "learning_runtime.db-wal",
    "learning_runtime.db-shm",
)
private val MAIN_FILE_NAMES = setOf("rikka_hub", "rikka_hub-wal", "rikka_hub-shm")
