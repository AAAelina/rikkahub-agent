package me.rerere.rikkahub.data.sync.backup.restore

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.CodingErrorAction
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.nio.file.StandardOpenOption
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import me.rerere.rikkahub.data.sync.backup.MAX_BACKUP_ARCHIVE_BYTES
import me.rerere.rikkahub.data.sync.backup.MAX_BACKUP_MAIN_DATABASE_BYTES
import me.rerere.rikkahub.data.sync.backup.isCanonicalBackupSha256
import me.rerere.rikkahub.data.sync.backup.isCanonicalStreamId

private const val COLD_RESTORE_JOURNAL_VERSION = 1
private const val MAX_COLD_RESTORE_JOURNAL_BYTES = 64 * 1_024
private const val MIN_SQLITE_DATABASE_BYTES = 100L

@Serializable
enum class ColdRestorePhase {
    STAGED,
    READY_TO_SWAP,
    LEARNING_QUARANTINE_STARTED,
    LEARNING_QUARANTINED,
    OLD_MAIN_QUARANTINE_STARTED,
    OLD_MAIN_QUARANTINED,
    MAIN_INSTALL_STARTED,
    MAIN_INSTALLED,
    SWAP_COMMITTED,
    REBUILD_REQUIRED,
    COMPLETE,
    FAILED_RESTART_REQUIRED,
}

@Serializable
enum class ColdRestoreFailureCode {
    STAGED_ARCHIVE_MISSING,
    STAGED_ARCHIVE_CHANGED,
    MANIFEST_REJECTED,
    LEARNING_PATH_UNSAFE,
    LEARNING_QUARANTINE_FAILED,
    MAIN_PATH_UNSAFE,
    MAIN_QUARANTINE_FAILED,
    MAIN_INSTALL_FAILED,
    MAIN_RECONCILE_FAILED,
    MAIN_VALIDATION_FAILED,
    SETTINGS_RESTORE_FAILED,
    FILES_RESTORE_FAILED,
    LEARNING_BOOTSTRAP_FAILED,
    JOURNAL_DURABILITY_FAILED,
}

/**
 * Content-free crash journal for one cold restore. All paths are reconstructed from the fixed
 * app-owned root and [requestId]; no caller-controlled path is persisted.
 */
@Serializable
data class ColdRestoreJournalV1(
    val journalVersion: Int,
    val requestId: String,
    val stateVersion: Long,
    val phase: ColdRestorePhase,
    val components: List<BackupArchiveComponent>,
    val archiveSize: Long,
    val archiveSha256: String,
    val mainDatabaseSize: Long,
    val mainDatabaseSha256: String,
    val preparedDatabaseSize: Long?,
    val preparedDatabaseSha256: String?,
    val mainStream: BackupAuthorityStreamV1,
    val createdAtMs: Long,
    val updatedAtMs: Long,
    val learningQuarantineId: String?,
    val mainQuarantineId: String?,
    val failureCode: ColdRestoreFailureCode?,
    // Defaults preserve journals created before component replay moved to cold startup.
    val deferredComponents: Boolean = false,
    val restoreFiles: Boolean = false,
) {
    companion object {
        fun staged(
            requestId: String,
            components: List<BackupArchiveComponent>,
            archiveSize: Long,
            archiveSha256: String,
            mainDatabaseSize: Long,
            mainDatabaseSha256: String,
            mainStream: BackupAuthorityStreamV1,
            createdAtMs: Long,
        ): ColdRestoreJournalV1 = ColdRestoreJournalV1(
            journalVersion = COLD_RESTORE_JOURNAL_VERSION,
            requestId = requestId,
            stateVersion = 0L,
            phase = ColdRestorePhase.STAGED,
            components = components.sortedBy { it.ordinal },
            archiveSize = archiveSize,
            archiveSha256 = archiveSha256,
            mainDatabaseSize = mainDatabaseSize,
            mainDatabaseSha256 = mainDatabaseSha256,
            preparedDatabaseSize = null,
            preparedDatabaseSha256 = null,
            mainStream = mainStream,
            createdAtMs = createdAtMs,
            updatedAtMs = createdAtMs,
            learningQuarantineId = null,
            mainQuarantineId = null,
            failureCode = null,
        )
    }
}

enum class ColdRestoreJournalValidationFailure {
    VERSION_UNSUPPORTED,
    REQUEST_ID_INVALID,
    STATE_VERSION_INVALID,
    COMPONENTS_INVALID,
    ARCHIVE_IDENTITY_INVALID,
    MAIN_DATABASE_IDENTITY_INVALID,
    PREPARED_DATABASE_IDENTITY_INVALID,
    MAIN_STREAM_INVALID,
    CLOCK_INVALID,
    QUARANTINE_ID_INVALID,
    PHASE_FIELDS_INVALID,
}

enum class ColdRestoreJournalReadFailure {
    SYMBOLIC_LINK,
    NOT_REGULAR_FILE,
    EMPTY,
    TOO_LARGE,
    INVALID_UTF8,
    MALFORMED_JSON,
    IO_FAILED,
}

sealed interface ColdRestoreJournalReadResult {
    data object Missing : ColdRestoreJournalReadResult

    data class Valid(val journal: ColdRestoreJournalV1) : ColdRestoreJournalReadResult

    data class Invalid(
        val readFailure: ColdRestoreJournalReadFailure? = null,
        val validationFailure: ColdRestoreJournalValidationFailure? = null,
    ) : ColdRestoreJournalReadResult
}

sealed interface ColdRestoreJournalWriteResult {
    data object Written : ColdRestoreJournalWriteResult

    data object AlreadyExists : ColdRestoreJournalWriteResult

    data object Conflict : ColdRestoreJournalWriteResult

    data object CurrentJournalInvalid : ColdRestoreJournalWriteResult

    data object AtomicMoveUnsupported : ColdRestoreJournalWriteResult

    data object IoFailed : ColdRestoreJournalWriteResult

    data class Rejected(val failure: ColdRestoreJournalValidationFailure) :
        ColdRestoreJournalWriteResult
}

object ColdRestoreJournalCodec {
    private val json = Json {
        encodeDefaults = true
        explicitNulls = true
        ignoreUnknownKeys = false
        isLenient = false
        coerceInputValues = false
        allowSpecialFloatingPointValues = false
    }

    fun encode(journal: ColdRestoreJournalV1): ByteArray {
        val failure = validate(journal)
        require(failure == null) { "Invalid cold restore journal: ${failure?.name}" }
        val bytes = json.encodeToString(journal).toByteArray(Charsets.UTF_8)
        require(bytes.size in 1..MAX_COLD_RESTORE_JOURNAL_BYTES) {
            "Cold restore journal exceeds its size limit"
        }
        return bytes
    }

    fun decode(bytes: ByteArray): ColdRestoreJournalReadResult {
        if (bytes.isEmpty()) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.EMPTY,
            )
        }
        if (bytes.size > MAX_COLD_RESTORE_JOURNAL_BYTES) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.TOO_LARGE,
            )
        }
        val text = try {
            Charsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes))
                .toString()
        } catch (_: Exception) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.INVALID_UTF8,
            )
        }
        val journal = try {
            json.decodeFromString<ColdRestoreJournalV1>(text)
        } catch (_: Exception) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.MALFORMED_JSON,
            )
        }
        val failure = validate(journal)
        return if (failure == null) {
            ColdRestoreJournalReadResult.Valid(journal)
        } else {
            ColdRestoreJournalReadResult.Invalid(validationFailure = failure)
        }
    }

    fun validate(journal: ColdRestoreJournalV1): ColdRestoreJournalValidationFailure? {
        if (journal.journalVersion != COLD_RESTORE_JOURNAL_VERSION) {
            return ColdRestoreJournalValidationFailure.VERSION_UNSUPPORTED
        }
        if (!REQUEST_ID.matches(journal.requestId)) {
            return ColdRestoreJournalValidationFailure.REQUEST_ID_INVALID
        }
        if (journal.stateVersion < 0L) {
            return ColdRestoreJournalValidationFailure.STATE_VERSION_INVALID
        }
        if (journal.components.isEmpty() ||
            journal.components.distinct().size != journal.components.size ||
            BackupArchiveComponent.DATABASE !in journal.components
        ) {
            return ColdRestoreJournalValidationFailure.COMPONENTS_INVALID
        }
        if (journal.archiveSize <= 0L || journal.archiveSize > MAX_BACKUP_ARCHIVE_BYTES ||
            !isCanonicalBackupSha256(journal.archiveSha256)
        ) {
            return ColdRestoreJournalValidationFailure.ARCHIVE_IDENTITY_INVALID
        }
        if (journal.mainDatabaseSize < MIN_SQLITE_DATABASE_BYTES ||
            journal.mainDatabaseSize > MAX_BACKUP_MAIN_DATABASE_BYTES ||
            !isCanonicalBackupSha256(journal.mainDatabaseSha256)
        ) {
            return ColdRestoreJournalValidationFailure.MAIN_DATABASE_IDENTITY_INVALID
        }
        val preparedIdentityPresent = journal.preparedDatabaseSize != null &&
            journal.preparedDatabaseSha256 != null
        if ((journal.preparedDatabaseSize == null) !=
            (journal.preparedDatabaseSha256 == null)
        ) {
            return ColdRestoreJournalValidationFailure.PREPARED_DATABASE_IDENTITY_INVALID
        }
        if (preparedIdentityPresent &&
            (requireNotNull(journal.preparedDatabaseSize) < MIN_SQLITE_DATABASE_BYTES ||
                requireNotNull(journal.preparedDatabaseSize) >
                MAX_BACKUP_MAIN_DATABASE_BYTES ||
                !isCanonicalBackupSha256(requireNotNull(journal.preparedDatabaseSha256)))
        ) {
            return ColdRestoreJournalValidationFailure.PREPARED_DATABASE_IDENTITY_INVALID
        }
        if (!isCanonicalStreamId(journal.mainStream.streamId) ||
            journal.mainStream.headSeq <= 0L
        ) {
            return ColdRestoreJournalValidationFailure.MAIN_STREAM_INVALID
        }
        if (journal.createdAtMs < 0L || journal.updatedAtMs < journal.createdAtMs) {
            return ColdRestoreJournalValidationFailure.CLOCK_INVALID
        }
        if (journal.learningQuarantineId != null &&
            !QUARANTINE_ID.matches(journal.learningQuarantineId)
        ) {
            return ColdRestoreJournalValidationFailure.QUARANTINE_ID_INVALID
        }
        if (journal.mainQuarantineId != null &&
            !QUARANTINE_ID.matches(journal.mainQuarantineId)
        ) {
            return ColdRestoreJournalValidationFailure.QUARANTINE_ID_INVALID
        }

        val failed = journal.phase == ColdRestorePhase.FAILED_RESTART_REQUIRED
        val expectedStateVersion = journal.phase.ordinal.toLong()
        if ((!failed && journal.stateVersion != expectedStateVersion) ||
            (failed && journal.stateVersion !in 1L..ColdRestorePhase.COMPLETE.ordinal.toLong())
        ) {
            return ColdRestoreJournalValidationFailure.STATE_VERSION_INVALID
        }
        if (failed != (journal.failureCode != null)) {
            return ColdRestoreJournalValidationFailure.PHASE_FIELDS_INVALID
        }
        val fieldPhaseOrdinal = if (failed) {
            (journal.stateVersion - 1L).toInt()
        } else {
            journal.phase.ordinal
        }
        val learningIdRequired =
            fieldPhaseOrdinal >= ColdRestorePhase.LEARNING_QUARANTINE_STARTED.ordinal
        val mainIdRequired =
            fieldPhaseOrdinal >= ColdRestorePhase.OLD_MAIN_QUARANTINE_STARTED.ordinal
        val preparedIdentityRequired = fieldPhaseOrdinal >= ColdRestorePhase.READY_TO_SWAP.ordinal
        if (preparedIdentityPresent != preparedIdentityRequired ||
            (journal.learningQuarantineId != null) != learningIdRequired ||
            (journal.mainQuarantineId != null) != mainIdRequired
        ) {
            return ColdRestoreJournalValidationFailure.PHASE_FIELDS_INVALID
        }
        return null
    }
}

/**
 * Bounded journal persistence. The caller owns the cross-process lock; this class supplies the
 * state-version CAS and refuses to fall back when the filesystem cannot provide atomic rename.
 */
internal class ColdRestoreJournalStore(
    private val pendingFile: Path,
) {
    @Synchronized
    fun read(): ColdRestoreJournalReadResult {
        if (!Files.exists(pendingFile, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreJournalReadResult.Missing
        }
        if (Files.isSymbolicLink(pendingFile)) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.SYMBOLIC_LINK,
            )
        }
        if (!Files.isRegularFile(pendingFile, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.NOT_REGULAR_FILE,
            )
        }
        val bytes = try {
            readBounded(pendingFile)
        } catch (_: JournalTooLargeException) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.TOO_LARGE,
            )
        } catch (_: Exception) {
            return ColdRestoreJournalReadResult.Invalid(
                readFailure = ColdRestoreJournalReadFailure.IO_FAILED,
            )
        }
        return ColdRestoreJournalCodec.decode(bytes)
    }

    @Synchronized
    fun create(journal: ColdRestoreJournalV1): ColdRestoreJournalWriteResult {
        ColdRestoreJournalCodec.validate(journal)?.let {
            return ColdRestoreJournalWriteResult.Rejected(it)
        }
        if (journal.phase != ColdRestorePhase.STAGED || journal.stateVersion != 0L) {
            return ColdRestoreJournalWriteResult.Rejected(
                ColdRestoreJournalValidationFailure.PHASE_FIELDS_INVALID,
            )
        }
        if (Files.exists(pendingFile, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreJournalWriteResult.AlreadyExists
        }
        return writeAtomically(journal, replaceExisting = false)
    }

    @Synchronized
    fun transition(
        expectedRequestId: String,
        expectedStateVersion: Long,
        next: ColdRestoreJournalV1,
    ): ColdRestoreJournalWriteResult {
        val current = when (val result = read()) {
            is ColdRestoreJournalReadResult.Valid -> result.journal
            ColdRestoreJournalReadResult.Missing -> return ColdRestoreJournalWriteResult.Conflict
            is ColdRestoreJournalReadResult.Invalid ->
                return ColdRestoreJournalWriteResult.CurrentJournalInvalid
        }
        if (current.requestId != expectedRequestId ||
            current.stateVersion != expectedStateVersion
        ) {
            return ColdRestoreJournalWriteResult.Conflict
        }
        val expectedNextVersion = try {
            Math.addExact(current.stateVersion, 1L)
        } catch (_: ArithmeticException) {
            return ColdRestoreJournalWriteResult.Conflict
        }
        if (next.stateVersion != expectedNextVersion || !immutableFieldsMatch(current, next) ||
            !isAllowedTransition(current.phase, next.phase) ||
            !preparedIdentityFollowsTransition(current, next) ||
            !quarantineFieldsFollowTransition(current, next)
        ) {
            return ColdRestoreJournalWriteResult.Conflict
        }
        ColdRestoreJournalCodec.validate(next)?.let {
            return ColdRestoreJournalWriteResult.Rejected(it)
        }
        return writeAtomically(next, replaceExisting = true)
    }

    private fun writeAtomically(
        journal: ColdRestoreJournalV1,
        replaceExisting: Boolean,
    ): ColdRestoreJournalWriteResult {
        val parent = pendingFile.parent ?: return ColdRestoreJournalWriteResult.IoFailed
        if (Files.isSymbolicLink(parent) || !Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreJournalWriteResult.IoFailed
        }
        val payload = try {
            ColdRestoreJournalCodec.encode(journal)
        } catch (_: Exception) {
            return ColdRestoreJournalWriteResult.IoFailed
        }
        val temporary = try {
            Files.createTempFile(parent, ".pending_restore_", ".tmp")
        } catch (_: Exception) {
            return ColdRestoreJournalWriteResult.IoFailed
        }
        try {
            FileChannel.open(
                temporary,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
            ).use { channel ->
                val buffer = ByteBuffer.wrap(payload)
                while (buffer.hasRemaining()) channel.write(buffer)
                channel.force(true)
            }
            val options = if (replaceExisting) {
                arrayOf(StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING)
            } else {
                arrayOf(StandardCopyOption.ATOMIC_MOVE)
            }
            Files.move(temporary, pendingFile, *options)
            return ColdRestoreJournalWriteResult.Written
        } catch (_: AtomicMoveNotSupportedException) {
            return ColdRestoreJournalWriteResult.AtomicMoveUnsupported
        } catch (_: java.nio.file.FileAlreadyExistsException) {
            return ColdRestoreJournalWriteResult.AlreadyExists
        } catch (_: Exception) {
            return ColdRestoreJournalWriteResult.IoFailed
        } finally {
            runCatching { Files.deleteIfExists(temporary) }
        }
    }
}

private fun immutableFieldsMatch(
    current: ColdRestoreJournalV1,
    next: ColdRestoreJournalV1,
): Boolean =
    current.journalVersion == next.journalVersion &&
        current.requestId == next.requestId &&
        current.components == next.components &&
        current.deferredComponents == next.deferredComponents &&
        current.restoreFiles == next.restoreFiles &&
        current.archiveSize == next.archiveSize &&
        current.archiveSha256 == next.archiveSha256 &&
        current.mainDatabaseSize == next.mainDatabaseSize &&
        current.mainDatabaseSha256 == next.mainDatabaseSha256 &&
        current.mainStream == next.mainStream &&
        current.createdAtMs == next.createdAtMs &&
        next.updatedAtMs >= current.updatedAtMs

private fun preparedIdentityFollowsTransition(
    current: ColdRestoreJournalV1,
    next: ColdRestoreJournalV1,
): Boolean = if (current.preparedDatabaseSize != null) {
    next.preparedDatabaseSize == current.preparedDatabaseSize &&
        next.preparedDatabaseSha256 == current.preparedDatabaseSha256
} else if (next.phase == ColdRestorePhase.READY_TO_SWAP) {
    next.preparedDatabaseSize != null && next.preparedDatabaseSha256 != null
} else {
    next.preparedDatabaseSize == null && next.preparedDatabaseSha256 == null
}

private fun isAllowedTransition(current: ColdRestorePhase, next: ColdRestorePhase): Boolean {
    if (current == ColdRestorePhase.COMPLETE ||
        current == ColdRestorePhase.FAILED_RESTART_REQUIRED
    ) {
        return false
    }
    if (next == ColdRestorePhase.FAILED_RESTART_REQUIRED) return true
    return when (current) {
        ColdRestorePhase.STAGED -> next == ColdRestorePhase.READY_TO_SWAP
        ColdRestorePhase.READY_TO_SWAP ->
            next == ColdRestorePhase.LEARNING_QUARANTINE_STARTED
        ColdRestorePhase.LEARNING_QUARANTINE_STARTED ->
            next == ColdRestorePhase.LEARNING_QUARANTINED
        ColdRestorePhase.LEARNING_QUARANTINED ->
            next == ColdRestorePhase.OLD_MAIN_QUARANTINE_STARTED
        ColdRestorePhase.OLD_MAIN_QUARANTINE_STARTED ->
            next == ColdRestorePhase.OLD_MAIN_QUARANTINED
        ColdRestorePhase.OLD_MAIN_QUARANTINED ->
            next == ColdRestorePhase.MAIN_INSTALL_STARTED
        ColdRestorePhase.MAIN_INSTALL_STARTED -> next == ColdRestorePhase.MAIN_INSTALLED
        ColdRestorePhase.MAIN_INSTALLED -> next == ColdRestorePhase.SWAP_COMMITTED
        ColdRestorePhase.SWAP_COMMITTED -> next == ColdRestorePhase.REBUILD_REQUIRED
        ColdRestorePhase.REBUILD_REQUIRED -> next == ColdRestorePhase.COMPLETE
        ColdRestorePhase.COMPLETE,
        ColdRestorePhase.FAILED_RESTART_REQUIRED -> false
    }
}

private fun quarantineFieldsFollowTransition(
    current: ColdRestoreJournalV1,
    next: ColdRestoreJournalV1,
): Boolean {
    if (next.phase == ColdRestorePhase.FAILED_RESTART_REQUIRED) {
        return next.learningQuarantineId == current.learningQuarantineId &&
            next.mainQuarantineId == current.mainQuarantineId
    }
    val learningIdValid = when {
        current.learningQuarantineId != null ->
            next.learningQuarantineId == current.learningQuarantineId
        next.phase == ColdRestorePhase.LEARNING_QUARANTINE_STARTED ->
            next.learningQuarantineId != null
        next.phase.ordinal < ColdRestorePhase.LEARNING_QUARANTINE_STARTED.ordinal ->
            next.learningQuarantineId == null
        else -> false
    }
    val mainIdValid = when {
        current.mainQuarantineId != null -> next.mainQuarantineId == current.mainQuarantineId
        next.phase == ColdRestorePhase.OLD_MAIN_QUARANTINE_STARTED -> next.mainQuarantineId != null
        next.phase.ordinal < ColdRestorePhase.OLD_MAIN_QUARANTINE_STARTED.ordinal ->
            next.mainQuarantineId == null
        else -> false
    }
    return learningIdValid && mainIdValid
}

private fun readBounded(path: Path): ByteArray {
    val output = ByteArrayOutputStream()
    Files.newInputStream(path, StandardOpenOption.READ).use { input ->
        val buffer = ByteArray(8_192)
        var total = 0
        while (true) {
            val count = input.read(buffer)
            if (count < 0) break
            if (count == 0) continue
            total = Math.addExact(total, count)
            if (total > MAX_COLD_RESTORE_JOURNAL_BYTES) throw JournalTooLargeException()
            output.write(buffer, 0, count)
        }
    }
    return output.toByteArray()
}

private class JournalTooLargeException : RuntimeException()

private val REQUEST_ID = Regex("[0-9a-f]{32}")
private val QUARANTINE_ID = Regex("none|[a-zA-Z0-9_-]{16,64}")
