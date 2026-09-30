package me.rerere.rikkahub.data.sync.backup.restore

import java.io.ByteArrayOutputStream
import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.FileChannel
import java.nio.channels.OverlappingFileLockException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.security.MessageDigest
import java.util.zip.ZipEntry
import java.util.zip.ZipInputStream
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_MANIFEST_ENTRY
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestCodec
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestDecodeResult
import me.rerere.rikkahub.data.sync.backup.BackupArchiveManifestV1
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import me.rerere.rikkahub.data.sync.backup.MAX_BACKUP_MAIN_DATABASE_BYTES
import me.rerere.rikkahub.data.sync.backup.isCanonicalBackupSha256
import me.rerere.rikkahub.data.sync.backup.isSafeBackupEntryName

enum class ColdRestoreBootstrapPathFailure {
    MAIN_DATABASE_PATH_NOT_ABSOLUTE,
    MAIN_DATABASE_PATH_NOT_EXACT,
    DATABASE_DIRECTORY_NOT_OWNED,
    DATABASE_DIRECTORY_NOT_DIRECTORY,
    DATABASE_DIRECTORY_SYMBOLIC_LINK,
    MAIN_DATABASE_FILE_UNSAFE,
    PATH_CANONICALIZATION_FAILED,
}

sealed interface ColdRestoreBootstrapPathValidation {
    class Valid internal constructor(val paths: ColdRestoreBootstrapPaths) :
        ColdRestoreBootstrapPathValidation

    data class Invalid(val failure: ColdRestoreBootstrapPathFailure) :
        ColdRestoreBootstrapPathValidation
}

/** Exact cold-start paths. No caller-controlled child name is accepted. */
class ColdRestoreBootstrapPaths private constructor(
    internal val mainDatabase: Path,
    internal val mainWal: Path,
    internal val mainShm: Path,
) {
    internal val databaseDirectory: Path = mainDatabase.parent

    internal fun preparedDatabase(requestId: String): Path =
        databaseDirectory.resolve(".rikka_hub.restore_$requestId.ready").normalize()

    override fun toString(): String = "ColdRestoreBootstrapPaths(paths=<redacted>)"

    companion object {
        fun verify(
            applicationDataDirectory: File,
            mainDatabaseFile: File,
        ): ColdRestoreBootstrapPathValidation {
            if (!applicationDataDirectory.isAbsolute || !mainDatabaseFile.isAbsolute) {
                return invalidBootstrapPath(
                    ColdRestoreBootstrapPathFailure.MAIN_DATABASE_PATH_NOT_ABSOLUTE,
                )
            }
            return try {
                val appData = applicationDataDirectory.toPath().toAbsolutePath().normalize()
                if (Files.isSymbolicLink(appData) ||
                    !Files.isDirectory(appData, LinkOption.NOFOLLOW_LINKS) ||
                    appData.toFile().canonicalFile.toPath().normalize() != appData
                ) {
                    return invalidBootstrapPath(
                        ColdRestoreBootstrapPathFailure.DATABASE_DIRECTORY_NOT_OWNED,
                    )
                }
                val databaseDirectory = appData.resolve(DATABASES_DIRECTORY).normalize()
                if (databaseDirectory.parent != appData) {
                    return invalidBootstrapPath(
                        ColdRestoreBootstrapPathFailure.DATABASE_DIRECTORY_NOT_OWNED,
                    )
                }
                if (Files.isSymbolicLink(databaseDirectory)) {
                    return invalidBootstrapPath(
                        ColdRestoreBootstrapPathFailure.DATABASE_DIRECTORY_SYMBOLIC_LINK,
                    )
                }
                if (!Files.isDirectory(databaseDirectory, LinkOption.NOFOLLOW_LINKS)) {
                    return invalidBootstrapPath(
                        ColdRestoreBootstrapPathFailure.DATABASE_DIRECTORY_NOT_DIRECTORY,
                    )
                }
                if (databaseDirectory.toFile().canonicalFile.toPath().normalize() !=
                    databaseDirectory
                ) {
                    return invalidBootstrapPath(
                        ColdRestoreBootstrapPathFailure.DATABASE_DIRECTORY_SYMBOLIC_LINK,
                    )
                }

                val expectedMain = databaseDirectory.resolve(MAIN_DATABASE_NAME).normalize()
                val actualMain = mainDatabaseFile.toPath().toAbsolutePath().normalize()
                if (actualMain != expectedMain) {
                    return invalidBootstrapPath(
                        ColdRestoreBootstrapPathFailure.MAIN_DATABASE_PATH_NOT_EXACT,
                    )
                }
                for (candidate in listOf(
                    expectedMain,
                    databaseDirectory.resolve(MAIN_WAL_NAME),
                    databaseDirectory.resolve(MAIN_SHM_NAME),
                )) {
                    if (Files.isSymbolicLink(candidate) ||
                        (Files.exists(candidate, LinkOption.NOFOLLOW_LINKS) &&
                            !Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS))
                    ) {
                        return invalidBootstrapPath(
                            ColdRestoreBootstrapPathFailure.MAIN_DATABASE_FILE_UNSAFE,
                        )
                    }
                }
                ColdRestoreBootstrapPathValidation.Valid(
                    ColdRestoreBootstrapPaths(
                        mainDatabase = expectedMain,
                        mainWal = databaseDirectory.resolve(MAIN_WAL_NAME),
                        mainShm = databaseDirectory.resolve(MAIN_SHM_NAME),
                    ),
                )
            } catch (_: Exception) {
                invalidBootstrapPath(
                    ColdRestoreBootstrapPathFailure.PATH_CANONICALIZATION_FAILED,
                )
            }
        }

        private const val DATABASES_DIRECTORY = "databases"
        private const val MAIN_DATABASE_NAME = "rikka_hub"
        private const val MAIN_WAL_NAME = "rikka_hub-wal"
        private const val MAIN_SHM_NAME = "rikka_hub-shm"
    }
}

enum class ColdRestoreBootstrapFailure {
    STAGING_PATH_UNSAFE,
    MAIN_DATABASE_PATH_UNSAFE,
    STAGING_PATH_CHANGED,
    MAIN_DATABASE_PATH_CHANGED,
    PENDING_JOURNAL_INVALID,
    PENDING_PHASE_UNSUPPORTED,
    STAGED_ARCHIVE_MISSING,
    STAGED_ARCHIVE_CHANGED,
    ARCHIVE_UNSAFE_ENTRY,
    ARCHIVE_DUPLICATE_ENTRY,
    ARCHIVE_ENTRY_UNDECLARED,
    MANIFEST_MISSING,
    MANIFEST_DUPLICATE,
    MANIFEST_REJECTED,
    MANIFEST_IDENTITY_MISMATCH,
    MAIN_DATABASE_MISSING,
    MAIN_DATABASE_DUPLICATE,
    MAIN_DATABASE_INVALID,
    MAIN_DATABASE_IDENTITY_MISMATCH,
    PREPARED_DATABASE_COLLISION,
    DATABASE_RECONCILE_FAILED,
    DATABASE_VALIDATION_FAILED,
    JOURNAL_UPDATE_FAILED,
    STORAGE_IO_FAILED,
}

sealed interface ColdRestoreBootstrapResult {
    data object NoPendingRestore : ColdRestoreBootstrapResult

    data object Busy : ColdRestoreBootstrapResult

    /**
     * Preparation evidence, not a live-swap capability. A future swap executor must reacquire the
     * restore lock and re-read the READY journal and prepared digest before crossing its boundary.
     */
    class ReadyToSwap internal constructor(
        val requestId: String,
        val preparedDatabase: File,
    ) : ColdRestoreBootstrapResult {
        override fun toString(): String =
            "ReadyToSwap(request=<redacted>, database=<redacted>)"
    }

    data class Failed(val failure: ColdRestoreBootstrapFailure) : ColdRestoreBootstrapResult
}

/**
 * Staged-file compatibility seam. Implementations may mutate only [databaseFile], must verify
 * [expectedMainStream], throw on every refusal/failure, and return only when strict validation can
 * prove the candidate belongs to the manifest's authoritative timeline.
 */
fun interface ColdRestorePreparedDatabaseReconciler {
    @Throws(Exception::class)
    fun reconcile(databaseFile: File, expectedMainStream: BackupAuthorityStreamV1)
}

/** Validates the reconciled staged file and authority stream without opening a live Room graph. */
fun interface ColdRestorePreparedDatabaseValidator {
    @Throws(Exception::class)
    fun validate(databaseFile: File, expectedMainStream: BackupAuthorityStreamV1)
}

/**
 * Consumes a committed staging journal before Room/Koin and prepares one same-directory DB file.
 * The v1 manifest must be the first ZIP entry, allowing every later entry to be bounded and
 * authenticated before decompression rather than trusting attacker-controlled ZIP metadata.
 *
 * This preparation executor deliberately stops at [ColdRestoreBootstrapResult.ReadyToSwap] and
 * never mutates a live DB itself. [ColdRestoreSwapExecutor], invoked by the pre-Koin startup seam,
 * owns the separate crash-resumable quarantine/install phases recorded in the same journal.
 */
class ColdRestoreBootstrap(
    private val stagingPaths: ColdRestoreStagingPathValidation,
    private val bootstrapPaths: ColdRestoreBootstrapPathValidation,
    private val reconciler: ColdRestorePreparedDatabaseReconciler,
    private val validator: ColdRestorePreparedDatabaseValidator,
    private val clockMs: () -> Long = System::currentTimeMillis,
) {
    fun prepare(): ColdRestoreBootstrapResult {
        val staging = when (stagingPaths) {
            is ColdRestoreStagingPathValidation.Invalid -> {
                return failed(ColdRestoreBootstrapFailure.STAGING_PATH_UNSAFE)
            }
            is ColdRestoreStagingPathValidation.Valid -> stagingPaths.paths
        }
        if (!Files.exists(staging.rootDirectory, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreBootstrapResult.NoPendingRestore
        }
        if (!stagingRootIsUnchanged(staging)) {
            return failed(ColdRestoreBootstrapFailure.STAGING_PATH_CHANGED)
        }
        if (!Files.exists(staging.pendingJournal, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreBootstrapResult.NoPendingRestore
        }
        val bootstrap = when (bootstrapPaths) {
            is ColdRestoreBootstrapPathValidation.Invalid -> {
                return failed(ColdRestoreBootstrapFailure.MAIN_DATABASE_PATH_UNSAFE)
            }
            is ColdRestoreBootstrapPathValidation.Valid -> bootstrapPaths.paths
        }
        if (!databaseDirectoryIsUnchanged(bootstrap)) {
            return failed(ColdRestoreBootstrapFailure.MAIN_DATABASE_PATH_CHANGED)
        }
        if (!lockFileIsSafe(staging.lockFile)) {
            return failed(ColdRestoreBootstrapFailure.STAGING_PATH_CHANGED)
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
                    ColdRestoreBootstrapResult.Busy
                } else {
                    lock.use { prepareWhileLocked(staging, bootstrap) }
                }
            }
        } catch (_: Exception) {
            failed(ColdRestoreBootstrapFailure.STORAGE_IO_FAILED)
        }
    }

    private fun prepareWhileLocked(
        staging: ColdRestoreStagingPaths,
        bootstrap: ColdRestoreBootstrapPaths,
    ): ColdRestoreBootstrapResult {
        val journal = when (val read = ColdRestoreJournalStore(staging.pendingJournal).read()) {
            ColdRestoreJournalReadResult.Missing -> return ColdRestoreBootstrapResult.NoPendingRestore
            is ColdRestoreJournalReadResult.Invalid -> {
                return failed(ColdRestoreBootstrapFailure.PENDING_JOURNAL_INVALID)
            }
            is ColdRestoreJournalReadResult.Valid -> read.journal
        }
        if (journal.phase != ColdRestorePhase.STAGED &&
            journal.phase != ColdRestorePhase.READY_TO_SWAP
        ) {
            return failed(ColdRestoreBootstrapFailure.PENDING_PHASE_UNSUPPORTED)
        }
        val requestDirectory = staging.requestDirectory(journal.requestId)
        val stagedArchive = staging.stagedArchive(journal.requestId)
        if (!exactRequestDirectory(requestDirectory, staging.rootDirectory) ||
            !exactRegularFile(stagedArchive, requestDirectory)
        ) {
            return failed(ColdRestoreBootstrapFailure.STAGED_ARCHIVE_MISSING)
        }
        val prepared = bootstrap.preparedDatabase(journal.requestId)
        if (prepared.parent != bootstrap.databaseDirectory) {
            return failed(ColdRestoreBootstrapFailure.PREPARED_DATABASE_COLLISION)
        }

        var retainPrepared = journal.phase == ColdRestorePhase.READY_TO_SWAP
        try {
            if (!fileIdentityMatches(
                    path = stagedArchive,
                    expectedSize = journal.archiveSize,
                    expectedSha256 = journal.archiveSha256,
                )
            ) {
                return failed(ColdRestoreBootstrapFailure.STAGED_ARCHIVE_CHANGED)
            }
            if (journal.phase == ColdRestorePhase.READY_TO_SWAP) {
                if (!exactRegularFile(prepared, bootstrap.databaseDirectory) ||
                    preparedSidecars(prepared).any { Files.exists(it, LinkOption.NOFOLLOW_LINKS) } ||
                    !fileIdentityMatches(
                        prepared,
                        requireNotNull(journal.preparedDatabaseSize),
                        requireNotNull(journal.preparedDatabaseSha256),
                    )
                ) {
                    return failed(ColdRestoreBootstrapFailure.DATABASE_VALIDATION_FAILED)
                }
                val resumed = validatePreparedForReady(
                    journal.requestId,
                    prepared,
                    journal.mainStream,
                )
                if (resumed is ColdRestoreBootstrapResult.ReadyToSwap &&
                    !fileIdentityMatches(
                        prepared,
                        requireNotNull(journal.preparedDatabaseSize),
                        requireNotNull(journal.preparedDatabaseSha256),
                    )
                ) {
                    return failed(ColdRestoreBootstrapFailure.DATABASE_VALIDATION_FAILED)
                }
                return resumed
            }

            if (!deleteExactStalePreparedArtifacts(prepared, bootstrap.databaseDirectory)) {
                return failed(ColdRestoreBootstrapFailure.PREPARED_DATABASE_COLLISION)
            }
            when (val extraction = extractAndVerify(stagedArchive, prepared, journal)) {
                is ExtractionResult.Rejected -> return failed(extraction.failure)
                is ExtractionResult.Verified -> Unit
            }
            if (!fileIdentityMatches(
                    path = stagedArchive,
                    expectedSize = journal.archiveSize,
                    expectedSha256 = journal.archiveSha256,
                )
            ) {
                return failed(ColdRestoreBootstrapFailure.STAGED_ARCHIVE_CHANGED)
            }
            when (val validated = reconcileAndValidatePrepared(
                journal.requestId,
                prepared,
                journal.mainStream,
            )) {
                is ColdRestoreBootstrapResult.Failed -> return validated
                is ColdRestoreBootstrapResult.ReadyToSwap -> Unit
                ColdRestoreBootstrapResult.Busy,
                ColdRestoreBootstrapResult.NoPendingRestore ->
                    return failed(ColdRestoreBootstrapFailure.DATABASE_VALIDATION_FAILED)
            }
            if (!fileIdentityMatches(
                    path = stagedArchive,
                    expectedSize = journal.archiveSize,
                    expectedSha256 = journal.archiveSha256,
                )
            ) {
                return failed(ColdRestoreBootstrapFailure.STAGED_ARCHIVE_CHANGED)
            }

            val now = try {
                clockMs()
            } catch (_: Exception) {
                return failed(ColdRestoreBootstrapFailure.JOURNAL_UPDATE_FAILED)
            }
            if (now < journal.updatedAtMs) {
                return failed(ColdRestoreBootstrapFailure.JOURNAL_UPDATE_FAILED)
            }
            val preparedSize = Files.size(prepared)
            val preparedSha256 = sha256(prepared)
            val ready = journal.copy(
                stateVersion = journal.stateVersion + 1L,
                phase = ColdRestorePhase.READY_TO_SWAP,
                updatedAtMs = now,
                preparedDatabaseSize = preparedSize,
                preparedDatabaseSha256 = preparedSha256,
            )
            val write = ColdRestoreJournalStore(staging.pendingJournal).transition(
                expectedRequestId = journal.requestId,
                expectedStateVersion = journal.stateVersion,
                next = ready,
            )
            if (write != ColdRestoreJournalWriteResult.Written) {
                val committed = ColdRestoreJournalStore(staging.pendingJournal).read()
                if (committed is ColdRestoreJournalReadResult.Valid &&
                    committed.journal == ready
                ) {
                    retainPrepared = true
                    return ColdRestoreBootstrapResult.ReadyToSwap(
                        journal.requestId,
                        prepared.toFile(),
                    )
                }
                return failed(ColdRestoreBootstrapFailure.JOURNAL_UPDATE_FAILED)
            }
            retainPrepared = true
            return ColdRestoreBootstrapResult.ReadyToSwap(journal.requestId, prepared.toFile())
        } catch (_: Exception) {
            return failed(ColdRestoreBootstrapFailure.STORAGE_IO_FAILED)
        } finally {
            if (!retainPrepared) {
                runCatching { Files.deleteIfExists(prepared) }
                preparedSidecars(prepared).forEach { sidecar ->
                    runCatching { Files.deleteIfExists(sidecar) }
                }
            }
        }
    }

    private fun validatePreparedForReady(
        requestId: String,
        prepared: Path,
        expectedMainStream: BackupAuthorityStreamV1,
    ): ColdRestoreBootstrapResult =
        reconcileAndValidatePrepared(requestId, prepared, expectedMainStream)

    private fun reconcileAndValidatePrepared(
        requestId: String,
        prepared: Path,
        expectedMainStream: BackupAuthorityStreamV1,
    ): ColdRestoreBootstrapResult {
        try {
            reconciler.reconcile(prepared.toFile(), expectedMainStream)
        } catch (_: Exception) {
            return failed(ColdRestoreBootstrapFailure.DATABASE_RECONCILE_FAILED)
        }
        if (!exactRegularFile(prepared, prepared.parent) ||
            preparedSidecars(prepared).any { Files.exists(it, LinkOption.NOFOLLOW_LINKS) }
        ) {
            return failed(ColdRestoreBootstrapFailure.DATABASE_VALIDATION_FAILED)
        }
        try {
            validator.validate(prepared.toFile(), expectedMainStream)
        } catch (_: Exception) {
            return failed(ColdRestoreBootstrapFailure.DATABASE_VALIDATION_FAILED)
        }
        if (!exactRegularFile(prepared, prepared.parent) ||
            preparedSidecars(prepared).any { Files.exists(it, LinkOption.NOFOLLOW_LINKS) }
        ) {
            return failed(ColdRestoreBootstrapFailure.DATABASE_VALIDATION_FAILED)
        }
        forceFile(prepared)
        return ColdRestoreBootstrapResult.ReadyToSwap(requestId, prepared.toFile())
    }

    private fun extractAndVerify(
        archive: Path,
        prepared: Path,
        journal: ColdRestoreJournalV1,
    ): ExtractionResult {
        var manifest: BackupArchiveManifestV1? = null
        var databaseFound = false
        val seen = mutableSetOf<String>()
        FileChannel.open(
            archive,
            StandardOpenOption.READ,
            LinkOption.NOFOLLOW_LINKS,
        ).use { archiveChannel ->
            ZipInputStream(Channels.newInputStream(archiveChannel).buffered()).use { zip ->
                while (true) {
                    val entry = zip.nextEntry ?: break
                    val name = entry.name
                    if (!isSafeZipEntry(entry)) {
                        return rejectedExtraction(ColdRestoreBootstrapFailure.ARCHIVE_UNSAFE_ENTRY)
                    }
                    if (!seen.add(name)) {
                        return rejectedExtraction(
                            when (name) {
                                BACKUP_ARCHIVE_MANIFEST_ENTRY ->
                                    ColdRestoreBootstrapFailure.MANIFEST_DUPLICATE
                                BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY ->
                                    ColdRestoreBootstrapFailure.MAIN_DATABASE_DUPLICATE
                                else -> ColdRestoreBootstrapFailure.ARCHIVE_DUPLICATE_ENTRY
                            },
                        )
                    }
                    if (seen.size > MAX_BOOTSTRAP_ZIP_ENTRIES) {
                        return rejectedExtraction(
                            ColdRestoreBootstrapFailure.ARCHIVE_ENTRY_UNDECLARED,
                        )
                    }
                    when (name) {
                        BACKUP_ARCHIVE_MANIFEST_ENTRY -> {
                            if (seen.size != 1 || manifest != null) {
                                return rejectedExtraction(
                                    ColdRestoreBootstrapFailure.MANIFEST_DUPLICATE,
                                )
                            }
                            val bytes = readBounded(zip, MAX_BOOTSTRAP_MANIFEST_BYTES)
                                ?: return rejectedExtraction(
                                    ColdRestoreBootstrapFailure.MANIFEST_REJECTED,
                                )
                            manifest = when (
                                val decoded = BackupArchiveManifestCodec.decode(bytes)
                            ) {
                                is BackupArchiveManifestDecodeResult.Rejected -> {
                                    return rejectedExtraction(
                                        ColdRestoreBootstrapFailure.MANIFEST_REJECTED,
                                    )
                                }
                                is BackupArchiveManifestDecodeResult.Verified -> decoded.manifest
                            }
                            if (!manifestMatchesJournal(requireNotNull(manifest), journal)) {
                                return rejectedExtraction(
                                    ColdRestoreBootstrapFailure.MANIFEST_IDENTITY_MISMATCH,
                                )
                            }
                        }
                        else -> {
                            val declared = manifest?.entries?.get(name)
                                ?: return rejectedExtraction(
                                    if (manifest == null) {
                                        ColdRestoreBootstrapFailure.MANIFEST_MISSING
                                    } else {
                                        ColdRestoreBootstrapFailure.ARCHIVE_ENTRY_UNDECLARED
                                    },
                                )
                            if (name == BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY) {
                                val identity = extractDatabase(zip, prepared, declared.size)
                                    ?: return rejectedExtraction(
                                        ColdRestoreBootstrapFailure.MAIN_DATABASE_INVALID,
                                    )
                                if (identity.size != journal.mainDatabaseSize ||
                                    !constantTimeDigestEquals(
                                        journal.mainDatabaseSha256,
                                        identity.sha256,
                                    )
                                ) {
                                    return rejectedExtraction(
                                        ColdRestoreBootstrapFailure.MAIN_DATABASE_IDENTITY_MISMATCH,
                                    )
                                }
                                databaseFound = true
                            } else if (!scanDeclaredEntry(zip, declared.size, declared.sha256)) {
                                return rejectedExtraction(
                                    ColdRestoreBootstrapFailure.MANIFEST_IDENTITY_MISMATCH,
                                )
                            }
                        }
                    }
                    zip.closeEntry()
                }
            }
        }
        val verifiedManifest = manifest
            ?: return rejectedExtraction(ColdRestoreBootstrapFailure.MANIFEST_MISSING)
        if (!databaseFound) {
            return rejectedExtraction(ColdRestoreBootstrapFailure.MAIN_DATABASE_MISSING)
        }
        if (verifiedManifest.entries.keys.any { it !in seen }) {
            return rejectedExtraction(ColdRestoreBootstrapFailure.ARCHIVE_ENTRY_UNDECLARED)
        }
        return ExtractionResult.Verified
    }
}

private sealed interface ExtractionResult {
    data object Verified : ExtractionResult

    data class Rejected(val failure: ColdRestoreBootstrapFailure) : ExtractionResult
}

private data class ExtractedDatabaseIdentity(val size: Long, val sha256: String)

private fun manifestMatchesJournal(
    manifest: BackupArchiveManifestV1,
    journal: ColdRestoreJournalV1,
): Boolean =
    manifest.components.sortedBy { it.ordinal } == journal.components &&
        manifest.mainStream == journal.mainStream &&
        manifest.entries[BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY]?.size == journal.mainDatabaseSize &&
        manifest.entries[BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY]?.sha256 ==
        journal.mainDatabaseSha256

private fun scanDeclaredEntry(
    zip: ZipInputStream,
    declaredSize: Long,
    declaredSha256: String,
): Boolean {
    if (declaredSize < 0L || !isCanonicalBackupSha256(declaredSha256)) return false
    val digest = MessageDigest.getInstance(SHA_256)
    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
    var total = 0L
    while (true) {
        val count = zip.read(buffer)
        if (count < 0) break
        if (count == 0) continue
        total = try {
            Math.addExact(total, count.toLong())
        } catch (_: ArithmeticException) {
            return false
        }
        if (total > declaredSize) return false
        digest.update(buffer, 0, count)
    }
    return total == declaredSize &&
        constantTimeDigestEquals(declaredSha256, digest.digest().toHex())
}

private fun extractDatabase(
    zip: ZipInputStream,
    destination: Path,
    declaredSize: Long,
): ExtractedDatabaseIdentity? {
    if (declaredSize !in SQLITE_HEADER_SIZE.toLong()..MAX_BACKUP_MAIN_DATABASE_BYTES) return null
    val digest = MessageDigest.getInstance(SHA_256)
    val header = ByteArray(SQLITE_HEADER_SIZE)
    var headerBytes = 0
    var total = 0L
    FileChannel.open(
        destination,
        StandardOpenOption.CREATE_NEW,
        StandardOpenOption.WRITE,
        LinkOption.NOFOLLOW_LINKS,
    ).use { output ->
        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
        while (true) {
            val count = zip.read(buffer)
            if (count < 0) break
            if (count == 0) continue
            total = Math.addExact(total, count.toLong())
            if (total > declaredSize || total > MAX_BACKUP_MAIN_DATABASE_BYTES) return null
            if (headerBytes < header.size) {
                val copied = minOf(count, header.size - headerBytes)
                buffer.copyInto(
                    destination = header,
                    destinationOffset = headerBytes,
                    startIndex = 0,
                    endIndex = copied,
                )
                headerBytes += copied
            }
            digest.update(buffer, 0, count)
            val bytes = ByteBuffer.wrap(buffer, 0, count)
            while (bytes.hasRemaining()) output.write(bytes)
        }
        output.force(true)
    }
    if (headerBytes != SQLITE_HEADER_SIZE || total != declaredSize || !validSqliteHeader(header, total)) {
        return null
    }
    return ExtractedDatabaseIdentity(total, digest.digest().toHex())
}

private fun validSqliteHeader(header: ByteArray, size: Long): Boolean {
    if (!header.copyOfRange(0, SQLITE_MAGIC.size).contentEquals(SQLITE_MAGIC)) return false
    val rawPageSize = ((header[16].toInt() and 0xff) shl 8) or (header[17].toInt() and 0xff)
    val pageSize = if (rawPageSize == 1) 65_536 else rawPageSize
    val validPageSize = pageSize == 65_536 ||
        (pageSize in 512..32_768 && pageSize and (pageSize - 1) == 0)
    val writeVersion = header[18].toInt() and 0xff
    val readVersion = header[19].toInt() and 0xff
    return validPageSize && writeVersion in 1..2 && readVersion in 1..2 && size % pageSize == 0L
}

private fun isSafeZipEntry(entry: ZipEntry): Boolean {
    val name = entry.name
    return !entry.isDirectory && !name.endsWith('/') && isSafeBackupEntryName(name)
}

private fun readBounded(input: ZipInputStream, limit: Int): ByteArray? {
    val output = ByteArrayOutputStream(minOf(limit, DEFAULT_BUFFER_SIZE))
    val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
    var total = 0
    while (true) {
        val count = input.read(buffer)
        if (count < 0) break
        if (count == 0) continue
        total = Math.addExact(total, count)
        if (total > limit) return null
        output.write(buffer, 0, count)
    }
    return output.toByteArray()
}

private fun fileIdentityMatches(path: Path, expectedSize: Long, expectedSha256: String): Boolean =
    try {
        exactRegularFile(path, path.parent) && Files.size(path) == expectedSize &&
            constantTimeDigestEquals(expectedSha256, sha256(path))
    } catch (_: Exception) {
        false
    }

private fun exactRegularFile(path: Path, expectedParent: Path): Boolean =
    path.parent == expectedParent && !Files.isSymbolicLink(path) &&
        Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)

private fun exactRequestDirectory(path: Path, expectedParent: Path): Boolean = runCatching {
    path.parent == expectedParent && !Files.isSymbolicLink(path) &&
        Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS) &&
        path.toFile().canonicalFile.toPath().normalize() == path
}.getOrDefault(false)

private fun preparedSidecars(prepared: Path): List<Path> = listOf(
    prepared.resolveSibling("${prepared.fileName}-wal"),
    prepared.resolveSibling("${prepared.fileName}-shm"),
    prepared.resolveSibling("${prepared.fileName}-journal"),
)

private fun deleteExactStalePreparedArtifacts(prepared: Path, expectedParent: Path): Boolean {
    for (candidate in listOf(prepared) + preparedSidecars(prepared)) {
        if (!Files.exists(candidate, LinkOption.NOFOLLOW_LINKS)) continue
        if (!exactRegularFile(candidate, expectedParent)) return false
        if (!Files.deleteIfExists(candidate)) return false
    }
    return true
}

private fun stagingRootIsUnchanged(paths: ColdRestoreStagingPaths): Boolean = runCatching {
    !Files.isSymbolicLink(paths.rootDirectory) &&
        Files.isDirectory(paths.rootDirectory, LinkOption.NOFOLLOW_LINKS) &&
        paths.rootDirectory.toFile().canonicalFile.toPath().normalize() == paths.rootDirectory
}.getOrDefault(false)

private fun databaseDirectoryIsUnchanged(paths: ColdRestoreBootstrapPaths): Boolean = runCatching {
    !Files.isSymbolicLink(paths.databaseDirectory) &&
        Files.isDirectory(paths.databaseDirectory, LinkOption.NOFOLLOW_LINKS) &&
        paths.databaseDirectory.toFile().canonicalFile.toPath().normalize() ==
        paths.databaseDirectory &&
        listOf(paths.mainDatabase, paths.mainWal, paths.mainShm).all { candidate ->
            candidate.parent == paths.databaseDirectory &&
                !Files.isSymbolicLink(candidate) &&
                (!Files.exists(candidate, LinkOption.NOFOLLOW_LINKS) ||
                    Files.isRegularFile(candidate, LinkOption.NOFOLLOW_LINKS))
        }
}.getOrDefault(false)

private fun lockFileIsSafe(path: Path): Boolean =
    !Files.isSymbolicLink(path) && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)

private fun forceFile(path: Path) {
    FileChannel.open(
        path,
        StandardOpenOption.WRITE,
        LinkOption.NOFOLLOW_LINKS,
    ).use { it.force(true) }
}

private fun sha256(path: Path): String {
    val digest = MessageDigest.getInstance(SHA_256)
    FileChannel.open(
        path,
        StandardOpenOption.READ,
        LinkOption.NOFOLLOW_LINKS,
    ).use { channel ->
        val buffer = ByteBuffer.allocate(DEFAULT_BUFFER_SIZE)
        while (true) {
            val count = channel.read(buffer)
            if (count < 0) break
            if (count == 0) continue
            digest.update(buffer.array(), 0, count)
            buffer.clear()
        }
    }
    return digest.digest().toHex()
}

private fun constantTimeDigestEquals(expected: String, actual: String): Boolean {
    if (!isCanonicalBackupSha256(expected) || !isCanonicalBackupSha256(actual)) return false
    return MessageDigest.isEqual(expected.hexToBytes(), actual.hexToBytes())
}

private fun String.hexToBytes(): ByteArray = ByteArray(length / 2) { index ->
    substring(index * 2, index * 2 + 2).toInt(16).toByte()
}

private fun ByteArray.toHex(): String = joinToString(separator = "") { byte ->
    (byte.toInt() and 0xff).toString(16).padStart(2, '0')
}

private fun invalidBootstrapPath(failure: ColdRestoreBootstrapPathFailure) =
    ColdRestoreBootstrapPathValidation.Invalid(failure)

private fun rejectedExtraction(failure: ColdRestoreBootstrapFailure) =
    ExtractionResult.Rejected(failure)

private fun failed(failure: ColdRestoreBootstrapFailure) =
    ColdRestoreBootstrapResult.Failed(failure)

private const val MAX_BOOTSTRAP_MANIFEST_BYTES = 1_048_576
private const val MAX_BOOTSTRAP_ZIP_ENTRIES = 4_097
private const val SQLITE_HEADER_SIZE = 100
private const val SHA_256 = "SHA-256"
private val SQLITE_MAGIC = "SQLite format 3\u0000".toByteArray(Charsets.US_ASCII)
