package me.rerere.rikkahub.data.sync.backup

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.StandardOpenOption
import java.util.UUID
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.datastore.Settings
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.datastore.migration.SettingsJsonMigrator
import me.rerere.rikkahub.data.db.readRetainedLearningOutboxOrThrow
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.files.FileFolders
import me.rerere.rikkahub.data.files.SkillPaths
import me.rerere.rikkahub.data.sync.backup.restore.ColdRestoreArchiveStager
import me.rerere.rikkahub.data.sync.backup.restore.ColdRestoreStageResult
import me.rerere.rikkahub.data.sync.backup.restore.ColdRestoreStagingPaths
import me.rerere.rikkahub.data.sync.backup.restore.VerifiedColdRestoreArchive
import me.rerere.rikkahub.data.sync.backup.restore.VerifiedColdRestoreArchiveResult

private const val MAIN_DATABASE_NAME = "rikka_hub"
private const val MAX_SETTINGS_RESTORE_BYTES = 4 * 1_024 * 1_024

sealed interface BackupRestoreDisposition {
    /** Only settings/files were selected; no database or Learning files were touched. */
    data object CompletedInCurrentProcess : BackupRestoreDisposition

    /** The database archive is durable in app-private staging and requires a full process restart. */
    data object ColdRestartRequired : BackupRestoreDisposition
}

enum class BackupArchiveServiceFailure {
    DATABASE_COMPONENT_MISSING,
    FILES_COMPONENT_MISSING,
    DATABASE_SNAPSHOT_FAILED,
    DATABASE_STREAM_INVALID,
    FILE_TARGET_UNSAFE,
    COLD_STAGING_PATH_UNSAFE,
    COLD_STAGING_REJECTED,
    COLD_STAGING_BUSY,
    COLD_RESTORE_ALREADY_PENDING,
    LEGACY_DATABASE_STREAM_UNKNOWN,
    LEGACY_DATABASE_CONVERSION_FAILED,
}

class BackupArchiveServiceException(
    val failure: BackupArchiveServiceFailure,
    cause: Throwable? = null,
) : Exception(failure.name, cause)

/**
 * Transport-independent backup/restore integration.
 *
 * Database export uses SQLite `VACUUM INTO` to create a consistent single-file snapshot. Database
 * restore never closes or overwrites the live Room database: after the entire archive is verified,
 * the entire archive is staged. Selected settings/files are replayed before the next dependency graph opens.
 */
class BackupArchiveService(
    private val settingsStore: SettingsStore,
    private val json: Json,
    private val context: Context,
    private val appDatabase: AppDatabase,
) {
    suspend fun createBackup(
        destination: File,
        includeDatabase: Boolean,
        includeFiles: Boolean,
    ): File {
        val components = linkedSetOf(BackupArchiveComponent.SETTINGS)
        val sources = mutableListOf<BackupArchiveSourceV1>(
            BackupArchiveSourceV1.Bytes(
                name = BACKUP_ARCHIVE_SETTINGS_ENTRY,
                bytes = json.encodeToString(settingsStore.settingsFlow.value)
                    .toByteArray(Charsets.UTF_8),
            ),
        )
        var snapshot: DatabaseSnapshot? = null
        try {
            if (includeDatabase) {
                snapshot = createConsistentDatabaseSnapshot()
                components += BackupArchiveComponent.DATABASE
                sources += BackupArchiveSourceV1.FileSource(
                    name = BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY,
                    file = snapshot.file,
                )
            }
            if (includeFiles) {
                components += BackupArchiveComponent.FILES
                sources += collectManagedFileSources()
            }
            BackupArchiveV1FileIO.write(
                destination = destination,
                components = components,
                mainStream = snapshot?.stream,
                sources = sources,
            )
            return destination
        } finally {
            snapshot?.deleteExactArtifacts()
        }
    }

    suspend fun restore(
        archiveFile: File,
        includeDatabase: Boolean,
        includeFiles: Boolean,
    ): BackupRestoreDisposition {
        val archive = BackupArchiveV1FileIO.inspectForRestore(archiveFile)
        if (includeDatabase && BackupArchiveComponent.DATABASE !in archive.manifest.components) {
            throw serviceFailure(BackupArchiveServiceFailure.DATABASE_COMPONENT_MISSING)
        }
        val canonical = when {
            !includeDatabase -> null
            archive.origin == BackupArchiveOrigin.LEGACY_V0 ->
                canonicalizeLegacyDatabaseArchive(archive)
            else -> CanonicalRestoreArchive(archive, emptyList())
        }
        try {
            // Verify before durable staging. A database restore never mutates live components;
            // cold startup replays them before Room/Koin opens, including after interruption.
            val coldArchive = canonical?.archive?.let { verifiedArchive ->
                when (val verified = VerifiedColdRestoreArchive.verify(
                    archiveFile = verifiedArchive.archiveFile,
                    archiveSize = verifiedArchive.archiveSize,
                    archiveSha256 = verifiedArchive.archiveSha256,
                    manifest = verifiedArchive.manifest,
                )) {
                    is VerifiedColdRestoreArchiveResult.Rejected -> {
                        throw serviceFailure(BackupArchiveServiceFailure.COLD_STAGING_REJECTED)
                    }
                    is VerifiedColdRestoreArchiveResult.Verified -> verified.archive
                }
            }

            if (coldArchive == null) {
                return ColdRestoreArchiveStager.withRestoreAdmission(context) {
                    val restorer = BackupArchiveComponentRestorer(settingsStore, json, context)
                    restorer.restoreSettingsIfPresent(archive)
                    if (includeFiles) restorer.restoreFiles(archive)
                    BackupRestoreDisposition.CompletedInCurrentProcess
                }
            }

            val stagingPaths = ColdRestoreStagingPaths.verify(
                applicationDataDirectory = File(context.applicationInfo.dataDir),
                noBackupFilesDirectory = context.noBackupFilesDir,
            )
            val staged = ColdRestoreArchiveStager(stagingPaths).stage(
                coldArchive, deferComponents = true, restoreFiles = includeFiles,
            )
            return when (staged) {
                is ColdRestoreStageResult.Staged -> BackupRestoreDisposition.ColdRestartRequired
                ColdRestoreStageResult.PendingRestoreExists ->
                    throw serviceFailure(BackupArchiveServiceFailure.COLD_RESTORE_ALREADY_PENDING)
                ColdRestoreStageResult.Busy ->
                    throw serviceFailure(BackupArchiveServiceFailure.COLD_STAGING_BUSY)
                is ColdRestoreStageResult.Rejected ->
                    throw serviceFailure(BackupArchiveServiceFailure.COLD_STAGING_REJECTED)
            }
        } finally {
            canonical?.temporaryArtifacts?.forEach(::deleteExactTemporaryArtifact)
        }
    }

    private fun createConsistentDatabaseSnapshot(): DatabaseSnapshot {
        val liveDatabase = context.getDatabasePath(MAIN_DATABASE_NAME)
        if (!liveDatabase.isFile || Files.isSymbolicLink(liveDatabase.toPath())) {
            throw serviceFailure(BackupArchiveServiceFailure.DATABASE_SNAPSHOT_FAILED)
        }
        val snapshot = File(
            context.cacheDir,
            ".rikka_hub_backup_${UUID.randomUUID().toString().replace("-", "")}.db",
        )
        if (snapshot.exists()) {
            throw serviceFailure(BackupArchiveServiceFailure.DATABASE_SNAPSHOT_FAILED)
        }
        try {
            appDatabase.openHelper.writableDatabase.execSQL(
                "VACUUM INTO ?",
                arrayOf(snapshot.absolutePath),
            )
            if (!snapshot.isFile || Files.isSymbolicLink(snapshot.toPath())) {
                throw serviceFailure(BackupArchiveServiceFailure.DATABASE_SNAPSHOT_FAILED)
            }
            FileChannel.open(snapshot.toPath(), StandardOpenOption.WRITE).use { it.force(true) }
            val stream = readAndValidateAuthorityStream(snapshot)
            return DatabaseSnapshot(snapshot, stream)
        } catch (error: BackupArchiveServiceException) {
            DatabaseSnapshot(snapshot, null).deleteExactArtifacts()
            throw error
        } catch (error: Exception) {
            DatabaseSnapshot(snapshot, null).deleteExactArtifacts()
            throw serviceFailure(BackupArchiveServiceFailure.DATABASE_SNAPSHOT_FAILED, error)
        }
    }

    /**
     * Converts only a provable current-v46 historical archive to canonical manifest v1. For the
     * exact pre-Learning v46 identity the stream was genuinely absent, so a fresh descriptor is
     * frozen into the canonical manifest and the staged reconciler must create that exact stream.
     * Older/foreign identities remain explicitly unsupported instead of being guessed.
     */
    private fun canonicalizeLegacyDatabaseArchive(
        legacy: VerifiedBackupArchiveV1,
    ): CanonicalRestoreArchive {
        val token = UUID.randomUUID().toString().replace("-", "")
        val extracted = File(context.cacheDir, ".legacy_restore_$token.db")
        val canonicalZip = File(context.cacheDir, ".legacy_restore_$token.zip")
        try {
            BackupArchiveV1FileIO.extractEntry(
                archive = legacy,
                name = BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY,
                destination = extracted,
            )
            val stream = readLegacyAuthorityDescriptor(extracted)
            BackupArchiveV1FileIO.write(
                destination = canonicalZip,
                components = setOf(BackupArchiveComponent.DATABASE),
                mainStream = stream,
                sources = listOf(
                    BackupArchiveSourceV1.FileSource(
                        name = BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY,
                        file = extracted,
                    ),
                ),
            )
            return CanonicalRestoreArchive(
                archive = BackupArchiveV1FileIO.inspect(canonicalZip),
                temporaryArtifacts = listOf(extracted, canonicalZip),
            )
        } catch (error: BackupArchiveServiceException) {
            deleteExactTemporaryArtifact(extracted)
            deleteExactTemporaryArtifact(canonicalZip)
            throw error
        } catch (error: Exception) {
            deleteExactTemporaryArtifact(extracted)
            deleteExactTemporaryArtifact(canonicalZip)
            throw serviceFailure(
                BackupArchiveServiceFailure.LEGACY_DATABASE_CONVERSION_FAILED,
                error,
            )
        }
    }

    private fun readLegacyAuthorityDescriptor(databaseFile: File): BackupAuthorityStreamV1 {
        try {
            SQLiteDatabase.openDatabase(
                databaseFile.absolutePath,
                null,
                SQLiteDatabase.OPEN_READONLY,
            ).use { database ->
                database.rawQuery("PRAGMA quick_check(1)", null).use { cursor ->
                    check(cursor.moveToFirst() && cursor.getString(0) == "ok" &&
                        !cursor.moveToNext())
                }
                val identity = database.rawQuery(
                    "SELECT identity_hash FROM room_master_table WHERE id = 42 LIMIT 1",
                    null,
                ).use { cursor -> if (cursor.moveToFirst()) cursor.getString(0) else null }
                return when (
                    me.rerere.rikkahub.data.db.ImportedDatabaseReconciler
                        .legacyV46AuthorityPlanOrThrow(database.version, identity)
                ) {
                    me.rerere.rikkahub.data.db.ImportedDatabaseReconciler
                        .LegacyV46AuthorityPlan.READ_EXISTING_STREAM ->
                        readAndValidateAuthorityStream(databaseFile)
                    me.rerere.rikkahub.data.db.ImportedDatabaseReconciler
                        .LegacyV46AuthorityPlan.CREATE_STREAM ->
                        BackupAuthorityStreamV1(
                            streamId = UUID.randomUUID().toString(),
                            headSeq = 1L,
                        )
                    else -> throw serviceFailure(
                        BackupArchiveServiceFailure.LEGACY_DATABASE_STREAM_UNKNOWN,
                    )
                }
            }
        } catch (error: BackupArchiveServiceException) {
            throw error
        } catch (error: Exception) {
            throw serviceFailure(BackupArchiveServiceFailure.LEGACY_DATABASE_STREAM_UNKNOWN, error)
        }
    }

    private fun readAndValidateAuthorityStream(snapshot: File): BackupAuthorityStreamV1 {
        try {
            SQLiteDatabase.openDatabase(
                snapshot.absolutePath,
                null,
                SQLiteDatabase.OPEN_READONLY,
            ).use { database ->
                database.rawQuery("PRAGMA quick_check(1)", null).use { cursor ->
                    check(cursor.moveToFirst() && cursor.getString(0) == "ok" &&
                        !cursor.moveToNext())
                }
                val retained = readRetainedLearningOutboxOrThrow(database)
                return BackupAuthorityStreamV1(streamId = retained.streamId, headSeq = retained.headSeq)
            }
        } catch (error: Exception) {
            throw serviceFailure(BackupArchiveServiceFailure.DATABASE_STREAM_INVALID, error)
        }
    }

    private fun collectManagedFileSources(): List<BackupArchiveSourceV1> {
        val result = mutableListOf<BackupArchiveSourceV1>()
        collectDirectFiles(
            directory = File(context.filesDir, FileFolders.UPLOAD),
            prefix = "${FileFolders.UPLOAD}/",
            destination = result,
        )
        collectDirectFiles(
            directory = File(context.filesDir, FileFolders.FONTS),
            prefix = "${FileFolders.FONTS}/",
            destination = result,
        )
        val skills = File(context.filesDir, FileFolders.SKILLS)
        if (skills.exists()) {
            val root = requireSafeOwnedDirectory(skills.toPath().toAbsolutePath().normalize())
            collectTreeFiles(
                root = root,
                current = root,
                prefix = "${FileFolders.SKILLS}/",
                destination = result,
            )
        }
        return result
    }

    private fun collectDirectFiles(
        directory: File,
        prefix: String,
        destination: MutableList<BackupArchiveSourceV1>,
    ) {
        if (!directory.exists()) return
        val root = requireSafeOwnedDirectory(directory.toPath().toAbsolutePath().normalize())
        Files.newDirectoryStream(root).use { children ->
            children.sortedBy { it.fileName.toString() }.forEach { child ->
                if (Files.isSymbolicLink(child)) {
                    throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
                }
                if (Files.isRegularFile(child, LinkOption.NOFOLLOW_LINKS)) {
                    destination += BackupArchiveSourceV1.FileSource(
                        name = "$prefix${child.fileName}",
                        file = child.toFile(),
                    )
                }
            }
        }
    }

    private fun collectTreeFiles(
        root: java.nio.file.Path,
        current: java.nio.file.Path,
        prefix: String,
        destination: MutableList<BackupArchiveSourceV1>,
    ) {
        Files.newDirectoryStream(current).use { children ->
            children.sortedBy { it.fileName.toString() }.forEach { child ->
                if (Files.isSymbolicLink(child) || !child.normalize().startsWith(root)) {
                    throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
                }
                when {
                    Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS) ->
                        collectTreeFiles(root, child, prefix, destination)
                    Files.isRegularFile(child, LinkOption.NOFOLLOW_LINKS) -> {
                        val relative = root.relativize(child).toString().replace(File.separatorChar, '/')
                        destination += BackupArchiveSourceV1.FileSource(
                            name = "$prefix$relative",
                            file = child.toFile(),
                        )
                    }
                    else -> throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
                }
            }
        }
    }
}

internal class BackupArchiveComponentRestorer(
    private val settingsStore: SettingsStore,
    private val json: Json,
    private val context: Context,
) {
    suspend fun restoreSettingsIfPresent(archive: VerifiedBackupArchiveV1) {
        if (BackupArchiveComponent.SETTINGS !in archive.manifest.components) return
        val settingsBytes = BackupArchiveV1FileIO.readSmallEntry(
            archive = archive,
            name = BACKUP_ARCHIVE_SETTINGS_ENTRY,
            maxBytes = MAX_SETTINGS_RESTORE_BYTES,
        )
        val migrated = SettingsJsonMigrator.migrate(settingsBytes.toString(Charsets.UTF_8))
        settingsStore.update(json.decodeFromString<Settings>(migrated))
    }

    fun restoreFiles(archive: VerifiedBackupArchiveV1) {
        val filesRoot = context.filesDir.toPath().toAbsolutePath().normalize()
        requireSafeOwnedDirectory(filesRoot)
        BackupArchiveV1FileIO.restoreFileEntries(archive) { entryName ->
            resolveRestoreTarget(entryName)?.also { target ->
                ensureSafeTargetParent(filesRoot, target)
            }
        }
    }

    private fun resolveRestoreTarget(entryName: String): File? = when {
        entryName.startsWith("${FileFolders.UPLOAD}/") -> {
            val relative = entryName.substringAfter("${FileFolders.UPLOAD}/")
            if (relative.isBlank() || '/' in relative) null else {
                val root = ensureDirectOwnedDirectory(File(context.filesDir, FileFolders.UPLOAD))
                SkillPaths.resolveSkillFile(root, relative)
            }
        }
        entryName.startsWith("${FileFolders.FONTS}/") -> {
            val relative = entryName.substringAfter("${FileFolders.FONTS}/")
            if (relative.isBlank() || '/' in relative) null else {
                File(ensureDirectOwnedDirectory(File(context.filesDir, FileFolders.FONTS)), relative)
            }
        }
        entryName.startsWith("${FileFolders.SKILLS}/") -> {
            val relative = entryName.substringAfter("${FileFolders.SKILLS}/")
            val skillName = relative.substringBefore('/', missingDelimiterValue = "")
            val skillRelativePath = relative.substringAfter('/', missingDelimiterValue = "")
            if (skillName.isBlank() || skillRelativePath.isBlank()) null else {
                val root = ensureDirectOwnedDirectory(File(context.filesDir, FileFolders.SKILLS))
                val skillDir = SkillPaths.resolveSkillDir(root, skillName) ?: return null
                ensureDirectoryTreeInside(root, skillDir)
                SkillPaths.resolveSkillFile(skillDir, skillRelativePath)?.also { target ->
                    ensureDirectoryTreeInside(root, requireNotNull(target.parentFile))
                }
            }
        }
        else -> null
    }

}

private class DatabaseSnapshot(
    val file: File,
    val stream: BackupAuthorityStreamV1?,
) {
    fun deleteExactArtifacts() {
        val parent = file.parentFile ?: return
        val exact = listOf(
            file,
            File(parent, "${file.name}-wal"),
            File(parent, "${file.name}-shm"),
            File(parent, "${file.name}-journal"),
        )
        exact.forEach { candidate ->
            if (candidate.parentFile == parent && !Files.isSymbolicLink(candidate.toPath()) &&
                (!candidate.exists() || candidate.isFile)
            ) {
                runCatching { Files.deleteIfExists(candidate.toPath()) }
            }
        }
    }
}

private data class CanonicalRestoreArchive(
    val archive: VerifiedBackupArchiveV1,
    val temporaryArtifacts: List<File>,
)

private fun deleteExactTemporaryArtifact(file: File) {
    val path = file.toPath().toAbsolutePath().normalize()
    if (file.isAbsolute && path.parent != null && !Files.isSymbolicLink(path) &&
        (!Files.exists(path, LinkOption.NOFOLLOW_LINKS) ||
            Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
    ) {
        runCatching { Files.deleteIfExists(path) }
    }
}

private fun requireSafeOwnedDirectory(path: java.nio.file.Path): java.nio.file.Path {
    val canonical = runCatching { path.toFile().canonicalFile.toPath().normalize() }.getOrNull()
    if (canonical != path || Files.isSymbolicLink(path) ||
        !Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)
    ) {
        throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
    }
    return path
}

private fun ensureDirectOwnedDirectory(directory: File): File {
    val parent = directory.parentFile
        ?: throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
    requireSafeOwnedDirectory(parent.toPath().toAbsolutePath().normalize())
    if (!directory.exists() && !directory.mkdir()) {
        throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
    }
    requireSafeOwnedDirectory(directory.toPath().toAbsolutePath().normalize())
    return directory
}

private fun ensureDirectoryTreeInside(rootDirectory: File, targetDirectory: File) {
    val root = requireSafeOwnedDirectory(rootDirectory.toPath().toAbsolutePath().normalize())
    val target = targetDirectory.toPath().toAbsolutePath().normalize()
    if (!target.startsWith(root)) {
        throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
    }
    var current = root
    for (segment in root.relativize(target)) {
        current = current.resolve(segment).normalize()
        if (!Files.exists(current, LinkOption.NOFOLLOW_LINKS)) {
            Files.createDirectory(current)
        }
        requireSafeOwnedDirectory(current)
    }
}

private fun ensureSafeTargetParent(filesRoot: java.nio.file.Path, target: File) {
    val normalized = target.toPath().toAbsolutePath().normalize()
    if (!target.isAbsolute || !normalized.startsWith(filesRoot) ||
        normalized.parent == null || Files.isSymbolicLink(normalized)
    ) {
        throw serviceFailure(BackupArchiveServiceFailure.FILE_TARGET_UNSAFE)
    }
    requireSafeOwnedDirectory(normalized.parent)
}

private fun serviceFailure(
    failure: BackupArchiveServiceFailure,
    cause: Throwable? = null,
) = BackupArchiveServiceException(failure, cause)
