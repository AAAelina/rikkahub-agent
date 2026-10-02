package me.rerere.rikkahub.data.sync.backup.restore

import android.content.Context
import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption
import me.rerere.rikkahub.data.db.ImportedDatabaseReconciler

sealed interface ColdRestoreStartupResult {
    data object NoPendingRestore : ColdRestoreStartupResult

    data object RebuildRequired : ColdRestoreStartupResult

    data object Complete : ColdRestoreStartupResult

    /** Preparation failed before any live DB file moved; normal startup remains safe. */
    data class LiveDatabaseUnchanged(val reasonCode: String) : ColdRestoreStartupResult

    /** Another process owns the restore lock. Room/Koin must not race it. */
    data object Busy : ColdRestoreStartupResult

    /** An irreversible intent exists or path/journal state is ambiguous. Do not open Room/Koin. */
    data class DegradedRestartRequired(val reasonCode: String) : ColdRestoreStartupResult
}

/**
 * Android cold-start seam. [run] is called synchronously before ImportedDatabaseReconciler, Room,
 * WorkManager's Koin factory, SettingsStore, or any other dependency graph is created.
 */
object ColdRestoreStartupCoordinator {
    fun run(
        context: Context,
        settingsStore: () -> me.rerere.rikkahub.data.datastore.SettingsStore = { ColdRestoreComponents.settingsStore(context) },
    ): ColdRestoreStartupResult {
        val appData = File(context.applicationInfo.dataDir)
        val staging = ColdRestoreStagingPaths.verify(appData, context.noBackupFilesDir)
        val validStaging = staging as? ColdRestoreStagingPathValidation.Valid
            ?: return ColdRestoreStartupResult.LiveDatabaseUnchanged("STAGING_PATH_UNSAFE")
        val pending = validStaging.paths.pendingJournal
        if (!Files.exists(pending, LinkOption.NOFOLLOW_LINKS)) {
            return ColdRestoreStartupResult.NoPendingRestore
        }

        val bootstrapPaths = ColdRestoreBootstrapPaths.verify(
            applicationDataDirectory = appData,
            mainDatabaseFile = context.getDatabasePath(MAIN_DATABASE_NAME),
        )
        val learningPaths = LearningOwnedDatabasePaths.verify(
            applicationDataDirectory = appData,
            learningDatabaseFile = context.getDatabasePath(LEARNING_DATABASE_FILE_NAME),
        )
        val preparedReconciler = ColdRestorePreparedDatabaseReconciler { file, stream ->
            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                databaseFile = file,
                expectedStreamId = stream.streamId,
                expectedHeadSeq = stream.headSeq,
            )
        }
        val preparedValidator = ColdRestorePreparedDatabaseValidator { file, stream ->
            if (file.name == MAIN_DATABASE_NAME) {
                ImportedDatabaseReconciler.validateInstalledFileOrThrow(
                    databaseFile = file,
                    expectedStreamId = stream.streamId,
                    expectedHeadSeq = stream.headSeq,
                )
            } else {
                ImportedDatabaseReconciler.validateStagedFileOrThrow(
                    databaseFile = file,
                    expectedStreamId = stream.streamId,
                    expectedHeadSeq = stream.headSeq,
                )
            }
        }

        val journal = when (val read = ColdRestoreJournalStore(pending).read()) {
            ColdRestoreJournalReadResult.Missing ->
                return ColdRestoreStartupResult.NoPendingRestore
            is ColdRestoreJournalReadResult.Invalid ->
                return ColdRestoreStartupResult.DegradedRestartRequired("PENDING_JOURNAL_INVALID")
            is ColdRestoreJournalReadResult.Valid -> read.journal
        }

        if (journal.phase == ColdRestorePhase.STAGED) {
            when (val prepared = ColdRestoreBootstrap(
                stagingPaths = staging,
                bootstrapPaths = bootstrapPaths,
                reconciler = preparedReconciler,
                validator = preparedValidator,
            ).prepare()) {
                ColdRestoreBootstrapResult.NoPendingRestore ->
                    return ColdRestoreStartupResult.NoPendingRestore
                ColdRestoreBootstrapResult.Busy -> return ColdRestoreStartupResult.Busy
                is ColdRestoreBootstrapResult.Failed -> {
                    return ColdRestoreStartupResult.LiveDatabaseUnchanged(
                        "PREPARE_${prepared.failure.name}",
                    )
                }
                is ColdRestoreBootstrapResult.ReadyToSwap -> Unit
            }
        }

        val result = when (val swapped = ColdRestoreSwapExecutor(
            stagingPaths = staging,
            bootstrapPaths = bootstrapPaths,
            learningPaths = learningPaths,
            validator = preparedValidator,
        ).execute()) {
            ColdRestoreSwapResult.NoPendingRestore -> ColdRestoreStartupResult.NoPendingRestore
            ColdRestoreSwapResult.Busy -> ColdRestoreStartupResult.Busy
            ColdRestoreSwapResult.RebuildRequired -> ColdRestoreStartupResult.RebuildRequired
            ColdRestoreSwapResult.Complete -> ColdRestoreStartupResult.Complete
            is ColdRestoreSwapResult.LiveDatabaseUnchanged ->
                ColdRestoreStartupResult.LiveDatabaseUnchanged("SWAP_${swapped.failure.name}")
            is ColdRestoreSwapResult.DegradedRestartRequired ->
                ColdRestoreStartupResult.DegradedRestartRequired("SWAP_${swapped.failure.name}")
        }
        if ((result == ColdRestoreStartupResult.RebuildRequired || result == ColdRestoreStartupResult.Complete) &&
            !ColdRestoreComponents.applyBeforeGraph(context, settingsStore)) {
            return ColdRestoreStartupResult.DegradedRestartRequired("COMPONENT_REPLAY_FAILED")
        }
        return result
    }

    /** Deletes the restore journal and quarantine once the installed database is validated. */
    fun finalizeDisabledDerivedState(context: Context): Boolean {
        return ColdRestoreRebuildFinalizer.completeWhenDerivedStateDisabled(context)
    }

    /** Pure decision seam so the flags-off safety order is covered by host JVM tests. */
    internal fun finalizeDisabledDerivedState(
        journalRead: ColdRestoreJournalReadResult,
        validateInstalled: (streamId: String, headSeq: Long) -> Unit,
        complete: (streamId: String, headSeq: Long) -> Boolean,
    ): Boolean {
        val journal = when (journalRead) {
            ColdRestoreJournalReadResult.Missing -> return true
            is ColdRestoreJournalReadResult.Invalid -> return false
            is ColdRestoreJournalReadResult.Valid -> journalRead.journal
        }
        if (journal.phase != ColdRestorePhase.REBUILD_REQUIRED &&
            journal.phase != ColdRestorePhase.COMPLETE
        ) {
            return false
        }
        return try {
            val stream = journal.mainStream
            if (journal.phase != ColdRestorePhase.COMPLETE) validateInstalled(stream.streamId, stream.headSeq)
            complete(stream.streamId, stream.headSeq)
        } catch (_: Exception) {
            false
        }
    }

    private const val MAIN_DATABASE_NAME = "rikka_hub"
}
