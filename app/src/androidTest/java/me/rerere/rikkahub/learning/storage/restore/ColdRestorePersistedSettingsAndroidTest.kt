package me.rerere.rikkahub.learning.storage.restore

import android.content.Context
import android.content.ContextWrapper
import android.content.pm.ApplicationInfo
import androidx.datastore.core.DataStore
import androidx.datastore.preferences.core.Preferences
import androidx.datastore.preferences.core.PreferenceDataStoreFactory
import androidx.datastore.preferences.core.edit
import androidx.datastore.preferences.core.stringPreferencesKey
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import java.io.File
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.util.UUID
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.data.datastore.Settings
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.createAppSQLiteOpenHelperFactory
import me.rerere.rikkahub.data.files.FileFolders
import me.rerere.rikkahub.data.sync.backup.*
import me.rerere.rikkahub.learning.handoff.*
import me.rerere.rikkahub.learning.model.LearningPreferencesV1
import me.rerere.rikkahub.learning.runtime.LearningRuntimeFacade
import me.rerere.rikkahub.learning.storage.*
import me.rerere.rikkahub.utils.JsonInstant
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/** Synthetic files, real Preferences DataStore and both Room databases on a disposable GMD. */
@RunWith(AndroidJUnit4::class)
class ColdRestorePersistedSettingsAndroidTest {
    private lateinit var root: File
    private lateinit var context: Context
    private lateinit var database: AppDatabase
    private lateinit var preferences: DataStore<Preferences>
    private lateinit var settings: SettingsStore
    private lateinit var scope: CoroutineScope
    private var runtime: LearningRuntimeFacade? = null

    @Before fun setUp() {
        val base = ApplicationProvider.getApplicationContext<Context>()
        root = File(base.cacheDir.canonicalFile, "r3-${UUID.randomUUID()}").apply { mkdirs() }
        context = object : ContextWrapper(base) {
            override fun getApplicationContext(): Context = this
            override fun getApplicationInfo() = ApplicationInfo(base.applicationInfo).apply { dataDir = root.path }
            override fun getFilesDir() = File(root, "files").apply { mkdirs() }
            override fun getCacheDir() = File(root, "cache").apply { mkdirs() }
            override fun getNoBackupFilesDir() = File(root, "no_backup").apply { mkdirs() }
            override fun getDatabasePath(name: String) = File(File(root, "databases").apply { mkdirs() }, name)
        }
        scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
        preferences = PreferenceDataStoreFactory.create(scope = scope) { File(root, "settings.preferences_pb") }
        settings = store(preferences)
        database = openDatabase()
        database.openHelper.writableDatabase.execSQL(
            "INSERT INTO learning_outbox(stream_id,event_id,event_type,event_schema_version,created_at_ms) VALUES(?,?,'STREAM_INIT',1,0)",
            arrayOf(STREAM, LEARNING_STREAM_INIT_EVENT_ID),
        )
        File(context.getDatabasePath(LearningDatabase.FILE_NAME).path).writeText("old-derived-timeline")
    }

    @After fun tearDown() = runBlocking {
        runtime?.close()
        database.close()
        scope.coroutineContext[Job]?.cancelAndJoin()
        root.deleteRecursively()
        Unit
    }

    @Test fun databaseOnlyEnabledSettingsRetainQuarantineUntilActualRoomBootstrapCoversNewHead() = runBlocking {
        persist(LearningPreferencesV1(handoff = true, capture = true, jobs = true))
        stageAndStart(databaseOnlyArchive())
        assertFalse(facade().finalizeColdRestore(settings))
        assertTrue(Files.exists(paths().pendingJournal))
        database = openDatabase()
        database.openHelper.writableDatabase.execSQL(
            "INSERT INTO learning_outbox(stream_id,event_id,event_type,event_schema_version,created_at_ms,source_type,source_id,source_revision,scope_kind,scope_id,occurred_at_ms) VALUES(?,'r3-append','FUTURE_EVENT',1,1,'EXECUTION_EVENT','r3-execution',1,'AUTHORITY_SUBJECT','r3-subject',1)",
            arrayOf(STREAM),
        )
        val learning = Room.databaseBuilder(context, LearningDatabase::class.java, LearningDatabase.FILE_NAME).build()
        try {
            learning.checkpointDao().insert(LearningStreamCheckpointEntity(
                streamId = STREAM, lastContiguousSeq = 0L, lastSeenHeadSeq = 2L, replayGeneration = 1L,
                resetReason = LearningStreamResetReason.DERIVED_DATABASE_RECREATED.name,
                bootstrapState = LearningBootstrapState.REQUIRED.name, bootstrapHeadSeq = 2L,
                coverageStartMs = null, commandCoverageStartMs = null, executionCoverageStartMs = null, updatedAtMs = 0L,
            ))
            LearningBootstrapCoordinator(learning, RoomLearningOutboxReader(database),
                RoomLearningReconciliationScanner(database), clockMs = { 100L }).bootstrap(100L)
            val checkpoint = requireNotNull(learning.checkpointDao().find(STREAM))
            assertEquals("COMPLETE", checkpoint.bootstrapState)
            assertEquals(1L, journal().mainStream.headSeq)
            assertTrue(ColdRestoreRebuildFinalizer.completeIfProven(context, checkpoint.streamId,
                requireNotNull(checkpoint.bootstrapHeadSeq), checkpoint.lastContiguousSeq))
            assertFalse(Files.exists(paths().pendingJournal))
        } finally { learning.close() }
    }

    @Test fun delayedPersistedReadCannotAuthorizeAndInvalidSettingsRemainClosed() = runBlocking {
        persist(LearningPreferencesV1())
        stageAndStart(databaseOnlyArchive())
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val delayed = store(object : DataStore<Preferences> {
            override val data: Flow<Preferences> = preferences.data
            override suspend fun updateData(transform: suspend (Preferences) -> Preferences): Preferences {
                entered.complete(Unit)
                release.await()
                return preferences.updateData(transform)
            }
        })
        val cleanup = async(Dispatchers.IO) { facade().finalizeColdRestore(delayed) }
        entered.await()
        assertFalse(cleanup.isCompleted)
        assertEquals(ColdRestorePhase.REBUILD_REQUIRED, journal().phase)
        preferences.edit { it[stringPreferencesKey("agent_learning_preferences_v1")] = "{broken" }
        release.complete(Unit)
        assertFalse(cleanup.await())
        assertTrue(Files.exists(paths().pendingJournal))
        persist(LearningPreferencesV1())
        assertTrue(facade().finalizeColdRestore(settings))
    }

    @Test fun persistedReadFailureNeverFallsBackToDisabled() = runBlocking {
        stageAndStart(databaseOnlyArchive())
        val failed = store(object : DataStore<Preferences> {
            override val data: Flow<Preferences> = preferences.data
            override suspend fun updateData(transform: suspend (Preferences) -> Preferences): Preferences =
                throw java.io.IOException("injected persisted read failure")
        })
        assertTrue(runCatching { facade().finalizeColdRestore(failed) }.isFailure)
        assertEquals(ColdRestorePhase.REBUILD_REQUIRED, journal().phase)
    }

    @Test fun durableCompleteRetriesEvenWhenPersistedSettingsBecomeUnavailable() = runBlocking {
        stageAndStart(databaseOnlyArchive())
        assertFalse(ColdRestoreRebuildFinalizer.completeWithProof(context,
            beforeDelete = { error("interrupted cleanup") }, proof = { true }))
        assertEquals(ColdRestorePhase.COMPLETE, journal().phase)
        val failed = store(object : DataStore<Preferences> {
            override val data: Flow<Preferences> = preferences.data
            override suspend fun updateData(transform: suspend (Preferences) -> Preferences): Preferences =
                error("COMPLETE must not need new settings permission")
        })
        assertTrue(facade().finalizeColdRestore(failed))
        assertFalse(Files.exists(paths().pendingJournal))
    }

    @Test fun maintenanceLaneAndSettingsWritesCannotRaceCleanupAuthorization() = runBlocking {
        stageAndStart(databaseOnlyArchive())
        persist(LearningPreferencesV1())
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val operation = async(Dispatchers.IO) { facade().withDatabase { entered.complete(Unit); release.await() } }
        entered.await()
        val cleanup = async(Dispatchers.IO) { facade().finalizeColdRestore(settings) }
        yield()
        assertFalse(cleanup.isCompleted)
        assertTrue(Files.exists(paths().pendingJournal))
        persist(LearningPreferencesV1(handoff = true, capture = true, jobs = true))
        release.complete(Unit)
        operation.await()
        assertFalse(cleanup.await())
        facade().beginRestore()
        persist(LearningPreferencesV1())
        assertFalse(facade().finalizeColdRestore(settings))
    }

    @Test fun pendingAndBusyRejectDatabaseAndComponentOnlyRestoreBeforeAnyLiveWrite() = runBlocking {
        val archive = componentArchive()
        settings.update(Settings(themeId = "live"))
        upload().writeText("live-file")
        assertEquals(BackupRestoreDisposition.ColdRestartRequired, service().restore(archive, true, true))
        for (includeDatabase in listOf(true, false)) {
            val pending = runCatching { service().restore(archive, includeDatabase, true) }.exceptionOrNull()
            assertEquals(BackupArchiveServiceFailure.COLD_RESTORE_ALREADY_PENDING, (pending as BackupArchiveServiceException).failure)
            FileChannel.open(paths().lockFile, StandardOpenOption.WRITE).use { channel ->
                channel.lock().use {
                    val busy = runCatching { service().restore(archive, includeDatabase, true) }.exceptionOrNull()
                    assertEquals(BackupArchiveServiceFailure.COLD_STAGING_BUSY, (busy as BackupArchiveServiceException).failure)
                }
            }
            assertEquals("live", settings.settingsFlow.value.themeId)
            assertEquals("live-file", upload().readText())
        }
    }

    @Test fun componentReplayFailureIsRetryableBeforeGraphAndReceiptPreventsLaterSettingsOverwrite() = runBlocking {
        val archive = componentArchive()
        settings.update(Settings(themeId = "live"))
        upload().writeText("live-file")
        service().restore(archive, true, true)
        database.close()
        val failed = store(object : DataStore<Preferences> {
            override val data: Flow<Preferences> = preferences.data
            override suspend fun updateData(transform: suspend (Preferences) -> Preferences): Preferences =
                throw java.io.IOException("injected settings commit failure")
        })
        assertTrue(ColdRestoreStartupCoordinator.run(context) { failed } is ColdRestoreStartupResult.DegradedRestartRequired)
        assertEquals(ColdRestorePhase.REBUILD_REQUIRED, journal().phase)
        assertEquals("archive-file", upload().readText())
        assertEquals(ColdRestoreStartupResult.RebuildRequired, ColdRestoreStartupCoordinator.run(context) { settings })
        assertEquals("archive", settings.settingsFlow.value.themeId)
        settings.update(Settings(themeId = "after-replay"))
        assertEquals(ColdRestoreStartupResult.RebuildRequired, ColdRestoreStartupCoordinator.run(context) { settings })
        assertEquals("after-replay", settings.settingsFlow.value.themeId)
        assertTrue(facade().finalizeColdRestore(settings))
    }

    @Test fun completeReceiptSurvivesEveryCleanupDeletionBoundaryAndRestart() = runBlocking {
        val archive = databaseOnlyArchive()
        // Learning file, its directory, old main file, its directory, archive, component receipt,
        // request directory, pending journal. Interrupt before each exact deletion in turn.
        for (boundary in 0..7) {
            if (boundary > 0) {
                database = openDatabase()
                context.getDatabasePath(LearningDatabase.FILE_NAME).writeText("old-derived-timeline")
            }
            stageAndStart(archive)
            var count = 0
            assertFalse(ColdRestoreRebuildFinalizer.completeWithProof(context,
                beforeDelete = { if (count++ == boundary) error("crash before delete $boundary") },
                proof = { true }))
            assertEquals(ColdRestorePhase.COMPLETE, journal().phase)
            assertEquals(ColdRestoreStartupResult.Complete, ColdRestoreStartupCoordinator.run(context) { settings })
            assertTrue(ColdRestoreRebuildFinalizer.completeWithProof(context, proof = { error("durable authorization was lost") }))
            assertFalse(Files.exists(paths().pendingJournal))
            assertTrue(context.getDatabasePath("rikka_hub").isFile)
        }
    }

    private fun store(data: DataStore<Preferences>) = SettingsStore(context, AppScope().apply { cancel() }, data)
    private suspend fun persist(flags: LearningPreferencesV1) = settings.update(Settings(learningPreferences = flags))
    private fun facade() = runtime ?: LearningRuntimeFacade(context, isEnabled = { true }, isMainProcess = { true })
        .also { runtime = it }
    private fun service() = BackupArchiveService(settings, JsonInstant, context, database)
    private fun openDatabase() = Room.databaseBuilder(context, AppDatabase::class.java, "rikka_hub")
        .openHelperFactory(createAppSQLiteOpenHelperFactory(context)).build()
    private fun paths() = (ColdRestoreStagingPaths.verify(root, context.noBackupFilesDir) as ColdRestoreStagingPathValidation.Valid).paths
    private fun journal() = (ColdRestoreJournalStore(paths().pendingJournal).read() as ColdRestoreJournalReadResult.Valid).journal
    private fun upload() = File(File(context.filesDir, FileFolders.UPLOAD).apply { mkdirs() }, "r3.txt")
    private suspend fun componentArchive(): File {
        settings.update(Settings(themeId = "archive"))
        upload().writeText("archive-file")
        return service().createBackup(File(context.cacheDir, "components.zip"), true, true)
    }
    private fun databaseOnlyArchive(): File {
        val snapshot = File(context.cacheDir, "frozen.db")
        database.openHelper.writableDatabase.execSQL("VACUUM INTO ?", arrayOf(snapshot.path))
        return File(context.cacheDir, "database-only.zip").also {
            BackupArchiveV1FileIO.write(it, setOf(BackupArchiveComponent.DATABASE),
                BackupAuthorityStreamV1(STREAM, 1L),
                listOf(BackupArchiveSourceV1.FileSource(BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY, snapshot)))
        }
    }
    private suspend fun stageAndStart(archive: File) {
        assertEquals(BackupRestoreDisposition.ColdRestartRequired, service().restore(archive, true, false))
        database.close()
        assertEquals(ColdRestoreStartupResult.RebuildRequired, ColdRestoreStartupCoordinator.run(context) { settings })
    }
    private companion object { const val STREAM = "00000000-0000-4000-8000-0000000000a3" }
}
