package me.rerere.rikkahub.data.sync.backup

import android.content.Context
import android.content.ContextWrapper
import android.content.pm.ApplicationInfo
import androidx.room.Room
import androidx.test.core.app.ApplicationProvider
import androidx.test.ext.junit.runners.AndroidJUnit4
import java.io.File
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import me.rerere.rikkahub.AppScope
import me.rerere.rikkahub.data.datastore.Settings
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.createAppSQLiteOpenHelperFactory
import me.rerere.rikkahub.learning.handoff.*
import me.rerere.rikkahub.learning.storage.*
import me.rerere.rikkahub.learning.retention.*
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreStartupCoordinator
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreStartupResult
import me.rerere.rikkahub.utils.JsonInstant
import org.junit.After
import org.junit.Assert.*
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith

/** Actual service export/import on synthetic app-private data; disposable emulator only. */
@RunWith(AndroidJUnit4::class)
class BackupArchiveServiceRetainedOutboxTest {
    private lateinit var context: Context
    private lateinit var root: File
    private lateinit var database: AppDatabase
    private lateinit var service: BackupArchiveService

    @Before
    fun setUp() {
        val base = ApplicationProvider.getApplicationContext<Context>()
        root = File(base.cacheDir.canonicalFile, "retained-backup-${UUID.randomUUID()}").apply { mkdirs() }
        context = object : ContextWrapper(base) {
            override fun getApplicationContext(): Context = this
            override fun getApplicationInfo() = ApplicationInfo(base.applicationInfo).apply { dataDir = root.path }
            override fun getFilesDir() = File(root, "files").apply { mkdirs() }
            override fun getCacheDir() = File(root, "cache").apply { mkdirs() }
            override fun getNoBackupFilesDir() = File(root, "no_backup").apply { mkdirs() }
            override fun getDatabasePath(name: String) = File(File(root, "databases").apply { mkdirs() }, name)
        }
        database = openDatabase()
        val settings = SettingsStore(base, AppScope().apply { cancel() })
        settings.settingsFlow.value = Settings()
        service = BackupArchiveService(settings, JsonInstant, context, database)
        database.openHelper.writableDatabase.execSQL(
            "INSERT INTO learning_outbox(stream_id,event_id,event_type,event_schema_version,created_at_ms) VALUES(?,?,'STREAM_INIT',1,0)",
            arrayOf(STREAM, LEARNING_STREAM_INIT_EVENT_ID),
        )
        repeat(4) { append(it + 1) }
    }

    @After
    fun tearDown() {
        database.close()
        root.deleteRecursively()
    }

    @Test
    fun denseServiceExportAndColdImport() = runBlocking { roundTrip(5L, listOf(1L, 2L, 3L, 4L, 5L)) }

    @Test
    fun retainedSparseServiceExportAndColdImport() = runBlocking {
        assertEquals(LearningOutboxRetentionResult.Completed(2, false),
            RoomLearningPrimaryOutboxRetentionPort(database).pruneOnce(LearningOutboxRetentionRequest(
                checkpoints = listOf(LearningDurableConsumerCheckpoint(
                    consumerId = LearningDurableConsumerId.LEARNING_DERIVED_RUNTIME,
                    streamId = STREAM, replayGeneration = 1L, lastContiguousSequence = 5L, bootstrapComplete = true,
                )),
                frozenNowMs = 100L, minimumAgeMs = 10L, safetyFloorRows = 2L, batchSize = 10,
            )))
        roundTrip(5L, listOf(1L, 4L, 5L))
    }

    @Test
    fun ignoredInsertAllocatorGapIsNotTheDescriptorHead() = runBlocking {
        append(4) // INSERT OR IGNORE consumes an allocator value without committing an event.
        roundTrip(5L, listOf(1L, 2L, 3L, 4L, 5L))
    }

    @Test
    fun missingSentinelRejectsServiceExport() = runBlocking {
        database.openHelper.writableDatabase.execSQL("DELETE FROM learning_outbox WHERE seq=1")
        expectExportRejected()
    }

    @Test
    fun malformedSentinelRejectsServiceExport() = runBlocking {
        database.openHelper.writableDatabase.execSQL("UPDATE learning_outbox SET event_id='invalid-sentinel' WHERE seq=1")
        expectExportRejected()
    }

    @Test
    fun sentinelPayloadRejectsServiceExport() = runBlocking {
        database.openHelper.writableDatabase.execSQL("UPDATE learning_outbox SET source_id='forbidden-payload' WHERE seq=1")
        expectExportRejected()
    }

    @Test
    fun mixedLineageRejectsServiceExport() = runBlocking {
        database.openHelper.writableDatabase.execSQL("UPDATE learning_outbox SET stream_id='00000000-0000-4000-8000-0000000000a2' WHERE seq=5")
        expectExportRejected()
    }

    @Test
    fun duplicateEventIdentityRejectsServiceExport() = runBlocking {
        database.openHelper.writableDatabase.execSQL("DROP INDEX index_learning_outbox_event_id")
        database.openHelper.writableDatabase.execSQL("UPDATE learning_outbox SET event_id='retained-event-1' WHERE seq=5")
        expectExportRejected()
    }

    @Test
    fun malformedEventIdentityRejectsServiceExport() = runBlocking {
        database.openHelper.writableDatabase.execSQL("UPDATE learning_outbox SET event_id='invalid event id' WHERE seq=5")
        expectExportRejected()
    }

    @Test
    fun duplicateSequenceRejectsServiceExport() = runBlocking {
        val db = database.openHelper.writableDatabase
        db.execSQL("CREATE TABLE copied_outbox AS SELECT * FROM learning_outbox")
        db.execSQL("DROP TABLE learning_outbox")
        db.execSQL("ALTER TABLE copied_outbox RENAME TO learning_outbox")
        db.execSQL("UPDATE learning_outbox SET seq=4 WHERE seq=5")
        expectExportRejected()
    }

    @Test
    fun ignoredInsertThenAppendLeavesLegalSurvivingGap() = runBlocking {
        append(4)
        append(5)
        roundTrip(7L, listOf(1L, 2L, 3L, 4L, 5L, 7L))
    }

    @Test
    fun descriptorMismatchRejectsColdImport() = runBlocking {
        rejectImportedSnapshot(expectedHead = 4L)
    }

    @Test
    fun malformedEventIdentityRejectsColdImport() = runBlocking {
        database.openHelper.writableDatabase.execSQL("UPDATE learning_outbox SET event_id='invalid event id' WHERE seq=5")
        rejectImportedSnapshot(expectedHead = 5L)
    }

    private suspend fun rejectImportedSnapshot(expectedHead: Long) {
        val snapshot = File(context.cacheDir, "invalid-import.db")
        database.openHelper.writableDatabase.execSQL("VACUUM INTO ?", arrayOf(snapshot.path))
        val archive = File(context.cacheDir, "invalid-import.zip")
        BackupArchiveV1FileIO.write(archive,
            components = setOf(BackupArchiveComponent.DATABASE),
            mainStream = BackupAuthorityStreamV1(STREAM, expectedHead),
            sources = listOf(BackupArchiveSourceV1.FileSource(BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY, snapshot)))
        assertEquals(BackupRestoreDisposition.ColdRestartRequired, service.restore(archive, true, false))
        database.close()
        assertTrue(ColdRestoreStartupCoordinator.run(context) is ColdRestoreStartupResult.LiveDatabaseUnchanged)
    }

    private fun append(index: Int) {
        database.openHelper.writableDatabase.execSQL(
            "INSERT OR IGNORE INTO learning_outbox(stream_id,event_id,event_type,event_schema_version,created_at_ms,source_type,source_id,source_revision,scope_kind,scope_id,occurred_at_ms) VALUES(?,?,'FUTURE_EVENT',1,1,'EXECUTION_EVENT',?,1,'AUTHORITY_SUBJECT','retained-subject',1)",
            arrayOf(STREAM, "retained-event-$index", "execution-$index"),
        )
    }

    private suspend fun expectExportRejected() {
        try {
            service.createBackup(File(context.cacheDir, "rejected.zip"), true, false)
            fail("Malformed retained log must not export")
        } catch (failure: BackupArchiveServiceException) {
            assertEquals(BackupArchiveServiceFailure.DATABASE_STREAM_INVALID, failure.failure)
        }
    }

    private suspend fun roundTrip(head: Long, expected: List<Long>) {
        val archive = File(context.cacheDir, "archive.zip")
        service.createBackup(archive, true, false)
        assertEquals(head, BackupArchiveV1FileIO.inspect(archive).manifest.mainStream?.headSeq)
        assertEquals(BackupRestoreDisposition.ColdRestartRequired, service.restore(archive, true, false))
        database.close()
        assertEquals(ColdRestoreStartupResult.RebuildRequired, ColdRestoreStartupCoordinator.run(context))
        database = openDatabase()
        database.openHelper.writableDatabase.query("SELECT seq FROM learning_outbox ORDER BY seq").use {
            assertEquals(expected, buildList { while (it.moveToNext()) add(it.getLong(0)) })
        }
        val learning = Room.inMemoryDatabaseBuilder(context, LearningDatabase::class.java).build()
        try {
            learning.checkpointDao().insert(LearningStreamCheckpointEntity(
                streamId = STREAM, lastContiguousSeq = 0L, lastSeenHeadSeq = head,
                replayGeneration = 1L, resetReason = LearningStreamResetReason.DERIVED_DATABASE_RECREATED.name,
                bootstrapState = LearningBootstrapState.REQUIRED.name, bootstrapHeadSeq = head,
                coverageStartMs = null, commandCoverageStartMs = null, executionCoverageStartMs = null,
                updatedAtMs = 0L,
            ))
            LearningBootstrapCoordinator(learning, RoomLearningOutboxReader(database),
                RoomLearningReconciliationScanner(database), clockMs = { 100L }).bootstrap(100L)
            val checkpoint = requireNotNull(learning.checkpointDao().find(STREAM))
            assertEquals(LearningBootstrapState.COMPLETE.name, checkpoint.bootstrapState)
            assertEquals(head, checkpoint.lastContiguousSeq)
        } finally {
            learning.close()
        }
    }

    private fun openDatabase() = Room.databaseBuilder(context, AppDatabase::class.java, "rikka_hub")
        .openHelperFactory(createAppSQLiteOpenHelperFactory(context)).build()

    private companion object { const val STREAM = "00000000-0000-4000-8000-0000000000a1" }
}
