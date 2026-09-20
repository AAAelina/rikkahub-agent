package me.rerere.rikkahub.data.db

import android.database.sqlite.SQLiteDatabase
import androidx.room.testing.MigrationTestHelper
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import java.io.File
import java.util.UUID
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import me.rerere.rikkahub.data.sync.backup.BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY
import me.rerere.rikkahub.data.sync.backup.BackupArchiveComponent
import me.rerere.rikkahub.data.sync.backup.BackupArchiveSourceV1
import me.rerere.rikkahub.data.sync.backup.BackupArchiveV1FileIO
import me.rerere.rikkahub.data.sync.backup.BackupAuthorityStreamV1
import me.rerere.rikkahub.learning.grant.policyGrantId
import me.rerere.rikkahub.learning.model.LearningScope
import me.rerere.rikkahub.learning.storage.LearningDatabase
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreArchiveStager
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreBootstrap
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreBootstrapFailure
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreBootstrapPathValidation
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreBootstrapPaths
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreBootstrapResult
import me.rerere.rikkahub.learning.storage.restore.ColdRestorePreparedDatabaseReconciler
import me.rerere.rikkahub.learning.storage.restore.ColdRestorePreparedDatabaseValidator
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreRequestIdSource
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreStageResult
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreStagingPathValidation
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreStagingPaths
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreSwapExecutor
import me.rerere.rikkahub.learning.storage.restore.ColdRestoreSwapResult
import me.rerere.rikkahub.learning.storage.restore.LearningOwnedDatabasePaths
import me.rerere.rikkahub.learning.storage.restore.LearningOwnedDatabasePathValidation
import me.rerere.rikkahub.learning.storage.restore.VerifiedColdRestoreArchive
import me.rerere.rikkahub.learning.storage.restore.VerifiedColdRestoreArchiveResult
import me.rerere.rikkahub.learning.workflow.WorkflowArtifactCanonicalizer
import me.rerere.rikkahub.workflow.model.TriggerSpec
import me.rerere.rikkahub.workflow.model.WorkflowAction
import me.rerere.rikkahub.workflow.model.WorkflowDefinition
import me.rerere.rikkahub.workflow.model.WorkflowJson
import me.rerere.rikkahub.workflow.model.WorkflowOrigin
import me.rerere.rikkahub.workflow.model.WorkflowToolSchemaSnapshot
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.uuid.Uuid

/** Emulator/disposable-device only. Never run this instrumentation test on the primary phone. */
@RunWith(AndroidJUnit4::class)
class AppDatabaseV49BackupRestoreRoundTripTest {
    @get:Rule
    val helper = MigrationTestHelper(
        InstrumentationRegistry.getInstrumentation(),
        AppDatabase::class.java,
        emptyList(),
        createAppSQLiteOpenHelperFactory(InstrumentationRegistry.getInstrumentation().targetContext),
    )

    @Test
    fun currentV50RejectsWrongForeignKeyActionsWithOtherwiseIntactSchema() {
        listOf("RESTRICT" to "CASCADE", "CASCADE" to "RESTRICT").forEach { (before, after) ->
            val name = stagedDatabaseName()
            try {
                helper.createDatabase(name, 50).use { db ->
                    insertV49Fixture(db)
                    val table = "dream_claim_experience_sources"
                    val ddl = db.query("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '$table'").use {
                        check(it.moveToFirst())
                        it.getString(0)
                    }
                    val indices = db.query("SELECT sql FROM sqlite_master WHERE type = 'index' AND tbl_name = '$table' AND sql IS NOT NULL").use {
                        buildList { while (it.moveToNext()) add(it.getString(0)) }
                    }
                    db.execSQL("DROP TABLE $table")
                    db.execSQL(ddl.replace("ON DELETE $before", "ON DELETE $after"))
                    indices.forEach(db::execSQL)
                }
                org.junit.Assert.assertThrows(IllegalStateException::class.java) {
                    ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                        context().getDatabasePath(name).canonicalFile, STREAM_ID, 1L,
                    )
                }
            } finally {
                context().deleteDatabase(name)
            }
        }
    }

    @Test
    fun currentV50RejectsMissingTableColumnIndexAndForeignKeyBeforeQuarantine() {
        listOf(
            "DROP TABLE dream_claim_experience_sources",
            "ALTER TABLE dream_claims RENAME COLUMN subject_kind TO broken_subject_kind",
            "DROP INDEX index_dream_experiences_pair_scope_id_experience_epoch",
            "ALTER TABLE dream_claim_experience_sources RENAME TO broken_sources",
        ).forEach { corruption ->
            val name = stagedDatabaseName()
            try {
                helper.createDatabase(name, 50).use { db ->
                    insertV49Fixture(db)
                    db.execSQL(corruption)
                    if (corruption.contains("broken_sources")) {
                        db.execSQL("CREATE TABLE dream_claim_experience_sources AS SELECT * FROM broken_sources")
                    }
                }
                val staged = context().getDatabasePath(name).canonicalFile
                org.junit.Assert.assertThrows(IllegalStateException::class.java) {
                    ImportedDatabaseReconciler.reconcileStagedFileOrThrow(staged, STREAM_ID, 1L)
                }
                SQLiteDatabase.openDatabase(staged.path, null, SQLiteDatabase.OPEN_READONLY).use { db ->
                    db.rawQuery("SELECT stateVersion FROM workflows WHERE id = ?", arrayOf(WORKFLOW_ID)).use {
                        assertTrue(it.moveToFirst())
                        assertEquals(1L, it.getLong(0))
                    }
                }
            } finally {
                context().deleteDatabase(name)
            }
        }
    }

    @Test
    fun exactSameVersionV50StagedImportPreservesWorkflowAndGrant() {
        val name = stagedDatabaseName()
        try {
            helper.createDatabase(name, 50).use(::insertV49Fixture)
            val staged = context().getDatabasePath(name).canonicalFile
            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(staged, STREAM_ID, 1L)
            assertRestoredFixture(staged)
        } finally {
            context().deleteDatabase(name)
        }
    }

    @Test
    fun falselyStampedV50IsRejectedBeforeWorkflowQuarantine() {
        val name = stagedDatabaseName()
        try {
            helper.createDatabase(name, 49).use { db ->
                insertV49Fixture(db)
                db.execSQL("PRAGMA user_version = 50")
                db.execSQL("UPDATE room_master_table SET identity_hash = '73b32c82384f8b24fe576d123524249a' WHERE id = 42")
            }
            val staged = context().getDatabasePath(name).canonicalFile
            org.junit.Assert.assertThrows(IllegalStateException::class.java) {
                ImportedDatabaseReconciler.reconcileStagedFileOrThrow(staged, STREAM_ID, 1L)
            }
            SQLiteDatabase.openDatabase(staged.path, null, SQLiteDatabase.OPEN_READONLY).use { db ->
                db.rawQuery("SELECT enabled, stateVersion FROM workflows WHERE id = ?", arrayOf(WORKFLOW_ID)).use {
                    assertTrue(it.moveToFirst())
                    assertEquals(0L, it.getLong(0))
                    assertEquals(1L, it.getLong(1))
                }
            }
        } finally {
            context().deleteDatabase(name)
        }
    }

    @Test
    fun exactV49StagedImportPreservesWorkflowAndGrant() {
        val name = stagedDatabaseName()
        try {
            helper.createDatabase(name, 49).use(::insertV49Fixture)
            val staged = context().getDatabasePath(name).canonicalFile

            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                databaseFile = staged,
                expectedStreamId = STREAM_ID,
                expectedHeadSeq = 1L,
            )

            assertRestoredFixture(staged)
        } finally {
            context().deleteDatabase(name)
        }
    }

    @Test
    fun v49AndV50ArchivesSurviveStagePrepareSwapAndRoomOpen() {
        listOf(49, 50).forEach { version ->
            val sourceName = "v49-backup-source-${randomToken()}"
            val root = File(context().cacheDir.canonicalFile, "v49-backup-roundtrip-${randomToken()}")
            val archive = File(context().cacheDir.canonicalFile, "v49-backup-${randomToken()}.zip")
            try {
                helper.createDatabase(sourceName, version).use(::insertV49Fixture)
                val sourceDatabase = context().getDatabasePath(sourceName).canonicalFile
                val verified = writeAndVerifyArchive(sourceDatabase, archive)
                assertEquals(
                    BackupAuthorityStreamV1(STREAM_ID, 1L),
                    verified.manifest.mainStream,
                )

                val fixture = prepareColdRestoreFixture(root, liveBytes = "old-live".toByteArray())
                val staged = ColdRestoreArchiveStager(
                    pathValidation = fixture.stagingPaths,
                    requestIdSource = ColdRestoreRequestIdSource { REQUEST_ID },
                    clockMs = { 10L },
                ).stage(verified)
                assertTrue(staged is ColdRestoreStageResult.Staged)

                val prepared = ColdRestoreBootstrap(
                    stagingPaths = fixture.stagingPaths,
                    bootstrapPaths = fixture.bootstrapPaths,
                    reconciler = stagedReconciler(),
                    validator = stagedValidator(),
                    clockMs = { 20L },
                ).prepare()
                assertTrue(prepared is ColdRestoreBootstrapResult.ReadyToSwap)

                val swapped = ColdRestoreSwapExecutor(
                    stagingPaths = fixture.stagingPaths,
                    bootstrapPaths = fixture.bootstrapPaths,
                    learningPaths = fixture.learningPaths,
                    validator = installedOrStagedValidator(),
                    clockMs = { 30L },
                ).execute()
                assertEquals(ColdRestoreSwapResult.RebuildRequired, swapped)
                assertFalse(fixture.learningDatabase.exists())
                assertRestoredFixture(fixture.liveDatabase)
            } finally {
                context().deleteDatabase(sourceName)
                archive.delete()
                root.deleteRecursively()
            }
        }
    }

    @Test
    fun unknownV49IdentityIsRejectedDuringColdPreparationBeforeLiveSwap() {
        val sourceName = "v49-unknown-source-${randomToken()}"
        val root = File(context().cacheDir.canonicalFile, "v49-unknown-roundtrip-${randomToken()}")
        val archive = File(context().cacheDir.canonicalFile, "v49-unknown-${randomToken()}.zip")
        val oldLive = "old-live-must-remain".toByteArray()
        try {
            helper.createDatabase(sourceName, 49).use { database ->
                insertStream(database)
                database.execSQL(
                    "UPDATE room_master_table SET identity_hash = 'unknown-v49' WHERE id = 42",
                )
            }
            val verified = writeAndVerifyArchive(
                context().getDatabasePath(sourceName).canonicalFile,
                archive,
            )
            val fixture = prepareColdRestoreFixture(root, liveBytes = oldLive)
            assertTrue(
                ColdRestoreArchiveStager(
                    pathValidation = fixture.stagingPaths,
                    requestIdSource = ColdRestoreRequestIdSource { REQUEST_ID },
                    clockMs = { 10L },
                ).stage(verified) is ColdRestoreStageResult.Staged,
            )

            val result = ColdRestoreBootstrap(
                stagingPaths = fixture.stagingPaths,
                bootstrapPaths = fixture.bootstrapPaths,
                reconciler = stagedReconciler(),
                validator = stagedValidator(),
                clockMs = { 20L },
            ).prepare()

            assertEquals(
                ColdRestoreBootstrapResult.Failed(
                    ColdRestoreBootstrapFailure.DATABASE_RECONCILE_FAILED,
                ),
                result,
            )
            assertTrue(oldLive.contentEquals(fixture.liveDatabase.readBytes()))
            assertFalse(
                File(fixture.liveDatabase.parentFile, ".rikka_hub.restore_$REQUEST_ID.ready")
                    .exists(),
            )
        } finally {
            context().deleteDatabase(sourceName)
            archive.delete()
            root.deleteRecursively()
        }
    }

    private fun insertV49Fixture(database: androidx.sqlite.db.SupportSQLiteDatabase) {
        insertStream(database)
        insertGrantHeadAndRevision(database)

        val action = WorkflowAction(
            tool = "show_toast",
            args = buildJsonObject { put("text", JsonPrimitive("restored")) },
            toolSchemaFingerprint = TOOL_SCHEMA_SHA,
        )
        val definition = WorkflowDefinition(
            id = WORKFLOW_ID,
            name = WORKFLOW_NAME,
            enabled = false,
            trigger = TriggerSpec.Manual,
            actions = listOf(action),
            createdAtMs = 10L,
            updatedAtMs = 10L,
            authoringAssistantId = CONSUMING_ASSISTANT_ID,
            capabilitySnapshot = setOf("tool.show_toast"),
            origin = WorkflowOrigin.LEARNED,
            sourceCandidateId = CANDIDATE_ID,
            sourceArtifactHash = WORKFLOW_ARTIFACT_SHA,
            grantDigest = GRANT_DIGEST,
            authoritySubjectId = AUTHORITY_SUBJECT_ID,
        )
        val definitionJson = requireNotNull(WorkflowJson.encodeForLearned(definition))
        database.execSQL(
            "INSERT INTO workflows(id, name, enabled, definitionJson, createdAtMs, updatedAtMs, " +
                "stateVersion, origin, sourceCandidateId, sourceArtifactHash, grantDigest, " +
                "authoringAssistantId, capabilitySnapshotJson, toolSchemaFingerprintsJson) " +
                "VALUES(?, ?, 0, ?, 10, 10, 1, 'LEARNED', ?, ?, ?, ?, ?, ?)",
            arrayOf(
                WORKFLOW_ID,
                WORKFLOW_NAME,
                definitionJson,
                CANDIDATE_ID,
                WORKFLOW_ARTIFACT_SHA,
                GRANT_DIGEST,
                CONSUMING_ASSISTANT_ID,
                "[\"tool.show_toast\"]",
                WorkflowToolSchemaSnapshot.canonicalProjection(definition.actions),
            ),
        )
        val userDefinition = definition.copy(
            id = USER_WORKFLOW_ID,
            name = USER_WORKFLOW_NAME,
            enabled = true,
            origin = WorkflowOrigin.USER,
            sourceCandidateId = null,
            sourceArtifactHash = null,
            grantDigest = null,
            authoritySubjectId = null,
        )
        database.execSQL(
            "INSERT INTO workflows(id, name, enabled, definitionJson, createdAtMs, updatedAtMs, " +
                "stateVersion, origin, authoringAssistantId, capabilitySnapshotJson, " +
                "toolSchemaFingerprintsJson) VALUES(?, ?, 1, ?, 10, 10, 1, 'USER', ?, ?, ?)",
            arrayOf(
                USER_WORKFLOW_ID,
                USER_WORKFLOW_NAME,
                WorkflowJson.encode(userDefinition),
                CONSUMING_ASSISTANT_ID,
                "[\"tool.show_toast\"]",
                WorkflowToolSchemaSnapshot.canonicalProjection(userDefinition.actions),
            ),
        )
    }

    private fun insertStream(database: androidx.sqlite.db.SupportSQLiteDatabase) {
        database.execSQL(
            "INSERT INTO learning_outbox(stream_id, event_id, event_type, " +
                "event_schema_version, created_at_ms) VALUES(?, ?, 'STREAM_INIT', 1, 1)",
            arrayOf(STREAM_ID, STREAM_INIT_EVENT_ID),
        )
    }

    private fun insertGrantHeadAndRevision(database: androidx.sqlite.db.SupportSQLiteDatabase) {
        val values = arrayOf<Any?>(
            GRANT_ID,
            STREAM_ID,
            POLICY_ID,
            7L,
            POLICY_ARTIFACT_SHA,
            "AUTHORITY_SUBJECT",
            AUTHORITY_SUBJECT_ID,
            CONSUMING_ASSISTANT_ID,
            "USER_REVIEW",
            "GRANTED",
            1L,
            10L,
            null,
            "USER_APPROVED_CONTEXTUAL_ADVICE",
            10L,
            10L,
        )
        database.execSQL(
            "INSERT INTO learning_policy_grants($GRANT_COLUMNS) VALUES(" +
                placeholders(GRANT_COLUMN_COUNT) + ")",
            values,
        )
        database.execSQL(
            "INSERT INTO learning_policy_grant_revisions(" +
                "$GRANT_COLUMNS, previous_state_version, changed_at_ms) VALUES(" +
                placeholders(GRANT_COLUMN_COUNT) + ", NULL, ?)",
            arrayOf<Any?>(*values, 10L),
        )
    }

    private fun writeAndVerifyArchive(
        sourceDatabase: File,
        archive: File,
    ): VerifiedColdRestoreArchive {
        BackupArchiveV1FileIO.write(
            destination = archive,
            components = setOf(BackupArchiveComponent.DATABASE),
            mainStream = BackupAuthorityStreamV1(STREAM_ID, 1L),
            sources = listOf(
                BackupArchiveSourceV1.FileSource(
                    name = BACKUP_ARCHIVE_MAIN_DATABASE_ENTRY,
                    file = sourceDatabase,
                ),
            ),
        )
        val inspected = BackupArchiveV1FileIO.inspectForRestore(archive)
        val verified = VerifiedColdRestoreArchive.verify(
            archiveFile = inspected.archiveFile,
            archiveSize = inspected.archiveSize,
            archiveSha256 = inspected.archiveSha256,
            manifest = inspected.manifest,
        )
        assertTrue(verified is VerifiedColdRestoreArchiveResult.Verified)
        return (verified as VerifiedColdRestoreArchiveResult.Verified).archive
    }

    private fun prepareColdRestoreFixture(root: File, liveBytes: ByteArray): RestoreFixture {
        assertTrue(root.mkdir())
        val noBackup = File(root, "no_backup").apply { assertTrue(mkdir()) }
        val databases = File(root, "databases").apply { assertTrue(mkdir()) }
        val liveDatabase = File(databases, "rikka_hub").apply { writeBytes(liveBytes) }
        val learningDatabase = File(databases, LearningDatabase.FILE_NAME).apply {
            writeText("old-derived-learning")
        }
        return RestoreFixture(
            stagingPaths = ColdRestoreStagingPaths.verify(root, noBackup),
            bootstrapPaths = ColdRestoreBootstrapPaths.verify(root, liveDatabase),
            learningPaths = LearningOwnedDatabasePaths.verify(root, learningDatabase),
            liveDatabase = liveDatabase,
            learningDatabase = learningDatabase,
        )
    }

    private fun stagedReconciler() = ColdRestorePreparedDatabaseReconciler { file, stream ->
        ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
            file,
            stream.streamId,
            stream.headSeq,
        )
    }

    private fun stagedValidator() = ColdRestorePreparedDatabaseValidator { file, stream ->
        ImportedDatabaseReconciler.validateStagedFileOrThrow(
            file,
            stream.streamId,
            stream.headSeq,
        )
    }

    private fun installedOrStagedValidator() =
        ColdRestorePreparedDatabaseValidator { file, stream ->
            if (file.name == "rikka_hub") {
                ImportedDatabaseReconciler.validateInstalledFileOrThrow(
                    file,
                    stream.streamId,
                    stream.headSeq,
                )
            } else {
                ImportedDatabaseReconciler.validateStagedFileOrThrow(
                    file,
                    stream.streamId,
                    stream.headSeq,
                )
            }
        }

    private fun assertRestoredFixture(databaseFile: File) {
        val room = androidx.room.Room.databaseBuilder(context(), AppDatabase::class.java, databaseFile.absolutePath)
            .openHelperFactory(createAppSQLiteOpenHelperFactory(context()))
            .build()
        try {
            assertEquals(50, room.openHelper.writableDatabase.version)
        } finally {
            room.close()
        }
        SQLiteDatabase.openDatabase(
            databaseFile.absolutePath,
            null,
            SQLiteDatabase.OPEN_READONLY,
        ).use { database ->
            assertEquals(50, database.version)
            database.rawQuery(
                "SELECT identity_hash FROM room_master_table WHERE id = 42",
                null,
            ).use { cursor ->
                assertTrue(cursor.moveToFirst())
                assertEquals(ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH, cursor.getString(0))
                assertFalse(cursor.moveToNext())
            }
            database.rawQuery(
                "SELECT origin, enabled, stateVersion, sourceCandidateId, sourceArtifactHash, " +
                    "grantDigest, authoringAssistantId, definitionJson, staleReason, name " +
                    "FROM workflows WHERE id = ?",
                arrayOf(WORKFLOW_ID),
            ).use { cursor ->
                assertTrue(cursor.moveToFirst())
                assertEquals("LEARNED", cursor.getString(0))
                assertEquals(0L, cursor.getLong(1))
                assertEquals(2L, cursor.getLong(2))
                assertEquals(CANDIDATE_ID, cursor.getString(3))
                assertTrue(cursor.isNull(4))
                assertTrue(cursor.isNull(5))
                assertTrue(cursor.isNull(6))
                assertEquals("{}", cursor.getString(7))
                assertEquals("learning_scope_erased_definition_v1", cursor.getString(8))
                assertEquals("Erased learned workflow", cursor.getString(9))
                assertEquals(null, WorkflowJson.parseStored(cursor.getString(7)))
                assertFalse(cursor.moveToNext())
            }
            database.rawQuery(
                "SELECT name, enabled, stateVersion, origin, staleReason, definitionJson " +
                    "FROM workflows WHERE id = ?",
                arrayOf(USER_WORKFLOW_ID),
            ).use { cursor ->
                assertTrue(cursor.moveToFirst())
                assertEquals(USER_WORKFLOW_NAME, cursor.getString(0))
                assertEquals(1L, cursor.getLong(1))
                assertEquals(1L, cursor.getLong(2))
                assertEquals("USER", cursor.getString(3))
                assertTrue(cursor.isNull(4))
                assertEquals(USER_WORKFLOW_ID, WorkflowJson.parseStored(cursor.getString(5))?.id)
                assertFalse(cursor.moveToNext())
            }
            database.rawQuery(
                "SELECT policy_revision, artifact_sha256, state, state_version " +
                    "FROM learning_policy_grants WHERE grant_id = ?",
                arrayOf(GRANT_ID),
            ).use { cursor ->
                assertTrue(cursor.moveToFirst())
                assertEquals(7L, cursor.getLong(0))
                assertEquals(POLICY_ARTIFACT_SHA, cursor.getString(1))
                assertEquals("GRANTED", cursor.getString(2))
                assertEquals(1L, cursor.getLong(3))
                assertFalse(cursor.moveToNext())
            }
            database.rawQuery(
                "SELECT COUNT(*) FROM learning_policy_grant_revisions " +
                    "WHERE grant_id = ? AND state_version = 1",
                arrayOf(GRANT_ID),
            ).use { cursor ->
                assertTrue(cursor.moveToFirst())
                assertEquals(1L, cursor.getLong(0))
            }
        }
    }

    private fun context() = InstrumentationRegistry.getInstrumentation().targetContext

    private fun stagedDatabaseName(): String = ".rikka_hub.restore_${randomToken()}.ready"

    private fun randomToken(): String = UUID.randomUUID().toString().replace("-", "")

    private data class RestoreFixture(
        val stagingPaths: ColdRestoreStagingPathValidation,
        val bootstrapPaths: ColdRestoreBootstrapPathValidation,
        val learningPaths: LearningOwnedDatabasePathValidation,
        val liveDatabase: File,
        val learningDatabase: File,
    )

    private companion object {
        const val REQUEST_ID = "0123456789abcdef0123456789abcdef"
        const val STREAM_ID = "80000000-0000-0000-0000-000000000008"
        const val STREAM_INIT_EVENT_ID = "learning-stream-init:v1"
        const val POLICY_ID = "policy-backup-roundtrip"
        const val AUTHORITY_SUBJECT_ID = "authority-subject-backup-roundtrip"
        const val CONSUMING_ASSISTANT_ID = "a0000000-0000-0000-0000-000000000001"
        const val CANDIDATE_ID = "candidate-backup-roundtrip"
        const val WORKFLOW_ID = "learned:candidate-backup-roundtrip"
        const val WORKFLOW_NAME = "Backup restore contract"
        const val USER_WORKFLOW_ID = "user-backup-roundtrip"
        const val USER_WORKFLOW_NAME = "User backup restore contract"
        val POLICY_ARTIFACT_SHA = "a".repeat(64)
        val WORKFLOW_ARTIFACT_SHA = "b".repeat(64)
        val TOOL_SCHEMA_SHA = "c".repeat(64)
        val GRANT_ID = policyGrantId(
            sourceStreamId = STREAM_ID,
            scope = LearningScope.AuthoritySubject(AUTHORITY_SUBJECT_ID),
            consumingAssistantId = Uuid.parse(CONSUMING_ASSISTANT_ID),
            policyId = POLICY_ID,
        )
        val GRANT_DIGEST = WorkflowArtifactCanonicalizer.grantDigest(
            grantId = GRANT_ID,
            sourceStreamId = STREAM_ID,
            stateVersion = 1L,
            policyRevision = 7L,
            artifactSha256 = POLICY_ARTIFACT_SHA,
        )
        const val GRANT_COLUMN_COUNT = 16
        const val GRANT_COLUMNS =
            "grant_id, source_stream_id, policy_id, policy_revision, artifact_sha256, " +
                "scope_kind, scope_id, consuming_assistant_id, actor, state, state_version, " +
                "granted_at_ms, revoked_at_ms, reason_code, created_at_ms, updated_at_ms"
    }
}

private fun placeholders(count: Int): String = List(count) { "?" }.joinToString(", ")
