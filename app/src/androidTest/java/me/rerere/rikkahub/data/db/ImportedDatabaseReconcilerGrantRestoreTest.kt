package me.rerere.rikkahub.data.db

import android.database.sqlite.SQLiteDatabase
import androidx.room.testing.MigrationTestHelper
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import me.rerere.rikkahub.learning.grant.policyGrantId
import me.rerere.rikkahub.learning.model.LearningScope
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertThrows
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import kotlin.uuid.Uuid

/** Emulator/disposable-device only. Never run this instrumentation test on the primary phone. */
@RunWith(AndroidJUnit4::class)
class ImportedDatabaseReconcilerGrantRestoreTest {
    @get:Rule
    val helper = MigrationTestHelper(
        InstrumentationRegistry.getInstrumentation(),
        AppDatabase::class.java,
        emptyList(),
        createAppSQLiteOpenHelperFactory(InstrumentationRegistry.getInstrumentation().targetContext),
    )

    @Test
    fun exactV46AndV47StagedSnapshotsReachV50ThroughTheFrozenChain() {
        listOf(46, 47, 48, 49).forEachIndexed { index, version ->
            val name = stagedName(index + 1)
            helper.createDatabase(name, version).use { database ->
                database.execSQL(
                    "INSERT INTO learning_outbox(stream_id, event_id, event_type, " +
                        "event_schema_version, created_at_ms) VALUES(?, ?, 'STREAM_INIT', 1, 1)",
                    arrayOf(STREAM, STREAM_INIT_EVENT_ID),
                )
            }
            val file = context().getDatabasePath(name).canonicalFile

            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(file, STREAM, 1L)

            SQLiteDatabase.openDatabase(
                file.absolutePath,
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
                assertTrue(tableExists(database, "learning_policy_grants"))
                assertTrue(tableExists(database, "learning_policy_grant_revisions"))
            }
        }
    }

    @Test
    fun exactCurrentSnapshotRetainsARebindableContentDigestReceipt() {
        val name = stagedName(3)
        helper.createDatabase(name, 48).use { database ->
            insertStream(database)
            insertGrantedHeadAndRevision(database)
        }
        val file = context().getDatabasePath(name).canonicalFile

        ImportedDatabaseReconciler.reconcileStagedFileOrThrow(file, STREAM, 1L)

        SQLiteDatabase.openDatabase(
            file.absolutePath,
            null,
            SQLiteDatabase.OPEN_READONLY,
        ).use { database ->
            database.rawQuery(
                "SELECT policy_revision, artifact_sha256, scope_kind, scope_id, " +
                    "consuming_assistant_id, state, state_version " +
                    "FROM learning_policy_grants WHERE grant_id = ?",
                arrayOf(GRANT_ID),
            ).use { cursor ->
                assertTrue(cursor.moveToFirst())
                assertEquals(7L, cursor.getLong(0))
                assertEquals(ARTIFACT_SHA, cursor.getString(1))
                assertEquals("AUTHORITY_SUBJECT", cursor.getString(2))
                assertEquals(SUBJECT, cursor.getString(3))
                assertEquals(CONSUMER, cursor.getString(4))
                assertEquals("GRANTED", cursor.getString(5))
                assertEquals(1L, cursor.getLong(6))
                assertFalse(cursor.moveToNext())
            }
        }
    }

    @Test
    fun currentGrantWithoutACompleteExactAuditJournalIsRejected() {
        val missingCurrentName = stagedName(4)
        helper.createDatabase(missingCurrentName, 48).use { database ->
            insertStream(database)
            insertGrantedHead(database, stateVersion = 1L)
        }
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                context().getDatabasePath(missingCurrentName).canonicalFile,
                STREAM,
                1L,
            )
        }

        val missingFirstName = stagedName(5)
        helper.createDatabase(missingFirstName, 48).use { database ->
            insertStream(database)
            insertRevokedHead(database)
            insertRevokedCurrentRevisionOnly(database)
        }
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                context().getDatabasePath(missingFirstName).canonicalFile,
                STREAM,
                1L,
            )
        }
    }

    @Test
    fun completeGrantThenRevokeJournalSurvivesRestoreAsInertAuthority() {
        val name = stagedName(12)
        helper.createDatabase(name, 48).use { database ->
            insertStream(database)
            insertRevokedHead(database)
            insertGrantedRevision(database)
            insertRevokedCurrentRevisionOnly(database)
        }

        ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
            context().getDatabasePath(name).canonicalFile,
            STREAM,
            1L,
        )
    }

    @Test
    fun currentGrantRejectsNonCanonicalNilConsumerAndCurrentRevisionFieldDrift() {
        listOf(
            "A0000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000000",
        ).forEachIndexed { index, malformedConsumer ->
            val name = stagedName(6 + index)
            helper.createDatabase(name, 48).use { database ->
                insertStream(database)
                insertGrantedHeadAndRevision(database, consumer = malformedConsumer)
            }
            assertThrows(IllegalStateException::class.java) {
                ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                    context().getDatabasePath(name).canonicalFile,
                    STREAM,
                    1L,
                )
            }
        }

        val driftName = stagedName(8)
        helper.createDatabase(driftName, 48).use { database ->
            insertStream(database)
            insertGrantedHeadAndRevision(database)
            database.execSQL(
                "UPDATE learning_policy_grant_revisions SET artifact_sha256 = ? " +
                    "WHERE grant_id = ? AND state_version = 1",
                arrayOf("b".repeat(64), GRANT_ID),
            )
        }
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                context().getDatabasePath(driftName).canonicalFile,
                STREAM,
                1L,
            )
        }
    }

    @Test
    fun unknownV46ToV49IdentitiesAreRefusedBeforeAnyRawMigration() {
        listOf(46, 47, 48, 49).forEachIndexed { index, version ->
            val name = stagedName(20 + index)
            helper.createDatabase(name, version).use { database ->
                database.execSQL(
                    "UPDATE room_master_table SET identity_hash = 'unknown' WHERE id = 42",
                )
            }
            val file = context().getDatabasePath(name).canonicalFile

            assertThrows(IllegalStateException::class.java) {
                ImportedDatabaseReconciler.reconcileStagedFileOrThrow(file, STREAM, 1L)
            }

            SQLiteDatabase.openDatabase(
                file.absolutePath,
                null,
                SQLiteDatabase.OPEN_READONLY,
            ).use { database -> assertEquals(version, database.version) }
        }
    }

    private fun insertStream(database: androidx.sqlite.db.SupportSQLiteDatabase) {
        database.execSQL(
            "INSERT INTO learning_outbox(stream_id, event_id, event_type, " +
                "event_schema_version, created_at_ms) VALUES(?, ?, 'STREAM_INIT', 1, 1)",
            arrayOf(STREAM, STREAM_INIT_EVENT_ID),
        )
    }

    private fun insertGrantedHeadAndRevision(
        database: androidx.sqlite.db.SupportSQLiteDatabase,
        consumer: String = CONSUMER,
    ) {
        insertGrantedHead(database, stateVersion = 1L, consumer = consumer)
        insertGrantedRevision(database, consumer)
    }

    private fun insertGrantedRevision(
        database: androidx.sqlite.db.SupportSQLiteDatabase,
        consumer: String = CONSUMER,
    ) {
        database.execSQL(
            "INSERT INTO learning_policy_grant_revisions(" +
                GRANT_COLUMNS + ", previous_state_version, changed_at_ms) VALUES(" +
                placeholders(GRANT_COLUMN_COUNT) + ", NULL, ?)",
            arrayOf<Any?>(*grantedValues(stateVersion = 1L, consumer = consumer), 10L),
        )
    }

    private fun insertGrantedHead(
        database: androidx.sqlite.db.SupportSQLiteDatabase,
        stateVersion: Long,
        consumer: String = CONSUMER,
    ) {
        database.execSQL(
            "INSERT INTO learning_policy_grants($GRANT_COLUMNS) VALUES(" +
                placeholders(GRANT_COLUMN_COUNT) + ")",
            grantedValues(stateVersion = stateVersion, consumer = consumer),
        )
    }

    private fun insertRevokedHead(database: androidx.sqlite.db.SupportSQLiteDatabase) {
        database.execSQL(
            "INSERT INTO learning_policy_grants($GRANT_COLUMNS) VALUES(" +
                placeholders(GRANT_COLUMN_COUNT) + ")",
            revokedValues(),
        )
    }

    private fun insertRevokedCurrentRevisionOnly(
        database: androidx.sqlite.db.SupportSQLiteDatabase,
    ) {
        database.execSQL(
            "INSERT INTO learning_policy_grant_revisions(" +
                GRANT_COLUMNS + ", previous_state_version, changed_at_ms) VALUES(" +
                placeholders(GRANT_COLUMN_COUNT) + ", 1, ?)",
            arrayOf<Any?>(*revokedValues(), 20L),
        )
    }

    private fun grantedValues(stateVersion: Long, consumer: String): Array<Any?> = arrayOf(
        GRANT_ID,
        STREAM,
        POLICY_ID,
        7L,
        ARTIFACT_SHA,
        "AUTHORITY_SUBJECT",
        SUBJECT,
        consumer,
        "USER_REVIEW",
        "GRANTED",
        stateVersion,
        10L,
        null,
        "USER_APPROVED_CONTEXTUAL_ADVICE",
        10L,
        10L,
    )

    private fun revokedValues(): Array<Any?> = arrayOf(
        GRANT_ID,
        STREAM,
        POLICY_ID,
        7L,
        ARTIFACT_SHA,
        "AUTHORITY_SUBJECT",
        SUBJECT,
        CONSUMER,
        "USER_REVIEW",
        "REVOKED",
        2L,
        10L,
        20L,
        "USER_REVOKED_CONTEXTUAL_ADVICE",
        10L,
        20L,
    )

    private fun tableExists(database: SQLiteDatabase, table: String): Boolean =
        database.rawQuery(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            arrayOf(table),
        ).use { cursor -> cursor.moveToFirst() }

    private fun context() = InstrumentationRegistry.getInstrumentation().targetContext

    private fun stagedName(suffix: Int): String =
        ".rikka_hub.restore_${suffix.toString(16).padStart(32, '0')}.ready"
}

private const val STREAM = "80000000-0000-0000-0000-000000000008"
private const val STREAM_INIT_EVENT_ID = "learning-stream-init:v1"
private const val POLICY_ID = "policy-restore-contract"
private const val SUBJECT = "authority-subject-restore-contract"
private const val CONSUMER = "a0000000-0000-0000-0000-000000000001"
private val ARTIFACT_SHA = "a".repeat(64)
private val GRANT_ID = policyGrantId(
    sourceStreamId = STREAM,
    scope = LearningScope.AuthoritySubject(SUBJECT),
    consumingAssistantId = Uuid.parse(CONSUMER),
    policyId = POLICY_ID,
)
private const val GRANT_COLUMN_COUNT = 16
private const val GRANT_COLUMNS =
    "grant_id, source_stream_id, policy_id, policy_revision, artifact_sha256, " +
        "scope_kind, scope_id, consuming_assistant_id, actor, state, state_version, " +
        "granted_at_ms, revoked_at_ms, reason_code, created_at_ms, updated_at_ms"

private fun placeholders(count: Int): String = List(count) { "?" }.joinToString(", ")
