package me.rerere.rikkahub.data.db.migrations

import androidx.room.testing.MigrationTestHelper
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.createAppSQLiteOpenHelperFactory
import org.junit.Assert.*
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/** Synthetic fixtures on disposable managed emulator only. */
@RunWith(AndroidJUnit4::class)
class Migration_49_50_Test {
    @get:Rule
    val helper = MigrationTestHelper(
        InstrumentationRegistry.getInstrumentation(), AppDatabase::class.java, emptyList(),
        createAppSQLiteOpenHelperFactory(InstrumentationRegistry.getInstrumentation().targetContext),
    )

    @Test
    fun preservesClaimsAndRecoversOnlyPendingLegacySynthesis() {
        val name = "migration-49-50-restore"
        helper.createDatabase(name, 49).use { db ->
            db.execSQL("INSERT INTO memory_scope_state(scope_id, updated_at_ms) VALUES('scope', 1)")
            db.execSQL("INSERT INTO dream_claims(claim_id, scope_id, claim_revision, claim_key, storage_class, epistemic_type, title, statement, state, confidence, temporal_state, learned_at_ms, source_timezone, claim_hash, created_by_run_id, last_validated_memory_epoch, created_at_ms, updated_at_ms) VALUES('claim', 'scope', 1, 'key', 'SEMANTIC', 'INFERRED', 'title', 'preserved claim', 'ACTIVE', 0.8, 'CURRENT', 1, 'UTC', 'hash', 'orphan', 1, 1, 1)")
            listOf(
                Triple("orphan", "INCREMENTAL", "PENDING"),
                Triple("full", "FULL", "PENDING"),
                Triple("done", "FULL", "COMPLETE"),
                Triple("observer", "OBSERVER", "PENDING"),
            ).forEach { (id, mode, status) ->
                db.execSQL("INSERT INTO dream_runs(run_id, scope_id, mode, status, base_memory_epoch, base_observer_checkpoint_epoch, created_at_ms, updated_at_ms) VALUES(?, 'scope', ?, ?, 1, 1, 1, 1)", arrayOf(id, mode, status))
            }
        }
        helper.runMigrationsAndValidate(name, 50, true, MIGRATION_49_50).use { db ->
            db.query("SELECT statement, subject_kind, profile_section, epistemic_origin, content_type FROM dream_claims WHERE claim_id = 'claim'").use {
                assertTrue(it.moveToFirst())
                assertEquals(listOf("preserved claim", "USER", "general", "INFERRED", "OTHER"),
                    (0..4).map { index -> it.getString(index) })
            }
            db.query("SELECT run_id, status, failure_code FROM dream_runs ORDER BY run_id").use {
                val rows = buildMap { while (it.moveToNext()) put(it.getString(0), it.getString(1) to it.getString(2)) }
                assertEquals("CANCELLED" to "LEGACY_ORPHAN_RECOVERY", rows["orphan"])
                assertEquals("CANCELLED" to "LEGACY_ORPHAN_RECOVERY", rows["full"])
                assertEquals("COMPLETE" to null, rows["done"])
                assertEquals("PENDING" to null, rows["observer"])
            }
        }
    }
}
