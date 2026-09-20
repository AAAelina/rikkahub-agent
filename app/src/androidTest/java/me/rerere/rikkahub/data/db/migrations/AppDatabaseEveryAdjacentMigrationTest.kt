package me.rerere.rikkahub.data.db.migrations

import androidx.room.migration.Migration
import androidx.room.testing.MigrationTestHelper
import androidx.sqlite.db.SupportSQLiteDatabase
import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.createAppSQLiteOpenHelperFactory
import org.junit.Assert.assertEquals
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith

/**
 * Disposable-emulator-only release gate. Every exported AppDatabase version must prove its own
 * adjacent edge; a later full-chain success is not evidence that an individual migration exists.
 * Never run this instrumentation test on the Honor AAK-AN00 primary phone.
 */
@RunWith(AndroidJUnit4::class)
class AppDatabaseEveryAdjacentMigrationTest {
    @get:Rule
    val helper = MigrationTestHelper(
        InstrumentationRegistry.getInstrumentation(),
        AppDatabase::class.java,
        emptyList(),
        createAppSQLiteOpenHelperFactory(
            InstrumentationRegistry.getInstrumentation().targetContext,
        ),
    )

    @Test
    fun everyExportedAdjacentMigrationValidatesExactly() {
        (1 until CURRENT_APP_DATABASE_VERSION).forEach { from ->
            val target = from + 1
            val name = "app-every-adjacent-$from-$target"
            helper.createDatabase(name, from).apply {
                // v46+ AppDatabase instances always have a canonical stream sentinel. Empty raw
                // schema fixtures are not valid inputs to migrations whose first action is to
                // verify that authority invariant.
                if (from >= 46) {
                    insertLearningOutboxStreamSentinel(
                        db = this,
                        streamId = ADJACENT_FIXTURE_STREAM_ID,
                        createdAtMs = 0L,
                    )
                }
                close()
            }
            try {
                // memory_fts is an intentionally unmanaged, on-open-reconciled projection.
                // Room's exact managed-schema validator must ignore it only on the three edges
                // that create/rebuild that projection. The managed schema remains exact, and the
                // intentionally unmanaged projection is verified explicitly below.
                val validateDroppedTables = target !in 30..32
                val migrated = helper.runMigrationsAndValidate(
                    name,
                    target,
                    validateDroppedTables,
                    *manualMigrationOrEmpty(from, target),
                )
                if (!validateDroppedTables) verifyUnmanagedMemoryFts(migrated, target)
                migrated.close()
            } catch (failure: Throwable) {
                throw AssertionError(
                    "AppDatabase migration $from->$target failed exact validation",
                    failure,
                )
            }
        }
    }

    private fun manualMigrationOrEmpty(from: Int, target: Int): Array<Migration> =
        when (from to target) {
        6 to 7 -> arrayOf(Migration_6_7)
        11 to 12 -> arrayOf(Migration_11_12)
        13 to 14 -> arrayOf(Migration_13_14)
        14 to 15 -> arrayOf(Migration_14_15)
        15 to 16 -> arrayOf(Migration_15_16)
        23 to 24 -> arrayOf(Migration_23_24)
        26 to 27 -> arrayOf(MIGRATION_26_27)
        27 to 28 -> arrayOf(MIGRATION_27_28)
        28 to 29 -> arrayOf(MIGRATION_28_29)
        29 to 30 -> arrayOf(MIGRATION_29_30)
        30 to 31 -> arrayOf(MIGRATION_30_31)
        31 to 32 -> arrayOf(MIGRATION_31_32)
        32 to 33 -> arrayOf(MIGRATION_32_33)
        33 to 34 -> arrayOf(MIGRATION_33_34)
        34 to 35 -> arrayOf(MIGRATION_34_35)
        35 to 36 -> arrayOf(MIGRATION_35_36)
        36 to 37 -> arrayOf(MIGRATION_36_37)
        37 to 38 -> arrayOf(MIGRATION_37_38)
        38 to 39 -> arrayOf(MIGRATION_38_39)
        39 to 40 -> arrayOf(MIGRATION_39_40)
        40 to 41 -> arrayOf(MIGRATION_40_41)
        41 to 42 -> arrayOf(MIGRATION_41_42)
        42 to 43 -> arrayOf(MIGRATION_42_43)
        43 to 44 -> arrayOf(MIGRATION_43_44)
        44 to 45 -> arrayOf(MIGRATION_44_45)
        45 to 46 -> arrayOf(MIGRATION_45_46)
        46 to 47 -> arrayOf(MIGRATION_46_47)
        47 to 48 -> arrayOf(MIGRATION_47_48)
        48 to 49 -> arrayOf(MIGRATION_48_49)
        49 to 50 -> arrayOf(MIGRATION_49_50)
            else -> emptyArray()
        }

    private fun verifyUnmanagedMemoryFts(database: SupportSQLiteDatabase, target: Int) {
        val expectedColumns = when (target) {
            30 -> setOf(
                "title",
                "content",
                "memory_id",
                "assistant_id",
                "updated_at_ms",
                "importance",
            )
            31 -> setOf(
                "title",
                "content",
                "tags_search",
                "memory_id",
                "assistant_id",
                "updated_at_ms",
                "importance",
                "lifecycle_status",
                "expires_at_ms",
            )
            32 -> setOf(
                "title",
                "content",
                "outcome",
                "tags_search",
                "memory_id",
                "assistant_id",
                "updated_at_ms",
                "importance",
                "lifecycle_status",
                "expires_at_ms",
            )
            else -> error("Unexpected unmanaged FTS target $target")
        }
        val actualColumns = database.query("PRAGMA table_info(`memory_fts`)").use { cursor ->
            val nameIndex = cursor.getColumnIndexOrThrow("name")
            buildSet {
                while (cursor.moveToNext()) add(cursor.getString(nameIndex))
            }
        }
        assertEquals("memory_fts columns at v$target", expectedColumns, actualColumns)

        val triggers = database.query(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'memory_fts_%'",
        ).use { cursor ->
            buildSet {
                while (cursor.moveToNext()) add(cursor.getString(0))
            }
        }
        assertEquals(
            "memory_fts triggers at v$target",
            setOf("memory_fts_ai", "memory_fts_au", "memory_fts_ad"),
            triggers,
        )
    }

    private companion object {
        const val CURRENT_APP_DATABASE_VERSION = 50
        const val ADJACENT_FIXTURE_STREAM_ID = "10000000-0000-4000-8000-000000000001"
    }
}
