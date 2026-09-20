package me.rerere.rikkahub.data.db

import android.content.Context
import android.database.sqlite.SQLiteDatabase
import android.util.Log
import java.io.File
import java.nio.file.Files
import java.nio.file.LinkOption
import java.util.UUID
import me.rerere.rikkahub.data.db.fts.MEMORY_FTS_BACKFILL_SQL
import me.rerere.rikkahub.data.db.fts.MEMORY_FTS_PORTABLE_CREATE_SQL
import me.rerere.rikkahub.data.db.fts.MEMORY_FTS_TRIGGER_SQL
import me.rerere.rikkahub.data.db.migrations.MEMORY_V44_EVIDENCE_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.MEMORY_V44_OLD_CAPTURE_UNIQUE_INDEX
import me.rerere.rikkahub.data.db.migrations.MEMORY_V44_SCOPED_CAPTURE_UNIQUE_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.MEMORY_V44_SOURCE_TOMBSTONE_BACKFILL_SQL
import me.rerere.rikkahub.data.db.migrations.MEMORY_V44_SOURCE_TOMBSTONES_TABLE_SQL
import me.rerere.rikkahub.data.db.migrations.MEMORY_V45_OBSERVER_SCHEMA_SQL
import me.rerere.rikkahub.data.db.migrations.MEMORY_V46_DREAM_RUN_COLUMNS
import me.rerere.rikkahub.data.db.migrations.MEMORY_V46_SCOPE_STATE_COLUMNS
import me.rerere.rikkahub.data.db.migrations.MEMORY_V46_SYNTHESIS_TABLE_AND_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_COMMAND_AUTHORITY_COLUMNS
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_EXECUTION_SCOPE_COLUMNS
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_OUTBOX_P1_COLUMNS
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_OUTBOX_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_OUTBOX_TABLE_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_P1_AUTHORITY_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_SENTINEL_PAYLOAD_COLUMNS
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_SOURCE_AUTHORITY_TABLE_AND_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_STREAM_INIT_EVENT_ID
import me.rerere.rikkahub.data.db.migrations.LEARNING_V47_SENTINEL_PAYLOAD_COLUMNS
import me.rerere.rikkahub.data.db.migrations.LEARNING_V47_OUTBOX_COLUMNS
import me.rerere.rikkahub.data.db.migrations.LEARNING_V47_REWARD_FEEDBACK_AUTHORITY_TABLE_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V47_REWARD_FEEDBACK_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V47_REWARD_FEEDBACK_REVISIONS_TABLE_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V48_POLICY_GRANTS_TABLE_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V48_POLICY_GRANT_INDEX_SQL
import me.rerere.rikkahub.data.db.migrations.LEARNING_V48_POLICY_GRANT_REVISIONS_TABLE_SQL
import me.rerere.rikkahub.data.db.migrations.MIGRATION_46_47
import me.rerere.rikkahub.data.db.migrations.MIGRATION_47_48
import me.rerere.rikkahub.data.db.migrations.MIGRATION_49_50
import me.rerere.rikkahub.data.db.migrations.applyMigration49To50Sql
import me.rerere.rikkahub.data.db.migrations.MIGRATION_48_49
import me.rerere.rikkahub.data.db.migrations.WORKFLOW_V49_COLUMNS
import me.rerere.rikkahub.data.db.migrations.workflowV49Backfill

/**
 * Reconciles a database file that was just restored from a backup so Room can open it.
 *
 * The fork added several tables (scheduled jobs, workflows, ssh hosts, telegram chats,
 * the agent-run ledger) on top of upstream RikkaHub. A backup exported from *upstream*
 * RikkaHub does not contain those tables, yet upstream and the fork share the same Room
 * schema version number. When such a backup is restored, Room reopens the file at the
 * matching version, runs no migration, and then either fails its integrity check or hits
 * "no such table: scheduled_jobs" at first query — the app crashes on the very first launch
 * after the import (see issue #8).
 *
 * This step runs once, right after the restore writes `rikka_hub.db`, on the raw file before
 * Room touches it:
 *  - It creates any of the fork-only tables that are missing, empty, with the exact current
 *    schema Room expects (copied verbatim from the current exported schema) — so the file looks
 *    like a clean agent install for those tables.
 *  - v46+ identities are exact-only. Frozen v46-v49 inputs migrate through the explicit
 *    raw chain to v50; unknown identities are never repaired by relabeling them.
 *  - If the file is at an older version (upgrade scenario, e.g. official v24 to agent v30),
 *    it keeps the original user_version so Room runs every real migration, including the
 *    explicit 28→29 migration. The fork-only tables are pre-created only as compatibility
 *    scaffolding for upstream backups.
 *
 * Room then runs its normal migrations up to current and sets the identity itself; the
 *    pre-created tables simply let those migrations find the fork-only schema.
 * Backups newer than the app are left untouched (Room will report the downgrade).
 *
 * Best-effort: any failure here is logged and swallowed so a restore never half-breaks. The
 * worst case is the same pre-existing crash on next open, never data loss — there is no
 * destructive-migration fallback configured, so the restored rows always survive on disk.
 */
object ImportedDatabaseReconciler {

    private const val TAG = "DbReconciler"
    private const val DB_NAME = "rikka_hub"

    /** Room schema version and exact identity exported from AppDatabase/50.json. */
    internal const val EXPECTED_VERSION = 50
    internal const val EXPECTED_IDENTITY_HASH = "73b32c82384f8b24fe576d123524249a"
    internal const val FINAL_V49_VERSION = 49
    internal const val FINAL_V49_IDENTITY_HASH = "967f2a908998f5bac733c1ae71bee5bb"
    internal const val FINAL_V48_IDENTITY_HASH = "74be67f9e9e32264c091b1d6c4a32b17"
    internal const val FINAL_V47_IDENTITY_HASH = "3208afdfb6ec01eb325a598464e56940"
    internal const val FINAL_V46_IDENTITY_HASH = "670bbac26f583e5c08349fe9a950570b"
    internal const val PRE_P1_V46_IDENTITY_HASH = "102b6a6fc51154abdac792d133d461a3"
    internal const val PRE_LEARNING_V46_IDENTITY_HASH = "8ef3ddc71d855013202bb11b0493d6e6"
    internal const val WORKFLOW_DEFINITION_TOMBSTONE =
        "learning_scope_erased_definition_v1"
    internal const val WORKFLOW_CLAIM_TOMBSTONE = "learning_scope_erased_claim_v1"
    internal const val WORKFLOW_REDACTED_NAME = "Erased learned workflow"
    internal val STAGED_COLD_RESTORE_MIGRATIONS =
        listOf(MIGRATION_46_47, MIGRATION_47_48, MIGRATION_48_49, MIGRATION_49_50)

    /** Canonical database identities are lowercase UUIDs and never the nil sentinel. */
    internal fun isCanonicalNonNilDatabaseUuid(value: String): Boolean =
        value != NIL_UUID && runCatching { UUID.fromString(value).toString() == value }
            .getOrDefault(false)

    internal enum class ReconcilePlan {
        SKIP,
        FULL_COMPATIBILITY,
        REFUSE_UNKNOWN_CURRENT,
    }

    /** v46+ inputs are exact-only; older schemas still receive their normal Room migration path. */
    internal fun reconcilePlan(version: Int, identityHash: String?): ReconcilePlan = when {
        version > EXPECTED_VERSION -> ReconcilePlan.SKIP
        version == EXPECTED_VERSION && identityHash == EXPECTED_IDENTITY_HASH ->
            ReconcilePlan.SKIP
        version == EXPECTED_VERSION -> ReconcilePlan.REFUSE_UNKNOWN_CURRENT
        version == FINAL_V49_VERSION && identityHash == FINAL_V49_IDENTITY_HASH ->
            ReconcilePlan.FULL_COMPATIBILITY
        version == FINAL_V49_VERSION -> ReconcilePlan.REFUSE_UNKNOWN_CURRENT
        version == 48 && identityHash == FINAL_V48_IDENTITY_HASH ->
            ReconcilePlan.FULL_COMPATIBILITY
        version == 48 -> ReconcilePlan.REFUSE_UNKNOWN_CURRENT
        version == 47 && identityHash == FINAL_V47_IDENTITY_HASH ->
            ReconcilePlan.FULL_COMPATIBILITY
        version == 47 -> ReconcilePlan.REFUSE_UNKNOWN_CURRENT
        version == 46 && identityHash in setOf(
            FINAL_V46_IDENTITY_HASH,
            PRE_P1_V46_IDENTITY_HASH,
            PRE_LEARNING_V46_IDENTITY_HASH,
        ) -> ReconcilePlan.FULL_COMPATIBILITY
        version == 46 -> ReconcilePlan.REFUSE_UNKNOWN_CURRENT
        else -> ReconcilePlan.FULL_COMPATIBILITY
    }

    internal enum class StagedReconcilePlan {
        ALREADY_CURRENT,
        MIGRATE_FINAL_V49,
        MIGRATE_FINAL_V48,
        MIGRATE_FINAL_V47,
        MIGRATE_FINAL_V46,
        MIGRATE_PRE_P1_V46,
        MIGRATE_PRE_LEARNING_V46,
    }

    internal enum class LegacyV46AuthorityPlan {
        READ_EXISTING_STREAM,
        CREATE_STREAM,
    }

    /** Pure fail-closed gate shared by the staged-file API and its local JVM contract tests. */
    internal fun stagedReconcilePlanOrThrow(
        version: Int,
        identityHash: String?,
    ): StagedReconcilePlan = when {
        version == EXPECTED_VERSION && identityHash == EXPECTED_IDENTITY_HASH ->
            StagedReconcilePlan.ALREADY_CURRENT
        version == FINAL_V49_VERSION && identityHash == FINAL_V49_IDENTITY_HASH ->
            StagedReconcilePlan.MIGRATE_FINAL_V49
        version == 48 && identityHash == FINAL_V48_IDENTITY_HASH ->
            StagedReconcilePlan.MIGRATE_FINAL_V48
        version == 47 && identityHash == FINAL_V47_IDENTITY_HASH ->
            StagedReconcilePlan.MIGRATE_FINAL_V47
        version == 46 && identityHash == FINAL_V46_IDENTITY_HASH ->
            StagedReconcilePlan.MIGRATE_FINAL_V46
        version == 46 && identityHash == PRE_P1_V46_IDENTITY_HASH ->
            StagedReconcilePlan.MIGRATE_PRE_P1_V46
        version == 46 && identityHash == PRE_LEARNING_V46_IDENTITY_HASH ->
            StagedReconcilePlan.MIGRATE_PRE_LEARNING_V46
        else -> error("Staged database identity is outside the exact cold-restore allowlist")
    }

    internal fun legacyV46AuthorityPlanOrThrow(
        version: Int,
        identityHash: String?,
    ): LegacyV46AuthorityPlan {
        check(version == 46) { "Legacy authority archive is not v46" }
        return when (identityHash) {
            FINAL_V46_IDENTITY_HASH,
            PRE_P1_V46_IDENTITY_HASH,
            -> LegacyV46AuthorityPlan.READ_EXISTING_STREAM
            PRE_LEARNING_V46_IDENTITY_HASH -> LegacyV46AuthorityPlan.CREATE_STREAM
            else -> error("Legacy v46 identity is outside the exact allowlist")
        }
    }

    /**
     * Fork-only tables absent from an upstream backup, with their exact current create + index
     * statements. Every statement is IF NOT EXISTS so running it against a genuine agent
     * backup (where the tables already exist) is a no-op.
     */
    private val FORK_ONLY_DDL: List<String> = listOf(
        "CREATE TABLE IF NOT EXISTS `scheduled_jobs` (`id` TEXT NOT NULL, `name` TEXT NOT NULL, `prompt` TEXT, `assistantId` TEXT NOT NULL, `scheduleType` TEXT NOT NULL, `atUnixMs` INTEGER, `intervalSeconds` INTEGER, `enabled` INTEGER NOT NULL, `createdAtMs` INTEGER NOT NULL, `lastRunAtMs` INTEGER, `nextRunAtMs` INTEGER, `mode` TEXT NOT NULL DEFAULT 'llm', `actionsJson` TEXT, `cronExpression` TEXT, `timezone` TEXT, `startAtUnixMs` INTEGER, `endAtUnixMs` INTEGER, `maxRuns` INTEGER, `runsSoFar` INTEGER NOT NULL DEFAULT 0, `catchup` TEXT NOT NULL DEFAULT 'fire_once', `description` TEXT, `tags` TEXT, `targetConversationId` TEXT, PRIMARY KEY(`id`))",
        "CREATE TABLE IF NOT EXISTS `scheduled_job_runs` (`id` TEXT NOT NULL, `jobId` TEXT NOT NULL, `mode` TEXT NOT NULL, `scheduledAtMs` INTEGER NOT NULL, `startedAtMs` INTEGER NOT NULL, `finishedAtMs` INTEGER, `outcome` TEXT NOT NULL, `conversationId` TEXT, `errorMessage` TEXT, PRIMARY KEY(`id`))",
        "CREATE TABLE IF NOT EXISTS `ssh_hosts` (`name` TEXT NOT NULL, `host` TEXT NOT NULL, `port` INTEGER NOT NULL, `user` TEXT NOT NULL, `password` TEXT, `privateKey` TEXT, `passphrase` TEXT, `createdAtMs` INTEGER NOT NULL, PRIMARY KEY(`name`))",
        "CREATE TABLE IF NOT EXISTS `telegram_chats` (`chatId` INTEGER NOT NULL, `conversationId` TEXT NOT NULL, `createdAtMs` INTEGER NOT NULL, `lastMessageAtMs` INTEGER NOT NULL, PRIMARY KEY(`chatId`))",
        "CREATE TABLE IF NOT EXISTS `workflows` (`id` TEXT NOT NULL, `name` TEXT NOT NULL, `description` TEXT, `enabled` INTEGER NOT NULL DEFAULT 1, `definitionJson` TEXT NOT NULL, `createdAtMs` INTEGER NOT NULL, `updatedAtMs` INTEGER NOT NULL, `lastRunAtMs` INTEGER, `lastRunStatus` TEXT, `lastRunError` TEXT, `runsTodayCount` INTEGER NOT NULL DEFAULT 0, `runsTodayDate` TEXT NOT NULL DEFAULT '', PRIMARY KEY(`id`))",
        "CREATE TABLE IF NOT EXISTS `workflow_runs` (`rowId` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `workflowId` TEXT NOT NULL, `firedAtMs` INTEGER NOT NULL, `status` TEXT NOT NULL, `durationMs` INTEGER NOT NULL, `errorMessage` TEXT)",
        "CREATE INDEX IF NOT EXISTS `index_workflow_runs_workflowId_firedAtMs` ON `workflow_runs` (`workflowId`, `firedAtMs`)",
        "CREATE TABLE IF NOT EXISTS `agent_runs` (`id` TEXT NOT NULL, `kind` TEXT NOT NULL, `domain_id` TEXT NOT NULL, `parent_run_id` TEXT, `status` TEXT NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, `started_at_ms` INTEGER, `finished_at_ms` INTEGER, `last_error` TEXT, `metadata_json` TEXT, PRIMARY KEY(`id`))",
        "CREATE INDEX IF NOT EXISTS `idx_runs_status` ON `agent_runs` (`status`)",
        "CREATE INDEX IF NOT EXISTS `idx_runs_kind_dom` ON `agent_runs` (`kind`, `domain_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_runs_parent` ON `agent_runs` (`parent_run_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_runs_updated_at` ON `agent_runs` (`updated_at_ms`)",
        "CREATE TABLE IF NOT EXISTS `execution_records` (`id` TEXT NOT NULL, `trace_id` TEXT NOT NULL, `parent_execution_id` TEXT, `command_id` TEXT, `conversation_id` TEXT, `learning_scope_kind` TEXT, `learning_scope_id` TEXT, `subject_id` TEXT NOT NULL, `subject_type` TEXT NOT NULL, `origin` TEXT NOT NULL, `capability_keys` TEXT NOT NULL, `resource_summary` TEXT NOT NULL, `runtime` TEXT NOT NULL, `idempotency_key` TEXT, `runtime_handle_summary` TEXT, `status` TEXT NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, `started_at_ms` INTEGER, `heartbeat_at_ms` INTEGER, `finished_at_ms` INTEGER, `cancellation_result` TEXT, `terminal_detail` TEXT, PRIMARY KEY(`id`))",
        "CREATE INDEX IF NOT EXISTS `idx_execution_records_status` ON `execution_records` (`status`)",
        "CREATE INDEX IF NOT EXISTS `idx_execution_records_trace` ON `execution_records` (`trace_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_execution_records_parent` ON `execution_records` (`parent_execution_id`)",
        "CREATE INDEX IF NOT EXISTS `idx_execution_records_idempotency` ON `execution_records` (`idempotency_key`)",
        "CREATE INDEX IF NOT EXISTS `idx_execution_records_updated` ON `execution_records` (`updated_at_ms`)",
        "CREATE TABLE IF NOT EXISTS `capability_grants` (`id` TEXT NOT NULL, `subject_id` TEXT NOT NULL, `subject_type` TEXT NOT NULL, `capability_key` TEXT NOT NULL, `resource_kind` TEXT NOT NULL, `resource_identifier` TEXT NOT NULL, `allowed_origins` TEXT NOT NULL, `scope` TEXT NOT NULL, `expires_at_ms` INTEGER, `revoked` INTEGER NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, PRIMARY KEY(`id`))",
        "CREATE INDEX IF NOT EXISTS `idx_capability_grants_subject` ON `capability_grants` (`subject_id`, `subject_type`)",
        "CREATE INDEX IF NOT EXISTS `idx_capability_grants_active` ON `capability_grants` (`revoked`, `expires_at_ms`)",
        "CREATE TABLE IF NOT EXISTS `alarms` (`id` TEXT NOT NULL, `label` TEXT NOT NULL, `note` TEXT, `scheduleType` TEXT NOT NULL, `time` TEXT, `hour` INTEGER, `minute` INTEGER, `daysOfWeek` TEXT, `timezone` TEXT NOT NULL, `enabled` INTEGER NOT NULL, `vibrate` INTEGER NOT NULL, `createdAtMs` INTEGER NOT NULL, `updatedAtMs` INTEGER NOT NULL, `lastFiredAtMs` INTEGER, `nextFireAtMs` INTEGER, PRIMARY KEY(`id`))",
        "DROP INDEX IF EXISTS `index_alarms_enabled_nextFireAtMs`",
        "CREATE TABLE IF NOT EXISTS `pending_chat_commands` (" +
            "`id` TEXT NOT NULL, `schemaVersion` INTEGER NOT NULL, `conversationId` TEXT NOT NULL, `authoritySubjectId` TEXT, " +
            "`assistantIdSnapshot` TEXT, `lineageId` TEXT, `parentCommandId` TEXT, " +
            "`branchAnchorMessageId` TEXT, `stateVersion` INTEGER NOT NULL DEFAULT 0, " +
            "`type` TEXT NOT NULL, `payloadJson` TEXT NOT NULL, `state` TEXT NOT NULL, " +
            "`priority` INTEGER NOT NULL, `sequence` INTEGER NOT NULL, " +
            "`expectedTargetVersion` INTEGER, `expectedBranchHeadMessageId` TEXT, `dedupeKey` TEXT, " +
            "`idempotencyKey` TEXT NOT NULL, `attempt` INTEGER NOT NULL, `claimedBy` TEXT, " +
            "`leaseUntil` INTEGER, `createdAt` INTEGER NOT NULL, `startedAt` INTEGER, " +
            "`finishedAt` INTEGER, `expiresAt` INTEGER, `lastErrorCode` TEXT, `lastErrorMessage` TEXT, " +
            "PRIMARY KEY(`id`))",
        "CREATE INDEX IF NOT EXISTS `index_pending_chat_commands_conversationId` " +
            "ON `pending_chat_commands` (`conversationId`)",
        "CREATE INDEX IF NOT EXISTS `index_pending_chat_commands_conversationId_state_priority_sequence` " +
            "ON `pending_chat_commands` (`conversationId`, `state`, `priority`, `sequence`)",
        "CREATE INDEX IF NOT EXISTS `index_pending_chat_commands_leaseUntil` " +
            "ON `pending_chat_commands` (`leaseUntil`)",
        "CREATE INDEX IF NOT EXISTS `index_pending_chat_commands_dedupeKey` " +
            "ON `pending_chat_commands` (`dedupeKey`)",
        "CREATE INDEX IF NOT EXISTS `index_pending_chat_commands_authoritySubjectId` " +
            "ON `pending_chat_commands` (`authoritySubjectId`)",
        "CREATE UNIQUE INDEX IF NOT EXISTS `index_pending_chat_commands_idempotencyKey` " +
            "ON `pending_chat_commands` (`idempotencyKey`)",
        "CREATE TABLE IF NOT EXISTS `memory_captures` (`id` TEXT NOT NULL, `assistant_id` TEXT NOT NULL, `scope_id` TEXT NOT NULL, `conversation_id` TEXT NOT NULL, `user_message_id` TEXT NOT NULL, `assistant_message_id` TEXT NOT NULL, `origin` TEXT NOT NULL, `capture_source` TEXT NOT NULL DEFAULT 'AUTOMATIC_TURN', `auto_save_mode` TEXT NOT NULL, `user_text` TEXT NOT NULL, `assistant_text` TEXT NOT NULL, `state` TEXT NOT NULL DEFAULT 'PENDING', `retry_count` INTEGER NOT NULL DEFAULT 0, `last_error_code` TEXT, `last_error_message` TEXT, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, `lease_owner` TEXT, `lease_until_ms` INTEGER, `processed_at_ms` INTEGER, PRIMARY KEY(`id`))",
        "CREATE UNIQUE INDEX IF NOT EXISTS `index_memory_captures_conversation_id_assistant_message_id_capture_source` ON `memory_captures` (`conversation_id`, `assistant_message_id`, `capture_source`)",
        "CREATE INDEX IF NOT EXISTS `index_memory_captures_scope_id_state_created_at_ms` ON `memory_captures` (`scope_id`, `state`, `created_at_ms`)",
        "CREATE INDEX IF NOT EXISTS `index_memory_captures_lease_until_ms` ON `memory_captures` (`lease_until_ms`)",
        "CREATE INDEX IF NOT EXISTS `index_memory_captures_conversation_id` ON `memory_captures` (`conversation_id`)",
        "CREATE TABLE IF NOT EXISTS `memory_candidates` (`id` TEXT NOT NULL, `scope_id` TEXT NOT NULL, `assistant_id` TEXT NOT NULL, `source_conversation_id` TEXT NOT NULL, `capture_ids_json` TEXT NOT NULL, `action` TEXT NOT NULL, `target_memory_ids_json` TEXT NOT NULL, `expected_revisions_json` TEXT NOT NULL, `title` TEXT NOT NULL, `content` TEXT NOT NULL, `memory_kind` TEXT NOT NULL, `tags_json` TEXT NOT NULL, `importance` REAL NOT NULL, `confidence` REAL NOT NULL, `expires_at_ms` INTEGER, `risk_flags_json` TEXT NOT NULL, `reason` TEXT NOT NULL, `evidence_message_ids_json` TEXT NOT NULL, `status` TEXT NOT NULL DEFAULT 'PENDING_REVIEW', `applied_memory_id` INTEGER, `resolution_error` TEXT, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, PRIMARY KEY(`id`))",
        "CREATE INDEX IF NOT EXISTS `index_memory_candidates_scope_id_status_created_at_ms` ON `memory_candidates` (`scope_id`, `status`, `created_at_ms`)",
        "CREATE INDEX IF NOT EXISTS `index_memory_candidates_source_conversation_id` ON `memory_candidates` (`source_conversation_id`)",
        "CREATE INDEX IF NOT EXISTS `index_memory_candidates_applied_memory_id` ON `memory_candidates` (`applied_memory_id`)",
        "CREATE TABLE IF NOT EXISTS `memory_revisions` (`id` TEXT NOT NULL, `memory_id` INTEGER NOT NULL, `revision` INTEGER NOT NULL, `operation` TEXT NOT NULL, `before_snapshot_json` TEXT, `after_snapshot_json` TEXT, `actor` TEXT NOT NULL, `candidate_id` TEXT, `source_conversation_id` TEXT, `source_message_ids_json` TEXT NOT NULL DEFAULT '[]', `created_at_ms` INTEGER NOT NULL, PRIMARY KEY(`id`))",
        "CREATE UNIQUE INDEX IF NOT EXISTS `index_memory_revisions_memory_id_revision` ON `memory_revisions` (`memory_id`, `revision`)",
        "CREATE INDEX IF NOT EXISTS `index_memory_revisions_memory_id_created_at_ms` ON `memory_revisions` (`memory_id`, `created_at_ms`)",
        "CREATE INDEX IF NOT EXISTS `index_memory_revisions_candidate_id` ON `memory_revisions` (`candidate_id`)",
    )

    private fun ensureConversationFolderV29Column(db: SQLiteDatabase) {
        val hasFolderId = db.rawQuery("PRAGMA table_info(`ConversationEntity`)", null).use { cursor ->
            val nameIndex = cursor.getColumnIndex("name")
            if (nameIndex < 0) {
                false
            } else {
                var found = false
                while (cursor.moveToNext()) {
                    if (cursor.getString(nameIndex) == "folder_id") {
                        found = true
                        break
                    }
                }
                found
            }
        }
        if (!hasFolderId) {
            db.execSQL(
                "ALTER TABLE `ConversationEntity` ADD COLUMN `folder_id` TEXT NOT NULL DEFAULT ''"
            )
        }
    }

    private fun ensureScheduledJobsV29Column(db: SQLiteDatabase) {
        val hasTargetConversationId = db.rawQuery("PRAGMA table_info(`scheduled_jobs`)", null).use { cursor ->
            val nameIndex = cursor.getColumnIndex("name")
            if (nameIndex < 0) {
                false
            } else {
                var found = false
                while (cursor.moveToNext()) {
                    if (cursor.getString(nameIndex) == "targetConversationId") {
                        found = true
                        break
                    }
                }
                found
            }
        }
        if (!hasTargetConversationId) {
            db.execSQL("ALTER TABLE `scheduled_jobs` ADD COLUMN `targetConversationId` TEXT")
        }
    }

    private fun ensureMemoryV31Columns(db: SQLiteDatabase) {
        val columns = db.rawQuery("PRAGMA table_info(`MemoryEntity`)", null).use { cursor ->
            val nameIndex = cursor.getColumnIndex("name")
            buildSet {
                if (nameIndex >= 0) {
                    while (cursor.moveToNext()) add(cursor.getString(nameIndex))
                }
            }
        }
        if ("title" !in columns) {
            db.execSQL("ALTER TABLE `MemoryEntity` ADD COLUMN `title` TEXT")
        }
        if ("updated_at_ms" !in columns) {
            db.execSQL(
                "ALTER TABLE `MemoryEntity` ADD COLUMN `updated_at_ms` INTEGER NOT NULL DEFAULT 0",
            )
        }
        if ("importance" !in columns) {
            db.execSQL(
                "ALTER TABLE `MemoryEntity` ADD COLUMN `importance` REAL NOT NULL DEFAULT 0.5",
            )
        }
        val additions = listOf(
            "created_at_ms" to "INTEGER NOT NULL DEFAULT 0",
            "last_accessed_at_ms" to "INTEGER",
            "expires_at_ms" to "INTEGER",
            "memory_kind" to "TEXT NOT NULL DEFAULT 'OTHER'",
            "confidence" to "REAL NOT NULL DEFAULT 1.0",
            "tags_json" to "TEXT NOT NULL DEFAULT '[]'",
            "tags_search" to "TEXT NOT NULL DEFAULT ''",
            "content_hash" to "TEXT NOT NULL DEFAULT ''",
            "source_type" to "TEXT NOT NULL DEFAULT 'LEGACY'",
            "source_conversation_id" to "TEXT",
            "source_message_ids_json" to "TEXT NOT NULL DEFAULT '[]'",
            "lifecycle_status" to "TEXT NOT NULL DEFAULT 'ACTIVE'",
            "approval_source" to "TEXT NOT NULL DEFAULT 'LEGACY'",
            "revision" to "INTEGER NOT NULL DEFAULT 1",
        )
        additions.forEach { (name, declaration) ->
            if (name !in columns) {
                db.execSQL("ALTER TABLE `MemoryEntity` ADD COLUMN `$name` $declaration")
            }
        }
        db.execSQL(
            "UPDATE `MemoryEntity` SET `created_at_ms` = " +
                "CASE WHEN `updated_at_ms` > 0 THEN `updated_at_ms` ELSE ? END " +
                "WHERE `created_at_ms` = 0",
            arrayOf(System.currentTimeMillis()),
        )
    }

    /** Supports an early v31 Memory V2 preview database restored before source isolation existed. */
    private fun ensureMemoryV31CaptureColumns(db: SQLiteDatabase) {
        val columns = db.rawQuery("PRAGMA table_info(`memory_captures`)", null).use { cursor ->
            val nameIndex = cursor.getColumnIndex("name")
            buildSet {
                if (nameIndex >= 0) while (cursor.moveToNext()) add(cursor.getString(nameIndex))
            }
        }
        if ("capture_source" !in columns) {
            db.execSQL(
                "ALTER TABLE `memory_captures` ADD COLUMN `capture_source` " +
                    "TEXT NOT NULL DEFAULT 'AUTOMATIC_TURN'",
            )
        }
        db.execSQL(
            "DROP INDEX IF EXISTS `index_memory_captures_conversation_id_assistant_message_id`",
        )
        db.execSQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS " +
                "`index_memory_captures_conversation_id_assistant_message_id_capture_source` " +
            "ON `memory_captures` (`conversation_id`, `assistant_message_id`, `capture_source`)",
        )
    }

    /**
     * Upstream and agent builds can share a Room user_version while only the agent has these
     * tables. For a restored database already at a later schema step, Room will not replay the
     * missing intermediate migration, so provide the same idempotent scaffolding here.
     */
    private fun ensureMemoryV32Schema(db: SQLiteDatabase) {
        ensureColumns(
            db,
            "MemoryEntity",
            listOf(
                "origin_assistant_id" to "TEXT",
                "attribution" to "TEXT NOT NULL DEFAULT 'UNKNOWN'",
                "truth_status" to "TEXT NOT NULL DEFAULT 'CONFIRMED'",
                "occurred_at_ms" to "INTEGER",
                "participants_json" to "TEXT NOT NULL DEFAULT '[]'",
                "outcome" to "TEXT",
            ),
        )
        ensureColumns(
            db,
            "memory_candidates",
            listOf(
                "proposal_key" to "TEXT",
                "attribution" to "TEXT NOT NULL DEFAULT 'UNKNOWN'",
                "truth_status" to "TEXT NOT NULL DEFAULT 'CONFIRMED'",
                "occurred_at_ms" to "INTEGER",
                "participants_json" to "TEXT NOT NULL DEFAULT '[]'",
                "outcome" to "TEXT",
            ),
        )
        ensureColumns(
            db,
            "memory_captures",
            listOf(
                "processing_outcome" to "TEXT",
                "candidate_count" to "INTEGER NOT NULL DEFAULT 0",
                "supersedes_capture_id" to "TEXT",
                "narrative_events_enabled" to "INTEGER NOT NULL DEFAULT 0",
                "insights_theories_enabled" to "INTEGER NOT NULL DEFAULT 0",
            ),
        )
        listOf(
            "CREATE TABLE IF NOT EXISTS `memory_evidence` (`id` TEXT NOT NULL, `memory_id` INTEGER, `candidate_id` TEXT, `conversation_id` TEXT NOT NULL, `message_id` TEXT NOT NULL, `role` TEXT NOT NULL, `excerpt` TEXT NOT NULL, `content_hash` TEXT NOT NULL, `captured_at_ms` INTEGER NOT NULL, `quality` TEXT NOT NULL DEFAULT 'ORIGINAL_MESSAGE', PRIMARY KEY(`id`))",
            "CREATE INDEX IF NOT EXISTS `index_memory_evidence_memory_id` ON `memory_evidence` (`memory_id`)",
            "CREATE INDEX IF NOT EXISTS `index_memory_evidence_candidate_id` ON `memory_evidence` (`candidate_id`)",
            "CREATE INDEX IF NOT EXISTS `index_memory_evidence_message_id` ON `memory_evidence` (`message_id`)",
            "CREATE TABLE IF NOT EXISTS `memory_links` (`id` TEXT NOT NULL, `source_memory_id` INTEGER NOT NULL, `target_memory_id` INTEGER NOT NULL, `relation_type` TEXT NOT NULL, `weight` REAL NOT NULL, `description` TEXT NOT NULL, `evidence_message_ids_json` TEXT NOT NULL, `created_by_assistant_id` TEXT NOT NULL, `created_at_ms` INTEGER NOT NULL, `revision` INTEGER NOT NULL DEFAULT 1, PRIMARY KEY(`id`))",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_memory_links_source_memory_id_target_memory_id_relation_type` ON `memory_links` (`source_memory_id`, `target_memory_id`, `relation_type`)",
            "CREATE INDEX IF NOT EXISTS `index_memory_links_target_memory_id` ON `memory_links` (`target_memory_id`)",
            "CREATE TABLE IF NOT EXISTS `memory_relation_candidates` (`id` TEXT NOT NULL, `batch_id` TEXT NOT NULL, `source_proposal_key` TEXT, `source_memory_id` INTEGER, `target_proposal_key` TEXT, `target_memory_id` INTEGER, `relation_type` TEXT NOT NULL, `weight` REAL NOT NULL, `description` TEXT NOT NULL, `evidence_message_ids_json` TEXT NOT NULL, `status` TEXT NOT NULL DEFAULT 'PENDING', `created_at_ms` INTEGER NOT NULL, PRIMARY KEY(`id`))",
            "CREATE INDEX IF NOT EXISTS `index_memory_relation_candidates_batch_id` ON `memory_relation_candidates` (`batch_id`)",
            "CREATE INDEX IF NOT EXISTS `index_memory_relation_candidates_status` ON `memory_relation_candidates` (`status`)",
            "CREATE TABLE IF NOT EXISTS `memory_backfill_runs` (`id` TEXT NOT NULL, `assistant_id` TEXT NOT NULL, `scope_id` TEXT NOT NULL, `selection_json` TEXT NOT NULL, `total_turns` INTEGER NOT NULL, `processed_turns` INTEGER NOT NULL DEFAULT 0, `failed_turns` INTEGER NOT NULL DEFAULT 0, `status` TEXT NOT NULL DEFAULT 'PENDING', `last_error` TEXT, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, PRIMARY KEY(`id`))",
            "CREATE INDEX IF NOT EXISTS `index_memory_backfill_runs_assistant_id` ON `memory_backfill_runs` (`assistant_id`)",
            "CREATE INDEX IF NOT EXISTS `index_memory_backfill_runs_status` ON `memory_backfill_runs` (`status`)",
        ).forEach(db::execSQL)
        rebuildPortableMemoryFts(db)
    }

    private fun ensureMemoryV33Schema(db: SQLiteDatabase) {
        ensureColumns(
            db,
            "memory_captures",
            listOf("context_turn_limit" to "INTEGER NOT NULL DEFAULT 12"),
        )
    }

    /** Same-version imports need the scope/revision-aware Memory V2 relation schema. */
    private fun ensureMemoryV43Schema(db: SQLiteDatabase) {
        ensureColumns(
            db,
            "memory_captures",
            listOf("payload_purged_at_ms" to "INTEGER"),
        )
        ensureColumns(
            db,
            "memory_candidates",
            listOf("batch_id" to "TEXT"),
        )
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS `index_memory_candidates_batch_id` " +
                "ON `memory_candidates` (`batch_id`)",
        )
        ensureColumns(
            db,
            "memory_evidence",
            listOf(
                "relation_candidate_id" to "TEXT",
                "link_id" to "TEXT",
            ),
        )
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_evidence_relation_candidate_id` ON `memory_evidence` (`relation_candidate_id`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_evidence_link_id` ON `memory_evidence` (`link_id`)")

        ensureColumns(
            db,
            "memory_relation_candidates",
            listOf(
                "scope_id" to "TEXT NOT NULL DEFAULT ''",
                "created_by_assistant_id" to "TEXT NOT NULL DEFAULT ''",
                "source_candidate_id" to "TEXT",
                "target_candidate_id" to "TEXT",
                "source_expected_revision" to "INTEGER",
                "target_expected_revision" to "INTEGER",
                "resolved_link_id" to "TEXT",
                "resolution_error" to "TEXT",
                "updated_at_ms" to "INTEGER NOT NULL DEFAULT 0",
            ),
        )
        db.execSQL(
            "UPDATE `memory_relation_candidates` SET `status` = 'INVALIDATED', " +
                "`resolution_error` = COALESCE(`resolution_error`, 'MIGRATION_UNVERIFIED'), " +
                "`updated_at_ms` = CASE WHEN `updated_at_ms` = 0 THEN `created_at_ms` ELSE `updated_at_ms` END " +
                "WHERE `scope_id` = ''",
        )
        db.execSQL("DROP INDEX IF EXISTS `index_memory_relation_candidates_status`")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_relation_candidates_scope_id_status_created_at_ms` ON `memory_relation_candidates` (`scope_id`, `status`, `created_at_ms`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_relation_candidates_source_candidate_id` ON `memory_relation_candidates` (`source_candidate_id`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_relation_candidates_target_candidate_id` ON `memory_relation_candidates` (`target_candidate_id`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_relation_candidates_resolved_link_id` ON `memory_relation_candidates` (`resolved_link_id`)")

        ensureColumns(
            db,
            "memory_links",
            listOf(
                "scope_id" to "TEXT NOT NULL DEFAULT ''",
                "lifecycle_status" to "TEXT NOT NULL DEFAULT 'ACTIVE'",
                "source_revision" to "INTEGER NOT NULL DEFAULT 1",
                "target_revision" to "INTEGER NOT NULL DEFAULT 1",
                "source_semantic_hash" to "TEXT NOT NULL DEFAULT ''",
                "target_semantic_hash" to "TEXT NOT NULL DEFAULT ''",
                "relation_candidate_id" to "TEXT",
                "updated_at_ms" to "INTEGER NOT NULL DEFAULT 0",
                "invalidated_at_ms" to "INTEGER",
                "invalidation_reason" to "TEXT",
            ),
        )
        db.execSQL(
            "UPDATE `memory_links` SET `scope_id` = COALESCE((" +
                "SELECT s.`assistant_id` FROM `MemoryEntity` s " +
                "INNER JOIN `MemoryEntity` t ON t.`id` = `memory_links`.`target_memory_id` " +
                "WHERE s.`id` = `memory_links`.`source_memory_id` " +
                "AND s.`assistant_id` = t.`assistant_id`), ''), " +
                "`lifecycle_status` = 'INVALIDATED', " +
                "`updated_at_ms` = CASE WHEN `updated_at_ms` = 0 THEN `created_at_ms` ELSE `updated_at_ms` END, " +
                "`invalidated_at_ms` = COALESCE(`invalidated_at_ms`, `created_at_ms`), " +
                "`invalidation_reason` = COALESCE(`invalidation_reason`, 'MIGRATION_UNVERIFIED') " +
                "WHERE `source_semantic_hash` = '' OR `target_semantic_hash` = ''",
        )
        db.execSQL("DROP INDEX IF EXISTS `index_memory_links_source_memory_id_target_memory_id_relation_type`")
        db.execSQL("DROP INDEX IF EXISTS `index_memory_links_target_memory_id`")
        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_memory_links_scope_id_source_memory_id_target_memory_id_relation_type` ON `memory_links` (`scope_id`, `source_memory_id`, `target_memory_id`, `relation_type`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_links_scope_id_source_memory_id_lifecycle_status` ON `memory_links` (`scope_id`, `source_memory_id`, `lifecycle_status`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_links_scope_id_target_memory_id_lifecycle_status` ON `memory_links` (`scope_id`, `target_memory_id`, `lifecycle_status`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_memory_links_relation_candidate_id` ON `memory_links` (`relation_candidate_id`)")

        ensureColumns(
            db,
            "memory_revisions",
            listOf(
                "reason_code" to "TEXT",
                "cause_memory_id" to "INTEGER",
                "cause_link_id" to "TEXT",
            ),
        )
        listOf(
            "CREATE TABLE IF NOT EXISTS `memory_link_revisions` (`id` TEXT NOT NULL, `link_id` TEXT NOT NULL, `revision` INTEGER NOT NULL, `operation` TEXT NOT NULL, `before_snapshot_json` TEXT, `after_snapshot_json` TEXT, `actor` TEXT NOT NULL, `relation_candidate_id` TEXT, `reason_code` TEXT, `created_at_ms` INTEGER NOT NULL, PRIMARY KEY(`id`))",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_memory_link_revisions_link_id_revision` ON `memory_link_revisions` (`link_id`, `revision`)",
            "CREATE INDEX IF NOT EXISTS `index_memory_link_revisions_link_id_created_at_ms` ON `memory_link_revisions` (`link_id`, `created_at_ms`)",
            "CREATE INDEX IF NOT EXISTS `index_memory_link_revisions_relation_candidate_id` ON `memory_link_revisions` (`relation_candidate_id`)",
        ).forEach(db::execSQL)
    }

    /** Same-version v44 imports need scope-safe capture identity and source invalidation state. */
    private fun ensureMemoryV44Schema(db: SQLiteDatabase) {
        ensureColumns(
            db,
            "memory_captures",
            listOf("source_identities_json" to "TEXT NOT NULL DEFAULT '[]'"),
        )
        ensureColumns(
            db,
            "MemoryEntity",
            listOf("source_identities_json" to "TEXT NOT NULL DEFAULT '[]'"),
        )
        ensureColumns(
            db,
            "memory_revisions",
            listOf("source_identities_json" to "TEXT NOT NULL DEFAULT '[]'"),
        )
        db.execSQL("DROP INDEX IF EXISTS `$MEMORY_V44_OLD_CAPTURE_UNIQUE_INDEX`")
        db.execSQL(MEMORY_V44_SCOPED_CAPTURE_UNIQUE_INDEX_SQL)

        ensureColumns(
            db,
            "memory_evidence",
            listOf(
                "evidence_group_id" to "TEXT NOT NULL DEFAULT ''",
                "source_digest" to "TEXT NOT NULL DEFAULT ''",
                "source_kind" to "TEXT NOT NULL DEFAULT 'TEXT'",
            ),
        )
        db.execSQL(
            "UPDATE `memory_evidence` SET `evidence_group_id` = `id` " +
                "WHERE `evidence_group_id` = ''",
        )
        MEMORY_V44_EVIDENCE_INDEX_SQL.forEach(db::execSQL)

        db.execSQL(MEMORY_V44_SOURCE_TOMBSTONES_TABLE_SQL)
        MEMORY_V44_SOURCE_TOMBSTONE_BACKFILL_SQL.forEach(db::execSQL)
    }

    /** Same-version v45 imports need the dormant Observer ledger Room expects. */
    private fun ensureMemoryV45Schema(db: SQLiteDatabase) {
        MEMORY_V45_OBSERVER_SCHEMA_SQL.forEach(db::execSQL)
    }

    /** Same-version v46 imports need the dormant Shadow Synthesis schema Room expects. */
    private fun ensureMemoryV46Schema(db: SQLiteDatabase) {
        ensureColumns(db, "memory_scope_state", MEMORY_V46_SCOPE_STATE_COLUMNS)
        ensureColumns(db, "dream_runs", MEMORY_V46_DREAM_RUN_COLUMNS)
        MEMORY_V46_SYNTHESIS_TABLE_AND_INDEX_SQL.forEach(db::execSQL)
    }

    /**
     * Exact v46 floor required before Room can run 46 -> 47 on an upstream/same-version import.
     * It never stamps v46 or v47 and never repairs an existing malformed/mixed outbox.
     */
    private fun ensureLearningV46SchemaForUpgrade(
        db: SQLiteDatabase,
        newStreamId: String? = null,
    ) {
        ensureColumns(db, "pending_chat_commands", LEARNING_V46_COMMAND_AUTHORITY_COLUMNS)
        ensureColumns(db, "execution_records", LEARNING_V46_EXECUTION_SCOPE_COLUMNS)
        db.execSQL(LEARNING_V46_OUTBOX_TABLE_SQL)
        ensureColumns(db, "learning_outbox", LEARNING_V46_OUTBOX_P1_COLUMNS)
        LEARNING_V46_P1_AUTHORITY_INDEX_SQL.forEach(db::execSQL)
        LEARNING_V46_OUTBOX_INDEX_SQL.forEach(db::execSQL)
        LEARNING_V46_SOURCE_AUTHORITY_TABLE_AND_INDEX_SQL.forEach(db::execSQL)
        val rowCount = db.rawQuery("SELECT COUNT(*) FROM `learning_outbox`", null).use { cursor ->
            check(cursor.moveToFirst())
            cursor.getLong(0)
        }
        if (rowCount == 0L) {
            insertLearningOutboxStreamSentinel(
                db = db,
                streamId = newStreamId ?: UUID.randomUUID().toString(),
                createdAtMs = System.currentTimeMillis(),
            )
        } else {
            check(newStreamId == null) {
                "Pre-Learning staged database unexpectedly contains Learning authority rows"
            }
        }
        requireHealthyLearningOutbox(db, LEARNING_V46_SENTINEL_PAYLOAD_COLUMNS)
        requireP1LearningAuthoritySchema(db)
    }

    private fun tableExists(db: SQLiteDatabase, table: String): Boolean =
        db.rawQuery(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
            arrayOf(table),
        ).use { it.moveToFirst() }

    private fun requireP1LearningAuthoritySchema(db: SQLiteDatabase) {
        val requiredAdditiveColumns = mapOf(
            "pending_chat_commands" to
                LEARNING_V46_COMMAND_AUTHORITY_COLUMNS.mapTo(linkedSetOf()) { it.first },
            "execution_records" to
                LEARNING_V46_EXECUTION_SCOPE_COLUMNS.mapTo(linkedSetOf()) { it.first },
            "learning_outbox" to
                LEARNING_V46_OUTBOX_P1_COLUMNS.mapTo(linkedSetOf()) { it.first },
        )
        requiredAdditiveColumns.forEach { (table, requiredColumns) ->
            check(tableExists(db, table)) { "Missing P1 Learning authority host table" }
            val actualColumns = db.rawQuery("PRAGMA table_info(`$table`)", null).use { cursor ->
                val name = cursor.getColumnIndexOrThrow("name")
                buildSet { while (cursor.moveToNext()) add(cursor.getString(name)) }
            }
            check(actualColumns.containsAll(requiredColumns)) {
                "Missing P1 Learning authority column"
            }
        }
        val expectedTables = mapOf(
            "learning_conversation_source_authority" to setOf(
                "scope_kind",
                "scope_id",
                "conversation_id",
                "assistant_id_snapshot",
                "source_revision",
                "previous_source_revision",
                "source_state",
                "change_kind",
                "branch_head_message_id",
                "branch_head_message_revision",
                "occurred_at_ms",
                "updated_at_ms",
            ),
            "learning_message_source_authority" to setOf(
                "scope_kind",
                "scope_id",
                "conversation_id",
                "message_id",
                "message_role",
                "source_revision",
                "previous_source_revision",
                "source_state",
                "change_kind",
                "payload_integrity_sha256",
                "occurred_at_ms",
                "updated_at_ms",
            ),
        )
        expectedTables.forEach { (table, expectedColumns) ->
            check(tableExists(db, table)) { "Missing P1 Learning authority table" }
            val actualColumns = db.rawQuery("PRAGMA table_info(`$table`)", null).use { cursor ->
                val name = cursor.getColumnIndexOrThrow("name")
                buildSet { while (cursor.moveToNext()) add(cursor.getString(name)) }
            }
            check(actualColumns == expectedColumns) { "Invalid P1 Learning authority columns" }
        }
        val requiredIndexes = LEARNING_V46_P1_AUTHORITY_INDEX_SQL
            .plus(LEARNING_V46_SOURCE_AUTHORITY_TABLE_AND_INDEX_SQL)
            .filter { it.startsWith("CREATE INDEX") }
            .mapNotNull { statement ->
                Regex("`([^`]+)`").find(statement)?.groupValues?.get(1)
            }
        requiredIndexes.forEach { index ->
            val present = db.rawQuery(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1",
                arrayOf(index),
            ).use { cursor -> cursor.moveToFirst() }
            check(present) { "Missing P1 Learning authority index" }
        }
    }

    private fun requireV47RewardAuthoritySchema(db: SQLiteDatabase) {
        val expectedTables = mapOf(
            "learning_reward_feedback_authority" to setOf(
                "feedback_id", "scope_kind", "scope_id", "conversation_id",
                "conversation_source_revision", "command_id", "command_revision", "lineage_id",
                "branch_anchor_message_id", "branch_anchor_message_revision",
                "target_assistant_message_id", "target_assistant_message_revision", "dimension",
                "signal_kind", "value_milli", "source_state", "source_revision",
                "previous_source_revision", "integrity_sha256", "created_at_ms", "updated_at_ms",
            ),
            "learning_reward_feedback_revisions" to setOf(
                "feedback_id", "scope_kind", "scope_id", "conversation_id",
                "conversation_source_revision", "command_id", "command_revision", "lineage_id",
                "branch_anchor_message_id", "branch_anchor_message_revision",
                "target_assistant_message_id", "target_assistant_message_revision", "dimension",
                "signal_kind", "value_milli", "source_state", "source_revision",
                "previous_source_revision", "integrity_sha256", "created_at_ms", "updated_at_ms",
            ),
        )
        expectedTables.forEach { (table, expectedColumns) ->
            check(tableExists(db, table)) { "Missing v47 reward authority table" }
            val actualColumns = db.rawQuery("PRAGMA table_info(`$table`)", null).use { cursor ->
                val name = cursor.getColumnIndexOrThrow("name")
                buildSet { while (cursor.moveToNext()) add(cursor.getString(name)) }
            }
            check(actualColumns == expectedColumns) { "Invalid v47 reward authority columns" }
        }
        val outboxColumns = db.rawQuery("PRAGMA table_info(`learning_outbox`)", null).use { cursor ->
            val name = cursor.getColumnIndexOrThrow("name")
            buildSet { while (cursor.moveToNext()) add(cursor.getString(name)) }
        }
        check(outboxColumns.containsAll(LEARNING_V47_OUTBOX_COLUMNS.map { it.first })) {
            "Missing v47 reward handoff columns"
        }
        LEARNING_V47_REWARD_FEEDBACK_INDEX_SQL.mapNotNull { statement ->
            Regex("`([^`]+)`").find(statement)?.groupValues?.get(1)
        }.forEach { index ->
            val present = db.rawQuery(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1",
                arrayOf(index),
            ).use { cursor -> cursor.moveToFirst() }
            check(present) { "Missing v47 reward authority index" }
        }
    }

    private fun requireV48PolicyGrantSchema(db: SQLiteDatabase) {
        val headColumns = V48_POLICY_GRANT_HEAD_COLUMNS.toSet()
        val expectedTables = mapOf(
            "learning_policy_grants" to headColumns,
            "learning_policy_grant_revisions" to headColumns.plus(
                setOf("previous_state_version", "changed_at_ms"),
            ),
        )
        expectedTables.forEach { (table, expectedColumns) ->
            check(tableExists(db, table)) { "Missing v48 policy grant table" }
            val actualColumns = db.rawQuery("PRAGMA table_info(`$table`)", null).use { cursor ->
                val name = cursor.getColumnIndexOrThrow("name")
                buildSet { while (cursor.moveToNext()) add(cursor.getString(name)) }
            }
            check(actualColumns == expectedColumns) { "Invalid v48 policy grant columns" }
        }
        LEARNING_V48_POLICY_GRANT_INDEX_SQL.mapNotNull { statement ->
            Regex("`([^`]+)`").find(statement)?.groupValues?.get(1)
        }.forEach { index ->
            val present = db.rawQuery(
                "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ? LIMIT 1",
                arrayOf(index),
            ).use { cursor -> cursor.moveToFirst() }
            check(present) { "Missing v48 policy grant index" }
        }
        db.rawQuery("PRAGMA foreign_key_list(`learning_policy_grant_revisions`)", null).use {
            cursor ->
            val table = cursor.getColumnIndexOrThrow("table")
            val from = cursor.getColumnIndexOrThrow("from")
            val to = cursor.getColumnIndexOrThrow("to")
            val onDelete = cursor.getColumnIndexOrThrow("on_delete")
            check(
                cursor.moveToFirst() &&
                    cursor.getString(table) == "learning_policy_grants" &&
                    cursor.getString(from) == "grant_id" && cursor.getString(to) == "grant_id" &&
                    cursor.getString(onDelete).equals("CASCADE", ignoreCase = true) &&
                    !cursor.moveToNext(),
            ) { "Invalid v48 policy grant revision authority" }
        }
        val invalidHeadPredicate =
            "typeof(grant_id) != 'text' OR typeof(source_stream_id) != 'text' OR " +
                "typeof(policy_id) != 'text' OR typeof(artifact_sha256) != 'text' OR " +
                "typeof(scope_kind) != 'text' OR typeof(scope_id) != 'text' OR " +
                "typeof(consuming_assistant_id) != 'text' OR typeof(actor) != 'text' OR " +
                "typeof(state) != 'text' OR typeof(reason_code) != 'text' OR " +
                "typeof(policy_revision) != 'integer' OR typeof(state_version) != 'integer' OR " +
                "typeof(granted_at_ms) != 'integer' OR " +
                "(revoked_at_ms IS NOT NULL AND typeof(revoked_at_ms) != 'integer') OR " +
                "typeof(created_at_ms) != 'integer' OR typeof(updated_at_ms) != 'integer' OR " +
                "grant_id = '' OR length(grant_id) > 256 OR " +
                "grant_id GLOB '*[^A-Za-z0-9_.:@-]*' OR " +
                "policy_id = '' OR length(policy_id) > 256 OR " +
                "policy_id GLOB '*[^A-Za-z0-9_.:@-]*' OR scope_id = '' OR " +
                "${invalidCanonicalUuidSql("source_stream_id")} OR " +
                "${invalidCanonicalUuidSql("consuming_assistant_id")} OR " +
                "(scope_kind = 'ASSISTANT' AND scope_id != consuming_assistant_id) OR " +
                "policy_revision <= 0 OR length(artifact_sha256) != 64 OR " +
                "artifact_sha256 GLOB '*[^0-9a-f]*' OR " +
                "scope_kind NOT IN ('ASSISTANT', 'AUTHORITY_SUBJECT') OR " +
                "actor NOT IN ('USER_REVIEW', 'AUTHORITY_REVOCATION') OR " +
                "state NOT IN ('GRANTED', 'REVOKED') OR " +
                "state_version <= 0 OR (state_version = 1 AND state != 'GRANTED') OR " +
                "(state = 'GRANTED' AND revoked_at_ms IS NOT NULL) OR " +
                "(state = 'REVOKED' AND " +
                "(revoked_at_ms IS NULL OR revoked_at_ms < granted_at_ms OR " +
                "updated_at_ms < revoked_at_ms)) OR " +
                "granted_at_ms < 0 OR created_at_ms < 0 OR updated_at_ms < created_at_ms OR " +
                "reason_code NOT IN (" +
                "'USER_APPROVED_CONTEXTUAL_ADVICE', " +
                "'USER_REVIEWED_POLICY_UPDATE', " +
                "'USER_RESTORED_POLICY_REVISION', " +
                "'USER_REVOKED_CONTEXTUAL_ADVICE', " +
                "'SECOND_USER_AUTHORITY_REVOKED') OR " +
                "(state = 'REVOKED' AND reason_code NOT IN " +
                "('USER_REVOKED_CONTEXTUAL_ADVICE', 'SECOND_USER_AUTHORITY_REVOKED')) OR " +
                "(state = 'GRANTED' AND reason_code IN " +
                "('USER_REVOKED_CONTEXTUAL_ADVICE', 'SECOND_USER_AUTHORITY_REVOKED')) OR " +
                "(actor = 'AUTHORITY_REVOCATION' AND (scope_kind != 'AUTHORITY_SUBJECT' OR " +
                "state != 'REVOKED' OR reason_code != 'SECOND_USER_AUTHORITY_REVOKED')) OR " +
                "(actor = 'USER_REVIEW' AND reason_code = 'SECOND_USER_AUTHORITY_REVOKED') OR " +
                "(state_version = 1 AND reason_code NOT IN (" +
                "'USER_APPROVED_CONTEXTUAL_ADVICE', 'USER_RESTORED_POLICY_REVISION')) OR " +
                "(state_version = 1 AND actor != 'USER_REVIEW')"
        listOf("learning_policy_grants", "learning_policy_grant_revisions").forEach { table ->
            val invalid = db.rawQuery(
                "SELECT 1 FROM `$table` WHERE $invalidHeadPredicate LIMIT 1",
                null,
            ).use { cursor -> cursor.moveToFirst() }
            check(!invalid) { "Invalid v48 policy grant authority row" }
        }
        val invalidRevision = db.rawQuery(
            "SELECT 1 FROM `learning_policy_grant_revisions` WHERE " +
                "(typeof(changed_at_ms) != 'integer' OR " +
                "(previous_state_version IS NOT NULL AND " +
                "typeof(previous_state_version) != 'integer') OR " +
                "(state_version = 1 AND previous_state_version IS NOT NULL) OR " +
                "(state_version > 1 AND (previous_state_version IS NULL OR " +
                "previous_state_version != state_version - 1)) OR " +
                "changed_at_ms != updated_at_ms) LIMIT 1",
            null,
        ).use { cursor -> cursor.moveToFirst() }
        check(!invalidRevision) { "Invalid v48 policy grant revision row" }

        // A restored authority head is trusted only when its complete current snapshot is also
        // present in the append-only journal. The null-safe revoked_at comparison is deliberate.
        val currentRevisionEquality = V48_POLICY_GRANT_HEAD_COLUMNS.joinToString(" AND ") { column ->
            val valueEquality = if (column == "revoked_at_ms") {
                "revision.`$column` IS head.`$column`"
            } else {
                "revision.`$column` = head.`$column`"
            }
            "(typeof(revision.`$column`) = typeof(head.`$column`) AND $valueEquality)"
        }
        val missingExactCurrentRevision = db.rawQuery(
            "SELECT 1 FROM `learning_policy_grants` AS head WHERE NOT EXISTS (" +
                "SELECT 1 FROM `learning_policy_grant_revisions` AS revision WHERE " +
                "revision.`grant_id` = head.`grant_id` AND " +
                "revision.`state_version` = head.`state_version` AND " +
                currentRevisionEquality + ") LIMIT 1",
            null,
        ).use { cursor -> cursor.moveToFirst() }
        check(!missingExactCurrentRevision) {
            "Policy grant head has no byte-exact current audit revision"
        }

        // PK(grant_id,state_version) supplies uniqueness. Count/min/max plus the per-row previous
        // fence prove a complete 1..head.state_version journal rather than accepting a lone latest
        // row that merely claims an unavailable predecessor.
        val incompleteRevisionJournal = db.rawQuery(
            "SELECT 1 FROM `learning_policy_grants` AS head LEFT JOIN (" +
                "SELECT `grant_id`, COUNT(*) AS revision_count, " +
                "MIN(`state_version`) AS minimum_version, " +
                "MAX(`state_version`) AS maximum_version " +
                "FROM `learning_policy_grant_revisions` GROUP BY `grant_id`" +
                ") AS journal ON journal.`grant_id` = head.`grant_id` WHERE " +
                "journal.revision_count IS NULL OR " +
                "journal.revision_count != head.`state_version` OR " +
                "journal.minimum_version != 1 OR " +
                "journal.maximum_version != head.`state_version` LIMIT 1",
            null,
        ).use { cursor -> cursor.moveToFirst() }
        check(!incompleteRevisionJournal) { "Policy grant audit revision journal is incomplete" }

        val invalidRevisionTransition = db.rawQuery(
            "SELECT 1 FROM `learning_policy_grant_revisions` AS current " +
                "LEFT JOIN `learning_policy_grant_revisions` AS previous ON " +
                "previous.`grant_id` = current.`grant_id` AND " +
                "previous.`state_version` = current.`previous_state_version` WHERE " +
                "(current.`state_version` = 1 AND (" +
                "current.`state` != 'GRANTED' OR " +
                "current.`granted_at_ms` != current.`created_at_ms` OR " +
                "current.`updated_at_ms` != current.`created_at_ms`)) OR " +
                "(current.`state_version` > 1 AND (" +
                "previous.`grant_id` IS NULL OR " +
                "current.`updated_at_ms` < previous.`updated_at_ms` OR " +
                "((current.`policy_revision` = previous.`policy_revision`) != " +
                "(current.`artifact_sha256` = previous.`artifact_sha256`)) OR " +
                "(previous.`state` = 'GRANTED' AND current.`state` = 'GRANTED' AND (" +
                "current.`reason_code` NOT IN (" +
                "'USER_REVIEWED_POLICY_UPDATE', 'USER_RESTORED_POLICY_REVISION') OR " +
                "current.`granted_at_ms` != previous.`granted_at_ms`)) OR " +
                "(previous.`state` = 'GRANTED' AND current.`state` = 'REVOKED' AND (" +
                "current.`policy_revision` != previous.`policy_revision` OR " +
                "current.`artifact_sha256` != previous.`artifact_sha256` OR " +
                "current.`granted_at_ms` != previous.`granted_at_ms` OR " +
                "current.`revoked_at_ms` != current.`updated_at_ms`)) OR " +
                "(previous.`actor` = 'AUTHORITY_REVOCATION' AND " +
                "current.`state` = 'GRANTED') OR " +
                "(previous.`state` = 'REVOKED' AND current.`state` = 'GRANTED' AND (" +
                "current.`reason_code` NOT IN (" +
                "'USER_APPROVED_CONTEXTUAL_ADVICE', 'USER_RESTORED_POLICY_REVISION') OR " +
                "current.`granted_at_ms` != current.`updated_at_ms`)) OR " +
                "(previous.`state` = 'REVOKED' AND current.`state` = 'REVOKED'))) LIMIT 1",
            null,
        ).use { cursor -> cursor.moveToFirst() }
        check(!invalidRevisionTransition) { "Policy grant audit revision transition is invalid" }

        // Stream/scope/consumer/policy identify one immutable approval authority. Policy content,
        // state, reason and timestamps may advance, but an old revision may never be re-parented
        // under a different authority head during import or restore.
        val changedBaseIdentity = db.rawQuery(
            "SELECT 1 FROM `learning_policy_grant_revisions` AS revision " +
                "JOIN `learning_policy_grants` AS head " +
                "ON head.`grant_id` = revision.`grant_id` WHERE " +
                listOf(
                    "source_stream_id",
                    "policy_id",
                    "scope_kind",
                    "scope_id",
                    "consuming_assistant_id",
                    "actor",
                    "created_at_ms",
                ).joinToString(" OR ") { column ->
                    "revision.`$column` != head.`$column`"
                } + " LIMIT 1",
            null,
        ).use { cursor -> cursor.moveToFirst() }
        check(!changedBaseIdentity) { "Policy grant audit revision changed its base identity" }
    }

    private fun requireV49WorkflowSchema(db: SQLiteDatabase) {
        check(tableExists(db, "workflows")) { "Missing v49 workflow table" }
        val actualColumns = db.rawQuery("PRAGMA table_info(`workflows`)", null).use { cursor ->
            val name = cursor.getColumnIndexOrThrow("name")
            buildSet { while (cursor.moveToNext()) add(cursor.getString(name)) }
        }
        val required = setOf(
            "stateVersion", "origin", "sourceCandidateId", "sourceArtifactHash",
            "grantDigest", "authoringAssistantId", "capabilitySnapshotJson",
            "toolSchemaFingerprintsJson", "staleReason",
        )
        check(actualColumns.containsAll(required)) { "Missing v49 workflow authority columns" }
        val invalid = db.rawQuery(
            "SELECT 1 FROM `workflows` WHERE `stateVersion` < 1 OR " +
                "`origin` NOT IN ('USER', 'LEARNED') OR " +
                "`capabilitySnapshotJson` IS NULL OR `toolSchemaFingerprintsJson` IS NULL OR " +
                "(`origin` = 'USER' AND (`sourceCandidateId` IS NOT NULL OR " +
                "`sourceArtifactHash` IS NOT NULL OR `grantDigest` IS NOT NULL)) OR " +
                "(`origin` = 'LEARNED' AND CASE WHEN `staleReason` IN (" +
                "'$WORKFLOW_DEFINITION_TOMBSTONE', '$WORKFLOW_CLAIM_TOMBSTONE') THEN (" +
                "`enabled` != 0 OR `name` != '$WORKFLOW_REDACTED_NAME' OR " +
                "`description` IS NOT NULL OR `definitionJson` != '{}' OR " +
                "`createdAtMs` != 0 OR `lastRunAtMs` IS NOT NULL OR " +
                "`lastRunStatus` IS NOT NULL OR `lastRunError` IS NOT NULL OR " +
                "`runsTodayCount` != 0 OR `runsTodayDate` != '' OR " +
                "`sourceArtifactHash` IS NOT NULL OR `grantDigest` IS NOT NULL OR " +
                "`authoringAssistantId` IS NOT NULL OR `capabilitySnapshotJson` != '[]' OR " +
                "`toolSchemaFingerprintsJson` != '[]') ELSE (" +
                "`sourceCandidateId` IS NULL OR `sourceArtifactHash` IS NULL OR " +
                "`grantDigest` IS NULL OR `authoringAssistantId` IS NULL) END) LIMIT 1",
            null,
        ).use { cursor -> cursor.moveToFirst() }
        check(!invalid) { "Invalid v49 workflow authority row" }
    }

    /** Atomic staged-file fail-closed quarantine; identifiers never leave SQLite. */
    private fun quarantineStagedLearnedWorkflowsOrThrow(databaseFile: File) {
        requireSafeStagedFile(databaseFile)
        SQLiteDatabase.openDatabase(
            databaseFile.absolutePath,
            null,
            SQLiteDatabase.OPEN_READWRITE,
        ).use { db ->
            check(
                db.version == EXPECTED_VERSION &&
                    readRoomIdentityHash(db) == EXPECTED_IDENTITY_HASH,
            ) { "Staged workflow quarantine requires the exact current schema" }
            db.beginTransaction()
            try {
                db.execSQL(
                    "DELETE FROM `workflow_runs` WHERE `workflowId` IN (" +
                        "SELECT `id` FROM `workflows` WHERE `origin` = 'LEARNED')",
                )
                db.execSQL(
                    "UPDATE `workflows` SET " +
                        "`name` = '$WORKFLOW_REDACTED_NAME', `description` = NULL, " +
                        "`enabled` = 0, `definitionJson` = '{}', `createdAtMs` = 0, " +
                        "`updatedAtMs` = 0, `lastRunAtMs` = NULL, `lastRunStatus` = NULL, " +
                        "`lastRunError` = NULL, `runsTodayCount` = 0, `runsTodayDate` = '', " +
                        "`stateVersion` = `stateVersion` + 1, `sourceArtifactHash` = NULL, " +
                        "`grantDigest` = NULL, `authoringAssistantId` = NULL, " +
                        "`capabilitySnapshotJson` = '[]', `toolSchemaFingerprintsJson` = '[]', " +
                        "`staleReason` = '$WORKFLOW_DEFINITION_TOMBSTONE' " +
                        "WHERE `origin` = 'LEARNED' AND (`staleReason` IS NULL OR " +
                        "`staleReason` NOT IN ('$WORKFLOW_DEFINITION_TOMBSTONE', " +
                        "'$WORKFLOW_CLAIM_TOMBSTONE'))",
                )
                requireV49WorkflowSchema(db)
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
        }
    }

    private fun invalidCanonicalUuidSql(column: String): String =
        "length(`$column`) != 36 OR lower(`$column`) != `$column` OR " +
            "substr(`$column`, 9, 1) != '-' OR substr(`$column`, 14, 1) != '-' OR " +
            "substr(`$column`, 19, 1) != '-' OR substr(`$column`, 24, 1) != '-' OR " +
            "length(replace(`$column`, '-', '')) != 32 OR " +
            "replace(`$column`, '-', '') GLOB '*[^0-9a-f]*' OR `$column` = '$NIL_UUID'"

    /** Framework-SQLite mirror of the runtime reader's bounded stream integrity checks. */
    private fun requireHealthyLearningOutbox(
        db: SQLiteDatabase,
        payloadColumns: List<String> = LEARNING_V47_SENTINEL_PAYLOAD_COLUMNS,
    ) {
        readRetainedLearningOutboxOrThrow(db, payloadColumns)
    }

    private fun insertLearningOutboxStreamSentinel(
        db: SQLiteDatabase,
        streamId: String,
        createdAtMs: Long,
    ) {
        require(isCanonicalNonNilDatabaseUuid(streamId))
        require(createdAtMs >= 0L)
        db.execSQL(
            "INSERT INTO `learning_outbox` (" +
                "`stream_id`, `event_id`, `event_type`, `event_schema_version`, " +
                "`created_at_ms`) VALUES (?, ?, 'STREAM_INIT', 1, ?)",
            arrayOf<Any>(streamId, LEARNING_V46_STREAM_INIT_EVENT_ID, createdAtMs),
        )
    }

    private fun requireExactAuthorityStream(
        db: SQLiteDatabase,
        expectedStreamId: String,
        expectedHeadSeq: Long,
    ) {
        db.rawQuery(
            "SELECT COUNT(*), MIN(`seq`), MAX(`seq`), COUNT(DISTINCT `seq`), " +
                "COUNT(DISTINCT `stream_id`) FROM `learning_outbox` WHERE `stream_id` = ?",
            arrayOf(expectedStreamId),
        ).use { cursor ->
            check(cursor.moveToFirst()) { "Authority stream query returned no row" }
            val count = cursor.getLong(0)
            check(
                count > 0L && cursor.getLong(1) == 1L &&
                    cursor.getLong(2) == expectedHeadSeq && cursor.getLong(3) == count &&
                    cursor.getLong(4) == 1L
            ) { "Staged v46 authority stream does not match the archive descriptor" }
        }
    }

    private fun ensureBrowserV34Schema(db: SQLiteDatabase) {
        listOf(
            "CREATE TABLE IF NOT EXISTS `browser_bookmarks` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `normalized_url` TEXT NOT NULL, `url` TEXT NOT NULL, `title` TEXT NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_browser_bookmarks_normalized_url` ON `browser_bookmarks` (`normalized_url`)",
            "CREATE TABLE IF NOT EXISTS `browser_history` (`id` INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, `normalized_url` TEXT NOT NULL, `url` TEXT NOT NULL, `title` TEXT NOT NULL, `visited_at_ms` INTEGER NOT NULL)",
            "CREATE INDEX IF NOT EXISTS `index_browser_history_normalized_url` ON `browser_history` (`normalized_url`)",
            "CREATE INDEX IF NOT EXISTS `index_browser_history_visited_at_ms` ON `browser_history` (`visited_at_ms`)",
        ).forEach(db::execSQL)
    }

    private fun ensureExecutionV35Schema(db: SQLiteDatabase) {
        listOf(
            "CREATE TABLE IF NOT EXISTS `execution_records` (`id` TEXT NOT NULL, `trace_id` TEXT NOT NULL, `parent_execution_id` TEXT, `command_id` TEXT, `conversation_id` TEXT, `learning_scope_kind` TEXT, `learning_scope_id` TEXT, `subject_id` TEXT NOT NULL, `subject_type` TEXT NOT NULL, `origin` TEXT NOT NULL, `capability_keys` TEXT NOT NULL, `resource_summary` TEXT NOT NULL, `runtime` TEXT NOT NULL, `idempotency_key` TEXT, `runtime_handle_summary` TEXT, `status` TEXT NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, `started_at_ms` INTEGER, `heartbeat_at_ms` INTEGER, `finished_at_ms` INTEGER, `cancellation_result` TEXT, `terminal_detail` TEXT, PRIMARY KEY(`id`))",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_status` ON `execution_records` (`status`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_trace` ON `execution_records` (`trace_id`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_parent` ON `execution_records` (`parent_execution_id`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_idempotency` ON `execution_records` (`idempotency_key`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_updated` ON `execution_records` (`updated_at_ms`)",
        ).forEach(db::execSQL)
    }

    /** Same-version upstream v37 backups need the complete fork execution schema before Room opens. */
    private fun ensureExecutionV37Schema(db: SQLiteDatabase) {
        ensureColumns(
            db,
            "execution_records",
            listOf(
                "execution_kind" to "TEXT NOT NULL DEFAULT 'TOOL_CALL'",
                "state_version" to "INTEGER NOT NULL DEFAULT 0",
                "last_state_source" to "TEXT NOT NULL DEFAULT 'LEGACY'",
                "last_reason_code" to "TEXT",
                "verification_state" to "TEXT NOT NULL DEFAULT 'UNKNOWN'",
                "last_probe_at_ms" to "INTEGER",
                "completion_policy" to "TEXT NOT NULL DEFAULT 'WAIT_FOR_CHILDREN'",
                "runtime_instance_marker" to "TEXT",
                "cancellation_requested_at_ms" to "INTEGER",
                "requested_terminal_outcome" to "TEXT NOT NULL DEFAULT 'NONE'",
            ),
        )
        listOf(
            "CREATE TABLE IF NOT EXISTS `execution_events` (`event_id` TEXT NOT NULL, " +
                "`execution_id` TEXT NOT NULL, `sequence` INTEGER NOT NULL, " +
                "`previous_status` TEXT, `next_status` TEXT NOT NULL, " +
                "`previous_verification` TEXT, `next_verification` TEXT NOT NULL, " +
                "`source` TEXT NOT NULL, `reason_code` TEXT, `created_at_ms` INTEGER NOT NULL, " +
                "PRIMARY KEY(`event_id`), FOREIGN KEY(`execution_id`) REFERENCES " +
                "`execution_records`(`id`) ON UPDATE NO ACTION ON DELETE CASCADE)",
            "CREATE TABLE IF NOT EXISTS `pending_tool_approvals` (`approval_id` TEXT NOT NULL, " +
                "`execution_id` TEXT NOT NULL, `trace_id` TEXT, `tool_call_id` TEXT NOT NULL, " +
                "`conversation_id` TEXT NOT NULL, `subject_id` TEXT NOT NULL, " +
                "`subject_type` TEXT NOT NULL, `origin` TEXT NOT NULL, " +
                "`capability_key` TEXT NOT NULL, `resource_category` TEXT NOT NULL, " +
                "`requested_at_ms` INTEGER NOT NULL, `status` TEXT NOT NULL DEFAULT 'PENDING', " +
                "`state_version` INTEGER NOT NULL DEFAULT 0, `resolved_at_ms` INTEGER, " +
                "`resolution_reason` TEXT, `resolution_request_id` TEXT, " +
                "PRIMARY KEY(`approval_id`))",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_conversation_status_updated` " +
                "ON `execution_records` (`conversation_id`, `status`, `updated_at_ms`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_subject_status_updated` " +
                "ON `execution_records` (`subject_id`, `status`, `updated_at_ms`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_parent_status` " +
                "ON `execution_records` (`parent_execution_id`, `status`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_runtime_handle` " +
                "ON `execution_records` (`runtime`, `runtime_handle_summary`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_records_heartbeat_status` " +
                "ON `execution_records` (`heartbeat_at_ms`, `status`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_events_execution` " +
                "ON `execution_events` (`execution_id`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `idx_execution_events_execution_sequence` " +
                "ON `execution_events` (`execution_id`, `sequence`)",
            "CREATE INDEX IF NOT EXISTS `idx_execution_events_created` " +
                "ON `execution_events` (`created_at_ms`)",
            "CREATE INDEX IF NOT EXISTS `idx_tool_approvals_execution` " +
                "ON `pending_tool_approvals` (`execution_id`)",
            "CREATE INDEX IF NOT EXISTS `idx_tool_approvals_conversation_status_requested` " +
                "ON `pending_tool_approvals` (`conversation_id`, `status`, `requested_at_ms`)",
            "CREATE INDEX IF NOT EXISTS `idx_tool_approvals_resolved` " +
                "ON `pending_tool_approvals` (`resolved_at_ms`)",
        ).forEach(db::execSQL)
    }

    /** Same-version upstream v38 backups need the fork-only pet sidecar schema before Room opens. */
    private fun ensurePetV38Schema(db: SQLiteDatabase) {
        listOf(
            "CREATE TABLE IF NOT EXISTS `pet_dialogue_sessions` (`sessionId` TEXT NOT NULL, `assistantId` TEXT NOT NULL, `privilegedConversationId` TEXT NOT NULL, `localDate` TEXT NOT NULL, `zoneId` TEXT NOT NULL, `activeOwnerKey` TEXT, `status` TEXT NOT NULL, `archiveReason` TEXT, `title` TEXT NOT NULL, `summary` TEXT NOT NULL, `notes` TEXT NOT NULL, `tagsJson` TEXT NOT NULL, `summaryState` TEXT NOT NULL, `stateVersion` INTEGER NOT NULL, `createdAtMs` INTEGER NOT NULL, `updatedAtMs` INTEGER NOT NULL, `archivedAtMs` INTEGER, `deletedAtMs` INTEGER, PRIMARY KEY(`sessionId`))",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_sessions_assistantId` ON `pet_dialogue_sessions` (`assistantId`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_sessions_privilegedConversationId` ON `pet_dialogue_sessions` (`privilegedConversationId`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_sessions_assistantId_status` ON `pet_dialogue_sessions` (`assistantId`, `status`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_sessions_assistantId_localDate` ON `pet_dialogue_sessions` (`assistantId`, `localDate`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_pet_dialogue_sessions_activeOwnerKey` ON `pet_dialogue_sessions` (`activeOwnerKey`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_sessions_deletedAtMs` ON `pet_dialogue_sessions` (`deletedAtMs`)",
            "CREATE TABLE IF NOT EXISTS `pet_dialogue_turns` (`turnId` TEXT NOT NULL, `sessionId` TEXT NOT NULL, `sequence` INTEGER NOT NULL, `inputKind` TEXT NOT NULL, `userText` TEXT, `interactionJson` TEXT, `assistantText` TEXT, `action` TEXT, `handoffRequestId` TEXT, `createdAtMs` INTEGER NOT NULL, PRIMARY KEY(`turnId`), FOREIGN KEY(`sessionId`) REFERENCES `pet_dialogue_sessions`(`sessionId`) ON UPDATE NO ACTION ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_turns_sessionId` ON `pet_dialogue_turns` (`sessionId`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_pet_dialogue_turns_sessionId_sequence` ON `pet_dialogue_turns` (`sessionId`, `sequence`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_turns_handoffRequestId` ON `pet_dialogue_turns` (`handoffRequestId`)",
            "CREATE TABLE IF NOT EXISTS `pet_handoff_requests` (`requestId` TEXT NOT NULL, `sessionId` TEXT NOT NULL, `turnId` TEXT NOT NULL, `assistantId` TEXT NOT NULL, `privilegedConversationId` TEXT NOT NULL, `mode` TEXT NOT NULL, `status` TEXT NOT NULL, `title` TEXT NOT NULL, `request` TEXT NOT NULL, `targetCommandId` TEXT, `stateVersion` INTEGER NOT NULL, `createdAtMs` INTEGER NOT NULL, `submittedAtMs` INTEGER, `resolvedAtMs` INTEGER, `expiresAtMs` INTEGER, PRIMARY KEY(`requestId`), FOREIGN KEY(`sessionId`) REFERENCES `pet_dialogue_sessions`(`sessionId`) ON UPDATE NO ACTION ON DELETE CASCADE, FOREIGN KEY(`turnId`) REFERENCES `pet_dialogue_turns`(`turnId`) ON UPDATE NO ACTION ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS `index_pet_handoff_requests_sessionId` ON `pet_handoff_requests` (`sessionId`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_handoff_requests_turnId` ON `pet_handoff_requests` (`turnId`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_handoff_requests_assistantId_status` ON `pet_handoff_requests` (`assistantId`, `status`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_handoff_requests_targetCommandId` ON `pet_handoff_requests` (`targetCommandId`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_handoff_requests_expiresAtMs` ON `pet_handoff_requests` (`expiresAtMs`)",
            "CREATE TABLE IF NOT EXISTS `pet_dialogue_revisions` (`revisionId` TEXT NOT NULL, `sessionId` TEXT NOT NULL, `revision` INTEGER NOT NULL, `actor` TEXT NOT NULL, `operation` TEXT NOT NULL, `previousTitle` TEXT NOT NULL, `previousSummary` TEXT NOT NULL, `previousNotes` TEXT NOT NULL, `previousTagsJson` TEXT NOT NULL, `previousStatus` TEXT NOT NULL, `createdAtMs` INTEGER NOT NULL, PRIMARY KEY(`revisionId`), FOREIGN KEY(`sessionId`) REFERENCES `pet_dialogue_sessions`(`sessionId`) ON UPDATE NO ACTION ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_revisions_sessionId` ON `pet_dialogue_revisions` (`sessionId`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_pet_dialogue_revisions_sessionId_revision` ON `pet_dialogue_revisions` (`sessionId`, `revision`)",
            "CREATE INDEX IF NOT EXISTS `index_pet_dialogue_revisions_createdAtMs` ON `pet_dialogue_revisions` (`createdAtMs`)",
        ).forEach(db::execSQL)
    }

    /** Additive same-version guard for backups restored from a pre-v39 fork build. */
    private fun ensurePendingCommandAuthorityV39Schema(db: SQLiteDatabase) {
        ensureColumns(
            db,
            "pending_chat_commands",
            listOf("authoritySubjectId" to "TEXT"),
        )
        db.execSQL(
            "CREATE INDEX IF NOT EXISTS `index_pending_chat_commands_authoritySubjectId` " +
                "ON `pending_chat_commands` (`authoritySubjectId`)",
        )
    }

    /** Same-version upstream v40 backups need the private, redacted experience store. */
    private fun ensureToolExperienceV40Schema(db: SQLiteDatabase) {
        listOf(
            "CREATE TABLE IF NOT EXISTS `tool_experiences` (`experience_id` TEXT NOT NULL, `authority_subject_id` TEXT NOT NULL, `primary_tool_name` TEXT NOT NULL, `tool_names_json` TEXT NOT NULL, `category_path` TEXT NOT NULL, `schema_fingerprint` TEXT NOT NULL, `title` TEXT NOT NULL, `body` TEXT NOT NULL, `tags_json` TEXT NOT NULL, `state` TEXT NOT NULL, `confidence` TEXT NOT NULL, `state_version` INTEGER NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, `last_observed_at_ms` INTEGER NOT NULL, `last_verified_at_ms` INTEGER, `deleted_at_ms` INTEGER, PRIMARY KEY(`experience_id`))",
            "CREATE INDEX IF NOT EXISTS `index_tool_experiences_authority_subject_id_state_updated_at_ms` ON `tool_experiences` (`authority_subject_id`, `state`, `updated_at_ms`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_tool_experiences_authority_subject_id_primary_tool_name_schema_fingerprint` ON `tool_experiences` (`authority_subject_id`, `primary_tool_name`, `schema_fingerprint`)",
            "CREATE INDEX IF NOT EXISTS `index_tool_experiences_primary_tool_name_state` ON `tool_experiences` (`primary_tool_name`, `state`)",
            "CREATE INDEX IF NOT EXISTS `index_tool_experiences_deleted_at_ms` ON `tool_experiences` (`deleted_at_ms`)",
            "CREATE TABLE IF NOT EXISTS `tool_experience_evidence` (`evidence_id` TEXT NOT NULL, `experience_id` TEXT NOT NULL, `execution_id` TEXT NOT NULL, `tool_name` TEXT NOT NULL, `schema_fingerprint` TEXT NOT NULL, `outcome_kind` TEXT NOT NULL, `created_at_ms` INTEGER NOT NULL, PRIMARY KEY(`evidence_id`), FOREIGN KEY(`experience_id`) REFERENCES `tool_experiences`(`experience_id`) ON UPDATE NO ACTION ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS `index_tool_experience_evidence_experience_id_created_at_ms` ON `tool_experience_evidence` (`experience_id`, `created_at_ms`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_tool_experience_evidence_execution_id` ON `tool_experience_evidence` (`execution_id`)",
            "CREATE TABLE IF NOT EXISTS `tool_experience_revisions` (`revision_id` TEXT NOT NULL, `experience_id` TEXT NOT NULL, `revision` INTEGER NOT NULL, `actor` TEXT NOT NULL, `title` TEXT NOT NULL, `body` TEXT NOT NULL, `tags_json` TEXT NOT NULL, `created_at_ms` INTEGER NOT NULL, PRIMARY KEY(`revision_id`), FOREIGN KEY(`experience_id`) REFERENCES `tool_experiences`(`experience_id`) ON UPDATE NO ACTION ON DELETE CASCADE)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_tool_experience_revisions_experience_id_revision` ON `tool_experience_revisions` (`experience_id`, `revision`)",
            "CREATE INDEX IF NOT EXISTS `index_tool_experience_revisions_created_at_ms` ON `tool_experience_revisions` (`created_at_ms`)",
        ).forEach(db::execSQL)
    }

    /** Same-version upstream v41 backups need the private model-confirmed shortcut metadata. */
    private fun ensureToolShortcutV41Schema(db: SQLiteDatabase) {
        listOf(
            "CREATE TABLE IF NOT EXISTS `tool_shortcuts` (`shortcut_id` TEXT NOT NULL, `authority_subject_id` TEXT NOT NULL, `tool_name` TEXT NOT NULL, `source` TEXT NOT NULL, `category_path` TEXT NOT NULL, `risk` TEXT NOT NULL, `schema_fingerprint` TEXT NOT NULL, `state` TEXT NOT NULL, `state_version` INTEGER NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, `last_used_at_ms` INTEGER, `use_count` INTEGER NOT NULL, `model_confirmed_at_ms` INTEGER NOT NULL, PRIMARY KEY(`shortcut_id`))",
            "CREATE INDEX IF NOT EXISTS `index_tool_shortcuts_authority_subject_id_state_updated_at_ms` ON `tool_shortcuts` (`authority_subject_id`, `state`, `updated_at_ms`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `index_tool_shortcuts_authority_subject_id_tool_name_schema_fingerprint` ON `tool_shortcuts` (`authority_subject_id`, `tool_name`, `schema_fingerprint`)",
            "CREATE INDEX IF NOT EXISTS `index_tool_shortcuts_tool_name_state` ON `tool_shortcuts` (`tool_name`, `state`)",
        ).forEach(db::execSQL)
    }

    /** Same-version upstream or pre-Owner v42 backups need the recoverable host-operation ledger. */
    private fun ensureOwnerHostV42Schema(db: SQLiteDatabase) {
        listOf(
            "CREATE TABLE IF NOT EXISTS `host_operations` (`request_id` TEXT NOT NULL, `authority_subject_id` TEXT NOT NULL, `authority_epoch` INTEGER NOT NULL, `assistant_id` TEXT NOT NULL, `conversation_id` TEXT NOT NULL, `model_id` TEXT, `provider_id` TEXT, `tool_family` TEXT NOT NULL, `action_summary_json` TEXT NOT NULL, `state` TEXT NOT NULL, `state_version` INTEGER NOT NULL, `recovery_code` TEXT, `result_code` TEXT, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, `completed_at_ms` INTEGER, PRIMARY KEY(`request_id`))",
            "CREATE INDEX IF NOT EXISTS `idx_host_operations_authority_state` ON `host_operations` (`authority_subject_id`, `state`)",
            "CREATE INDEX IF NOT EXISTS `idx_host_operations_conversation_updated` ON `host_operations` (`conversation_id`, `updated_at_ms`)",
            "CREATE INDEX IF NOT EXISTS `idx_host_operations_state_updated` ON `host_operations` (`state`, `updated_at_ms`)",
            "CREATE TABLE IF NOT EXISTS `host_operation_events` (`event_id` TEXT NOT NULL, `request_id` TEXT NOT NULL, `sequence` INTEGER NOT NULL, `previous_state` TEXT, `next_state` TEXT NOT NULL, `action_index` INTEGER, `action_type` TEXT, `reason_code` TEXT, `created_at_ms` INTEGER NOT NULL, PRIMARY KEY(`event_id`), FOREIGN KEY(`request_id`) REFERENCES `host_operations`(`request_id`) ON UPDATE NO ACTION ON DELETE CASCADE)",
            "CREATE INDEX IF NOT EXISTS `idx_host_operation_events_request` ON `host_operation_events` (`request_id`)",
            "CREATE UNIQUE INDEX IF NOT EXISTS `idx_host_operation_events_request_sequence` ON `host_operation_events` (`request_id`, `sequence`)",
            "CREATE INDEX IF NOT EXISTS `idx_host_operation_events_created` ON `host_operation_events` (`created_at_ms`)",
            "CREATE TABLE IF NOT EXISTS `host_local_services` (`service_id` TEXT NOT NULL, `authority_subject_id` TEXT NOT NULL, `authority_epoch` INTEGER NOT NULL, `manifest_json` TEXT NOT NULL, `manifest_hash` TEXT NOT NULL, `execution_id` TEXT, `health_state` TEXT NOT NULL, `restart_policy` TEXT NOT NULL, `restart_count` INTEGER NOT NULL, `next_probe_at_ms` INTEGER, `last_probe_at_ms` INTEGER, `last_reason_code` TEXT, `enabled` INTEGER NOT NULL, `state_version` INTEGER NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, PRIMARY KEY(`service_id`))",
            "CREATE INDEX IF NOT EXISTS `idx_host_local_services_authority_enabled` ON `host_local_services` (`authority_subject_id`, `enabled`)",
            "CREATE INDEX IF NOT EXISTS `idx_host_local_services_execution` ON `host_local_services` (`execution_id`)",
            "CREATE INDEX IF NOT EXISTS `idx_host_local_services_health` ON `host_local_services` (`health_state`, `next_probe_at_ms`)",
        ).forEach(db::execSQL)
    }

    private fun ensureCapabilityGrantsV35Schema(db: SQLiteDatabase) {
        listOf(
            "CREATE TABLE IF NOT EXISTS `capability_grants` (`id` TEXT NOT NULL, `subject_id` TEXT NOT NULL, `subject_type` TEXT NOT NULL, `capability_key` TEXT NOT NULL, `resource_kind` TEXT NOT NULL, `resource_identifier` TEXT NOT NULL, `allowed_origins` TEXT NOT NULL, `scope` TEXT NOT NULL, `expires_at_ms` INTEGER, `revoked` INTEGER NOT NULL, `created_at_ms` INTEGER NOT NULL, `updated_at_ms` INTEGER NOT NULL, PRIMARY KEY(`id`))",
            "CREATE INDEX IF NOT EXISTS `idx_capability_grants_subject` ON `capability_grants` (`subject_id`, `subject_type`)",
            "CREATE INDEX IF NOT EXISTS `idx_capability_grants_active` ON `capability_grants` (`revoked`, `expires_at_ms`)",
        ).forEach(db::execSQL)
    }

    private fun rebuildPortableMemoryFts(db: SQLiteDatabase) {
        db.execSQL("DROP TRIGGER IF EXISTS memory_fts_ai")
        db.execSQL("DROP TRIGGER IF EXISTS memory_fts_au")
        db.execSQL("DROP TRIGGER IF EXISTS memory_fts_ad")
        db.execSQL("DROP TABLE IF EXISTS memory_fts")
        db.execSQL(MEMORY_FTS_PORTABLE_CREATE_SQL.trimIndent())
        db.execSQL(MEMORY_FTS_BACKFILL_SQL.trimIndent())
        MEMORY_FTS_TRIGGER_SQL.forEach(db::execSQL)
    }

    private fun ensureColumns(
        db: SQLiteDatabase,
        table: String,
        additions: List<Pair<String, String>>,
    ) {
        val existing = db.rawQuery("PRAGMA table_info(`$table`)", null).use { cursor ->
            val nameIndex = cursor.getColumnIndex("name")
            buildSet {
                if (nameIndex >= 0) while (cursor.moveToNext()) add(cursor.getString(nameIndex))
            }
        }
        additions.forEach { (name, declaration) ->
            if (name !in existing) db.execSQL("ALTER TABLE `$table` ADD COLUMN `$name` $declaration")
        }
    }

    private fun readRoomIdentityHash(db: SQLiteDatabase): String? = runCatching {
        db.rawQuery(
            "SELECT identity_hash FROM room_master_table WHERE id = 42",
            null,
        ).use { cursor -> if (cursor.moveToFirst()) cursor.getString(0) else null }
    }.getOrNull()

    private fun stampIdentity(
        db: SQLiteDatabase,
        identityHash: String,
    ) {
        db.execSQL(
            "CREATE TABLE IF NOT EXISTS room_master_table " +
                "(id INTEGER PRIMARY KEY, identity_hash TEXT)",
        )
        db.execSQL(
            "INSERT OR REPLACE INTO room_master_table (id, identity_hash) VALUES (42, ?)",
            arrayOf(identityHash),
        )
    }

    /**
     * Call after a restore has written the database file, and only when the restore actually
     * included the database. Safe to call when the file is a genuine agent backup (every
     * statement is idempotent) or when the file does not exist (no-op).
     */
    fun reconcile(context: Context) {
        val dbFile = context.getDatabasePath(DB_NAME)
        if (!dbFile.exists()) {
            Log.i(TAG, "reconcile: no database file at ${dbFile.absolutePath}, skipping")
            return
        }
        reconcileFile(dbFile, swallowFailures = true)
    }

    /**
     * Reconciles one already-staged database file and throws on every refusal or failure.
     *
     * Unlike [reconcile], this API never logs-and-continues. It is intended only for a private
     * same-directory cold-restore candidate before the candidate can replace the live database.
     */
    @Throws(Exception::class)
    fun reconcileStagedFileOrThrow(
        databaseFile: File,
        expectedStreamId: String,
        expectedHeadSeq: Long,
    ) {
        requireAuthorityStreamDescriptor(expectedStreamId, expectedHeadSeq)
        requireSafeStagedFile(databaseFile)
        val plan = SQLiteDatabase.openDatabase(
            databaseFile.absolutePath,
            null,
            SQLiteDatabase.OPEN_READONLY,
        ).use { db ->
            stagedReconcilePlanOrThrow(db.version, readRoomIdentityHash(db))
        }
        when (plan) {
            StagedReconcilePlan.ALREADY_CURRENT -> Unit
            StagedReconcilePlan.MIGRATE_FINAL_V49 ->
                migrateExactStagedToCurrent(
                    databaseFile = databaseFile,
                    expectedStreamId = expectedStreamId,
                    expectedHeadSeq = expectedHeadSeq,
                    expectedStartVersion = FINAL_V49_VERSION,
                    expectedStartIdentity = FINAL_V49_IDENTITY_HASH,
                )
            StagedReconcilePlan.MIGRATE_FINAL_V48 ->
                migrateExactStagedToCurrent(
                    databaseFile = databaseFile,
                    expectedStreamId = expectedStreamId,
                    expectedHeadSeq = expectedHeadSeq,
                    expectedStartVersion = 48,
                    expectedStartIdentity = FINAL_V48_IDENTITY_HASH,
                )
            StagedReconcilePlan.MIGRATE_FINAL_V47 ->
                migrateExactStagedToCurrent(
                    databaseFile = databaseFile,
                    expectedStreamId = expectedStreamId,
                    expectedHeadSeq = expectedHeadSeq,
                    expectedStartVersion = 47,
                    expectedStartIdentity = FINAL_V47_IDENTITY_HASH,
                )
            StagedReconcilePlan.MIGRATE_FINAL_V46 ->
                migrateExactStagedToCurrent(
                    databaseFile = databaseFile,
                    expectedStreamId = expectedStreamId,
                    expectedHeadSeq = expectedHeadSeq,
                    expectedStartVersion = 46,
                    expectedStartIdentity = FINAL_V46_IDENTITY_HASH,
                )
            StagedReconcilePlan.MIGRATE_PRE_P1_V46 ->
                migrateExactStagedToCurrent(
                    databaseFile = databaseFile,
                    expectedStreamId = expectedStreamId,
                    expectedHeadSeq = expectedHeadSeq,
                    expectedStartVersion = 46,
                    expectedStartIdentity = PRE_P1_V46_IDENTITY_HASH,
                    includeP1Floor = true,
                )
            StagedReconcilePlan.MIGRATE_PRE_LEARNING_V46 -> {
                check(expectedHeadSeq == 1L) {
                    "A pre-Learning staged database can only create its stream sentinel"
                }
                migrateExactStagedToCurrent(
                    databaseFile = databaseFile,
                    expectedStreamId = expectedStreamId,
                    expectedHeadSeq = expectedHeadSeq,
                    expectedStartVersion = 46,
                    expectedStartIdentity = PRE_LEARNING_V46_IDENTITY_HASH,
                    includeP1Floor = true,
                    createStream = true,
                )
            }
        }
        // Prove the candidate before destructive workflow quarantine, including same-version imports.
        validateStagedFileOrThrow(databaseFile, expectedStreamId, expectedHeadSeq)
        // The LearningDatabase is quarantined by the following cold swap. Any promoted
        // definition in this staged AppDatabase would lose its candidate/review authority, so
        // redact all LEARNED rows before the file can become live. USER rows are untouched.
        quarantineStagedLearnedWorkflowsOrThrow(databaseFile)
        normalizeStagedToSingleFileOrThrow(databaseFile)
        validateStagedFileOrThrow(databaseFile, expectedStreamId, expectedHeadSeq)
    }

    /** Exact-identity migration of the private cold-restore candidate; never accepts a live path. */
    private fun migrateExactStagedToCurrent(
        databaseFile: File,
        expectedStreamId: String,
        expectedHeadSeq: Long,
        expectedStartVersion: Int,
        expectedStartIdentity: String,
        includeP1Floor: Boolean = false,
        createStream: Boolean = false,
    ) {
        requireSafeStagedFile(databaseFile)
        SQLiteDatabase.openDatabase(
            databaseFile.absolutePath,
            null,
            SQLiteDatabase.OPEN_READWRITE,
        ).use { db ->
            check(db.version == expectedStartVersion) {
                "Staged database version changed before migration"
            }
            check(readRoomIdentityHash(db) == expectedStartIdentity) {
                "Staged database identity changed before migration"
            }
            db.beginTransaction()
            try {
                when (expectedStartVersion) {
                    46 -> {
                        if (includeP1Floor) {
                            ensureMemoryV46Schema(db)
                            ensureLearningV46SchemaForUpgrade(
                                db = db,
                                newStreamId = expectedStreamId.takeIf { createStream },
                            )
                        } else {
                            requireHealthyLearningOutbox(
                                db,
                                LEARNING_V46_SENTINEL_PAYLOAD_COLUMNS,
                            )
                            requireP1LearningAuthoritySchema(db)
                        }
                    }
                    47 -> {
                        check(!includeP1Floor && !createStream)
                        requireHealthyLearningOutbox(db)
                        requireP1LearningAuthoritySchema(db)
                        requireV47RewardAuthoritySchema(db)
                    }
                    48 -> {
                        check(!includeP1Floor && !createStream)
                        requireHealthyLearningOutbox(db)
                        requireP1LearningAuthoritySchema(db)
                        requireV47RewardAuthoritySchema(db)
                        requireV48PolicyGrantSchema(db)
                    }
                    49 -> {
                        check(!includeP1Floor && !createStream)
                        requireHealthyLearningOutbox(db)
                        requireP1LearningAuthoritySchema(db)
                        requireV47RewardAuthoritySchema(db)
                        requireV48PolicyGrantSchema(db)
                        requireV49WorkflowSchema(db)
                    }
                    else -> error("Unsupported staged migration start version")
                }
                requireExactAuthorityStream(db, expectedStreamId, expectedHeadSeq)
                STAGED_COLD_RESTORE_MIGRATIONS
                    .dropWhile { it.startVersion < expectedStartVersion }
                    .forEach { migration ->
                        check(db.version == migration.startVersion) {
                            "Staged cold-restore migration chain is discontinuous"
                        }
                        when (migration) {
                            MIGRATION_46_47 -> migrateV46ToV47Raw(db)
                            MIGRATION_47_48 -> migrateV47ToV48Raw(db)
                            MIGRATION_48_49 -> migrateV48ToV49Raw(db)
                            MIGRATION_49_50 -> migrateV49ToV50Raw(db)
                            else -> error(
                                "Staged cold-restore migration chain contains an unsupported migration",
                            )
                        }
                    }
                check(db.version == EXPECTED_VERSION && readRoomIdentityHash(db) == EXPECTED_IDENTITY_HASH) {
                    "Staged cold-restore migration did not reach the current version"
                }
                db.setTransactionSuccessful()
            } finally {
                db.endTransaction()
            }
        }
    }

    /** Raw-SQL adapter for the exact DDL exported by [MIGRATION_46_47]. */
    private fun migrateV46ToV47Raw(db: SQLiteDatabase) {
        check(db.version == 46) { "Raw 46 -> 47 migration received the wrong version" }
        requireHealthyLearningOutbox(db, LEARNING_V46_SENTINEL_PAYLOAD_COLUMNS)
        ensureColumns(db, "learning_outbox", LEARNING_V47_OUTBOX_COLUMNS)
        db.execSQL(LEARNING_V47_REWARD_FEEDBACK_AUTHORITY_TABLE_SQL)
        db.execSQL(LEARNING_V47_REWARD_FEEDBACK_REVISIONS_TABLE_SQL)
        LEARNING_V47_REWARD_FEEDBACK_INDEX_SQL.forEach(db::execSQL)
        requireHealthyLearningOutbox(db, LEARNING_V47_SENTINEL_PAYLOAD_COLUMNS)
        requireV47RewardAuthoritySchema(db)
        db.version = 47
        stampIdentity(db, FINAL_V47_IDENTITY_HASH)
    }

    /** Raw-SQL adapter for the exact DDL exported by [MIGRATION_47_48]. */
    private fun migrateV47ToV48Raw(db: SQLiteDatabase) {
        check(db.version == 47) { "Raw 47 -> 48 migration received the wrong version" }
        requireHealthyLearningOutbox(db)
        requireP1LearningAuthoritySchema(db)
        requireV47RewardAuthoritySchema(db)
        db.execSQL(LEARNING_V48_POLICY_GRANTS_TABLE_SQL)
        db.execSQL(LEARNING_V48_POLICY_GRANT_REVISIONS_TABLE_SQL)
        LEARNING_V48_POLICY_GRANT_INDEX_SQL.forEach(db::execSQL)
        requireV48PolicyGrantSchema(db)
        db.version = 48
        stampIdentity(db, FINAL_V48_IDENTITY_HASH)
    }

    /** Raw-SQL mirror of [MIGRATION_48_49], including Kotlin row-wise JSON backfill. */
    private fun migrateV48ToV49Raw(db: SQLiteDatabase) {
        check(db.version == 48) { "Raw 48 -> 49 migration received the wrong version" }
        requireHealthyLearningOutbox(db)
        requireP1LearningAuthoritySchema(db)
        requireV47RewardAuthoritySchema(db)
        requireV48PolicyGrantSchema(db)
        ensureColumns(db, "workflows", WORKFLOW_V49_COLUMNS)
        backfillWorkflowV49RowsRaw(db)
        requireV49WorkflowSchema(db)
        db.version = FINAL_V49_VERSION
        stampIdentity(db, FINAL_V49_IDENTITY_HASH)
    }

    private fun migrateV49ToV50Raw(db: SQLiteDatabase) {
        check(db.version == FINAL_V49_VERSION && readRoomIdentityHash(db) == FINAL_V49_IDENTITY_HASH) {
            "Raw 49 -> 50 migration requires the frozen v49 identity"
        }
        requireV49WorkflowSchema(db)
        applyMigration49To50Sql(db::execSQL)
        requireV50DreamSchema(db)
        db.version = EXPECTED_VERSION
        stampIdentity(db, EXPECTED_IDENTITY_HASH)
    }

    private fun backfillWorkflowV49RowsRaw(db: SQLiteDatabase) {
        val rows = db.rawQuery(
            "SELECT `id`, `definitionJson`, `enabled` FROM `workflows`",
            null,
        ).use { cursor ->
            val idIndex = cursor.getColumnIndexOrThrow("id")
            val definitionIndex = cursor.getColumnIndexOrThrow("definitionJson")
            val enabledIndex = cursor.getColumnIndexOrThrow("enabled")
            buildList {
                while (cursor.moveToNext()) {
                    workflowV49Backfill(
                        definitionJson = cursor.getString(definitionIndex),
                        projectedEnabled = cursor.getInt(enabledIndex) != 0,
                    )?.let { backfill ->
                        add(cursor.getString(idIndex) to backfill)
                    }
                }
            }
        }
        val statement = db.compileStatement(
            "UPDATE `workflows` SET `definitionJson` = ?, `origin` = 'USER', " +
                "`sourceCandidateId` = NULL, `sourceArtifactHash` = NULL, `grantDigest` = NULL, " +
                "`authoringAssistantId` = ?, `capabilitySnapshotJson` = ?, " +
                "`toolSchemaFingerprintsJson` = '[]', `staleReason` = NULL WHERE `id` = ?",
        )
        rows.forEach { (id, backfill) ->
            statement.clearBindings()
            statement.bindString(1, backfill.definitionJson)
            backfill.authoringAssistantId?.let { statement.bindString(2, it) }
                ?: statement.bindNull(2)
            statement.bindString(3, backfill.capabilitySnapshotJson)
            statement.bindString(4, id)
            check(statement.executeUpdateDelete() == 1) { "Workflow v49 raw backfill lost row" }
        }
    }

    /** Reopens a reconciled candidate read-only and throws unless its identity is safe to swap. */
    @Throws(Exception::class)
    fun validateStagedFileOrThrow(
        databaseFile: File,
        expectedStreamId: String,
        expectedHeadSeq: Long,
    ) {
        requireAuthorityStreamDescriptor(expectedStreamId, expectedHeadSeq)
        requireSafeStagedFile(databaseFile)
        validateCurrentAuthorityFileOrThrow(
            databaseFile = databaseFile,
            expectedStreamId = expectedStreamId,
            expectedHeadSeq = expectedHeadSeq,
        )
    }

    /**
     * Validates the exact live main database after a cold-start swap.
     *
     * This is deliberately separate from [validateStagedFileOrThrow]: allowing the installed
     * filename in the staged reconciler would let a caller mutate the live database through a
     * migration-only API. This method is read-only and accepts only `<app data>/databases/rikka_hub`.
     */
    @Throws(Exception::class)
    fun validateInstalledFileOrThrow(
        databaseFile: File,
        expectedStreamId: String,
        expectedHeadSeq: Long,
    ) {
        requireAuthorityStreamDescriptor(expectedStreamId, expectedHeadSeq)
        requireSafeInstalledFile(databaseFile)
        validateCurrentAuthorityFileOrThrow(
            databaseFile = databaseFile,
            expectedStreamId = expectedStreamId,
            expectedHeadSeq = expectedHeadSeq,
        )
    }

    private fun validateCurrentAuthorityFileOrThrow(
        databaseFile: File,
        expectedStreamId: String,
        expectedHeadSeq: Long,
    ) {
        SQLiteDatabase.openDatabase(
            databaseFile.absolutePath,
            null,
            SQLiteDatabase.OPEN_READONLY,
        ).use { db ->
            val version = db.version
            check(version == EXPECTED_VERSION) {
                "Reconciled staged database is not at the current version"
            }
            check(readRoomIdentityHash(db) == EXPECTED_IDENTITY_HASH) {
                "Staged current-version database identity was not reconciled"
            }
            db.rawQuery("PRAGMA quick_check(1)", null).use { cursor ->
                check(cursor.moveToFirst() && cursor.getString(0) == "ok" && !cursor.moveToNext()) {
                    "Staged database failed SQLite quick_check"
                }
            }
            db.rawQuery("PRAGMA foreign_key_check", null).use { cursor ->
                check(!cursor.moveToFirst()) { "Staged database failed foreign_key_check" }
            }
            requireHealthyLearningOutbox(db)
            requireP1LearningAuthoritySchema(db)
            requireV47RewardAuthoritySchema(db)
            requireV48PolicyGrantSchema(db)
            requireV49WorkflowSchema(db)
            requireV50DreamSchema(db)
            db.rawQuery(
                "SELECT `seq` FROM `learning_outbox` " +
                    "WHERE `stream_id` = ? AND `event_type` = 'STREAM_INIT'",
                arrayOf(expectedStreamId),
            ).use { cursor ->
                check(cursor.moveToFirst() && cursor.getLong(0) == 1L && !cursor.moveToNext()) {
                    "Staged database authority stream does not start with its sentinel"
                }
            }
            db.rawQuery(
                "SELECT COUNT(*), MIN(`seq`), MAX(`seq`), COUNT(DISTINCT `seq`) " +
                    "FROM `learning_outbox` WHERE `stream_id` = ?",
                arrayOf(expectedStreamId),
            ).use { cursor ->
                check(cursor.moveToFirst()) { "Authority stream query returned no row" }
                val count = cursor.getLong(0)
                val minimum = cursor.getLong(1)
                val maximum = cursor.getLong(2)
                val distinct = cursor.getLong(3)
                check(minimum == 1L && maximum == expectedHeadSeq &&
                    count > 0L && distinct == count
                ) {
                    "Staged database authority stream does not match the manifest"
                }
            }
        }
    }

    private fun requireAuthorityStreamDescriptor(
        expectedStreamId: String,
        expectedHeadSeq: Long,
    ) {
        require(isCanonicalNonNilDatabaseUuid(expectedStreamId)) {
            "Expected authority stream ID is not canonical"
        }
        require(expectedHeadSeq > 0L) { "Expected authority stream head is invalid" }
    }

    private fun normalizeStagedToSingleFileOrThrow(databaseFile: File) {
        requireSafeStagedFile(databaseFile)
        SQLiteDatabase.openDatabase(
            databaseFile.absolutePath,
            null,
            SQLiteDatabase.OPEN_READWRITE,
        ).use { db ->
            db.rawQuery("PRAGMA wal_checkpoint(TRUNCATE)", null).use { cursor ->
                check(cursor.moveToFirst() && cursor.getInt(0) == 0) {
                    "Staged database WAL checkpoint was busy"
                }
                val logFrames = cursor.getLong(1)
                val checkpointedFrames = cursor.getLong(2)
                check(logFrames == checkpointedFrames ||
                    (logFrames == -1L && checkpointedFrames == -1L)
                ) {
                    "Staged database WAL checkpoint was incomplete"
                }
            }
            db.rawQuery("PRAGMA journal_mode=DELETE", null).use { cursor ->
                check(cursor.moveToFirst() && cursor.getString(0).equals("delete", true)) {
                    "Staged database could not enter single-file journal mode"
                }
            }
        }
        val stagedPath = databaseFile.toPath().toAbsolutePath().normalize()
        for (suffix in listOf("-wal", "-shm", "-journal")) {
            val sidecar = stagedPath.resolveSibling("${stagedPath.fileName}$suffix")
            if (!Files.exists(sidecar, LinkOption.NOFOLLOW_LINKS)) continue
            check(!Files.isSymbolicLink(sidecar) &&
                Files.isRegularFile(sidecar, LinkOption.NOFOLLOW_LINKS)
            ) {
                "Staged database sidecar is unsafe"
            }
            check(Files.deleteIfExists(sidecar)) { "Staged database sidecar cleanup failed" }
        }
    }

    private fun reconcileFile(
        dbFile: File,
        swallowFailures: Boolean,
    ) {
        try {
            SQLiteDatabase.openDatabase(
                dbFile.absolutePath,
                null,
                SQLiteDatabase.OPEN_READWRITE,
            ).use { db ->
                val version = db.version // PRAGMA user_version
                val identityHash = readRoomIdentityHash(db)
                when (reconcilePlan(version, identityHash)) {
                    ReconcilePlan.SKIP -> {
                        if (version > EXPECTED_VERSION) {
                            Log.w(
                                TAG,
                                "reconcile: backup db version $version is newer than " +
                                    "$EXPECTED_VERSION; leaving untouched",
                            )
                        } else {
                            Log.i(TAG, "reconcile: current schema already verified; skipping")
                        }
                        return
                    }
                    ReconcilePlan.REFUSE_UNKNOWN_CURRENT -> {
                        Log.e(
                            TAG,
                            "reconcile: refusing unknown v46+ identity; left untouched",
                        )
                        return
                    }
                    ReconcilePlan.FULL_COMPATIBILITY -> Unit
                }

                // A pre-v46 import normally has no Learning table and reaches it through 45 -> 46.
                // If a preview table is already present, verify it before compatibility writes.
                // v46 is handled transactionally below: columns are brought to the exact final-v46
                // floor and then its sentinel/mixed-stream invariants are checked before commit.
                if (tableExists(db, "learning_outbox") && version < 46) {
                    requireHealthyLearningOutbox(
                        db,
                        LEARNING_V46_SENTINEL_PAYLOAD_COLUMNS,
                    )
                }

                db.beginTransaction()
                try {
                    FORK_ONLY_DDL.forEach(db::execSQL)
                    ensureScheduledJobsV29Column(db)
                    ensureConversationFolderV29Column(db)

                    // A shared upstream user_version may already be ahead of the first agent
                    // memory migration. Add only the schema floors Room will no longer visit.
                    if (version >= 31) {
                        ensureMemoryV31Columns(db)
                        ensureMemoryV31CaptureColumns(db)
                    }
                    if (version >= 32) ensureMemoryV32Schema(db)
                    if (version >= 33) ensureMemoryV33Schema(db)
                    if (version >= 34) ensureBrowserV34Schema(db)
                    if (version >= 35) {
                        ensureColumns(
                            db,
                            "workspaces",
                            listOf("storage_mode" to "TEXT NOT NULL DEFAULT 'PRIVATE'"),
                        )
                        ensureExecutionV35Schema(db)
                        ensureCapabilityGrantsV35Schema(db)
                    }
                    if (version >= 36) {
                        db.execSQL(
                            "CREATE INDEX IF NOT EXISTS `index_message_node_conversation_id_node_index` " +
                                "ON `message_node` (`conversation_id`, `node_index`)",
                        )
                    }
                    if (version >= 37) ensureExecutionV37Schema(db)
                    if (version >= 38) ensurePetV38Schema(db)
                    if (version >= 39) ensurePendingCommandAuthorityV39Schema(db)
                    if (version >= 40) ensureToolExperienceV40Schema(db)
                    if (version >= 41) ensureToolShortcutV41Schema(db)
                    if (version >= 42) ensureOwnerHostV42Schema(db)
                    if (version >= 43) ensureMemoryV43Schema(db)
                    if (version >= 44) ensureMemoryV44Schema(db)
                    if (version >= 45) ensureMemoryV45Schema(db)
                    if (version >= 46) {
                        ensureMemoryV46Schema(db)
                        ensureLearningV46SchemaForUpgrade(db)
                    }

                    when (version) {
                        46 -> {
                            migrateV46ToV47Raw(db)
                            migrateV47ToV48Raw(db)
                            migrateV48ToV49Raw(db)
                        }
                        47 -> {
                            migrateV47ToV48Raw(db)
                            migrateV48ToV49Raw(db)
                        }
                        48 -> migrateV48ToV49Raw(db)
                    }
                    if (version in 46..49) migrateV49ToV50Raw(db)

                    // Pre-v46 backups must keep their original user_version so Room can run
                    // every real migration (including 28→29). Precreating fork-only tables
                    // makes upstream backups compatible, but stamping the current version here
                    // would silently skip migrations and risk losing schema changes. Exact v46
                    // and v47/v48/v49 inputs use the explicit raw migration chain above.
                    if (version < 46) {
                        Log.i(TAG, "reconcile: kept older user_version=$version for Room migrations")
                    } else {
                        check(db.version == EXPECTED_VERSION) {
                            "Exact v46+ raw migration did not reach the current version"
                        }
                        Log.i(TAG, "reconcile: migrated exact v$version input to v$EXPECTED_VERSION")
                    }
                    db.setTransactionSuccessful()
                } finally {
                    db.endTransaction()
                }
                Log.i(TAG, "reconcile: reconciled imported db (version=$version)")
            }
        } catch (t: Throwable) {
            if (!swallowFailures) throw t
            // Never let reconciliation break the restore. Worst case is the pre-existing
            // behaviour (a crash on next open); the user's rows are still on disk.
            Log.w(TAG, "reconcile: failed to reconcile imported db", t)
        }
    }

    private fun requireSafeStagedFile(databaseFile: File) {
        val path = requireSafeDatabaseFile(databaseFile)
        check(STAGED_DATABASE_FILE.matches(path.fileName.toString())) {
            "Staged database file name is outside the cold-restore contract"
        }
    }

    private fun requireSafeInstalledFile(databaseFile: File) {
        val path = requireSafeDatabaseFile(databaseFile)
        check(path.fileName.toString() == INSTALLED_DATABASE_FILE) {
            "Installed database file name is outside the cold-restore contract"
        }
    }

    private fun requireSafeDatabaseFile(databaseFile: File): java.nio.file.Path {
        require(databaseFile.isAbsolute && databaseFile.path.isNotBlank()) {
            "Database path must be absolute"
        }
        val path = databaseFile.toPath().toAbsolutePath().normalize()
        check(!Files.isSymbolicLink(path)) { "Database must not be a symbolic link" }
        check(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            "Database must be a regular file"
        }
        check(Files.isReadable(path) && Files.isWritable(path)) {
            "Database must be readable and writable"
        }
        check(databaseFile.canonicalFile.toPath().normalize() == path) {
            "Database path must be canonical"
        }
        val parent = path.parent ?: error("Database has no parent")
        check(parent.fileName.toString() == "databases") {
            "Database is not in an app databases directory"
        }
        check(!Files.isSymbolicLink(parent) &&
            Files.isDirectory(parent, LinkOption.NOFOLLOW_LINKS) &&
            parent.toFile().canonicalFile.toPath().normalize() == parent
        ) {
            "Database parent is unsafe"
        }
        return path
    }

    private val STAGED_DATABASE_FILE =
        Regex("\\.rikka_hub\\.restore_[0-9a-f]{32}\\.ready")
    private const val INSTALLED_DATABASE_FILE = "rikka_hub"
    private const val NIL_UUID = "00000000-0000-0000-0000-000000000000"
    /** Shared head fields compared one-for-one with the current audit revision on restore. */
    internal val V48_POLICY_GRANT_HEAD_COLUMNS = listOf(
        "grant_id",
        "source_stream_id",
        "policy_id",
        "policy_revision",
        "artifact_sha256",
        "scope_kind",
        "scope_id",
        "consuming_assistant_id",
        "actor",
        "state",
        "state_version",
        "granted_at_ms",
        "revoked_at_ms",
        "reason_code",
        "created_at_ms",
        "updated_at_ms",
    )
}
