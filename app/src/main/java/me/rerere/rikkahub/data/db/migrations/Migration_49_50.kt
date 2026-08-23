package me.rerere.rikkahub.data.db.migrations

import androidx.room.migration.Migration
import androidx.sqlite.db.SupportSQLiteDatabase

val MIGRATION_49_50 = object : Migration(49, 50) {
    override fun migrate(db: SupportSQLiteDatabase) {
        db.execSQL("ALTER TABLE `dream_claims` ADD COLUMN `subject_kind` TEXT NOT NULL DEFAULT 'USER'")
        db.execSQL("ALTER TABLE `dream_claims` ADD COLUMN `profile_section` TEXT NOT NULL DEFAULT 'general'")
        db.execSQL("ALTER TABLE `dream_claims` ADD COLUMN `epistemic_origin` TEXT NOT NULL DEFAULT 'INFERRED'")
        db.execSQL("ALTER TABLE `dream_claims` ADD COLUMN `content_type` TEXT NOT NULL DEFAULT 'OTHER'")
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS `dream_experience_state` (
                `pair_scope_id` TEXT NOT NULL,
                `experience_epoch` INTEGER NOT NULL DEFAULT 0,
                `observer_checkpoint_epoch` INTEGER NOT NULL DEFAULT 0,
                `applied_experience_epoch` INTEGER NOT NULL DEFAULT 0,
                `profile_revision` INTEGER NOT NULL DEFAULT 0,
                `active_snapshot_id` TEXT,
                `experience_debt` REAL NOT NULL DEFAULT 0.0,
                `active_run_id` TEXT,
                `active_run_lease_until_ms` INTEGER,
                `last_reason_code` TEXT,
                `history_backfilled_at_ms` INTEGER,
                `updated_at_ms` INTEGER NOT NULL,
                PRIMARY KEY(`pair_scope_id`)
            )
            """.trimIndent(),
        )
        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_dream_experience_state_active_run_id` ON `dream_experience_state` (`active_run_id`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_dream_experience_state_active_run_lease_until_ms` ON `dream_experience_state` (`active_run_lease_until_ms`)")
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS `dream_experiences` (
                `experience_id` TEXT NOT NULL,
                `pair_scope_id` TEXT NOT NULL,
                `experience_epoch` INTEGER NOT NULL,
                `source_kind` TEXT NOT NULL,
                `source_ref` TEXT NOT NULL,
                `conversation_id` TEXT,
                `occurred_at_ms` INTEGER NOT NULL,
                `ingested_at_ms` INTEGER NOT NULL,
                `actor` TEXT NOT NULL,
                `experience_kind` TEXT NOT NULL,
                `summary` TEXT NOT NULL,
                `salience` REAL NOT NULL,
                `novelty` REAL NOT NULL,
                `identity_weight` REAL NOT NULL,
                `relationship_weight` REAL NOT NULL,
                `emotional_weight` REAL NOT NULL,
                `confidence` REAL NOT NULL,
                `content_digest` TEXT NOT NULL,
                `status` TEXT NOT NULL DEFAULT 'PENDING',
                `source_manifest_json` TEXT NOT NULL DEFAULT '[]',
                PRIMARY KEY(`experience_id`)
            )
            """.trimIndent(),
        )
        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_dream_experiences_pair_scope_id_experience_epoch` ON `dream_experiences` (`pair_scope_id`, `experience_epoch`)")
        db.execSQL("CREATE UNIQUE INDEX IF NOT EXISTS `index_dream_experiences_pair_scope_id_source_kind_source_ref` ON `dream_experiences` (`pair_scope_id`, `source_kind`, `source_ref`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_dream_experiences_pair_scope_id_status_experience_epoch` ON `dream_experiences` (`pair_scope_id`, `status`, `experience_epoch`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_dream_experiences_conversation_id` ON `dream_experiences` (`conversation_id`)")
        db.execSQL(
            """
            CREATE TABLE IF NOT EXISTS `dream_claim_experience_sources` (
                `claim_id` TEXT NOT NULL,
                `claim_revision` INTEGER NOT NULL,
                `experience_id` TEXT NOT NULL,
                `experience_epoch` INTEGER NOT NULL,
                `content_digest` TEXT NOT NULL,
                `source_manifest_hash` TEXT NOT NULL,
                `support_type` TEXT NOT NULL,
                `created_at_ms` INTEGER NOT NULL,
                PRIMARY KEY(`claim_id`, `claim_revision`, `experience_id`, `support_type`),
                FOREIGN KEY(`claim_id`, `claim_revision`) REFERENCES `dream_claim_versions`(`claim_id`, `claim_revision`) ON UPDATE NO ACTION ON DELETE CASCADE,
                FOREIGN KEY(`experience_id`) REFERENCES `dream_experiences`(`experience_id`) ON UPDATE NO ACTION ON DELETE RESTRICT
            )
            """.trimIndent(),
        )
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_dream_claim_experience_sources_claim_id_claim_revision` ON `dream_claim_experience_sources` (`claim_id`, `claim_revision`)")
        db.execSQL("CREATE INDEX IF NOT EXISTS `index_dream_claim_experience_sources_experience_id` ON `dream_claim_experience_sources` (`experience_id`)")
        recoverOrphanedDreamSynthesisRuns(db)
    }
}
