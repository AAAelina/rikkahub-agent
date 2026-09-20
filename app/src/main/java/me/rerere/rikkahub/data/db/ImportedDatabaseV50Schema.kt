package me.rerere.rikkahub.data.db

import android.database.sqlite.SQLiteDatabase

/** Frozen v50 structural floor; identity and quick_check alone do not validate a schema. */
internal fun requireV50DreamSchema(db: SQLiteDatabase) {
    requireDreamColumns(db, "dream_experiences", setOf(
        "experience_id|TEXT|1|<null>|1",
        "pair_scope_id|TEXT|1|<null>|0",
        "experience_epoch|INTEGER|1|<null>|0",
        "source_kind|TEXT|1|<null>|0",
        "source_ref|TEXT|1|<null>|0",
        "conversation_id|TEXT|0|<null>|0",
        "occurred_at_ms|INTEGER|1|<null>|0",
        "ingested_at_ms|INTEGER|1|<null>|0",
        "actor|TEXT|1|<null>|0",
        "experience_kind|TEXT|1|<null>|0",
        "summary|TEXT|1|<null>|0",
        "salience|REAL|1|<null>|0",
        "novelty|REAL|1|<null>|0",
        "identity_weight|REAL|1|<null>|0",
        "relationship_weight|REAL|1|<null>|0",
        "emotional_weight|REAL|1|<null>|0",
        "confidence|REAL|1|<null>|0",
        "content_digest|TEXT|1|<null>|0",
        "status|TEXT|1|'PENDING'|0",
        "source_manifest_json|TEXT|1|'[]'|0",
    ), exact = true)
    requireDreamIndex(db, "dream_experiences", "index_dream_experiences_pair_scope_id_experience_epoch", true,
        listOf("pair_scope_id", "experience_epoch"))
    requireDreamIndex(db, "dream_experiences", "index_dream_experiences_pair_scope_id_source_kind_source_ref", true,
        listOf("pair_scope_id", "source_kind", "source_ref"))
    requireDreamIndex(db, "dream_experiences", "index_dream_experiences_pair_scope_id_status_experience_epoch", false,
        listOf("pair_scope_id", "status", "experience_epoch"))
    requireDreamIndex(db, "dream_experiences", "index_dream_experiences_conversation_id", false,
        listOf("conversation_id"))
    requireDreamForeignKeys(db, "dream_experiences", emptySet())
    requireDreamColumns(db, "dream_experience_state", setOf(
        "pair_scope_id|TEXT|1|<null>|1",
        "experience_epoch|INTEGER|1|0|0",
        "observer_checkpoint_epoch|INTEGER|1|0|0",
        "applied_experience_epoch|INTEGER|1|0|0",
        "profile_revision|INTEGER|1|0|0",
        "active_snapshot_id|TEXT|0|<null>|0",
        "experience_debt|REAL|1|0.0|0",
        "active_run_id|TEXT|0|<null>|0",
        "active_run_lease_until_ms|INTEGER|0|<null>|0",
        "last_reason_code|TEXT|0|<null>|0",
        "history_backfilled_at_ms|INTEGER|0|<null>|0",
        "updated_at_ms|INTEGER|1|<null>|0",
    ), exact = true)
    requireDreamIndex(db, "dream_experience_state", "index_dream_experience_state_active_run_id", true,
        listOf("active_run_id"))
    requireDreamIndex(db, "dream_experience_state", "index_dream_experience_state_active_run_lease_until_ms", false,
        listOf("active_run_lease_until_ms"))
    requireDreamForeignKeys(db, "dream_experience_state", emptySet())
    requireDreamColumns(db, "dream_claims", setOf(
        "subject_kind|TEXT|1|'USER'|0",
        "profile_section|TEXT|1|'general'|0",
        "epistemic_origin|TEXT|1|'INFERRED'|0",
        "content_type|TEXT|1|'OTHER'|0",
    ), exact = false)
    requireDreamColumns(db, "dream_claim_experience_sources", setOf(
        "claim_id|TEXT|1|<null>|1",
        "claim_revision|INTEGER|1|<null>|2",
        "experience_id|TEXT|1|<null>|3",
        "experience_epoch|INTEGER|1|<null>|0",
        "content_digest|TEXT|1|<null>|0",
        "source_manifest_hash|TEXT|1|<null>|0",
        "support_type|TEXT|1|<null>|4",
        "created_at_ms|INTEGER|1|<null>|0",
    ), exact = true)
    requireDreamIndex(db, "dream_claim_experience_sources", "index_dream_claim_experience_sources_claim_id_claim_revision", false,
        listOf("claim_id", "claim_revision"))
    requireDreamIndex(db, "dream_claim_experience_sources", "index_dream_claim_experience_sources_experience_id", false,
        listOf("experience_id"))
    requireDreamForeignKeys(db, "dream_claim_experience_sources", setOf(
        "dream_claim_versions|NO ACTION|CASCADE|claim_id:claim_id,claim_revision:claim_revision",
        "dream_experiences|NO ACTION|RESTRICT|experience_id:experience_id",
    ))
}

private fun requireDreamColumns(db: SQLiteDatabase, table: String, expected: Set<String>, exact: Boolean) {
    val actual = db.rawQuery("PRAGMA table_info(`$table`)", null).use { cursor ->
        buildSet {
            while (cursor.moveToNext()) {
                add(listOf(cursor.getString(1), cursor.getString(2).uppercase(),
                    cursor.getInt(3).toString(), cursor.getString(4) ?: "<null>",
                    cursor.getInt(5).toString()).joinToString("|"))
            }
        }
    }
    check(if (exact) actual == expected else actual.containsAll(expected)) {
        "Missing or incompatible v50 Dream columns/primary key: $table"
    }
}

private fun requireDreamIndex(db: SQLiteDatabase, table: String, name: String, unique: Boolean, columns: List<String>) {
    val valid = db.rawQuery("PRAGMA index_list(`$table`)", null).use { cursor ->
        var found = false
        while (cursor.moveToNext()) {
            if (cursor.getString(1) == name) {
                found = cursor.getInt(2) == (if (unique) 1 else 0) && cursor.getInt(4) == 0
            }
        }
        found
    }
    check(valid) { "Missing or incompatible v50 Dream index: $name" }
    val actual = db.rawQuery("PRAGMA index_xinfo(`$name`)", null).use { cursor ->
        buildList {
            while (cursor.moveToNext()) {
                if (cursor.getInt(5) == 1) {
                    check(cursor.getInt(3) == 0 && cursor.getString(4) == "BINARY") {
                        "Incompatible v50 Dream index ordering/collation: $name"
                    }
                    add(cursor.getString(2))
                }
            }
        }
    }
    check(actual == columns) { "Incompatible v50 Dream index columns: $name" }
}

private fun requireDreamForeignKeys(db: SQLiteDatabase, table: String, expected: Set<String>) {
    val actual = db.rawQuery("PRAGMA foreign_key_list(`$table`)", null).use { cursor ->
        val rows = mutableMapOf<Int, MutableList<Pair<Int, List<String>>>>()
        while (cursor.moveToNext()) {
            rows.getOrPut(cursor.getInt(0)) { mutableListOf() }.add(cursor.getInt(1) to
                listOf(cursor.getString(2), cursor.getString(5), cursor.getString(6),
                    cursor.getString(3) + ":" + cursor.getString(4)))
        }
        rows.values.map { parts ->
            val ordered = parts.sortedBy { it.first }.map { it.second }
            ordered.first().take(3).joinToString("|") + "|" + ordered.joinToString(",") { it[3] }
        }.toSet()
    }
    check(actual == expected) { "Missing or incompatible v50 Dream foreign keys: $table" }
}
