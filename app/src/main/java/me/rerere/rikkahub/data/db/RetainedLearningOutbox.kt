package me.rerere.rikkahub.data.db

import android.database.sqlite.SQLiteDatabase
import me.rerere.rikkahub.data.db.entity.isSafeLearningOutboxEventIdentity
import me.rerere.rikkahub.data.db.migrations.LEARNING_V46_STREAM_INIT_EVENT_ID
import me.rerere.rikkahub.data.db.migrations.LEARNING_V47_SENTINEL_PAYLOAD_COLUMNS

internal data class RetainedLearningOutboxHead(val streamId: String, val headSeq: Long)

/**
 * Validates surviving rows in one frozen/private snapshot, not historical completeness.
 * Retention and ignored inserts may leave holes. Neither COUNT nor allocator high-water is a head.
 * Event identity uses the storage contract; unknown future event payloads remain opaque.
 */
internal fun readRetainedLearningOutboxOrThrow(
    db: SQLiteDatabase,
    sentinelPayloadColumns: List<String>? = null,
): RetainedLearningOutboxHead {
    val head = db.rawQuery(
        "SELECT COUNT(*), MIN(seq), MAX(seq), COUNT(DISTINCT seq), " +
            "COUNT(DISTINCT stream_id), COUNT(DISTINCT event_id), " +
            "SUM(CASE WHEN event_type = 'STREAM_INIT' THEN 1 ELSE 0 END), MIN(stream_id) " +
            "FROM learning_outbox",
        null,
    ).use { cursor ->
        check(cursor.moveToFirst()) { "Missing retained outbox summary" }
        val count = cursor.getLong(0)
        check(count > 0L && cursor.getLong(1) == 1L && cursor.getLong(2) > 0L &&
            cursor.getLong(3) == count && cursor.getLong(4) == 1L &&
            cursor.getLong(5) == count && cursor.getLong(6) == 1L) {
            "Invalid retained outbox sequence, lineage, sentinel or event identity uniqueness"
        }
        val stream = cursor.getString(7)
        check(ImportedDatabaseReconciler.isCanonicalNonNilDatabaseUuid(stream)) {
            "Invalid retained outbox stream identity"
        }
        RetainedLearningOutboxHead(stream, cursor.getLong(2))
    }
    // Legacy v46 export may precede the P1/reward schema floor. Check every known payload
    // column that exists; raw migration callers explicitly require their version's full list.
    val payloadColumns = sentinelPayloadColumns ?: db.rawQuery("PRAGMA table_info(learning_outbox)", null).use {
        val actual = buildSet { while (it.moveToNext()) add(it.getString(1)) }
        LEARNING_V47_SENTINEL_PAYLOAD_COLUMNS.filter { column -> column in actual }
    }
    val projection = (listOf("seq", "event_id", "event_schema_version") + payloadColumns)
        .joinToString(", ") { "`$it`" }
    db.rawQuery("SELECT $projection FROM learning_outbox WHERE event_type = 'STREAM_INIT'", null).use {
        check(it.moveToFirst() && it.getLong(0) == 1L &&
            it.getString(1) == LEARNING_V46_STREAM_INIT_EVENT_ID && it.getLong(2) == 1L) {
            "Invalid retained outbox sentinel"
        }
        for (column in 3 until it.columnCount) {
            check(it.isNull(column)) { "Retained outbox sentinel contains payload" }
        }
        check(!it.moveToNext()) { "Duplicate retained outbox sentinel" }
    }
    db.rawQuery(
        "SELECT seq, stream_id, event_id, event_type, event_schema_version, created_at_ms " +
            "FROM learning_outbox ORDER BY seq",
        null,
    ).use {
        var previous = 0L
        while (it.moveToNext()) {
            val seq = it.getLong(0)
            val eventId = it.getString(2)
            val type = it.getString(3)
            check(seq > previous && it.getString(1) == head.streamId &&
                isSafeLearningOutboxEventIdentity(eventId) && STORAGE_EVENT_CODE.matches(type) &&
                (eventId == LEARNING_V46_STREAM_INIT_EVENT_ID) == (type == "STREAM_INIT") &&
                it.getLong(4) in 1L..Int.MAX_VALUE.toLong() && it.getLong(5) >= 0L) {
                "Invalid retained outbox row identity"
            }
            previous = seq
        }
        check(previous == head.headSeq) { "Retained outbox changed during validation" }
    }
    return head
}

private val STORAGE_EVENT_CODE = Regex("[A-Z][A-Z0-9_]{0,63}")
