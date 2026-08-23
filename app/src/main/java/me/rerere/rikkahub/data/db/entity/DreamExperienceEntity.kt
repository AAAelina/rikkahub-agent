package me.rerere.rikkahub.data.db.entity

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
    tableName = "dream_experiences",
    indices = [
        Index(value = ["pair_scope_id", "experience_epoch"], unique = true),
        Index(value = ["pair_scope_id", "source_kind", "source_ref"], unique = true),
        Index(value = ["pair_scope_id", "status", "experience_epoch"]),
        Index(value = ["conversation_id"]),
    ],
)
data class DreamExperienceEntity(
    @PrimaryKey
    @ColumnInfo("experience_id")
    val experienceId: String,
    @ColumnInfo("pair_scope_id")
    val pairScopeId: String,
    @ColumnInfo("experience_epoch")
    val experienceEpoch: Long,
    @ColumnInfo("source_kind")
    val sourceKind: String,
    @ColumnInfo("source_ref")
    val sourceRef: String,
    @ColumnInfo("conversation_id")
    val conversationId: String? = null,
    @ColumnInfo("occurred_at_ms")
    val occurredAtMs: Long,
    @ColumnInfo("ingested_at_ms")
    val ingestedAtMs: Long,
    val actor: String,
    @ColumnInfo("experience_kind")
    val experienceKind: String,
    val summary: String,
    val salience: Double,
    val novelty: Double,
    @ColumnInfo("identity_weight")
    val identityWeight: Double,
    @ColumnInfo("relationship_weight")
    val relationshipWeight: Double,
    @ColumnInfo("emotional_weight")
    val emotionalWeight: Double,
    val confidence: Double,
    @ColumnInfo("content_digest")
    val contentDigest: String,
    @ColumnInfo(defaultValue = "'PENDING'")
    val status: String = "PENDING",
    @ColumnInfo(name = "source_manifest_json", defaultValue = "'[]'")
    val sourceManifestJson: String = "[]",
)
