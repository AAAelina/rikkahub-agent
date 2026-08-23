package me.rerere.rikkahub.data.db.entity

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.ForeignKey
import androidx.room.Index
import androidx.room.PrimaryKey

/** Current derived Claim head. It never replaces an authoritative [MemoryEntity]. */
@Entity(
    tableName = "dream_claims",
    foreignKeys = [
        ForeignKey(
            entity = MemoryScopeStateEntity::class,
            parentColumns = ["scope_id"],
            childColumns = ["scope_id"],
            onDelete = ForeignKey.CASCADE,
        ),
    ],
    indices = [
        Index(value = ["scope_id", "claim_key"], unique = true),
        Index(value = ["scope_id", "state", "updated_at_ms"]),
        Index(value = ["scope_id", "last_validated_memory_epoch"]),
    ],
)
data class DreamClaimEntity(
    @PrimaryKey
    @ColumnInfo("claim_id")
    val claimId: String,
    @ColumnInfo("scope_id")
    val scopeId: String,
    @ColumnInfo("claim_revision")
    val claimRevision: Long,
    @ColumnInfo("claim_key")
    val claimKey: String,
    @ColumnInfo("storage_class")
    val storageClass: String,
    @ColumnInfo("epistemic_type")
    val epistemicType: String,
    @ColumnInfo(name = "subject_kind", defaultValue = "'USER'")
    val subjectKind: String = "USER",
    @ColumnInfo(name = "profile_section", defaultValue = "'general'")
    val profileSection: String = "general",
    @ColumnInfo(name = "epistemic_origin", defaultValue = "'INFERRED'")
    val epistemicOrigin: String = "INFERRED",
    @ColumnInfo(name = "content_type", defaultValue = "'OTHER'")
    val contentType: String = "OTHER",
    val title: String,
    val statement: String,
    val state: String,
    val confidence: Double,
    @ColumnInfo("temporal_state")
    val temporalState: String,
    @ColumnInfo("valid_from_ms")
    val validFromMs: Long? = null,
    @ColumnInfo("valid_to_ms")
    val validToMs: Long? = null,
    @ColumnInfo("learned_at_ms")
    val learnedAtMs: Long,
    @ColumnInfo("source_timezone")
    val sourceTimezone: String,
    @ColumnInfo("claim_hash")
    val claimHash: String,
    @ColumnInfo("created_by_run_id")
    val createdByRunId: String,
    @ColumnInfo("last_validated_memory_epoch")
    val lastValidatedMemoryEpoch: Long,
    @ColumnInfo("invalidated_at_ms")
    val invalidatedAtMs: Long? = null,
    @ColumnInfo("invalidation_reason")
    val invalidationReason: String? = null,
    @ColumnInfo("created_at_ms")
    val createdAtMs: Long,
    @ColumnInfo("updated_at_ms")
    val updatedAtMs: Long,
)
