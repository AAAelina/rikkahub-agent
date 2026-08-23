package me.rerere.rikkahub.data.db.entity

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.ForeignKey
import androidx.room.Index

/** Direct Experience provenance for one immutable pair-Dream Claim revision. */
@Entity(
    tableName = "dream_claim_experience_sources",
    primaryKeys = ["claim_id", "claim_revision", "experience_id", "support_type"],
    foreignKeys = [
        ForeignKey(
            entity = DreamClaimVersionEntity::class,
            parentColumns = ["claim_id", "claim_revision"],
            childColumns = ["claim_id", "claim_revision"],
            onDelete = ForeignKey.CASCADE,
        ),
        ForeignKey(
            entity = DreamExperienceEntity::class,
            parentColumns = ["experience_id"],
            childColumns = ["experience_id"],
            onDelete = ForeignKey.RESTRICT,
        ),
    ],
    indices = [
        Index(value = ["claim_id", "claim_revision"]),
        Index(value = ["experience_id"]),
    ],
)
data class DreamClaimExperienceSourceEntity(
    @ColumnInfo("claim_id") val claimId: String,
    @ColumnInfo("claim_revision") val claimRevision: Long,
    @ColumnInfo("experience_id") val experienceId: String,
    @ColumnInfo("experience_epoch") val experienceEpoch: Long,
    @ColumnInfo("content_digest") val contentDigest: String,
    @ColumnInfo("source_manifest_hash") val sourceManifestHash: String,
    @ColumnInfo("support_type") val supportType: String,
    @ColumnInfo("created_at_ms") val createdAtMs: Long,
)
