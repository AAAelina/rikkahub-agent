package me.rerere.rikkahub.data.db.entity

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.ForeignKey
import androidx.room.Index
import androidx.room.PrimaryKey

/**
 * Observer/Dream execution audit row.
 *
 * `memory_scope_state` remains a temporary referential parent for the v49 schema. Pair-Dream lease
 * and epoch authority live exclusively in `dream_experience_state`.
 */
@Entity(
    tableName = "dream_runs",
    foreignKeys = [
        ForeignKey(
            entity = MemoryScopeStateEntity::class,
            parentColumns = ["scope_id"],
            childColumns = ["scope_id"],
            onDelete = ForeignKey.CASCADE,
        ),
    ],
    indices = [
        Index(value = ["scope_id", "status", "started_at_ms"]),
        Index(value = ["scope_id", "created_at_ms"]),
        Index(value = ["status", "lease_until_ms"]),
    ],
)
data class DreamRunEntity(
    @PrimaryKey
    @ColumnInfo("run_id")
    val runId: String,
    @ColumnInfo("scope_id")
    val scopeId: String,
    val mode: String,
    @ColumnInfo(defaultValue = "'PENDING'")
    val status: String = "PENDING",
    @ColumnInfo("base_memory_epoch")
    val baseMemoryEpoch: Long,
    @ColumnInfo("base_observer_checkpoint_epoch")
    val baseObserverCheckpointEpoch: Long,
    @ColumnInfo(defaultValue = "0")
    val attempt: Int = 0,
    @ColumnInfo("lease_owner")
    val leaseOwner: String? = null,
    @ColumnInfo("lease_until_ms")
    val leaseUntilMs: Long? = null,
    @ColumnInfo(name = "checkpoint_epoch", defaultValue = "0")
    val checkpointEpoch: Long = 0,
    @ColumnInfo("failure_code")
    val failureCode: String? = null,
    @ColumnInfo("created_at_ms")
    val createdAtMs: Long,
    @ColumnInfo("started_at_ms")
    val startedAtMs: Long? = null,
    @ColumnInfo("updated_at_ms")
    val updatedAtMs: Long,
    @ColumnInfo("finished_at_ms")
    val finishedAtMs: Long? = null,
    @ColumnInfo(name = "base_dream_revision", defaultValue = "0")
    val baseDreamRevision: Long = 0,
    /**
     * Strict IANA timezone frozen by a synthesis run's first claim. Observer-only runs leave it
     * null; a resumed synthesis run must reuse this exact value instead of sampling the device.
     */
    @ColumnInfo("source_timezone_id")
    val sourceTimezoneId: String? = null,
    @ColumnInfo("model_identity_digest")
    val modelIdentityDigest: String? = null,
    @ColumnInfo("provider_kind")
    val providerKind: String? = null,
    @ColumnInfo("prompt_contract_version")
    val promptContractVersion: String? = null,
    @ColumnInfo("validator_version")
    val validatorVersion: String? = null,
    @ColumnInfo("input_memory_count")
    val inputMemoryCount: Int? = null,
    @ColumnInfo("input_tokens")
    val inputTokens: Long? = null,
    @ColumnInfo("output_claim_count")
    val outputClaimCount: Int? = null,
    @ColumnInfo("output_tokens")
    val outputTokens: Long? = null,
    @ColumnInfo("input_manifest_hash")
    val inputManifestHash: String? = null,
    @ColumnInfo("output_manifest_hash")
    val outputManifestHash: String? = null,
)
