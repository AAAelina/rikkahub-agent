package me.rerere.rikkahub.data.db.entity

import androidx.room.ColumnInfo
import androidx.room.Entity
import androidx.room.Index
import androidx.room.PrimaryKey

@Entity(
    tableName = "dream_experience_state",
    indices = [
        Index(value = ["active_run_id"], unique = true),
        Index(value = ["active_run_lease_until_ms"]),
    ],
)
data class DreamExperienceStateEntity(
    @PrimaryKey
    @ColumnInfo("pair_scope_id")
    val pairScopeId: String,
    @ColumnInfo(name = "experience_epoch", defaultValue = "0")
    val experienceEpoch: Long = 0L,
    @ColumnInfo(name = "observer_checkpoint_epoch", defaultValue = "0")
    val observerCheckpointEpoch: Long = 0L,
    @ColumnInfo(name = "applied_experience_epoch", defaultValue = "0")
    val appliedExperienceEpoch: Long = 0L,
    @ColumnInfo(name = "profile_revision", defaultValue = "0")
    val profileRevision: Long = 0L,
    @ColumnInfo("active_snapshot_id")
    val activeSnapshotId: String? = null,
    @ColumnInfo(name = "experience_debt", defaultValue = "0.0")
    val experienceDebt: Double = 0.0,
    @ColumnInfo("active_run_id")
    val activeRunId: String? = null,
    @ColumnInfo("active_run_lease_until_ms")
    val activeRunLeaseUntilMs: Long? = null,
    @ColumnInfo("last_reason_code")
    val lastReasonCode: String? = null,
    @ColumnInfo("history_backfilled_at_ms")
    val historyBackfilledAtMs: Long? = null,
    @ColumnInfo("updated_at_ms")
    val updatedAtMs: Long,
)
