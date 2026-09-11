package me.rerere.rikkahub.data.db.dao

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import kotlinx.coroutines.flow.Flow
import me.rerere.rikkahub.data.db.entity.DreamExperienceEntity
import me.rerere.rikkahub.data.db.entity.DreamExperienceStateEntity

@Dao
interface DreamExperienceDao {
    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertStateIfAbsent(state: DreamExperienceStateEntity): Long

    @Query("SELECT * FROM dream_experience_state WHERE pair_scope_id = :pairScopeId LIMIT 1")
    suspend fun getState(pairScopeId: String): DreamExperienceStateEntity?

    @Query("SELECT * FROM dream_experience_state WHERE pair_scope_id = :pairScopeId LIMIT 1")
    fun observeState(pairScopeId: String): Flow<DreamExperienceStateEntity?>

    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertExperienceIgnore(experience: DreamExperienceEntity): Long

    @Query(
        "SELECT * FROM dream_experiences WHERE experience_id = :experienceId " +
            "AND pair_scope_id = :pairScopeId LIMIT 1",
    )
    suspend fun getExperience(experienceId: String, pairScopeId: String): DreamExperienceEntity?

    @Query(
        "SELECT * FROM dream_experiences WHERE pair_scope_id = :pairScopeId " +
            "AND experience_epoch > :afterExclusiveEpoch AND experience_epoch <= :throughInclusiveEpoch " +
            "ORDER BY experience_epoch ASC LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listExperiences(
        pairScopeId: String,
        afterExclusiveEpoch: Long,
        throughInclusiveEpoch: Long,
        limit: Int,
    ): List<DreamExperienceEntity>

    /**
     * Returns a bounded, deterministic synthesis working set instead of materializing the entire
     * pair ledger in one Room transaction. Explicit user corrections/rejections are authoritative
     * and therefore lead the set; the remaining rows are ranked by their persisted Dream signal.
     */
    @Query(
        "SELECT * FROM dream_experiences WHERE pair_scope_id = :pairScopeId " +
            "AND experience_epoch > :afterExclusiveEpoch AND experience_epoch <= :throughInclusiveEpoch " +
            "AND status != 'DISCARDED' ORDER BY " +
            "CASE WHEN experience_kind IN ('USER_CORRECTION', 'USER_REJECTION') THEN 0 ELSE 1 END ASC, " +
            "CASE WHEN experience_kind IN ('USER_CORRECTION', 'USER_REJECTION') " +
            "THEN experience_epoch ELSE NULL END DESC, " +
            "(salience + novelty + identity_weight + relationship_weight + emotional_weight) DESC, " +
            "experience_epoch DESC, experience_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listSynthesisExperiences(
        pairScopeId: String,
        afterExclusiveEpoch: Long,
        throughInclusiveEpoch: Long,
        limit: Int,
    ): List<DreamExperienceEntity>

    @Query(
        "SELECT COUNT(*) FROM dream_experiences WHERE pair_scope_id = :pairScopeId " +
            "AND experience_epoch > :afterExclusiveEpoch AND status != 'DISCARDED'",
    )
    suspend fun countPending(pairScopeId: String, afterExclusiveEpoch: Long): Int

    @Query(
        "UPDATE dream_experience_state SET experience_epoch = :nextEpoch, observer_checkpoint_epoch = :nextEpoch, " +
            "experience_debt = experience_debt + :debtDelta, updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE pair_scope_id = :pairScopeId AND experience_epoch = :expectedEpoch",
    )
    suspend fun advanceExperienceEpoch(
        pairScopeId: String,
        expectedEpoch: Long,
        nextEpoch: Long,
        debtDelta: Double,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_experience_state SET observer_checkpoint_epoch = :targetEpoch, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) WHERE pair_scope_id = :pairScopeId " +
            "AND experience_epoch = :expectedExperienceEpoch " +
            "AND observer_checkpoint_epoch = :expectedCheckpointEpoch " +
            "AND :targetEpoch >= :expectedCheckpointEpoch AND :targetEpoch <= :expectedExperienceEpoch",
    )
    suspend fun advanceObserverCheckpoint(
        pairScopeId: String,
        expectedExperienceEpoch: Long,
        expectedCheckpointEpoch: Long,
        targetEpoch: Long,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_experience_state SET applied_experience_epoch = :targetEpoch, " +
            "profile_revision = :nextProfileRevision, active_snapshot_id = :activeSnapshotId, " +
            "experience_debt = :remainingDebt, updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE pair_scope_id = :pairScopeId AND experience_epoch = :expectedExperienceEpoch " +
            "AND applied_experience_epoch = :expectedAppliedEpoch AND profile_revision = :expectedProfileRevision " +
            "AND :targetEpoch >= :expectedAppliedEpoch AND :targetEpoch <= :expectedExperienceEpoch",
    )
    suspend fun advanceApplied(
        pairScopeId: String,
        expectedExperienceEpoch: Long,
        expectedAppliedEpoch: Long,
        targetEpoch: Long,
        expectedProfileRevision: Long,
        nextProfileRevision: Long,
        activeSnapshotId: String?,
        remainingDebt: Double,
        nowMs: Long,
    ): Int

    @Query(
        "SELECT * FROM dream_experience_state WHERE applied_experience_epoch < experience_epoch " +
            "AND observer_checkpoint_epoch = experience_epoch ORDER BY updated_at_ms ASC, pair_scope_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun findSynthesisDirtyStates(limit: Int): List<DreamExperienceStateEntity>

    @Query(
        "UPDATE dream_experience_state SET active_run_id = :runId, " +
            "active_run_lease_until_ms = :leaseUntilMs, updated_at_ms = MAX(updated_at_ms, :nowMs), " +
            "last_reason_code = :reasonCode WHERE pair_scope_id = :pairScopeId " +
            "AND :leaseUntilMs > :nowMs AND (active_run_id IS NULL OR active_run_lease_until_ms IS NULL " +
            "OR active_run_lease_until_ms <= :nowMs)",
    )
    suspend fun acquireLease(
        pairScopeId: String,
        runId: String,
        leaseUntilMs: Long,
        nowMs: Long,
        reasonCode: String,
    ): Int

    @Query(
        "UPDATE dream_experience_state SET active_run_lease_until_ms = :leaseUntilMs, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE pair_scope_id = :pairScopeId AND active_run_id = :runId " +
            "AND active_run_lease_until_ms IS NOT NULL AND active_run_lease_until_ms > :nowMs " +
            "AND :leaseUntilMs > active_run_lease_until_ms",
    )
    suspend fun heartbeatLease(
        pairScopeId: String,
        runId: String,
        leaseUntilMs: Long,
        nowMs: Long,
        reasonCode: String,
    ): Int

    @Query(
        "UPDATE dream_experience_state SET active_run_id = NULL, active_run_lease_until_ms = NULL, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE pair_scope_id = :pairScopeId AND active_run_id = :runId",
    )
    suspend fun releaseLease(
        pairScopeId: String,
        runId: String,
        nowMs: Long,
        reasonCode: String,
    ): Int

    @Query(
        "UPDATE dream_experience_state SET active_run_id = NULL, active_run_lease_until_ms = NULL, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE active_run_id IS NOT NULL AND (active_run_lease_until_ms IS NULL " +
            "OR active_run_lease_until_ms <= :nowMs OR NOT EXISTS (" +
            "SELECT 1 FROM dream_runs r WHERE r.run_id = dream_experience_state.active_run_id " +
            "AND r.scope_id = dream_experience_state.pair_scope_id AND r.status = 'RUNNING' " +
            "AND r.lease_until_ms = dream_experience_state.active_run_lease_until_ms " +
            "AND r.lease_until_ms > :nowMs))",
    )
    suspend fun recoverExpiredLeases(nowMs: Long, reasonCode: String): Int

    @Query(
        "UPDATE dream_experience_state SET history_backfilled_at_ms = :nowMs, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) WHERE pair_scope_id = :pairScopeId " +
            "AND history_backfilled_at_ms IS NULL",
    )
    suspend fun markHistoryBackfilled(pairScopeId: String, nowMs: Long): Int

    @Query(
        "UPDATE dream_runs SET status = 'RUNNING', attempt = attempt + 1, " +
            "base_memory_epoch = :baseExperienceEpoch, " +
            "base_observer_checkpoint_epoch = :baseObserverCheckpointEpoch, " +
            "base_dream_revision = :baseProfileRevision, checkpoint_epoch = :baseObserverCheckpointEpoch, " +
            "source_timezone_id = COALESCE(source_timezone_id, :sourceTimezoneId), " +
            "lease_owner = :leaseOwner, lease_until_ms = :leaseUntilMs, " +
            "started_at_ms = COALESCE(started_at_ms, MAX(created_at_ms, :nowMs)), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), failure_code = NULL " +
            "WHERE run_id = :runId AND scope_id = :pairScopeId AND status = 'PENDING' " +
            "AND checkpoint_epoch = base_observer_checkpoint_epoch " +
            "AND (source_timezone_id IS NULL OR source_timezone_id IS :sourceTimezoneId) " +
            "AND :leaseUntilMs > :nowMs AND EXISTS (SELECT 1 FROM dream_experience_state s " +
            "WHERE s.pair_scope_id = :pairScopeId AND s.active_run_id = :runId " +
            "AND s.active_run_lease_until_ms >= :leaseUntilMs " +
            "AND s.experience_epoch = :baseExperienceEpoch " +
            "AND s.observer_checkpoint_epoch = :baseObserverCheckpointEpoch " +
            "AND s.profile_revision = :baseProfileRevision)",
    )
    suspend fun startRunMirror(
        runId: String,
        pairScopeId: String,
        baseExperienceEpoch: Long,
        baseObserverCheckpointEpoch: Long,
        baseProfileRevision: Long,
        leaseOwner: String,
        leaseUntilMs: Long,
        nowMs: Long,
        sourceTimezoneId: String,
    ): Int

    @Query(
        "UPDATE dream_runs SET lease_until_ms = :leaseUntilMs, updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE run_id = :runId AND scope_id = :pairScopeId AND status = 'RUNNING' " +
            "AND lease_owner = :leaseOwner AND lease_until_ms IS NOT NULL AND lease_until_ms > :nowMs " +
            "AND :leaseUntilMs > lease_until_ms AND EXISTS (SELECT 1 FROM dream_experience_state s " +
            "WHERE s.pair_scope_id = :pairScopeId AND s.active_run_id = :runId " +
            "AND s.active_run_lease_until_ms >= :leaseUntilMs)",
    )
    suspend fun heartbeatRunMirror(
        runId: String,
        pairScopeId: String,
        leaseOwner: String,
        leaseUntilMs: Long,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_runs SET checkpoint_epoch = :targetCheckpointEpoch, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) WHERE run_id = :runId " +
            "AND scope_id = :pairScopeId AND status = 'RUNNING' AND lease_owner = :leaseOwner " +
            "AND lease_until_ms IS NOT NULL AND lease_until_ms > :nowMs " +
            "AND checkpoint_epoch = :expectedCheckpointEpoch " +
            "AND :targetCheckpointEpoch >= :expectedCheckpointEpoch " +
            "AND :targetCheckpointEpoch <= base_memory_epoch " +
            "AND EXISTS (SELECT 1 FROM dream_experience_state s WHERE s.pair_scope_id = :pairScopeId " +
            "AND s.active_run_id = :runId AND s.active_run_lease_until_ms = dream_runs.lease_until_ms " +
            "AND s.active_run_lease_until_ms > :nowMs)",
    )
    suspend fun advanceRunCheckpoint(
        runId: String,
        pairScopeId: String,
        leaseOwner: String,
        expectedCheckpointEpoch: Long,
        targetCheckpointEpoch: Long,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_runs SET status = :terminalStatus, failure_code = :failureCode, " +
            "lease_owner = NULL, lease_until_ms = NULL, finished_at_ms = MAX(updated_at_ms, :nowMs), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) WHERE run_id = :runId " +
            "AND scope_id = :pairScopeId AND status = 'RUNNING' AND lease_owner = :leaseOwner " +
            "AND lease_until_ms IS NOT NULL AND lease_until_ms > :nowMs " +
            "AND :terminalStatus IN ('SUCCEEDED', 'CONFLICT', 'CANCELLED', 'FAILED', 'DISCARDED') " +
            "AND ((:terminalStatus = 'SUCCEEDED' AND :failureCode IS NULL) OR " +
            "(:terminalStatus != 'SUCCEEDED' AND :failureCode IS NOT NULL)) " +
            "AND (:terminalStatus != 'SUCCEEDED' OR checkpoint_epoch = base_memory_epoch) " +
            "AND EXISTS (SELECT 1 FROM dream_experience_state s WHERE s.pair_scope_id = :pairScopeId " +
            "AND s.active_run_id = :runId AND s.active_run_lease_until_ms = dream_runs.lease_until_ms " +
            "AND s.active_run_lease_until_ms > :nowMs)",
    )
    suspend fun finishRunMirror(
        runId: String,
        pairScopeId: String,
        leaseOwner: String,
        terminalStatus: String,
        failureCode: String?,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_runs SET status = 'FAILED', failure_code = :failureCode, " +
            "lease_owner = NULL, lease_until_ms = NULL, finished_at_ms = MAX(updated_at_ms, :nowMs), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) WHERE status = 'RUNNING' " +
            "AND scope_id IN (SELECT pair_scope_id FROM dream_experience_state) " +
            "AND (lease_until_ms IS NULL OR lease_until_ms <= :nowMs OR NOT EXISTS (" +
            "SELECT 1 FROM dream_experience_state s WHERE s.pair_scope_id = dream_runs.scope_id " +
            "AND s.active_run_id = dream_runs.run_id AND s.active_run_lease_until_ms = dream_runs.lease_until_ms " +
            "AND s.active_run_lease_until_ms > :nowMs))",
    )
    suspend fun failExpiredRunMirrors(nowMs: Long, failureCode: String): Int

    @Query(
        "UPDATE dream_claims SET claim_revision = :nextClaimRevision, state = :nextState, " +
            "claim_hash = :claimHash, created_by_run_id = :mutationId, " +
            "last_validated_memory_epoch = :currentExperienceEpoch, " +
            "invalidated_at_ms = MAX(COALESCE(invalidated_at_ms, 0), :nowMs), " +
            "invalidation_reason = :reasonCode, updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE claim_id = :claimId AND scope_id = :pairScopeId " +
            "AND claim_revision = :expectedClaimRevision " +
            "AND state IN ('PENDING_REVIEW', 'ACTIVE_CONTEXTUAL') " +
            "AND :expectedClaimRevision < 9223372036854775807 " +
            "AND :nextClaimRevision = :expectedClaimRevision + 1 " +
            "AND :nowMs >= 0 AND ((:nextState = 'REJECTED' AND :reasonCode = 'USER_REJECTED') " +
            "OR (:nextState = 'SUPERSEDED' AND :reasonCode = 'USER_CORRECTION')) " +
            "AND EXISTS (SELECT 1 FROM dream_experience_state s WHERE s.pair_scope_id = :pairScopeId " +
            "AND s.experience_epoch = :currentExperienceEpoch " +
            "AND s.applied_experience_epoch = :expectedAppliedExperienceEpoch " +
            "AND s.profile_revision = :expectedProfileRevision " +
            "AND s.active_snapshot_id IS :expectedActiveSnapshotId)",
    )
    suspend fun advanceClaimForPairUserReviewCas(
        pairScopeId: String,
        claimId: String,
        expectedClaimRevision: Long,
        nextClaimRevision: Long,
        currentExperienceEpoch: Long,
        expectedAppliedExperienceEpoch: Long,
        expectedProfileRevision: Long,
        expectedActiveSnapshotId: String?,
        nextState: String,
        claimHash: String,
        mutationId: String,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_experience_state SET active_snapshot_id = :newSnapshotId, " +
            "profile_revision = profile_revision + 1, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE pair_scope_id = :pairScopeId AND experience_epoch = :currentExperienceEpoch " +
            "AND applied_experience_epoch = :expectedAppliedExperienceEpoch " +
            "AND profile_revision = :expectedProfileRevision " +
            "AND active_snapshot_id IS :expectedActiveSnapshotId " +
            "AND profile_revision < 9223372036854775807 " +
            "AND :nowMs >= 0 AND :reasonCode IN ('USER_REJECTED', 'USER_CORRECTION') " +
            "AND (:expectedActiveSnapshotId IS NULL OR EXISTS (" +
            "SELECT 1 FROM dream_snapshots prior WHERE prior.snapshot_id = :expectedActiveSnapshotId " +
            "AND prior.scope_id = :pairScopeId AND prior.status = 'SUPERSEDED')) " +
            "AND EXISTS (SELECT 1 FROM dream_snapshots p " +
            "WHERE p.snapshot_id = :newSnapshotId AND p.scope_id = :pairScopeId " +
            "AND p.status = 'ACTIVE' AND p.source_memory_epoch = :currentExperienceEpoch " +
            "AND p.snapshot_revision = :expectedProfileRevision + 1 " +
            "AND p.committed_dream_revision = :expectedProfileRevision + 1 " +
            "AND p.created_by_run_id = :mutationId " +
            "AND p.supersedes_snapshot_id IS :expectedActiveSnapshotId " +
            "AND p.reason_code = :reasonCode)",
    )
    suspend fun advancePairUserReviewSnapshotCas(
        pairScopeId: String,
        currentExperienceEpoch: Long,
        expectedAppliedExperienceEpoch: Long,
        expectedProfileRevision: Long,
        expectedActiveSnapshotId: String?,
        newSnapshotId: String,
        mutationId: String,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_experience_state SET active_snapshot_id = NULL, " +
            "profile_revision = profile_revision + 1, applied_experience_epoch = 0, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE pair_scope_id = :pairScopeId AND experience_epoch = :expectedExperienceEpoch " +
            "AND applied_experience_epoch = :expectedAppliedExperienceEpoch " +
            "AND profile_revision = :expectedProfileRevision " +
            "AND active_snapshot_id IS :expectedActiveSnapshotId " +
            "AND profile_revision < 9223372036854775807 " +
            "AND :nowMs >= 0 AND :reasonCode = 'USER_CLEAR_DERIVED'",
    )
    suspend fun advancePairClearDerivedCas(
        pairScopeId: String,
        expectedExperienceEpoch: Long,
        expectedAppliedExperienceEpoch: Long,
        expectedProfileRevision: Long,
        expectedActiveSnapshotId: String?,
        reasonCode: String,
        nowMs: Long,
    ): Int
}
