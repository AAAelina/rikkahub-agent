package me.rerere.rikkahub.data.db.dao

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import me.rerere.rikkahub.data.db.entity.DreamRunEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeChangeEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity

data class DreamDailyUsageRow(
    val startedRunCount: Long,
    val knownInputTokens: Long,
    val knownOutputTokens: Long,
    val unmeasuredInputRunCount: Long,
    val unmeasuredOutputRunCount: Long,
)

/**
 * Guarded, single-statement primitives for the observer ledger.
 *
 * Cross-row operations must be composed inside [androidx.room.RoomDatabase.withTransaction].
 * No generic update/replace API is exposed: epochs, checkpoints, run states, and leases all move
 * through compare-and-set statements. The scope-state row is the sole lease authority.
 */
@Dao
interface DreamDao {
    @Insert(onConflict = OnConflictStrategy.IGNORE)
    suspend fun insertScopeStateIfAbsent(state: MemoryScopeStateEntity): Long

    @Query("SELECT * FROM memory_scope_state WHERE scope_id = :scopeId LIMIT 1")
    suspend fun getScopeState(scopeId: String): MemoryScopeStateEntity?

    @Query(
        "SELECT * FROM memory_scope_state " +
            "WHERE memory_epoch > observer_checkpoint_epoch " +
            "ORDER BY updated_at_ms ASC, scope_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun findDirtyScopes(limit: Int): List<MemoryScopeStateEntity>

    @Query(
        "SELECT * FROM memory_scope_state " +
            "WHERE last_applied_memory_epoch < memory_epoch " +
            "AND observer_checkpoint_epoch = memory_epoch " +
            "ORDER BY CASE WHEN EXISTS (SELECT 1 FROM dream_runs r " +
            "WHERE r.scope_id = memory_scope_state.scope_id " +
            "AND r.mode IN ('INCREMENTAL', 'FULL') " +
            "AND r.status IN ('PENDING', 'RUNNING')) THEN 0 ELSE 1 END ASC, " +
            "updated_at_ms ASC, scope_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun findSynthesisDirtyScopes(limit: Int): List<MemoryScopeStateEntity>

    @Query(
        "SELECT * FROM memory_scope_state WHERE active_run_id IS NOT NULL " +
            "AND (active_run_lease_until_ms IS NULL OR active_run_lease_until_ms <= :nowMs) " +
            "ORDER BY active_run_lease_until_ms ASC, scope_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun findExpiredScopeLeases(
        nowMs: Long,
        limit: Int,
    ): List<MemoryScopeStateEntity>

    @Query(
        "UPDATE memory_scope_state SET memory_epoch = memory_epoch + 1, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE scope_id = :scopeId AND memory_epoch = :expectedMemoryEpoch " +
            "AND memory_epoch < 9223372036854775807",
    )
    suspend fun bumpMemoryEpoch(
        scopeId: String,
        expectedMemoryEpoch: Long,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertChanges(changes: List<MemoryScopeChangeEntity>)

    @Query(
        "SELECT * FROM memory_scope_changes WHERE scope_id = :scopeId " +
            "AND memory_epoch > :afterExclusiveEpoch " +
            "AND memory_epoch <= :throughInclusiveEpoch " +
            "ORDER BY memory_epoch ASC, change_id ASC",
    )
    suspend fun listChanges(
        scopeId: String,
        afterExclusiveEpoch: Long,
        throughInclusiveEpoch: Long,
    ): List<MemoryScopeChangeEntity>

    @Query(
        "SELECT COUNT(*) FROM memory_scope_changes WHERE scope_id = :scopeId " +
            "AND memory_epoch <= :throughInclusiveEpoch",
    )
    suspend fun countChangesThrough(scopeId: String, throughInclusiveEpoch: Long): Int

    @Query(
        "UPDATE memory_scope_state SET observer_checkpoint_epoch = :targetCheckpointEpoch, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE scope_id = :scopeId AND memory_epoch = :expectedMemoryEpoch " +
            "AND observer_checkpoint_epoch = :expectedCheckpointEpoch " +
            "AND active_run_id = :runId AND active_run_lease_until_ms > :nowMs " +
            "AND :targetCheckpointEpoch >= :expectedCheckpointEpoch " +
            "AND :targetCheckpointEpoch <= :expectedMemoryEpoch",
    )
    suspend fun advanceObserverCheckpoint(
        scopeId: String,
        runId: String,
        expectedMemoryEpoch: Long,
        expectedCheckpointEpoch: Long,
        targetCheckpointEpoch: Long,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Query(
        "DELETE FROM memory_scope_changes WHERE scope_id = :scopeId " +
            "AND memory_epoch <= :throughInclusiveEpoch " +
            "AND EXISTS (SELECT 1 FROM memory_scope_state s " +
            "WHERE s.scope_id = :scopeId " +
            "AND s.observer_checkpoint_epoch >= :throughInclusiveEpoch " +
            "AND s.last_applied_memory_epoch >= :throughInclusiveEpoch) " +
            "AND NOT EXISTS (SELECT 1 FROM dream_runs r " +
            "WHERE r.scope_id = :scopeId AND r.status IN ('PENDING', 'RUNNING') " +
            "AND r.base_observer_checkpoint_epoch < :throughInclusiveEpoch)",
    )
    suspend fun pruneChangesThrough(scopeId: String, throughInclusiveEpoch: Long): Int

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertRun(run: DreamRunEntity)

    @Query("SELECT * FROM dream_runs WHERE run_id = :runId AND scope_id = :scopeId LIMIT 1")
    suspend fun getRun(runId: String, scopeId: String): DreamRunEntity?

    @Query("SELECT * FROM dream_runs WHERE run_id = :runId LIMIT 1")
    suspend fun getRunById(runId: String): DreamRunEntity?

    @Query(
        "SELECT r.* FROM dream_runs r WHERE r.scope_id = :scopeId " +
            "AND r.mode IN ('INCREMENTAL', 'FULL') " +
            "AND r.status IN ('PENDING', 'RUNNING') " +
            "ORDER BY CASE " +
            "WHEN r.run_id = (SELECT s.active_run_id FROM memory_scope_state s " +
            "WHERE s.scope_id = :scopeId) THEN 0 " +
            "WHEN r.status = 'RUNNING' THEN 1 ELSE 2 END, " +
            "r.created_at_ms ASC, r.run_id ASC LIMIT 1",
    )
    suspend fun findPendingOrRunningSynthesisRun(scopeId: String): DreamRunEntity?

    @Query(
        "UPDATE dream_runs SET status = 'CANCELLED', failure_code = 'FEATURE_DISABLED', " +
            "lease_owner = NULL, lease_until_ms = NULL, " +
            "finished_at_ms = MAX(updated_at_ms, :nowMs), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE scope_id = :scopeId AND mode IN ('INCREMENTAL', 'FULL') " +
            "AND status = 'PENDING'",
    )
    suspend fun cancelPendingSynthesisRuns(scopeId: String, nowMs: Long): Int

    @Query(
        "SELECT COUNT(*) FROM dream_runs AS r " +
            "WHERE r.mode IN ('INCREMENTAL', 'FULL') AND r.status = 'PENDING' " +
            "AND EXISTS (SELECT 1 FROM dream_experience_state AS pair_state " +
            "WHERE pair_state.pair_scope_id = r.scope_id)",
    )
    suspend fun countPairPendingSynthesisRuns(): Long

    @Query(
        "SELECT COUNT(*) FROM dream_runs AS r " +
            "WHERE r.mode IN ('INCREMENTAL', 'FULL') AND r.status = 'RUNNING' " +
            "AND EXISTS (SELECT 1 FROM dream_experience_state AS pair_state " +
            "WHERE pair_state.pair_scope_id = r.scope_id)",
    )
    suspend fun countPairRunningSynthesisRuns(): Long

    @Query(
        "SELECT COUNT(*) AS startedRunCount, " +
            "COALESCE(SUM(CASE WHEN input_tokens IS NOT NULL THEN input_tokens ELSE 0 END), 0) " +
            "AS knownInputTokens, " +
            "COALESCE(SUM(CASE WHEN output_tokens IS NOT NULL THEN output_tokens ELSE 0 END), 0) " +
            "AS knownOutputTokens, " +
            "COALESCE(SUM(CASE WHEN input_tokens IS NULL THEN 1 ELSE 0 END), 0) " +
            "AS unmeasuredInputRunCount, " +
            "COALESCE(SUM(CASE WHEN output_tokens IS NULL THEN 1 ELSE 0 END), 0) " +
            "AS unmeasuredOutputRunCount " +
            "FROM dream_runs WHERE mode IN ('INCREMENTAL', 'FULL') " +
            "AND started_at_ms IS NOT NULL AND started_at_ms >= :startInclusiveEpochMs " +
            "AND started_at_ms < :endExclusiveEpochMs " +
            "AND (" +
            "(prompt_contract_version IS NOT NULL AND validator_version IS NOT NULL " +
            "AND input_memory_count IS NOT NULL AND input_manifest_hash IS NOT NULL) " +
            "OR failure_code IN ('MODEL_PERMANENT_FAILURE', 'MODEL_UNAVAILABLE', " +
            "'MODEL_PROVIDER_UNAVAILABLE', 'MODEL_TIMEOUT', 'MODEL_CANCELLED_BY_PROVIDER', " +
            "'MODEL_OUTPUT_LIMIT', 'MODEL_SAFETY_REJECTION', 'MODEL_INVALID_CONFIGURATION', " +
            "'MODEL_AUDIT_MISMATCH', " +
            "'MODEL_OUTPUT_PARSE_REJECTED', 'MODEL_OUTPUT_VALIDATION_REJECTED', " +
            "'SNAPSHOT_COMPILATION_FAILED')) " +
            "AND (:excludingRunId IS NULL OR run_id != :excludingRunId)",
    )
    suspend fun readGlobalDreamDailyUsage(
        startInclusiveEpochMs: Long,
        endExclusiveEpochMs: Long,
        excludingRunId: String?,
    ): DreamDailyUsageRow

    @Query(
        "SELECT r.* FROM dream_runs r WHERE r.status = 'RUNNING' AND (" +
            "r.lease_until_ms IS NULL OR r.lease_until_ms <= :nowMs OR NOT EXISTS (" +
            "SELECT 1 FROM memory_scope_state s WHERE s.scope_id = r.scope_id " +
            "AND s.active_run_id = r.run_id " +
            "AND s.active_run_lease_until_ms = r.lease_until_ms " +
            "AND s.active_run_lease_until_ms > :nowMs)) " +
            "ORDER BY r.lease_until_ms ASC, r.run_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun findExpiredRunningRuns(nowMs: Long, limit: Int): List<DreamRunEntity>

    @Query(
        "SELECT * FROM dream_runs WHERE scope_id = :scopeId " +
            "ORDER BY created_at_ms DESC, run_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listRecentRuns(scopeId: String, limit: Int): List<DreamRunEntity>

    @Query(
        "SELECT MIN(base_observer_checkpoint_epoch) FROM dream_runs " +
            "WHERE scope_id = :scopeId AND status IN ('PENDING', 'RUNNING')",
    )
    suspend fun getProtectedObserverWatermark(scopeId: String): Long?

    @Query(
        "SELECT MIN(s.observer_checkpoint_epoch, s.last_applied_memory_epoch, " +
            "COALESCE((SELECT MIN(r.base_observer_checkpoint_epoch) FROM dream_runs r " +
            "WHERE r.scope_id = s.scope_id AND r.status IN ('PENDING', 'RUNNING')), " +
            "9223372036854775807)) FROM memory_scope_state s WHERE s.scope_id = :scopeId",
    )
    suspend fun getSafeChangePruneWatermark(scopeId: String): Long?

    @Query(
        "UPDATE memory_scope_state SET active_run_id = :runId, " +
            "active_run_lease_until_ms = :leaseUntilMs, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), " +
            "last_reason_code = :reasonCode WHERE scope_id = :scopeId " +
            "AND :leaseUntilMs > :nowMs AND (active_run_id IS NULL " +
            "OR active_run_lease_until_ms IS NULL OR active_run_lease_until_ms <= :nowMs)",
    )
    suspend fun acquireScopeLease(
        scopeId: String,
        runId: String,
        leaseUntilMs: Long,
        nowMs: Long,
        reasonCode: String,
    ): Int

    @Query(
        "UPDATE memory_scope_state SET active_run_lease_until_ms = :leaseUntilMs, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE scope_id = :scopeId AND active_run_id = :runId " +
            "AND active_run_lease_until_ms IS NOT NULL " +
            "AND active_run_lease_until_ms > :nowMs " +
            "AND :leaseUntilMs > active_run_lease_until_ms",
    )
    suspend fun heartbeatScopeLease(
        scopeId: String,
        runId: String,
        leaseUntilMs: Long,
        nowMs: Long,
        reasonCode: String,
    ): Int

    @Query(
        "UPDATE memory_scope_state SET active_run_id = NULL, " +
            "active_run_lease_until_ms = NULL, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), " +
            "last_reason_code = :reasonCode WHERE scope_id = :scopeId " +
            "AND active_run_id = :runId",
    )
    suspend fun releaseScopeLease(
        scopeId: String,
        runId: String,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE memory_scope_state SET active_run_id = NULL, " +
            "active_run_lease_until_ms = NULL, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), " +
            "last_reason_code = :reasonCode WHERE active_run_id IS NOT NULL " +
            "AND (active_run_lease_until_ms IS NULL OR active_run_lease_until_ms <= :nowMs)",
    )
    suspend fun recoverExpiredScopeLeases(nowMs: Long, reasonCode: String): Int

    @Query(
        "UPDATE dream_runs SET status = 'RUNNING', attempt = attempt + 1, " +
            "base_memory_epoch = :baseMemoryEpoch, " +
            "base_observer_checkpoint_epoch = :baseObserverCheckpointEpoch, " +
            "base_dream_revision = :baseDreamRevision, " +
            "checkpoint_epoch = :baseObserverCheckpointEpoch, " +
            "source_timezone_id = COALESCE(source_timezone_id, :sourceTimezoneId), " +
            "lease_owner = :leaseOwner, lease_until_ms = :leaseUntilMs, " +
            "started_at_ms = COALESCE(started_at_ms, MAX(created_at_ms, :nowMs)), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), " +
            "failure_code = NULL WHERE run_id = :runId AND scope_id = :scopeId " +
            "AND status = 'PENDING' AND checkpoint_epoch = base_observer_checkpoint_epoch " +
            "AND (source_timezone_id IS NULL OR source_timezone_id IS :sourceTimezoneId) " +
            "AND :leaseUntilMs > :nowMs " +
            "AND EXISTS (SELECT 1 FROM memory_scope_state s WHERE s.scope_id = :scopeId " +
            "AND s.active_run_id = :runId AND s.active_run_lease_until_ms >= :leaseUntilMs " +
            "AND s.memory_epoch = :baseMemoryEpoch " +
            "AND s.observer_checkpoint_epoch = :baseObserverCheckpointEpoch " +
            "AND s.dream_state_revision = :baseDreamRevision)",
    )
    suspend fun startRunMirror(
        runId: String,
        scopeId: String,
        baseMemoryEpoch: Long,
        baseObserverCheckpointEpoch: Long,
        baseDreamRevision: Long,
        leaseOwner: String,
        leaseUntilMs: Long,
        nowMs: Long,
        /** Null for Observer replay; synthesis passes a domain-validated strict IANA zone. */
        sourceTimezoneId: String? = null,
    ): Int

    @Query(
        "UPDATE dream_runs SET lease_until_ms = :leaseUntilMs, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE run_id = :runId AND scope_id = :scopeId AND status = 'RUNNING' " +
            "AND lease_owner = :leaseOwner AND lease_until_ms IS NOT NULL " +
            "AND lease_until_ms > :nowMs AND :leaseUntilMs > lease_until_ms " +
            "AND EXISTS (SELECT 1 FROM memory_scope_state s WHERE s.scope_id = :scopeId " +
            "AND s.active_run_id = :runId AND s.active_run_lease_until_ms >= :leaseUntilMs)",
    )
    suspend fun heartbeatRunMirror(
        runId: String,
        scopeId: String,
        leaseOwner: String,
        leaseUntilMs: Long,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_runs SET checkpoint_epoch = :targetCheckpointEpoch, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE run_id = :runId AND scope_id = :scopeId " +
            "AND status = 'RUNNING' AND lease_owner = :leaseOwner " +
            "AND lease_until_ms IS NOT NULL AND lease_until_ms > :nowMs " +
            "AND checkpoint_epoch = :expectedCheckpointEpoch " +
            "AND :targetCheckpointEpoch >= :expectedCheckpointEpoch " +
            "AND :targetCheckpointEpoch <= base_memory_epoch " +
            "AND EXISTS (SELECT 1 FROM memory_scope_state s WHERE s.scope_id = :scopeId " +
            "AND s.active_run_id = :runId " +
            "AND s.active_run_lease_until_ms = dream_runs.lease_until_ms " +
            "AND s.active_run_lease_until_ms > :nowMs)",
    )
    suspend fun advanceRunCheckpoint(
        runId: String,
        scopeId: String,
        leaseOwner: String,
        expectedCheckpointEpoch: Long,
        targetCheckpointEpoch: Long,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_runs SET status = :terminalStatus, failure_code = :failureCode, " +
            "lease_owner = NULL, lease_until_ms = NULL, " +
            "finished_at_ms = MAX(updated_at_ms, :nowMs), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE run_id = :runId AND scope_id = :scopeId AND status = 'RUNNING' " +
            "AND lease_owner = :leaseOwner AND lease_until_ms IS NOT NULL " +
            "AND lease_until_ms > :nowMs " +
            "AND :terminalStatus IN ('SUCCEEDED', 'CONFLICT', 'CANCELLED', 'FAILED', 'DISCARDED') " +
            "AND ((:terminalStatus = 'SUCCEEDED' AND :failureCode IS NULL) OR " +
            "(:terminalStatus != 'SUCCEEDED' AND :failureCode IS NOT NULL)) " +
            "AND (:terminalStatus != 'SUCCEEDED' OR checkpoint_epoch = base_memory_epoch) " +
            "AND EXISTS (SELECT 1 FROM memory_scope_state s WHERE s.scope_id = :scopeId " +
            "AND s.active_run_id = :runId " +
            "AND s.active_run_lease_until_ms = dream_runs.lease_until_ms " +
            "AND s.active_run_lease_until_ms > :nowMs)",
    )
    suspend fun finishRunMirror(
        runId: String,
        scopeId: String,
        leaseOwner: String,
        terminalStatus: String,
        failureCode: String?,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_runs SET status = 'FAILED', failure_code = :failureCode, " +
            "lease_owner = NULL, lease_until_ms = NULL, " +
            "finished_at_ms = MAX(updated_at_ms, :nowMs), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE status = 'RUNNING' AND (lease_until_ms IS NULL " +
            "OR lease_until_ms <= :nowMs OR NOT EXISTS (" +
            "SELECT 1 FROM memory_scope_state s WHERE s.scope_id = dream_runs.scope_id " +
            "AND s.active_run_id = dream_runs.run_id " +
            "AND s.active_run_lease_until_ms = dream_runs.lease_until_ms " +
            "AND s.active_run_lease_until_ms > :nowMs))",
    )
    suspend fun failExpiredRunMirrors(nowMs: Long, failureCode: String): Int
}
