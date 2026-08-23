package me.rerere.rikkahub.data.db.dao

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.OnConflictStrategy
import androidx.room.Query
import kotlinx.coroutines.flow.Flow
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimExperienceSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionSourceEntity
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.MemoryEvidenceEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity

/**
 * Guarded primitives for the derived Dream layer.
 *
 * Multi-row commits and privacy erasure are composed inside one Room transaction. No generic
 * update API is exposed: Claim heads, active Snapshot pointers, and scrub exceptions all have an
 * explicit compare-and-set or identity fence.
 */
@Dao
interface DreamSynthesisDao {
    /**
     * One narrow invalidation source for the review projection. Every derived mutation and every
     * run lease transition also updates this row, so callers can re-read all bounded children in
     * one Room transaction without exposing a collection of independently racing Flows.
     */
    @Query("SELECT * FROM memory_scope_state WHERE scope_id = :scopeId LIMIT 1")
    fun observeReviewScopeState(scopeId: String): Flow<MemoryScopeStateEntity?>

    /**
     * Runtime context reads use this one-shot form inside [androidx.room.withTransaction]. It is
     * intentionally scope-qualified; callers must not infer a missing private row from global
     * state (or vice versa).
     */
    @Query("SELECT * FROM memory_scope_state WHERE scope_id = :scopeId LIMIT 1")
    suspend fun getRuntimeScopeState(scopeId: String): MemoryScopeStateEntity?

    @Query(
        "SELECT * FROM dream_claims WHERE scope_id = :scopeId " +
            "AND state = 'ACTIVE_CONTEXTUAL' ORDER BY claim_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listRuntimeActiveClaimHeads(
        scopeId: String,
        limit: Int,
    ): List<DreamClaimEntity>

    @Query(
        "SELECT v.* FROM dream_claim_versions v " +
            "INNER JOIN dream_claims c ON c.claim_id = v.claim_id " +
            "WHERE c.scope_id = :scopeId AND c.state = 'ACTIVE_CONTEXTUAL' " +
            "AND v.claim_revision = c.claim_revision ORDER BY c.claim_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listRuntimeActiveClaimVersions(
        scopeId: String,
        limit: Int,
    ): List<DreamClaimVersionEntity>

    /**
     * Exact source pins for current active heads. Authority content is joined only when it belongs
     * to the requested scope, so a corrupt cross-scope pin cannot read private/global content.
     * The immutable revision row is retained with a LEFT JOIN so corruption is fail-closed rather
     * than silently deleting a manifest member.
     */
    @Query(
        "SELECT s.claim_id AS claimId, s.claim_revision AS claimRevision, " +
            "s.memory_id AS memoryId, s.memory_revision AS memoryRevision, " +
            "s.memory_semantic_hash AS memorySemanticHash, s.support_type AS supportType, " +
            "r.id AS revisionRowId, " +
            "r.source_identities_json AS revisionSourceIdentitiesJson, " +
            "m.id AS currentMemoryId, m.revision AS currentMemoryRevision, " +
            "m.title AS currentTitle, m.content AS currentContent, " +
            "m.updated_at_ms AS currentUpdatedAtMs, m.created_at_ms AS currentCreatedAtMs, " +
            "m.expires_at_ms AS currentExpiresAtMs, m.memory_kind AS currentMemoryKind, " +
            "m.tags_json AS currentTagsJson, " +
            "m.source_identities_json AS currentSourceIdentitiesJson, " +
            "m.lifecycle_status AS currentLifecycleStatus, " +
            "m.approval_source AS currentApprovalSource, " +
            "m.origin_assistant_id AS currentOriginAssistantId, " +
            "m.attribution AS currentAttribution, m.truth_status AS currentTruthStatus, " +
            "m.occurred_at_ms AS currentOccurredAtMs, " +
            "m.participants_json AS currentParticipantsJson, m.outcome AS currentOutcome " +
            "FROM dream_claim_version_sources s " +
            "INNER JOIN dream_claims c ON c.claim_id = s.claim_id " +
            "LEFT JOIN memory_revisions r ON r.memory_id = s.memory_id " +
            "AND r.revision = s.memory_revision " +
            "LEFT JOIN memoryentity m ON m.id = s.memory_id AND m.assistant_id = :scopeId " +
            "WHERE c.scope_id = :scopeId AND c.state = 'ACTIVE_CONTEXTUAL' " +
            "AND s.claim_revision = c.claim_revision " +
            "ORDER BY s.claim_id ASC, s.claim_revision ASC, s.memory_id ASC, " +
            "s.memory_revision ASC, s.support_type ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listRuntimeActiveSourceRows(
        scopeId: String,
        limit: Int,
    ): List<DreamRuntimeSourceRow>

    @Query("SELECT * FROM dream_claims WHERE claim_id = :claimId AND scope_id = :scopeId LIMIT 1")
    suspend fun getClaim(claimId: String, scopeId: String): DreamClaimEntity?

    @Query(
        "SELECT * FROM dream_claims WHERE scope_id = :scopeId AND claim_key = :claimKey LIMIT 1",
    )
    suspend fun getClaimByKey(scopeId: String, claimKey: String): DreamClaimEntity?

    @Query(
        "SELECT * FROM dream_claims WHERE scope_id = :scopeId " +
            "ORDER BY updated_at_ms DESC, claim_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listClaims(scopeId: String, limit: Int): List<DreamClaimEntity>

    @Query("SELECT scope_id FROM dream_claims WHERE claim_id = :claimId LIMIT 1")
    suspend fun findClaimScopeId(claimId: String): String?

    @Query("SELECT assistant_id FROM memoryentity WHERE id = :memoryId LIMIT 1")
    suspend fun findMemoryScopeIdForReview(memoryId: Int): String?

    @Query(
        "SELECT e.* FROM memory_evidence e " +
            "INNER JOIN memoryentity m ON m.id = e.memory_id " +
            "WHERE e.id = :evidenceId AND e.memory_id = :memoryId " +
            "AND m.assistant_id = :scopeId LIMIT 1",
    )
    suspend fun getScopedEvidenceForReview(
        scopeId: String,
        evidenceId: String,
        memoryId: Int,
    ): MemoryEvidenceEntity?

    @Query(
        "SELECT v.* FROM dream_claim_versions v " +
            "INNER JOIN dream_claims c ON c.claim_id = v.claim_id " +
            "WHERE c.scope_id = :scopeId AND v.claim_revision = c.claim_revision " +
            "ORDER BY c.claim_id ASC LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listReviewHeadVersions(
        scopeId: String,
        limit: Int,
    ): List<DreamClaimVersionEntity>

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertClaim(claim: DreamClaimEntity)

    @Query(
        "UPDATE dream_claims SET claim_revision = :nextClaimRevision, " +
            "claim_key = :claimKey, storage_class = :storageClass, " +
            "epistemic_type = :epistemicType, subject_kind = :subjectKind, " +
            "profile_section = :profileSection, epistemic_origin = :epistemicOrigin, " +
            "content_type = :contentType, title = :title, statement = :statement, " +
            "state = :state, confidence = :confidence, temporal_state = :temporalState, " +
            "valid_from_ms = :validFromMs, valid_to_ms = :validToMs, " +
            "learned_at_ms = :learnedAtMs, source_timezone = :sourceTimezone, " +
            "claim_hash = :claimHash, created_by_run_id = :createdByRunId, " +
            "last_validated_memory_epoch = :lastValidatedMemoryEpoch, " +
            "invalidated_at_ms = :invalidatedAtMs, " +
            "invalidation_reason = :invalidationReason, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE claim_id = :claimId AND scope_id = :scopeId " +
            "AND claim_revision = :expectedClaimRevision " +
            "AND :expectedClaimRevision < 9223372036854775807 " +
            "AND :nextClaimRevision = :expectedClaimRevision + 1 " +
            "AND state != 'TOMBSTONED'",
    )
    suspend fun updateClaimHeadCas(
        claimId: String,
        scopeId: String,
        expectedClaimRevision: Long,
        nextClaimRevision: Long,
        claimKey: String,
        storageClass: String,
        epistemicType: String,
        subjectKind: String,
        profileSection: String,
        epistemicOrigin: String,
        contentType: String,
        title: String,
        statement: String,
        state: String,
        confidence: Double,
        temporalState: String,
        validFromMs: Long?,
        validToMs: Long?,
        learnedAtMs: Long,
        sourceTimezone: String,
        claimHash: String,
        createdByRunId: String,
        lastValidatedMemoryEpoch: Long,
        invalidatedAtMs: Long?,
        invalidationReason: String?,
        nowMs: Long,
    ): Int

    /**
     * Advances only the validation watermark of an unchanged active head. Semantic or lifecycle
     * changes must still create an immutable ClaimVersion and use [updateClaimHeadCas].
     */
    @Query(
        "UPDATE dream_claims SET last_validated_memory_epoch = :targetEpoch, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE scope_id = :scopeId AND claim_id = :claimId " +
            "AND claim_revision = :expectedRevision AND state = 'ACTIVE_CONTEXTUAL' " +
            "AND :targetEpoch >= 0 AND last_validated_memory_epoch < :targetEpoch",
    )
    suspend fun touchClaimValidationEpochCas(
        scopeId: String,
        claimId: String,
        expectedRevision: Long,
        targetEpoch: Long,
        nowMs: Long,
    ): Int

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertClaimVersion(version: DreamClaimVersionEntity)

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertClaimVersionSources(sources: List<DreamClaimVersionSourceEntity>)

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertClaimExperienceSources(sources: List<DreamClaimExperienceSourceEntity>)

    @Query(
        "SELECT * FROM dream_claim_versions " +
            "WHERE claim_id = :claimId AND claim_revision = :claimRevision LIMIT 1",
    )
    suspend fun getClaimVersion(
        claimId: String,
        claimRevision: Long,
    ): DreamClaimVersionEntity?

    @Query(
        "SELECT v.* FROM dream_claim_versions v " +
            "INNER JOIN dream_claims c ON c.claim_id = v.claim_id " +
            "WHERE v.claim_id = :claimId AND c.scope_id = :scopeId " +
            "ORDER BY v.claim_revision ASC",
    )
    suspend fun listClaimVersions(
        claimId: String,
        scopeId: String,
    ): List<DreamClaimVersionEntity>

    @Query(
        "SELECT v.* FROM dream_claim_versions v " +
            "INNER JOIN dream_claims c ON c.claim_id = v.claim_id " +
            "WHERE v.claim_id = :claimId AND c.scope_id = :scopeId " +
            "ORDER BY v.claim_revision ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listReviewClaimVersions(
        claimId: String,
        scopeId: String,
        limit: Int,
    ): List<DreamClaimVersionEntity>

    @Query(
        "SELECT * FROM dream_claim_version_sources " +
            "WHERE claim_id = :claimId AND claim_revision = :claimRevision " +
            "ORDER BY memory_id ASC, memory_revision ASC, support_type ASC",
    )
    suspend fun listClaimVersionSources(
        claimId: String,
        claimRevision: Long,
    ): List<DreamClaimVersionSourceEntity>

    @Query(
        "SELECT * FROM dream_claim_experience_sources WHERE claim_id = :claimId " +
            "AND claim_revision = :claimRevision ORDER BY experience_epoch ASC, experience_id ASC, support_type ASC",
    )
    suspend fun listClaimExperienceSources(
        claimId: String,
        claimRevision: Long,
    ): List<DreamClaimExperienceSourceEntity>

    @Query(
        "SELECT s.* FROM dream_claim_version_sources s " +
            "INNER JOIN dream_claims c ON c.claim_id = s.claim_id " +
            "WHERE c.scope_id = :scopeId AND s.claim_id = :claimId " +
            "AND s.claim_revision = :claimRevision AND s.memory_id = :memoryId " +
            "AND s.memory_revision = :memoryRevision AND s.support_type = :supportType LIMIT 1",
    )
    suspend fun getScopedClaimVersionSource(
        scopeId: String,
        claimId: String,
        claimRevision: Long,
        memoryId: Int,
        memoryRevision: Int,
        supportType: String,
    ): DreamClaimVersionSourceEntity?

    /**
     * Bounded, scope-qualified source projection used by review/detail. LEFT JOINs intentionally
     * preserve broken references so the store can report CORRUPT/SCOPE_MISMATCH instead of
     * silently dropping them. [claimId] null selects every Claim head in the scope; otherwise all
     * versions of that one Claim are selected. [headOnly] narrows to current head versions.
     */
    @Query(
        "SELECT s.claim_id AS claimId, s.claim_revision AS claimRevision, " +
            "s.memory_id AS memoryId, s.memory_revision AS memoryRevision, " +
            "s.memory_semantic_hash AS memorySemanticHash, " +
            "s.memory_evidence_id AS memoryEvidenceId, s.support_type AS supportType, " +
            "s.created_at_ms AS sourceCreatedAtMs, " +
            "m.assistant_id AS currentScopeId, m.revision AS currentMemoryRevision, " +
            "m.title AS currentTitle, m.content AS currentContent, " +
            "m.updated_at_ms AS currentUpdatedAtMs, m.created_at_ms AS currentCreatedAtMs, " +
            "m.expires_at_ms AS currentExpiresAtMs, m.memory_kind AS currentMemoryKind, " +
            "m.tags_json AS currentTagsJson, " +
            "m.source_identities_json AS currentSourceIdentitiesJson, " +
            "m.lifecycle_status AS currentLifecycleStatus, " +
            "m.approval_source AS currentApprovalSource, " +
            "m.origin_assistant_id AS currentOriginAssistantId, " +
            "m.attribution AS currentAttribution, m.truth_status AS currentTruthStatus, " +
            "m.occurred_at_ms AS currentOccurredAtMs, " +
            "m.participants_json AS currentParticipantsJson, m.outcome AS currentOutcome, " +
            "r.id AS revisionRowId, " +
            "r.source_identities_json AS revisionSourceIdentitiesJson, " +
            "e.id AS evidenceRowId, e.memory_id AS evidenceMemoryId, " +
            "e.quality AS evidenceQuality, e.source_kind AS evidenceSourceKind, " +
            "e.excerpt AS evidenceExcerpt, e.conversation_id AS evidenceConversationId, " +
            "e.message_id AS evidenceMessageId, e.role AS evidenceRole, " +
            "e.source_digest AS evidenceSourceDigest, " +
            "e.evidence_group_id AS evidenceGroupId " +
            "FROM dream_claim_version_sources s " +
            "INNER JOIN dream_claims c ON c.claim_id = s.claim_id " +
            "LEFT JOIN memoryentity m ON m.id = s.memory_id " +
            "LEFT JOIN memory_revisions r ON r.memory_id = s.memory_id " +
            "AND r.revision = s.memory_revision " +
            "LEFT JOIN memory_evidence e ON e.id = s.memory_evidence_id " +
            "WHERE c.scope_id = :scopeId " +
            "AND (:claimId IS NULL OR s.claim_id = :claimId) " +
            "AND (NOT :headOnly OR s.claim_revision = c.claim_revision) " +
            "ORDER BY s.claim_id ASC, s.claim_revision ASC, s.memory_id ASC, " +
            "s.memory_revision ASC, s.support_type ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listReviewSourceRows(
        scopeId: String,
        claimId: String?,
        headOnly: Boolean,
        limit: Int,
    ): List<DreamReviewSourceRow>

    @Query(
        "SELECT DISTINCT v.* FROM dream_claim_versions v " +
            "INNER JOIN dream_claim_version_sources s " +
            "ON s.claim_id = v.claim_id AND s.claim_revision = v.claim_revision " +
            "WHERE s.memory_id = :memoryId " +
            "AND (:memoryRevision IS NULL OR s.memory_revision = :memoryRevision) " +
            "ORDER BY v.claim_id ASC, v.claim_revision ASC",
    )
    suspend fun findClaimVersionsByMemory(
        memoryId: Int,
        memoryRevision: Int?,
    ): List<DreamClaimVersionEntity>

    @Query(
        "SELECT COUNT(*) FROM dream_claim_version_sources " +
            "WHERE memory_id = :memoryId AND memory_revision = :memoryRevision",
    )
    suspend fun countMemoryRevisionPins(memoryId: Int, memoryRevision: Int): Int

    @Insert(onConflict = OnConflictStrategy.ABORT)
    suspend fun insertSnapshot(snapshot: DreamSnapshotEntity)

    @Query(
        "SELECT * FROM dream_snapshots WHERE snapshot_id = :snapshotId " +
            "AND scope_id = :scopeId LIMIT 1",
    )
    suspend fun getSnapshot(snapshotId: String, scopeId: String): DreamSnapshotEntity?

    @Query(
        "SELECT * FROM dream_snapshots WHERE scope_id = :scopeId " +
            "ORDER BY created_at_ms DESC, snapshot_id ASC " +
            "LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun listSnapshots(scopeId: String, limit: Int): List<DreamSnapshotEntity>

    @Query(
        "SELECT p.* FROM memory_scope_state s " +
            "INNER JOIN dream_snapshots p ON p.snapshot_id = s.active_snapshot_id " +
            "WHERE s.scope_id = :scopeId AND p.scope_id = s.scope_id " +
            "AND p.status = 'ACTIVE' AND p.source_memory_epoch = s.memory_epoch " +
            "AND p.committed_dream_revision = s.dream_state_revision " +
            "AND p.snapshot_revision = s.dream_state_revision LIMIT 1",
    )
    suspend fun getCurrentSnapshot(scopeId: String): DreamSnapshotEntity?

    @Query(
        "UPDATE dream_snapshots SET status = 'SUPERSEDED' " +
            "WHERE snapshot_id = :snapshotId AND scope_id = :scopeId AND status = 'ACTIVE'",
    )
    suspend fun supersedeActiveSnapshot(snapshotId: String, scopeId: String): Int

    @Query(
        "UPDATE memory_scope_state SET active_snapshot_id = :newSnapshotId, " +
            "dream_state_revision = dream_state_revision + 1, " +
            "last_applied_memory_epoch = :baseMemoryEpoch, " +
            "last_full_rebuild_at_ms = CASE WHEN :fullRebuildAtMs IS NULL " +
            "THEN last_full_rebuild_at_ms ELSE MAX(COALESCE(last_full_rebuild_at_ms, 0), " +
            ":fullRebuildAtMs) END, updated_at_ms = MAX(updated_at_ms, :nowMs), " +
            "last_reason_code = :reasonCode " +
            "WHERE scope_id = :scopeId AND memory_epoch = :baseMemoryEpoch " +
            "AND dream_state_revision = :baseDreamRevision " +
            "AND last_applied_memory_epoch = :expectedLastAppliedMemoryEpoch " +
            "AND :expectedLastAppliedMemoryEpoch >= 0 " +
            "AND :expectedLastAppliedMemoryEpoch <= :baseMemoryEpoch " +
            "AND active_snapshot_id IS :expectedActiveSnapshotId " +
            "AND active_run_id = :runId AND active_run_lease_until_ms > :nowMs " +
            "AND dream_state_revision < 9223372036854775807 " +
            "AND EXISTS (SELECT 1 FROM dream_runs r WHERE r.run_id = :runId " +
            "AND r.scope_id = :scopeId AND r.status = 'RUNNING' " +
            "AND r.lease_owner = :leaseOwner AND r.lease_until_ms > :nowMs " +
            "AND r.lease_until_ms = memory_scope_state.active_run_lease_until_ms " +
            "AND r.base_memory_epoch = :baseMemoryEpoch " +
            "AND r.base_dream_revision = :baseDreamRevision " +
            "AND r.checkpoint_epoch = r.base_memory_epoch) " +
            "AND EXISTS (SELECT 1 FROM dream_snapshots p " +
            "WHERE p.snapshot_id = :newSnapshotId AND p.scope_id = :scopeId " +
            "AND p.status = 'ACTIVE' AND p.source_memory_epoch = :baseMemoryEpoch " +
            "AND p.committed_dream_revision = :baseDreamRevision + 1 " +
            "AND p.snapshot_revision = :baseDreamRevision + 1 " +
            "AND p.created_by_run_id = :runId)",
    )
    suspend fun commitActiveSnapshotCas(
        scopeId: String,
        runId: String,
        leaseOwner: String,
        baseMemoryEpoch: Long,
        baseDreamRevision: Long,
        expectedLastAppliedMemoryEpoch: Long,
        expectedActiveSnapshotId: String?,
        newSnapshotId: String,
        fullRebuildAtMs: Long?,
        reasonCode: String,
        nowMs: Long,
    ): Int

    /** Claim revision + all five scope fences in one guarded user-review write. */
    @Query(
        "UPDATE dream_claims SET claim_revision = :nextClaimRevision, state = :nextState, " +
            "claim_hash = :claimHash, created_by_run_id = :mutationId, " +
            "last_validated_memory_epoch = :currentMemoryEpoch, " +
            "invalidated_at_ms = MAX(COALESCE(invalidated_at_ms, 0), :nowMs), " +
            "invalidation_reason = :reasonCode, updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE claim_id = :claimId AND scope_id = :scopeId " +
            "AND claim_revision = :expectedClaimRevision " +
            "AND state IN ('PENDING_REVIEW', 'ACTIVE_CONTEXTUAL') " +
            "AND :expectedClaimRevision < 9223372036854775807 " +
            "AND :nextClaimRevision = :expectedClaimRevision + 1 " +
            "AND :nowMs >= 0 AND ((:nextState = 'REJECTED' " +
            "AND :reasonCode = 'USER_REJECTED') OR (:nextState = 'SUPERSEDED' " +
            "AND :reasonCode = 'USER_CORRECTION')) " +
            "AND EXISTS (SELECT 1 FROM memory_scope_state s WHERE s.scope_id = :scopeId " +
            "AND s.memory_epoch = :currentMemoryEpoch " +
            "AND s.last_applied_memory_epoch = :expectedLastAppliedMemoryEpoch " +
            "AND s.dream_state_revision = :expectedDreamRevision " +
            "AND s.active_snapshot_id IS :expectedActiveSnapshotId)",
    )
    suspend fun advanceClaimForUserReviewCas(
        scopeId: String,
        claimId: String,
        expectedClaimRevision: Long,
        nextClaimRevision: Long,
        currentMemoryEpoch: Long,
        expectedLastAppliedMemoryEpoch: Long,
        expectedDreamRevision: Long,
        expectedActiveSnapshotId: String?,
        nextState: String,
        claimHash: String,
        mutationId: String,
        reasonCode: String,
        nowMs: Long,
    ): Int

    /** Final short CAS for a user-review snapshot; no synthesis run lease is required. */
    @Query(
        "UPDATE memory_scope_state SET active_snapshot_id = :newSnapshotId, " +
            "dream_state_revision = dream_state_revision + 1, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE scope_id = :scopeId AND memory_epoch = :currentMemoryEpoch " +
            "AND last_applied_memory_epoch = :expectedLastAppliedMemoryEpoch " +
            "AND dream_state_revision = :expectedDreamRevision " +
            "AND active_snapshot_id IS :expectedActiveSnapshotId " +
            "AND dream_state_revision < 9223372036854775807 " +
            "AND :nowMs >= 0 AND :reasonCode IN ('USER_REJECTED', 'USER_CORRECTION') " +
            "AND (:expectedActiveSnapshotId IS NULL OR EXISTS (" +
            "SELECT 1 FROM dream_snapshots prior WHERE prior.snapshot_id = :expectedActiveSnapshotId " +
            "AND prior.scope_id = :scopeId AND prior.status = 'SUPERSEDED')) " +
            "AND EXISTS (SELECT 1 FROM dream_snapshots p " +
            "WHERE p.snapshot_id = :newSnapshotId AND p.scope_id = :scopeId " +
            "AND p.status = 'ACTIVE' AND p.source_memory_epoch = :currentMemoryEpoch " +
            "AND p.snapshot_revision = :expectedDreamRevision + 1 " +
            "AND p.committed_dream_revision = :expectedDreamRevision + 1 " +
            "AND p.created_by_run_id = :mutationId " +
            "AND p.supersedes_snapshot_id IS :expectedActiveSnapshotId " +
            "AND p.reason_code = :reasonCode)",
    )
    suspend fun advanceUserReviewSnapshotCas(
        scopeId: String,
        currentMemoryEpoch: Long,
        expectedLastAppliedMemoryEpoch: Long,
        expectedDreamRevision: Long,
        expectedActiveSnapshotId: String?,
        newSnapshotId: String,
        mutationId: String,
        reasonCode: String,
        nowMs: Long,
    ): Int

    /** Clears only derived state and forces the next synthesis to use FULL bootstrap. */
    @Query(
        "UPDATE memory_scope_state SET active_snapshot_id = NULL, " +
            "dream_state_revision = dream_state_revision + 1, last_applied_memory_epoch = 0, " +
            "last_full_rebuild_at_ms = NULL, updated_at_ms = MAX(updated_at_ms, :nowMs), " +
            "last_reason_code = :reasonCode " +
            "WHERE scope_id = :scopeId AND memory_epoch = :expectedMemoryEpoch " +
            "AND last_applied_memory_epoch = :expectedLastAppliedMemoryEpoch " +
            "AND dream_state_revision = :expectedDreamRevision " +
            "AND active_snapshot_id IS :expectedActiveSnapshotId " +
            "AND dream_state_revision < 9223372036854775807 " +
            "AND :nowMs >= 0 AND :reasonCode = 'USER_CLEAR_DERIVED'",
    )
    suspend fun advanceClearDerivedCas(
        scopeId: String,
        expectedMemoryEpoch: Long,
        expectedLastAppliedMemoryEpoch: Long,
        expectedDreamRevision: Long,
        expectedActiveSnapshotId: String?,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Query(
        "DELETE FROM dream_claim_version_sources WHERE claim_id IN " +
            "(SELECT claim_id FROM dream_claims WHERE scope_id = :scopeId)",
    )
    suspend fun deleteDerivedSourcesForScope(scopeId: String): Int

    @Query("DELETE FROM dream_claims WHERE scope_id = :scopeId")
    suspend fun deleteDerivedClaimsForScope(scopeId: String): Int

    @Query("DELETE FROM dream_snapshots WHERE scope_id = :scopeId")
    suspend fun deleteDerivedSnapshotsForScope(scopeId: String): Int

    @Query("SELECT COUNT(*) FROM dream_claims WHERE scope_id = :scopeId")
    suspend fun countDerivedClaimsForScope(scopeId: String): Int

    @Query("SELECT COUNT(*) FROM dream_snapshots WHERE scope_id = :scopeId")
    suspend fun countDerivedSnapshotsForScope(scopeId: String): Int

    @Query(
        "UPDATE dream_runs SET " +
            "prompt_contract_version = COALESCE(prompt_contract_version, :promptContractVersion), " +
            "validator_version = COALESCE(validator_version, :validatorVersion), " +
            "input_memory_count = COALESCE(input_memory_count, :inputMemoryCount), " +
            "input_manifest_hash = COALESCE(input_manifest_hash, :inputManifestHash), " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE run_id = :runId AND scope_id = :scopeId AND status = 'RUNNING' " +
            "AND lease_owner = :leaseOwner AND lease_until_ms > :nowMs " +
            "AND :inputMemoryCount >= 0 " +
            "AND (prompt_contract_version IS NULL OR prompt_contract_version = :promptContractVersion) " +
            "AND (validator_version IS NULL OR validator_version = :validatorVersion) " +
            "AND (input_memory_count IS NULL OR input_memory_count = :inputMemoryCount) " +
            "AND (input_manifest_hash IS NULL OR input_manifest_hash = :inputManifestHash) " +
            "AND (EXISTS (SELECT 1 FROM memory_scope_state s WHERE s.scope_id = :scopeId " +
            "AND s.active_run_id = :runId AND s.active_run_lease_until_ms > :nowMs " +
            "AND s.active_run_lease_until_ms = dream_runs.lease_until_ms) OR " +
            "EXISTS (SELECT 1 FROM dream_experience_state p WHERE p.pair_scope_id = :scopeId " +
            "AND p.active_run_id = :runId AND p.active_run_lease_until_ms > :nowMs " +
            "AND p.active_run_lease_until_ms = dream_runs.lease_until_ms))",
    )
    suspend fun markRunProviderDispatch(
        runId: String,
        scopeId: String,
        leaseOwner: String,
        promptContractVersion: String,
        validatorVersion: String,
        inputMemoryCount: Int,
        inputManifestHash: String,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_runs SET model_identity_digest = :modelIdentityDigest, " +
            "provider_kind = :providerKind, prompt_contract_version = :promptContractVersion, " +
            "validator_version = :validatorVersion, input_memory_count = :inputMemoryCount, " +
            "input_tokens = :inputTokens, output_claim_count = :outputClaimCount, " +
            "output_tokens = :outputTokens, input_manifest_hash = :inputManifestHash, " +
            "output_manifest_hash = :outputManifestHash, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE run_id = :runId AND scope_id = :scopeId AND status = 'RUNNING' " +
            "AND lease_owner = :leaseOwner AND lease_until_ms > :nowMs " +
            "AND :inputMemoryCount >= 0 AND (:inputTokens IS NULL OR :inputTokens >= 0) " +
            "AND :outputClaimCount >= 0 AND (:outputTokens IS NULL OR :outputTokens >= 0) " +
            "AND (EXISTS (SELECT 1 FROM memory_scope_state s WHERE s.scope_id = :scopeId " +
            "AND s.active_run_id = :runId AND s.active_run_lease_until_ms > :nowMs " +
            "AND s.active_run_lease_until_ms = dream_runs.lease_until_ms) OR " +
            "EXISTS (SELECT 1 FROM dream_experience_state p WHERE p.pair_scope_id = :scopeId " +
            "AND p.active_run_id = :runId AND p.active_run_lease_until_ms > :nowMs " +
            "AND p.active_run_lease_until_ms = dream_runs.lease_until_ms))",
    )
    suspend fun recordRunSynthesisAudit(
        runId: String,
        scopeId: String,
        leaseOwner: String,
        modelIdentityDigest: String,
        providerKind: String,
        promptContractVersion: String,
        validatorVersion: String,
        inputMemoryCount: Int,
        inputTokens: Long?,
        outputClaimCount: Int,
        outputTokens: Long?,
        inputManifestHash: String,
        outputManifestHash: String,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_claims SET title = '', statement = '', state = 'TOMBSTONED', " +
            "invalidated_at_ms = MAX(COALESCE(invalidated_at_ms, 0), :nowMs), " +
            "invalidation_reason = :reasonCode, updated_at_ms = MAX(updated_at_ms, :nowMs) " +
            "WHERE claim_id = :claimId AND scope_id = :scopeId",
    )
    suspend fun tombstoneClaimAndScrub(
        claimId: String,
        scopeId: String,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Query(
        "UPDATE dream_claim_versions SET canonical_claim_json = '' " +
            "WHERE claim_id = :claimId AND claim_revision = :claimRevision",
    )
    suspend fun scrubClaimVersion(claimId: String, claimRevision: Long): Int

    @Query(
        "UPDATE dream_snapshots SET status = 'TOMBSTONED', canonical_payload_json = '' " +
            "WHERE snapshot_id = :snapshotId AND scope_id = :scopeId",
    )
    suspend fun tombstoneSnapshotAndScrub(snapshotId: String, scopeId: String): Int

    @Query(
        "UPDATE memory_scope_state SET active_snapshot_id = " +
            "CASE WHEN :clearActiveSnapshot THEN NULL ELSE active_snapshot_id END, " +
            "dream_state_revision = dream_state_revision + 1, " +
            "updated_at_ms = MAX(updated_at_ms, :nowMs), last_reason_code = :reasonCode " +
            "WHERE scope_id = :scopeId AND memory_epoch = :expectedMemoryEpoch " +
            "AND dream_state_revision = :expectedDreamRevision " +
            "AND active_snapshot_id IS :expectedActiveSnapshotId " +
            "AND dream_state_revision < 9223372036854775807 " +
            "AND (NOT :clearActiveSnapshot OR (" +
            ":expectedActiveSnapshotId IS NOT NULL AND EXISTS (" +
            "SELECT 1 FROM dream_snapshots p " +
            "WHERE p.snapshot_id = :expectedActiveSnapshotId AND p.scope_id = :scopeId " +
            "AND p.status = 'TOMBSTONED' AND p.canonical_payload_json = '')))",
    )
    suspend fun advancePrivacyRevisionCas(
        scopeId: String,
        expectedMemoryEpoch: Long,
        expectedDreamRevision: Long,
        expectedActiveSnapshotId: String?,
        clearActiveSnapshot: Boolean,
        reasonCode: String,
        nowMs: Long,
    ): Int

    @Query(
        "DELETE FROM dream_claim_version_sources WHERE memory_id = :memoryId " +
            "AND (:memoryRevision IS NULL OR memory_revision = :memoryRevision)",
    )
    suspend fun deleteSourcesForMemory(memoryId: Int, memoryRevision: Int?): Int

    @Query(
        "DELETE FROM dream_claim_version_sources " +
            "WHERE claim_id = :claimId AND EXISTS (" +
            "SELECT 1 FROM dream_claims c " +
            "WHERE c.claim_id = dream_claim_version_sources.claim_id " +
            "AND c.scope_id = :scopeId)",
    )
    suspend fun deleteSourcesForClaim(claimId: String, scopeId: String): Int
}

/** Internal Room projection. User/source text never leaves the review store through list DTOs. */
data class DreamReviewSourceRow(
    val claimId: String,
    val claimRevision: Long,
    val memoryId: Int,
    val memoryRevision: Int,
    val memorySemanticHash: String,
    val memoryEvidenceId: String?,
    val supportType: String,
    val sourceCreatedAtMs: Long,
    val currentScopeId: String?,
    val currentMemoryRevision: Int?,
    val currentTitle: String?,
    val currentContent: String?,
    val currentUpdatedAtMs: Long?,
    val currentCreatedAtMs: Long?,
    val currentExpiresAtMs: Long?,
    val currentMemoryKind: String?,
    val currentTagsJson: String?,
    val currentSourceIdentitiesJson: String?,
    val currentLifecycleStatus: String?,
    val currentApprovalSource: String?,
    val currentOriginAssistantId: String?,
    val currentAttribution: String?,
    val currentTruthStatus: String?,
    val currentOccurredAtMs: Long?,
    val currentParticipantsJson: String?,
    val currentOutcome: String?,
    val revisionRowId: String?,
    val revisionSourceIdentitiesJson: String?,
    val evidenceRowId: String?,
    val evidenceMemoryId: Int?,
    val evidenceQuality: String?,
    val evidenceSourceKind: String?,
    val evidenceExcerpt: String?,
    val evidenceConversationId: String?,
    val evidenceMessageId: String?,
    val evidenceRole: String?,
    val evidenceSourceDigest: String?,
    val evidenceGroupId: String?,
)

/**
 * Internal, bounded runtime projection. Nullable authority columns mean the exact scoped current
 * row is absent; the reader converts that to a closed source fence and never exports row content.
 */
data class DreamRuntimeSourceRow(
    val claimId: String,
    val claimRevision: Long,
    val memoryId: Int,
    val memoryRevision: Int,
    val memorySemanticHash: String,
    val supportType: String,
    val revisionRowId: String?,
    val revisionSourceIdentitiesJson: String?,
    val currentMemoryId: Int?,
    val currentMemoryRevision: Int?,
    val currentTitle: String?,
    val currentContent: String?,
    val currentUpdatedAtMs: Long?,
    val currentCreatedAtMs: Long?,
    val currentExpiresAtMs: Long?,
    val currentMemoryKind: String?,
    val currentTagsJson: String?,
    val currentSourceIdentitiesJson: String?,
    val currentLifecycleStatus: String?,
    val currentApprovalSource: String?,
    val currentOriginAssistantId: String?,
    val currentAttribution: String?,
    val currentTruthStatus: String?,
    val currentOccurredAtMs: Long?,
    val currentParticipantsJson: String?,
    val currentOutcome: String?,
)
