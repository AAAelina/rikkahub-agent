package me.rerere.rikkahub.memory.dreaming.experience

import androidx.room.withTransaction
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.dao.DreamDao
import me.rerere.rikkahub.data.db.dao.DreamExperienceDao
import me.rerere.rikkahub.data.db.entity.DreamExperienceEntity
import me.rerere.rikkahub.data.db.entity.DreamExperienceStateEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId

data class DreamExperienceRecord(
    val id: String,
    val pairScopeId: DreamPairScopeId,
    val sourceKind: String,
    val sourceRef: String,
    val conversationId: String? = null,
    val occurredAtMs: Long,
    val actor: String,
    val experienceKind: String,
    val summary: String,
    val salience: Double,
    val novelty: Double = 0.5,
    val identityWeight: Double = 0.5,
    val relationshipWeight: Double = 0.5,
    val emotionalWeight: Double = 0.0,
    val confidence: Double = 1.0,
    val contentDigest: String,
    val sourceManifestJson: String = "[]",
) {
    val debt: Double get() = maxOf(salience, identityWeight, relationshipWeight).coerceIn(0.0, 1.0)
}

data class DreamExperienceState(
    val pairScopeId: DreamPairScopeId,
    val experienceEpoch: Long,
    val observerCheckpointEpoch: Long,
    val appliedExperienceEpoch: Long,
    val profileRevision: Long,
    val activeSnapshotId: String?,
    val experienceDebt: Double,
    val activeRunId: String?,
    val activeRunLeaseUntilMs: Long?,
    val historyBackfilledAtMs: Long?,
    val updatedAtMs: Long,
) {
    val lag: Long get() = experienceEpoch - appliedExperienceEpoch
}

sealed interface DreamExperienceIngestResult {
    data class Inserted(val epoch: Long) : DreamExperienceIngestResult
    data class Duplicate(val epoch: Long) : DreamExperienceIngestResult
}

class RoomDreamExperienceStore(
    private val database: AppDatabase,
    private val dao: DreamExperienceDao,
    private val legacyDreamDao: DreamDao,
) {
    suspend fun ensureState(pairScopeId: DreamPairScopeId, nowMs: Long): DreamExperienceState =
        database.withTransaction {
            dao.insertStateIfAbsent(
                DreamExperienceStateEntity(pairScopeId = pairScopeId.value, updatedAtMs = nowMs),
            )
            // Temporary v49 structural parent only; Pair-Dream authority lives in Experience state.
            legacyDreamDao.insertScopeStateIfAbsent(
                MemoryScopeStateEntity(scopeId = pairScopeId.value, updatedAtMs = nowMs),
            )
            checkNotNull(dao.getState(pairScopeId.value)).toModel()
        }

    suspend fun markHistoryBackfilled(pairScopeId: DreamPairScopeId, nowMs: Long) {
        dao.markHistoryBackfilled(pairScopeId.value, nowMs)
    }

    suspend fun ingest(record: DreamExperienceRecord, nowMs: Long): DreamExperienceIngestResult =
        database.withTransaction {
            val scope = record.pairScopeId.value
            dao.insertStateIfAbsent(DreamExperienceStateEntity(pairScopeId = scope, updatedAtMs = nowMs))
            // v49 Dream child tables still have a structural FK to memory_scope_state. This row is
            // never used as Pair-Dream clock/lease authority and is removed with the legacy schema.
            legacyDreamDao.insertScopeStateIfAbsent(MemoryScopeStateEntity(scopeId = scope, updatedAtMs = nowMs))
            val state = checkNotNull(dao.getState(scope))
            val nextEpoch = Math.addExact(state.experienceEpoch, 1L)
            val rowId = dao.insertExperienceIgnore(record.toEntity(nextEpoch, nowMs))
            if (rowId == -1L) {
                return@withTransaction DreamExperienceIngestResult.Duplicate(state.experienceEpoch)
            }
            check(
                dao.advanceExperienceEpoch(
                    pairScopeId = scope,
                    expectedEpoch = state.experienceEpoch,
                    nextEpoch = nextEpoch,
                    debtDelta = record.debt,
                    nowMs = nowMs,
                ) == 1,
            ) { "dream_experience_epoch_cas_lost" }
            DreamExperienceIngestResult.Inserted(nextEpoch)
        }

    suspend fun state(pairScopeId: DreamPairScopeId): DreamExperienceState? =
        dao.getState(pairScopeId.value)?.toModel()

    suspend fun pending(pairScopeId: DreamPairScopeId, limit: Int = 256): List<DreamExperienceEntity> {
        val state = dao.getState(pairScopeId.value) ?: return emptyList()
        return dao.listExperiences(
            pairScopeId = pairScopeId.value,
            afterExclusiveEpoch = state.appliedExperienceEpoch,
            throughInclusiveEpoch = state.experienceEpoch,
            limit = limit,
        )
    }

    suspend fun pendingCount(pairScopeId: DreamPairScopeId): Int {
        val state = dao.getState(pairScopeId.value) ?: return 0
        return dao.countPending(pairScopeId.value, state.appliedExperienceEpoch)
    }
}

private fun DreamExperienceRecord.toEntity(epoch: Long, nowMs: Long) = DreamExperienceEntity(
    experienceId = id,
    pairScopeId = pairScopeId.value,
    experienceEpoch = epoch,
    sourceKind = sourceKind,
    sourceRef = sourceRef,
    conversationId = conversationId,
    occurredAtMs = occurredAtMs,
    ingestedAtMs = nowMs,
    actor = actor,
    experienceKind = experienceKind,
    summary = summary,
    salience = salience.coerceIn(0.0, 1.0),
    novelty = novelty.coerceIn(0.0, 1.0),
    identityWeight = identityWeight.coerceIn(0.0, 1.0),
    relationshipWeight = relationshipWeight.coerceIn(0.0, 1.0),
    emotionalWeight = emotionalWeight.coerceIn(0.0, 1.0),
    confidence = confidence.coerceIn(0.0, 1.0),
    contentDigest = contentDigest,
    sourceManifestJson = sourceManifestJson,
)

private fun DreamExperienceStateEntity.toModel() = DreamExperienceState(
    pairScopeId = DreamPairScopeId.requireCanonical(pairScopeId),
    experienceEpoch = experienceEpoch,
    observerCheckpointEpoch = observerCheckpointEpoch,
    appliedExperienceEpoch = appliedExperienceEpoch,
    profileRevision = profileRevision,
    activeSnapshotId = activeSnapshotId,
    experienceDebt = experienceDebt,
    activeRunId = activeRunId,
    activeRunLeaseUntilMs = activeRunLeaseUntilMs,
    historyBackfilledAtMs = historyBackfilledAtMs,
    updatedAtMs = updatedAtMs,
)
