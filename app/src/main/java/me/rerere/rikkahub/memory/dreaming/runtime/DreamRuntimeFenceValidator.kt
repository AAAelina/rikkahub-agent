package me.rerere.rikkahub.memory.dreaming.runtime

import me.rerere.rikkahub.memory.dreaming.model.DREAM_SNAPSHOT_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId

/** Pure, deterministic validation of the storage projection used by one generation. */
object DreamRuntimeFenceValidator {
    fun validate(
        projection: DreamSnapshotProjection,
        expectedScopeId: DreamScopeId,
    ): DreamRuntimeFenceResult {
        if (projection is DreamSnapshotProjection.Unavailable) {
            return DreamRuntimeFenceResult.Invalid(
                failures = listOf(DreamRuntimeFenceFailure.PROJECTION_UNAVAILABLE),
                unavailableReason = projection.reason,
            )
        }
        projection as DreamSnapshotProjection.Available

        val failures = buildList {
            if (projection.readConsistency != DreamRuntimeReadConsistency.ATOMIC) {
                add(DreamRuntimeFenceFailure.READ_NOT_ATOMIC)
            }
            if (projection.scopeId != expectedScopeId) {
                add(DreamRuntimeFenceFailure.SCOPE_MISMATCH)
            }
            if (projection.schemaVersion != DREAM_SNAPSHOT_SCHEMA_VERSION) {
                add(DreamRuntimeFenceFailure.SCHEMA_UNSUPPORTED)
            }
            if (!isCanonicalStableId(projection.snapshotId) ||
                projection.activeSnapshotId?.let(::isCanonicalStableId) != true
            ) {
                add(DreamRuntimeFenceFailure.SNAPSHOT_ID_INVALID)
            }
            if (projection.snapshotStatus != DreamRuntimeSnapshotStatus.ACTIVE) {
                add(DreamRuntimeFenceFailure.SNAPSHOT_NOT_ACTIVE)
            }
            if (projection.snapshotId != projection.activeSnapshotId) {
                add(DreamRuntimeFenceFailure.ACTIVE_POINTER_MISMATCH)
            }
            if (projection.sourceMemoryEpoch < 0L || projection.currentMemoryEpoch < 0L ||
                projection.committedDreamRevision <= 0L || projection.currentDreamRevision <= 0L ||
                projection.snapshotRevision <= 0L
            ) {
                add(DreamRuntimeFenceFailure.EPOCH_VALUE_INVALID)
            }
            val pairScope = DreamPairScopeId.parseOrNull(projection.scopeId.value) != null
            if ((!pairScope && projection.sourceMemoryEpoch != projection.currentMemoryEpoch) ||
                (pairScope && projection.sourceMemoryEpoch > projection.currentMemoryEpoch)
            ) {
                add(DreamRuntimeFenceFailure.MEMORY_EPOCH_MISMATCH)
            }
            if (projection.committedDreamRevision != projection.currentDreamRevision) {
                add(DreamRuntimeFenceFailure.DREAM_REVISION_MISMATCH)
            }
            if (projection.snapshotRevision != projection.committedDreamRevision ||
                projection.snapshotRevision != projection.currentDreamRevision
            ) {
                add(DreamRuntimeFenceFailure.SNAPSHOT_REVISION_MISMATCH)
            }
            if (projection.payloadIntegrity != DreamRuntimePayloadIntegrity.VERIFIED) {
                add(DreamRuntimeFenceFailure.PAYLOAD_INTEGRITY_FAILED)
            }
            if (!COMPILER_REVISION.matches(projection.snapshotCompilerRevision)) {
                add(DreamRuntimeFenceFailure.COMPILER_REVISION_INVALID)
            }
            if (projection.expectedClaimCount !in 0..MAX_DREAM_RUNTIME_PROJECTION_CLAIMS ||
                projection.claims.size != projection.expectedClaimCount ||
                projection.claims.size > MAX_DREAM_RUNTIME_PROJECTION_CLAIMS
            ) {
                add(DreamRuntimeFenceFailure.CLAIM_COUNT_INVALID)
            }
            if (projection.claims.any { it.scopeId != projection.scopeId }) {
                add(DreamRuntimeFenceFailure.CLAIM_SCOPE_MISMATCH)
            }
            if (projection.claims.map { it.ref }.distinct().size != projection.claims.size) {
                add(DreamRuntimeFenceFailure.DUPLICATE_CLAIM_REF)
            }
            if (!hasCanonicalOrdinals(projection.claims)) {
                add(DreamRuntimeFenceFailure.MANIFEST_ORDINAL_INVALID)
            }
            if (projection.claims.any {
                    it.fragmentIntegrity != DreamRuntimeFragmentIntegrity.VERIFIED
                }
            ) {
                add(DreamRuntimeFenceFailure.CLAIM_FRAGMENT_INTEGRITY_FAILED)
            }
        }.distinct()

        return if (failures.isEmpty()) {
            DreamRuntimeFenceResult.Valid(projection)
        } else {
            DreamRuntimeFenceResult.Invalid(failures)
        }
    }

    private fun hasCanonicalOrdinals(claims: List<DreamRuntimeClaimProjection>): Boolean =
        claims.groupBy(DreamRuntimeClaimProjection::section).values.all { sectionClaims ->
            sectionClaims.map(DreamRuntimeClaimProjection::ordinal).sorted() ==
                sectionClaims.indices.toList()
        }

    private fun isCanonicalStableId(value: String): Boolean = runCatching {
        requireDreamStableId(value)
    }.isSuccess

    private val COMPILER_REVISION = Regex("^[A-Za-z0-9._-]{1,64}$")
}
