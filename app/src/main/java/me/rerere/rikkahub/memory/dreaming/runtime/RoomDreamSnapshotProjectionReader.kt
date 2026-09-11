package me.rerere.rikkahub.memory.dreaming.runtime

import androidx.room.withTransaction
import java.nio.charset.StandardCharsets
import kotlin.math.roundToInt
import kotlinx.coroutines.CancellationException
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.dao.DreamExperienceDao
import me.rerere.rikkahub.data.db.dao.DreamRuntimeSourceRow
import me.rerere.rikkahub.data.db.dao.DreamSynthesisDao
import me.rerere.rikkahub.data.db.entity.DreamClaimEntity
import me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.MemoryScopeStateEntity
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryAttribution
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
import me.rerere.rikkahub.memory.MemorySourceIdentity
import me.rerere.rikkahub.memory.MemoryTruthStatus
import me.rerere.rikkahub.memory.dreaming.model.DREAM_SNAPSHOT_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityFingerprintV1
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityMemory
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityPin
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthoritySource
import me.rerere.rikkahub.memory.dreaming.model.DreamCanonicalJson
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimHead
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimSourcePin
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimState
import me.rerere.rikkahub.memory.dreaming.model.DreamClaimVersionCanonicalV1
import me.rerere.rikkahub.memory.dreaming.model.DreamContentType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSupportType
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import me.rerere.rikkahub.memory.dreaming.model.DreamValidatedClaimVersion
import me.rerere.rikkahub.memory.dreaming.experience.toDreamInputCandidate
import me.rerere.rikkahub.memory.dreaming.input.DreamInputCandidateOrigin
import me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamPairSnapshotCompatibility
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileLimits
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompileRequest
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotCompiler
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState

/**
 * Fail-closed Room boundary for one request's Dream context.
 *
 * Scope state, active Snapshot, current Claim heads, immutable versions and live authority rows are
 * all read under the same Room transaction. The immutable Snapshot is then deterministically
 * rebuilt from those exact rows; no partially readable manifest is ever returned.
 */
class RoomDreamSnapshotProjectionReader(
    private val database: AppDatabase,
    private val synthesisDao: DreamSynthesisDao,
    private val experienceDao: DreamExperienceDao,
) : DreamSnapshotProjectionReader {
    override suspend fun read(
        request: DreamSnapshotProjectionReadRequest,
    ): DreamSnapshotProjection {
        if (request.frozenNowEpochMs < 0L) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.UNKNOWN)
        }
        return try {
            database.withTransaction { readInCurrentTransaction(request) }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            unavailable(DreamSnapshotProjectionUnavailableReason.DATABASE_READ_FAILED)
        }
    }

    private suspend fun readInCurrentTransaction(
        request: DreamSnapshotProjectionReadRequest,
    ): DreamSnapshotProjection {
        val scope = request.scopeId
        if (DreamPairScopeId.parseOrNull(scope.value) != null) {
            return readPairInCurrentTransaction(request)
        }
        val state = synthesisDao.getRuntimeScopeState(scope.value)
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.SCOPE_STATE_MISSING)
        if (!state.isRuntimeCurrentFor(scope)) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.ACTIVE_SNAPSHOT_MISSING)
        }
        val activeSnapshotId = state.activeSnapshotId
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.ACTIVE_SNAPSHOT_MISSING)
        if (!isStableId(activeSnapshotId)) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val snapshot = synthesisDao.getSnapshot(activeSnapshotId, scope.value)
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.SNAPSHOT_ROW_MISSING)
        if (!snapshot.matchesRuntimeState(state)) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        if (!snapshot.hasBoundedPayload()) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val storedPayloadHash = trySha256(snapshot.payloadSha256)
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_HASH_INVALID)
        if (DreamCanonicalJson.sha256(snapshot.canonicalPayloadJson.toByteArray(StandardCharsets.UTF_8)) !=
            storedPayloadHash
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_HASH_INVALID)
        }

        val claims = synthesisDao.listRuntimeActiveClaimHeads(
            scopeId = scope.value,
            limit = MAX_DREAM_RUNTIME_PROJECTION_CLAIMS + 1,
        )
        if (claims.size > MAX_DREAM_RUNTIME_PROJECTION_CLAIMS ||
            claims.map(DreamClaimEntity::claimId).distinct().size != claims.size
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val versions = synthesisDao.listRuntimeActiveClaimVersions(
            scopeId = scope.value,
            limit = MAX_DREAM_RUNTIME_PROJECTION_CLAIMS + 1,
        )
        if (versions.size != claims.size ||
            versions.map(DreamClaimVersionEntity::claimId).distinct().size != versions.size
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.CLAIM_VERSION_MISSING)
        }
        val sourceRows = synthesisDao.listRuntimeActiveSourceRows(
            scopeId = scope.value,
            limit = MAX_RUNTIME_SOURCE_PINS + 1,
        )
        if (sourceRows.size > MAX_RUNTIME_SOURCE_PINS) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }

        val claimsById = claims.associateBy(DreamClaimEntity::claimId)
        val versionsById = versions.associateBy(DreamClaimVersionEntity::claimId)
        if (claimsById.keys != versionsById.keys || sourceRows.any { it.claimId !in claimsById }) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val rowsByClaim = sourceRows.groupBy(DreamRuntimeSourceRow::claimId)
        val loaded = ArrayList<LoadedRuntimeClaim>(claims.size)
        for (claim in claims) {
            val rows = rowsByClaim[claim.claimId].orEmpty()
            if (rows.isEmpty() || rows.size > MAX_RUNTIME_SOURCE_PINS_PER_CLAIM ||
                rows.any { it.claimRevision != claim.claimRevision }
            ) {
                return unavailable(DreamSnapshotProjectionUnavailableReason.CLAIM_VERSION_MISSING)
            }
            val parsed = parseVersion(
                entity = versionsById[claim.claimId]
                    ?: return unavailable(DreamSnapshotProjectionUnavailableReason.CLAIM_VERSION_MISSING),
                rows = rows,
                scope = scope,
                head = claim,
            ) ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            val head = parsed.toHead(scope)
            if (!claim.matchesRuntimeHead(head, state.memoryEpoch)) {
                return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            }
            val validity = aggregateSourceValidity(rows, parsed.version.sources, scope, request.frozenNowEpochMs)
                ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            loaded += LoadedRuntimeClaim(claim, parsed, rows, validity)
        }

        val compiled = try {
            DreamSnapshotCompiler.compile(
                DreamSnapshotCompileRequest(
                    scopeId = scope,
                    compilerRevision = snapshot.compilerRevision,
                    claims = loaded.map { it.parsed.toHead(scope) },
                    limits = RUNTIME_SNAPSHOT_LIMITS,
                ),
            )
        } catch (_: Exception) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_PARSE_FAILED)
        }
        if (compiled.schemaVersion != DREAM_SNAPSHOT_SCHEMA_VERSION ||
            compiled.payloadJson != snapshot.canonicalPayloadJson ||
            compiled.payloadHash != storedPayloadHash ||
            compiled.compilerRevision != snapshot.compilerRevision ||
            compiled.claimCount != snapshot.claimCount ||
            compiled.estimatedTokens != snapshot.estimatedTokens ||
            compiled.manifest.size != loaded.size
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_HASH_INVALID)
        }

        val loadedByRef = loaded.associateBy { it.parsed.version.claimId to it.parsed.version.nextRevision }
        if (loadedByRef.size != loaded.size) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val projectionClaims = compiled.manifest.map { manifest ->
            val current = loadedByRef[manifest.claimId to manifest.claimRevision]
                ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            val version = current.parsed.version
            DreamRuntimeClaimProjection(
                ref = DreamRuntimeClaimRef(manifest.claimId, manifest.claimRevision),
                scopeId = scope,
                section = manifest.section,
                ordinal = manifest.ordinal,
                snapshotState = version.nextState,
                currentState = DreamClaimState.ACTIVE_CONTEXTUAL,
                currentRevision = current.entity.claimRevision,
                currentVersionHash = current.parsed.contentHash,
                storageClass = version.storageClass,
                epistemicType = version.epistemicType,
                title = version.title,
                statement = version.statement,
                confidencePermille = version.confidencePermille,
                temporalState = version.temporalState,
                validFromEpochMs = version.validFromEpochMs,
                validToEpochMs = version.validToEpochMs,
                versionHash = current.parsed.contentHash,
                fragmentIntegrity = DreamRuntimeFragmentIntegrity.VERIFIED,
                sourceFence = DreamRuntimeSourceFence(
                    validity = current.sourceValidity,
                    validatedAtEpochMs = request.frozenNowEpochMs,
                    validatedClaimRevision = version.nextRevision,
                    directAuthoritySourceCount = version.sources.size,
                    directSupportingSourceCount = version.sources.count { source ->
                        source.supportType in DIRECT_SUPPORT_TYPES
                    },
                    indirectDerivedSourceCount = 0,
                ),
            )
        }
        return DreamSnapshotProjection.Available(
            scopeId = scope,
            schemaVersion = compiled.schemaVersion,
            snapshotId = snapshot.snapshotId,
            activeSnapshotId = state.activeSnapshotId,
            snapshotStatus = DreamRuntimeSnapshotStatus.ACTIVE,
            snapshotRevision = snapshot.snapshotRevision,
            sourceMemoryEpoch = snapshot.sourceMemoryEpoch,
            currentMemoryEpoch = state.memoryEpoch,
            committedDreamRevision = snapshot.committedDreamRevision,
            currentDreamRevision = state.dreamStateRevision,
            payloadHash = storedPayloadHash,
            payloadIntegrity = DreamRuntimePayloadIntegrity.VERIFIED,
            snapshotCompilerRevision = snapshot.compilerRevision,
            expectedClaimCount = snapshot.claimCount,
            readConsistency = DreamRuntimeReadConsistency.ATOMIC,
            claims = projectionClaims,
        )
    }

    /** Pair-Dream keeps the last committed portrait usable while newer Experiences are pending. */
    private suspend fun readPairInCurrentTransaction(
        request: DreamSnapshotProjectionReadRequest,
    ): DreamSnapshotProjection {
        val scope = request.scopeId
        val state = experienceDao.getState(scope.value)
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.SCOPE_STATE_MISSING)
        if (state.profileRevision <= 0L || state.appliedExperienceEpoch < 0L ||
            state.experienceEpoch < state.appliedExperienceEpoch
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.ACTIVE_SNAPSHOT_MISSING)
        }
        val activeSnapshotId = state.activeSnapshotId
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.ACTIVE_SNAPSHOT_MISSING)
        if (!isStableId(activeSnapshotId)) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val snapshot = synthesisDao.getSnapshot(activeSnapshotId, scope.value)
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.SNAPSHOT_ROW_MISSING)
        if (snapshot.status != SNAPSHOT_ACTIVE || snapshot.scopeId != scope.value ||
            snapshot.snapshotId != activeSnapshotId || snapshot.snapshotRevision != state.profileRevision ||
            snapshot.committedDreamRevision != state.profileRevision ||
            snapshot.sourceMemoryEpoch != state.appliedExperienceEpoch
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        if (!snapshot.hasBoundedPayload()) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val storedPayloadHash = trySha256(snapshot.payloadSha256)
            ?: return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_HASH_INVALID)
        if (DreamCanonicalJson.sha256(snapshot.canonicalPayloadJson.toByteArray(StandardCharsets.UTF_8)) !=
            storedPayloadHash
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_HASH_INVALID)
        }

        val claims = synthesisDao.listRuntimeActiveClaimHeads(
            scopeId = scope.value,
            limit = MAX_DREAM_RUNTIME_PROJECTION_CLAIMS + 1,
        )
        if (claims.size > MAX_DREAM_RUNTIME_PROJECTION_CLAIMS ||
            claims.map(DreamClaimEntity::claimId).distinct().size != claims.size
        ) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
        }
        val versions = synthesisDao.listRuntimeActiveClaimVersions(
            scopeId = scope.value,
            limit = MAX_DREAM_RUNTIME_PROJECTION_CLAIMS + 1,
        )
        val versionsById = versions.associateBy(DreamClaimVersionEntity::claimId)
        if (versionsById.size != claims.size) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.CLAIM_VERSION_MISSING)
        }

        val parsedById = linkedMapOf<String, ParsedRuntimeVersion>()
        for (claim in claims) {
            val sourceRows = synthesisDao.listClaimExperienceSources(claim.claimId, claim.claimRevision)
            if (sourceRows.isEmpty() || sourceRows.size > MAX_RUNTIME_SOURCE_PINS_PER_CLAIM) {
                return unavailable(DreamSnapshotProjectionUnavailableReason.CLAIM_VERSION_MISSING)
            }
            val sources = sourceRows.map { source ->
                val experience = experienceDao.getExperience(source.experienceId, scope.value)
                    ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
                if (experience.status == "DISCARDED" ||
                    experience.experienceEpoch != source.experienceEpoch ||
                    experience.contentDigest != source.contentDigest
                ) {
                    return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
                }
                val candidate = experience.toDreamInputCandidate(
                    scopeId = scope,
                    origin = DreamInputCandidateOrigin.AUTHORITY_CHANGE,
                    json = RUNTIME_JSON,
                ) ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
                if (candidate.pin.expectedSourceManifestHash.value != source.sourceManifestHash) {
                    return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
                }
                DreamClaimSourcePin(
                    authority = candidate.pin,
                    supportType = runCatching { enumValueOf<DreamSupportType>(source.supportType) }.getOrNull()
                        ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID),
                    directAuthority = true,
                )
            }
            val version = versionsById[claim.claimId]
                ?: return unavailable(DreamSnapshotProjectionUnavailableReason.CLAIM_VERSION_MISSING)
            val parsed = parseVersion(version, sources, claim)
                ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            val head = parsed.toHead(scope)
            if (!claim.matchesRuntimeHead(head, state.appliedExperienceEpoch)) {
                return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            }
            parsedById[claim.claimId] = parsed
        }

        val compiled = try {
            DreamSnapshotCompiler.compile(
                DreamSnapshotCompileRequest(
                    scopeId = scope,
                    compilerRevision = snapshot.compilerRevision,
                    claims = claims.map { claim -> parsedById.getValue(claim.claimId).toHead(scope) },
                    limits = RUNTIME_SNAPSHOT_LIMITS,
                ),
            )
        } catch (_: Exception) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_PARSE_FAILED)
        }
        val exactSnapshot = compiled.payloadJson == snapshot.canonicalPayloadJson &&
            compiled.payloadHash == storedPayloadHash && compiled.claimCount == snapshot.claimCount &&
            compiled.estimatedTokens == snapshot.estimatedTokens
        val legacyPairMetadataDrift = !exactSnapshot && compiled.claimCount == snapshot.claimCount &&
            DreamPairSnapshotCompatibility.isLegacyV1MetadataProjectionDrift(
                scopeId = scope,
                compilerRevision = snapshot.compilerRevision,
                storedPayloadJson = snapshot.canonicalPayloadJson,
                expectedPayloadJson = compiled.payloadJson,
            )
        if (!exactSnapshot && !legacyPairMetadataDrift) {
            return unavailable(DreamSnapshotProjectionUnavailableReason.PAYLOAD_HASH_INVALID)
        }
        // For the narrowly recognized v1 Pair metadata projection bug, the persisted ClaimVersion
        // graph is authoritative and has just passed exact Experience/source verification above.
        // Bind the runtime context/cache to the deterministic reconstruction rather than to the
        // historically misclassified snapshot bytes. A later fixed synthesis will replace it with
        // a normal v2 snapshot and this branch stops applying automatically.
        val projectionPayloadHash = if (legacyPairMetadataDrift) compiled.payloadHash else storedPayloadHash
        val claimsById = claims.associateBy(DreamClaimEntity::claimId)
        val projectionClaims = compiled.manifest.map { manifest ->
            val current = claimsById[manifest.claimId]
                ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            val parsed = parsedById[manifest.claimId]
                ?: return unavailable(DreamSnapshotProjectionUnavailableReason.MANIFEST_INVALID)
            val version = parsed.version
            DreamRuntimeClaimProjection(
                ref = DreamRuntimeClaimRef(manifest.claimId, manifest.claimRevision),
                scopeId = scope,
                section = manifest.section,
                ordinal = manifest.ordinal,
                snapshotState = version.nextState,
                currentState = DreamClaimState.ACTIVE_CONTEXTUAL,
                currentRevision = current.claimRevision,
                currentVersionHash = parsed.contentHash,
                storageClass = version.storageClass,
                epistemicType = version.epistemicType,
                title = version.title,
                statement = version.statement,
                confidencePermille = version.confidencePermille,
                temporalState = version.temporalState,
                validFromEpochMs = version.validFromEpochMs,
                validToEpochMs = version.validToEpochMs,
                versionHash = parsed.contentHash,
                fragmentIntegrity = DreamRuntimeFragmentIntegrity.VERIFIED,
                sourceFence = DreamRuntimeSourceFence(
                    validity = DreamRuntimeSourceValidity.CURRENT_CONFIRMED,
                    validatedAtEpochMs = request.frozenNowEpochMs,
                    validatedClaimRevision = version.nextRevision,
                    directAuthoritySourceCount = version.sources.size,
                    directSupportingSourceCount = version.sources.count {
                        it.supportType in DIRECT_SUPPORT_TYPES
                    },
                    indirectDerivedSourceCount = 0,
                ),
            )
        }
        return DreamSnapshotProjection.Available(
            scopeId = scope,
            schemaVersion = compiled.schemaVersion,
            snapshotId = snapshot.snapshotId,
            activeSnapshotId = state.activeSnapshotId,
            snapshotStatus = DreamRuntimeSnapshotStatus.ACTIVE,
            snapshotRevision = snapshot.snapshotRevision,
            sourceMemoryEpoch = state.appliedExperienceEpoch,
            currentMemoryEpoch = state.experienceEpoch,
            committedDreamRevision = snapshot.committedDreamRevision,
            currentDreamRevision = state.profileRevision,
            payloadHash = projectionPayloadHash,
            payloadIntegrity = DreamRuntimePayloadIntegrity.VERIFIED,
            snapshotCompilerRevision = snapshot.compilerRevision,
            expectedClaimCount = compiled.claimCount,
            readConsistency = DreamRuntimeReadConsistency.ATOMIC,
            claims = projectionClaims,
        )
    }
}

private data class ParsedRuntimeVersion(
    val version: DreamValidatedClaimVersion,
    val contentHash: DreamSha256,
) {
    fun toHead(scope: DreamScopeId) = DreamClaimHead(
        claimId = version.claimId,
        scopeId = scope,
        revision = version.nextRevision,
        claimKey = version.claimKey,
        storageClass = version.storageClass,
        epistemicType = version.epistemicType,
        state = version.nextState,
        title = version.title,
        statement = version.statement,
        confidencePermille = version.confidencePermille,
        temporalState = version.temporalState,
        validFromEpochMs = version.validFromEpochMs,
        validToEpochMs = version.validToEpochMs,
        versionHash = contentHash,
        sources = version.sources,
        subjectKind = version.subjectKind,
        profileSection = version.profileSection,
        epistemicOrigin = version.epistemicOrigin,
        contentType = version.contentType,
    )
}

private data class LoadedRuntimeClaim(
    val entity: DreamClaimEntity,
    val parsed: ParsedRuntimeVersion,
    val rows: List<DreamRuntimeSourceRow>,
    val sourceValidity: DreamRuntimeSourceValidity,
)

private fun parseVersion(
    entity: DreamClaimVersionEntity,
    rows: List<DreamRuntimeSourceRow>,
    scope: DreamScopeId,
    head: DreamClaimEntity,
): ParsedRuntimeVersion? {
    val sources = rows.map { row -> row.toImmutableSourcePin(scope) ?: return null }
    return parseVersion(entity, sources, head)
}

private fun parseVersion(
    entity: DreamClaimVersionEntity,
    sources: List<DreamClaimSourcePin>,
    head: DreamClaimEntity,
): ParsedRuntimeVersion? {
    if (entity.canonicalClaimJson.toByteArray(StandardCharsets.UTF_8).size > MAX_CLAIM_VERSION_UTF8_BYTES) {
        return null
    }
    val root = try {
        RUNTIME_JSON.parseToJsonElement(entity.canonicalClaimJson) as? kotlinx.serialization.json.JsonObject
    } catch (_: Exception) {
        null
    } ?: return null
    if (root.keys != CLAIM_VERSION_KEYS || DreamCanonicalJson.encode(root) != entity.canonicalClaimJson) {
        return null
    }
    val revision = root.runtimeLong("revision") ?: return null
    if (revision != entity.claimRevision) return null
    val validFrom = root.runtimeNullableLong("valid_from_epoch_ms") ?: return null
    val validTo = root.runtimeNullableLong("valid_to_epoch_ms") ?: return null
    val version = try {
        DreamValidatedClaimVersion(
            claimId = root.runtimeString("claim_id") ?: return null,
            expectedPreviousRevision = if (revision == 1L) null else revision - 1L,
            nextRevision = revision,
            claimKey = root.runtimeString("claim_key") ?: return null,
            storageClass = enumValueOf(root.runtimeString("storage_class") ?: return null),
            epistemicType = enumValueOf(root.runtimeString("epistemic_type") ?: return null),
            nextState = enumValueOf(root.runtimeString("state") ?: return null),
            title = root.runtimeString("title") ?: return null,
            statement = root.runtimeString("statement") ?: return null,
            confidencePermille = root.runtimeInt("confidence_permille") ?: return null,
            temporalState = enumValueOf(root.runtimeString("temporal_state") ?: return null),
            validFromEpochMs = validFrom.value,
            validToEpochMs = validTo.value,
            sources = sources,
            reason = enumValueOf(root.runtimeString("reason") ?: return null),
            subjectKind = enumValueOf<DreamSubjectKind>(head.subjectKind),
            profileSection = head.profileSection,
            epistemicOrigin = enumValueOf<DreamEpistemicOrigin>(head.epistemicOrigin),
            contentType = enumValueOf<DreamContentType>(head.contentType),
        )
    } catch (_: Exception) {
        return null
    }
    if (version.nextState != DreamClaimState.ACTIVE_CONTEXTUAL) return null
    val canonical = DreamClaimVersionCanonicalV1.encode(version)
    if (canonical.canonicalClaimJson != entity.canonicalClaimJson ||
        canonical.contentHash.value != entity.contentHash ||
        canonical.sourceManifestHash.value != entity.sourceManifestHash ||
        canonical.sourceManifestHash.value != root.runtimeString("source_manifest_hash") ||
        version.reason.name != entity.reasonCode
    ) return null
    return ParsedRuntimeVersion(version, canonical.contentHash)
}

private fun DreamRuntimeSourceRow.toImmutableSourcePin(scope: DreamScopeId): DreamClaimSourcePin? {
    if (revisionRowId == null || revisionSourceIdentitiesJson == null ||
        revisionSourceIdentitiesJson.toByteArray(StandardCharsets.UTF_8).size > MAX_SOURCE_JSON_UTF8_BYTES
    ) return null
    val revisionSources = revisionSourceIdentitiesJson.decodeAuthoritySourcesOrNull() ?: return null
    return try {
        DreamClaimSourcePin(
            authority = DreamAuthorityPin(
                scopeId = scope,
                memoryId = memoryId.toString(),
                expectedRevision = memoryRevision.toLong(),
                expectedAuthorityFingerprint = DreamSha256(memorySemanticHash),
                expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(revisionSources),
            ),
            supportType = enumValueOf(supportType),
            directAuthority = true,
        )
    } catch (_: Exception) {
        null
    }
}

private fun aggregateSourceValidity(
    rows: List<DreamRuntimeSourceRow>,
    pins: List<DreamClaimSourcePin>,
    scope: DreamScopeId,
    frozenNowEpochMs: Long,
): DreamRuntimeSourceValidity? {
    if (rows.size != pins.size) return null
    val validities = rows.zip(pins).map { (row, pin) ->
        row.liveValidity(pin, scope, frozenNowEpochMs) ?: return null
    }
    return validities.maxByOrNull(::sourceValiditySeverity)
        ?: return null
}

private fun DreamRuntimeSourceRow.liveValidity(
    pin: DreamClaimSourcePin,
    scope: DreamScopeId,
    frozenNowEpochMs: Long,
): DreamRuntimeSourceValidity? {
    if (pin.authority.memoryId != memoryId.toString() ||
        pin.authority.expectedRevision != memoryRevision.toLong() ||
        pin.authority.expectedAuthorityFingerprint.value != memorySemanticHash
    ) return null
    if (currentMemoryId == null) return DreamRuntimeSourceValidity.MISSING
    if (currentMemoryId != memoryId) return null
    if (currentMemoryRevision != memoryRevision) return DreamRuntimeSourceValidity.REVISION_MISMATCH
    val currentSourceJson = currentSourceIdentitiesJson ?: return null
    if (currentSourceJson.toByteArray(StandardCharsets.UTF_8).size > MAX_SOURCE_JSON_UTF8_BYTES) return null
    val currentSources = currentSourceJson.decodeAuthoritySourcesOrNull() ?: return null
    if (DreamAuthorityFingerprintV1.sourceManifestHash(currentSources) !=
        pin.authority.expectedSourceManifestHash
    ) return DreamRuntimeSourceValidity.SOURCE_MANIFEST_MISMATCH
    val authority = toCurrentAuthority(scope, currentSources) ?: return null
    if (DreamAuthorityFingerprintV1.compute(authority) != pin.authority.expectedAuthorityFingerprint) {
        return DreamRuntimeSourceValidity.FINGERPRINT_MISMATCH
    }
    if (authority.lifecycleStatus != MemoryLifecycleStatus.ACTIVE) {
        return DreamRuntimeSourceValidity.LIFECYCLE_INVALID
    }
    if (authority.truthStatus != MemoryTruthStatus.CONFIRMED) {
        return DreamRuntimeSourceValidity.TRUTH_INVALID
    }
    if (authority.expiresAtEpochMs != null && authority.expiresAtEpochMs <= frozenNowEpochMs) {
        return DreamRuntimeSourceValidity.EXPIRED
    }
    return DreamRuntimeSourceValidity.CURRENT_CONFIRMED
}

private fun DreamRuntimeSourceRow.toCurrentAuthority(
    scope: DreamScopeId,
    sources: List<DreamAuthoritySource>,
): DreamAuthorityMemory? = try {
    DreamAuthorityMemory(
        scopeId = scope,
        memoryId = (currentMemoryId ?: return null).toString(),
        revision = (currentMemoryRevision ?: return null).toLong(),
        title = currentTitle,
        content = currentContent ?: return null,
        kind = enumValueOf<MemoryKind>(currentMemoryKind ?: return null),
        attribution = enumValueOf<MemoryAttribution>(currentAttribution ?: return null),
        truthStatus = enumValueOf<MemoryTruthStatus>(currentTruthStatus ?: return null),
        lifecycleStatus = enumValueOf<MemoryLifecycleStatus>(currentLifecycleStatus ?: return null),
        approvalSource = enumValueOf<MemoryApprovalSource>(currentApprovalSource ?: return null),
        tags = currentTagsJson?.decodeStringsOrNull() ?: return null,
        createdAtEpochMs = currentCreatedAtMs ?: return null,
        updatedAtEpochMs = currentUpdatedAtMs ?: return null,
        occurredAtEpochMs = currentOccurredAtMs,
        expiresAtEpochMs = currentExpiresAtMs,
        originAssistantId = currentOriginAssistantId,
        participants = currentParticipantsJson?.decodeStringsOrNull() ?: return null,
        outcome = currentOutcome,
        sources = sources,
        tombstoned = false,
    )
} catch (_: Exception) {
    null
}

private fun MemoryScopeStateEntity.isRuntimeCurrentFor(scope: DreamScopeId): Boolean =
    scopeId == scope.value && memoryEpoch >= 0L && observerCheckpointEpoch >= 0L &&
        lastAppliedMemoryEpoch == memoryEpoch && dreamStateRevision > 0L

private fun DreamSnapshotEntity.matchesRuntimeState(state: MemoryScopeStateEntity): Boolean =
    snapshotId == state.activeSnapshotId && scopeId == state.scopeId && status == SNAPSHOT_ACTIVE &&
        snapshotRevision > 0L && snapshotRevision == state.dreamStateRevision &&
        committedDreamRevision == state.dreamStateRevision && sourceMemoryEpoch == state.memoryEpoch &&
        claimCount in 0..MAX_DREAM_RUNTIME_PROJECTION_CLAIMS && estimatedTokens >= 0

private fun DreamSnapshotEntity.hasBoundedPayload(): Boolean =
    canonicalPayloadJson.isNotEmpty() &&
        canonicalPayloadJson.toByteArray(StandardCharsets.UTF_8).size <= RUNTIME_SNAPSHOT_LIMITS.maxPayloadUtf8Bytes

private fun DreamClaimEntity.matchesRuntimeHead(head: DreamClaimHead, memoryEpoch: Long): Boolean =
    scopeId == head.scopeId.value && claimId == head.claimId && claimRevision == head.revision &&
        claimKey == head.claimKey && storageClass == head.storageClass.name &&
        epistemicType == head.epistemicType.name && subjectKind == head.subjectKind.name &&
        profileSection == head.profileSection && epistemicOrigin == head.epistemicOrigin.name &&
        contentType == head.contentType.name && state == DreamClaimState.ACTIVE_CONTEXTUAL.name &&
        state == head.state.name && title == head.title && statement == head.statement &&
        confidence.isFinite() && (confidence * 1_000.0).roundToInt() == head.confidencePermille &&
        temporalState == head.temporalState.name && validFromMs == head.validFromEpochMs &&
        validToMs == head.validToEpochMs && claimHash == head.versionHash.value &&
        lastValidatedMemoryEpoch == memoryEpoch

private fun String.decodeStringsOrNull(): List<String>? = try {
    if (toByteArray(StandardCharsets.UTF_8).size > MAX_SOURCE_JSON_UTF8_BYTES) return null
    RUNTIME_JSON.decodeFromString<List<String>>(this)
} catch (_: Exception) {
    null
}

private fun String.decodeAuthoritySourcesOrNull(): List<DreamAuthoritySource>? = try {
    val identities = RUNTIME_JSON.decodeFromString<List<MemorySourceIdentity>>(this)
    if (identities.size > MAX_AUTHORITY_SOURCES) return null
    identities.map { source ->
        DreamAuthoritySource(
            conversationId = source.conversationId,
            messageId = source.messageId,
            role = source.role,
            sourceKind = source.sourceKind,
            consumedTextDigest = DreamSha256(source.consumedTextDigest),
            evidenceGroupId = source.evidenceGroupId,
        )
    }
} catch (_: Exception) {
    null
}

private fun kotlinx.serialization.json.JsonObject.runtimeString(key: String): String? =
    (this[key] as? kotlinx.serialization.json.JsonPrimitive)
        ?.takeIf(kotlinx.serialization.json.JsonPrimitive::isString)
        ?.contentOrNull

private fun kotlinx.serialization.json.JsonObject.runtimeInt(key: String): Int? =
    (this[key] as? kotlinx.serialization.json.JsonPrimitive)
        ?.takeUnless(kotlinx.serialization.json.JsonPrimitive::isString)
        ?.intOrNull

private fun kotlinx.serialization.json.JsonObject.runtimeLong(key: String): Long? =
    (this[key] as? kotlinx.serialization.json.JsonPrimitive)
        ?.takeUnless(kotlinx.serialization.json.JsonPrimitive::isString)
        ?.longOrNull

private fun kotlinx.serialization.json.JsonObject.runtimeNullableLong(key: String): RuntimeNullableLong? {
    val value = this[key] ?: return null
    if (value === kotlinx.serialization.json.JsonNull) return RuntimeNullableLong(null)
    return (value as? kotlinx.serialization.json.JsonPrimitive)
        ?.takeUnless(kotlinx.serialization.json.JsonPrimitive::isString)
        ?.longOrNull
        ?.let(::RuntimeNullableLong)
}

private data class RuntimeNullableLong(val value: Long?)

private fun trySha256(value: String): DreamSha256? = try {
    DreamSha256(value)
} catch (_: Exception) {
    null
}

private fun isStableId(value: String): Boolean = try {
    requireDreamStableId(value)
    true
} catch (_: Exception) {
    false
}

private fun unavailable(reason: DreamSnapshotProjectionUnavailableReason) =
    DreamSnapshotProjection.Unavailable(reason)

private fun sourceValiditySeverity(validity: DreamRuntimeSourceValidity): Int = when (validity) {
    DreamRuntimeSourceValidity.CURRENT_CONFIRMED -> 0
    DreamRuntimeSourceValidity.EXPIRED -> 1
    DreamRuntimeSourceValidity.TRUTH_INVALID -> 2
    DreamRuntimeSourceValidity.LIFECYCLE_INVALID -> 3
    DreamRuntimeSourceValidity.REVISION_MISMATCH -> 4
    DreamRuntimeSourceValidity.SOURCE_MANIFEST_MISMATCH -> 5
    DreamRuntimeSourceValidity.FINGERPRINT_MISMATCH -> 6
    DreamRuntimeSourceValidity.SCOPE_MISMATCH -> 7
    DreamRuntimeSourceValidity.TOMBSTONED -> 8
    DreamRuntimeSourceValidity.MISSING -> 9
    DreamRuntimeSourceValidity.UNKNOWN -> 10
}

private val RUNTIME_JSON = Json { ignoreUnknownKeys = false; isLenient = false }
private val RUNTIME_SNAPSHOT_LIMITS = DreamSnapshotCompileLimits(maxActiveClaims = MAX_DREAM_RUNTIME_PROJECTION_CLAIMS)
private val DIRECT_SUPPORT_TYPES = setOf(DreamSupportType.SUPPORTS, DreamSupportType.SUPERSEDES)
private val CLAIM_VERSION_KEYS = setOf(
    "claim_id",
    "claim_key",
    "confidence_permille",
    "epistemic_type",
    "reason",
    "revision",
    "source_manifest_hash",
    "state",
    "statement",
    "storage_class",
    "temporal_state",
    "title",
    "valid_from_epoch_ms",
    "valid_to_epoch_ms",
)
private const val SNAPSHOT_ACTIVE = "ACTIVE"
private const val MAX_RUNTIME_SOURCE_PINS = 32_768
private const val MAX_RUNTIME_SOURCE_PINS_PER_CLAIM = 4_096
private const val MAX_AUTHORITY_SOURCES = 4_096
private const val MAX_CLAIM_VERSION_UTF8_BYTES = 128 * 1_024
private const val MAX_SOURCE_JSON_UTF8_BYTES = 2 * 1_024 * 1_024
