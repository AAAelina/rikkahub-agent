package me.rerere.rikkahub.memory.dreaming.store

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.dao.DreamDao
import me.rerere.rikkahub.data.db.dao.DreamSynthesisDao
import me.rerere.rikkahub.data.db.dao.MemoryDAO
import me.rerere.rikkahub.data.db.dao.MemoryV2Dao
import me.rerere.rikkahub.data.db.entity.DreamSnapshotEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryRevisionEntity
import me.rerere.rikkahub.memory.MemorySourceIdentity
import me.rerere.rikkahub.memory.dreaming.model.DREAM_SNAPSHOT_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamCanonicalJson
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotSection

/** Child-first privacy erasure composed inside the caller's authority Room transaction. */
class RoomDreamPrivacyScrubber(
    private val database: AppDatabase,
    private val dreamDao: DreamDao,
    private val synthesisDao: DreamSynthesisDao,
    private val memoryDao: MemoryDAO,
    private val memoryV2Dao: MemoryV2Dao,
    private val json: Json,
) : DreamPrivacyScrubber {
    override suspend fun scrubInCurrentTransaction(
        request: DreamPrivacyScrubRequest,
    ): DreamPrivacyScrubResult {
        check(database.inTransaction()) { "dream_privacy_outer_transaction_required" }
        val scopeId = request.scopeId.value
        val state = dreamDao.getScopeState(scopeId)
            ?: return DreamPrivacyScrubResult.Rejected(DreamPrivacyScrubRejection.SCOPE_MISSING)
        val allScopeMemories = memoryDao.getMemoriesOfScopeIncludingInactive(
            scopeId,
            MAX_PRIVACY_MEMORIES + 1,
        )
        if (allScopeMemories.size > MAX_PRIVACY_MEMORIES) return boundedRejection()
        val targetMemoryIds = resolveTargetMemoryIds(request, allScopeMemories)
            ?: return boundedRejection()

        val entireScope = request.targets.singleOrNull() == DreamPrivacyTarget.EntireScope
        // Start from the bounded, scope-qualified parent set. Do not authorize a privacy mutation
        // through the otherwise useful global memory-pin lookup.
        val allScopeClaims = synthesisDao.listClaims(scopeId, MAX_PRIVACY_CLAIMS + 1)
        if (allScopeClaims.size > MAX_PRIVACY_CLAIMS) return boundedRejection()
        val allScopeVersions = linkedMapOf<String, List<me.rerere.rikkahub.data.db.entity.DreamClaimVersionEntity>>()
        var inspectedVersions = 0
        for (claim in allScopeClaims) {
            val claimVersions = synthesisDao.listClaimVersions(claim.claimId, scopeId)
            inspectedVersions += claimVersions.size
            if (inspectedVersions > MAX_PRIVACY_VERSIONS) return boundedRejection()
            allScopeVersions[claim.claimId] = claimVersions
        }
        var inspectedSourceRows = 0
        val affectedClaimIds = if (entireScope) {
            allScopeClaims.mapTo(linkedSetOf()) { it.claimId }
        } else {
            val affected = linkedSetOf<String>()
            for ((claimId, claimVersions) in allScopeVersions) {
                var claimAffected = false
                for (version in claimVersions) {
                    val sources = synthesisDao.listClaimVersionSources(
                        version.claimId,
                        version.claimRevision,
                    )
                    inspectedSourceRows += sources.size
                    if (inspectedSourceRows > MAX_PRIVACY_SOURCE_ROWS) return boundedRejection()
                    if (sources.any { it.memoryId in targetMemoryIds }) claimAffected = true
                }
                if (claimAffected) affected += claimId
            }
            affected
        }
        val claims = allScopeClaims.filter { it.claimId in affectedClaimIds }
        val versions = claims.flatMap { allScopeVersions.getValue(it.claimId) }

        // The active pointer itself is part of the Dream store integrity contract. Even when the
        // privacy target does not affect any claim, an existing active snapshot must still be
        // loaded so the pointer can be validated against the bounded snapshot set. Otherwise an
        // ordinary source deletion with no affected Dream claims produces snapshots = emptyList()
        // and is falsely rejected below as STORE_CORRUPTION whenever activeSnapshotId is non-null.
        val shouldInspectSnapshots = shouldInspectDreamPrivacySnapshots(
            entireScope = entireScope,
            hasAffectedClaims = claims.isNotEmpty(),
            activeSnapshotId = state.activeSnapshotId,
        )
        val snapshots = if (shouldInspectSnapshots) {
            synthesisDao.listSnapshots(scopeId, MAX_PRIVACY_SNAPSHOTS + 1)
        } else {
            emptyList()
        }
        if (snapshots.size > MAX_PRIVACY_SNAPSHOTS) return boundedRejection()
        if (state.activeSnapshotId != null &&
            snapshots.none { it.snapshotId == state.activeSnapshotId }
        ) {
            return DreamPrivacyScrubResult.Rejected(DreamPrivacyScrubRejection.STORE_CORRUPTION)
        }
        val affectedVersions = versions.mapTo(hashSetOf()) { it.claimId to it.claimRevision }
        val snapshotsToScrub = if (entireScope) {
            snapshots.filter { it.status != "TOMBSTONED" || it.canonicalPayloadJson.isNotEmpty() }
        } else {
            snapshots.mapNotNull { snapshot ->
                val manifest = snapshot.manifestOrNull()
                if (manifest == null) {
                    // Historical snapshots may have been compiled under an older snapshot
                    // contract. During privacy/source invalidation we cannot safely prove that
                    // an unreadable manifest is unrelated to the removed source, so scrub the
                    // bounded snapshot instead of rejecting the outer conversation transaction.
                    snapshot.takeIf { entry ->
                        entry.status != "TOMBSTONED" || entry.canonicalPayloadJson.isNotEmpty()
                    }
                } else {
                    snapshot.takeIf { entry -> manifest.any(affectedVersions::contains) &&
                        (entry.status != "TOMBSTONED" || entry.canonicalPayloadJson.isNotEmpty())
                    }
                }
            }
        }
        val activeSnapshotAffected = state.activeSnapshotId?.let { activeId ->
            snapshotsToScrub.any { it.snapshotId == activeId }
        } == true
        val claimsToTombstone = claims.filter {
            it.state != "TOMBSTONED" || it.title.isNotEmpty() || it.statement.isNotEmpty()
        }
        val versionsToClear = versions.filter { it.canonicalClaimJson.isNotEmpty() }
        var sourceRowsBefore = 0
        versions.forEach { version ->
            sourceRowsBefore += synthesisDao.listClaimVersionSources(
                version.claimId,
                version.claimRevision,
            ).size
            if (sourceRowsBefore > MAX_PRIVACY_SOURCE_ROWS) return boundedRejection()
        }
        val nonNoOp = claimsToTombstone.isNotEmpty() || versionsToClear.isNotEmpty() ||
            snapshotsToScrub.isNotEmpty() || sourceRowsBefore > 0
        if (!nonNoOp) {
            return DreamPrivacyScrubResult.Scrubbed(
                claimsTombstoned = 0,
                versionsCleared = 0,
                snapshotsTombstoned = 0,
                sourceRowsDeleted = 0,
                activeSnapshotCleared = false,
                nextDreamRevision = state.dreamStateRevision,
            )
        }

        var claimsTombstoned = 0
        claimsToTombstone.forEach { claim ->
                claimsTombstoned += synthesisDao.tombstoneClaimAndScrub(
                    claimId = claim.claimId,
                    scopeId = scopeId,
                    reasonCode = PRIVACY_REASON,
                    nowMs = request.scrubbedAtEpochMs,
                )
            }
        var versionsCleared = 0
        versionsToClear.forEach { version ->
            versionsCleared += synthesisDao.scrubClaimVersion(
                claimId = version.claimId,
                claimRevision = version.claimRevision,
            )
        }
        var snapshotsTombstoned = 0
        snapshotsToScrub.forEach { snapshot ->
                snapshotsTombstoned += synthesisDao.tombstoneSnapshotAndScrub(
                    snapshotId = snapshot.snapshotId,
                    scopeId = scopeId,
                )
            }

        if (synthesisDao.advancePrivacyRevisionCas(
                scopeId = scopeId,
                expectedMemoryEpoch = state.memoryEpoch,
                expectedDreamRevision = state.dreamStateRevision,
                expectedActiveSnapshotId = state.activeSnapshotId,
                clearActiveSnapshot = activeSnapshotAffected,
                reasonCode = PRIVACY_REASON,
                nowMs = request.scrubbedAtEpochMs,
            ) != 1
        ) {
            return DreamPrivacyScrubResult.Rejected(
                DreamPrivacyScrubRejection.ACTIVE_SNAPSHOT_CAS_CONFLICT,
            )
        }

        var sourceRowsDeleted = 0
        claims.forEach { claim ->
            sourceRowsDeleted += synthesisDao.deleteSourcesForClaim(claim.claimId, scopeId)
        }
        return DreamPrivacyScrubResult.Scrubbed(
            claimsTombstoned = claimsTombstoned,
            versionsCleared = versionsCleared,
            snapshotsTombstoned = snapshotsTombstoned,
            sourceRowsDeleted = sourceRowsDeleted,
            activeSnapshotCleared = activeSnapshotAffected,
            nextDreamRevision = state.dreamStateRevision + 1L,
        )
    }

    private suspend fun resolveTargetMemoryIds(
        request: DreamPrivacyScrubRequest,
        allScopeMemories: List<MemoryEntity>,
    ): Set<Int>? {
        if (DreamPrivacyTarget.EntireScope in request.targets) {
            return allScopeMemories.mapTo(linkedSetOf(), MemoryEntity::id)
        }
        val byId = allScopeMemories.associateBy(MemoryEntity::id)
        val result = linkedSetOf<Int>()
        request.targets.forEach { target ->
            when (target) {
                DreamPrivacyTarget.EntireScope -> Unit
                is DreamPrivacyTarget.AuthorityMemory -> {
                    val memoryId = target.memoryId.toIntOrNull() ?: return null
                    if (byId[memoryId] != null) result += memoryId
                }

                is DreamPrivacyTarget.ConversationSource -> {
                    allScopeMemories.forEach { memory ->
                        val headMatch = memory.matchesOrNull(target) ?: return null
                        val revisions = memoryV2Dao.getRevisionsForMemory(
                            memory.id,
                            request.scopeId.value,
                        )
                        val revisionMatches = revisions.map { it.matchesOrNull(target) }
                        if (revisionMatches.any { it == null }) return null
                        if (headMatch || revisionMatches.any { it == true }) {
                            result += memory.id
                        }
                    }
                }
            }
        }
        return result
    }

    private fun MemoryEntity.matchesOrNull(target: DreamPrivacyTarget.ConversationSource): Boolean? {
        if (sourceConversationId != target.conversationId) return false
        val identities = decodeIdentitiesOrNull(sourceIdentitiesJson) ?: return null
        val messageIds = decodeStringsOrNull(sourceMessageIdsJson) ?: return null
        return identities.any { it.messageId in target.messageIds } ||
            messageIds.any(target.messageIds::contains)
    }

    private fun MemoryRevisionEntity.matchesOrNull(target: DreamPrivacyTarget.ConversationSource): Boolean? {
        if (sourceConversationId != target.conversationId) return false
        val identities = decodeIdentitiesOrNull(sourceIdentitiesJson) ?: return null
        val messageIds = decodeStringsOrNull(sourceMessageIdsJson) ?: return null
        return identities.any { it.messageId in target.messageIds } ||
            messageIds.any(target.messageIds::contains)
    }

    private fun decodeIdentitiesOrNull(raw: String): List<MemorySourceIdentity>? = try {
        json.decodeFromString<List<MemorySourceIdentity>>(raw)
    } catch (_: Exception) {
        null
    }

    private fun decodeStringsOrNull(raw: String): List<String>? = try {
        json.decodeFromString<List<String>>(raw)
    } catch (_: Exception) {
        null
    }

    private fun DreamSnapshotEntity.manifestOrNull(): Set<Pair<String, Long>>? {
        if (status == "TOMBSTONED" && canonicalPayloadJson.isEmpty()) return emptySet()
        if (canonicalPayloadJson.isEmpty()) return null
        val expectedHash = try {
            DreamSha256(payloadSha256)
        } catch (_: Exception) {
            return null
        }
        if (DreamCanonicalJson.sha256(canonicalPayloadJson.toByteArray(Charsets.UTF_8)) != expectedHash) {
            return null
        }
        val root = try {
            json.parseToJsonElement(canonicalPayloadJson) as? JsonObject
        } catch (_: Exception) {
            null
        } ?: return null
        if (root.keys != SNAPSHOT_ROOT_KEYS || DreamCanonicalJson.encode(root) != canonicalPayloadJson) {
            return null
        }
        if (root["schema_version"]?.jsonPrimitive?.intOrNull != DREAM_SNAPSHOT_SCHEMA_VERSION ||
            root["compiler_revision"]?.jsonPrimitive?.contentOrNull != compilerRevision ||
            root["sections"] !is JsonObject
        ) {
            return null
        }
        val manifest = root["manifest"] as? JsonArray ?: return null
        if (manifest.size > MAX_PRIVACY_CLAIMS || manifest.size != claimCount) return null
        val sections = root["sections"] as JsonObject
        if (sections.keys != SNAPSHOT_SECTION_KEYS) return null
        val referencedFragments = linkedSetOf<Pair<String, Int>>()
        val parsed = manifest.mapTo(linkedSetOf()) { raw ->
            val entry = raw as? JsonObject ?: return null
            if (entry.keys != SNAPSHOT_MANIFEST_ENTRY_KEYS) return null
            val claimId = entry["claim_id"]?.jsonPrimitive?.contentOrNull ?: return null
            try {
                requireDreamStableId(claimId)
            } catch (_: Exception) {
                return null
            }
            val revision = entry["claim_revision"]?.jsonPrimitive?.longOrNull ?: return null
            if (revision <= 0L) return null
            val fragmentHash = entry["fragment_hash"]?.jsonPrimitive?.contentOrNull ?: return null
            val expectedFragmentHash = try {
                DreamSha256(fragmentHash)
            } catch (_: Exception) {
                return null
            }
            val ordinal = entry["ordinal"]?.jsonPrimitive?.intOrNull ?: return null
            if (ordinal < 0) return null
            val section = entry["section"]?.jsonPrimitive?.contentOrNull ?: return null
            if (DreamSnapshotSection.entries.none { it.wireName == section }) return null
            if (!referencedFragments.add(section to ordinal)) return null
            val sectionEntries = sections[section] as? JsonArray ?: return null
            val fragment = sectionEntries.getOrNull(ordinal) as? JsonObject ?: return null
            if (DreamCanonicalJson.sha256(fragment) != expectedFragmentHash) return null
            claimId to revision
        }
        val totalFragments = sections.values.sumOf { section ->
            (section as? JsonArray)?.size ?: return null
        }
        return parsed.takeIf {
            it.size == manifest.size && referencedFragments.size == totalFragments &&
                totalFragments == manifest.size
        }
    }

    private fun boundedRejection() = DreamPrivacyScrubResult.Rejected(
        DreamPrivacyScrubRejection.BOUNDED_MANIFEST_INVALID,
    )
}

internal fun shouldInspectDreamPrivacySnapshots(
    entireScope: Boolean,
    hasAffectedClaims: Boolean,
    activeSnapshotId: String?,
): Boolean = entireScope || hasAffectedClaims || activeSnapshotId != null

private const val PRIVACY_REASON = "PRIVACY_SCRUB"
private const val MAX_PRIVACY_MEMORIES = 10_000
private const val MAX_PRIVACY_CLAIMS = 10_000
private const val MAX_PRIVACY_VERSIONS = 50_000
private const val MAX_PRIVACY_SNAPSHOTS = 10_000
private const val MAX_PRIVACY_SOURCE_ROWS = 200_000
private val SNAPSHOT_ROOT_KEYS = setOf(
    "compiler_revision",
    "manifest",
    "schema_version",
    "sections",
)
private val SNAPSHOT_MANIFEST_ENTRY_KEYS = setOf(
    "claim_id",
    "claim_revision",
    "fragment_hash",
    "ordinal",
    "section",
)
private val SNAPSHOT_SECTION_KEYS = DreamSnapshotSection.entries.mapTo(linkedSetOf()) { it.wireName }
