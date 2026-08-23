package me.rerere.rikkahub.memory.dreaming.model

import java.text.Normalizer
import kotlin.uuid.Uuid
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryAttribution
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
import me.rerere.rikkahub.memory.MemorySourceKind
import me.rerere.rikkahub.memory.MemorySourceRole
import me.rerere.rikkahub.memory.MemoryTruthStatus
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState
import me.rerere.rikkahub.memory.dreaming.temporal.strictZoneOrNull

const val DREAM_PROPOSAL_SCHEMA_VERSION = 2
const val DREAM_SNAPSHOT_SCHEMA_VERSION = 1
const val DREAM_AUTHORITY_FINGERPRINT_VERSION = 1

@JvmInline
value class DreamSha256(val value: String) {
    init {
        require(LOWER_SHA_256.matches(value)) { "SHA-256 must be 64 lower-case hexadecimal characters" }
    }

    override fun toString(): String = value
}

/** A run-local identifier. It must never be persisted as an authority identity. */
@JvmInline
value class DreamOpaqueToken(val value: String) {
    init {
        require(OPAQUE_TOKEN.matches(value)) { "Invalid Dreaming opaque token" }
    }

    val kind: DreamOpaqueTokenKind
        get() = if (value.startsWith("m_")) DreamOpaqueTokenKind.MEMORY else DreamOpaqueTokenKind.CLAIM

    override fun toString(): String = value
}

enum class DreamOpaqueTokenKind(val prefix: String) {
    MEMORY("m_"),
    CLAIM("c_"),
}

@JvmInline
value class DreamProposalNonce(val value: String) {
    init {
        require(PROPOSAL_NONCE.matches(value)) { "Invalid Dreaming proposal nonce" }
    }

    override fun toString(): String = value
}

enum class DreamSynthesisMode {
    INCREMENTAL,
    FULL,
}

enum class DreamClaimState {
    PENDING_REVIEW,
    ACTIVE_CONTEXTUAL,
    DIRTY,
    STALE,
    SUPERSEDED,
    REJECTED,
    INVALID,
    TOMBSTONED,
}

enum class DreamStorageClass {
    PROFILE,
    EPISODIC,
}

enum class DreamEpistemicType {
    OBSERVATION,
    BELIEF,
    PROJECT_STATE,
    PLAN,
    CONSTRAINT,
    PREFERENCE_SUMMARY,
}

enum class DreamSubjectKind {
    USER,
    ASSISTANT,
    RELATIONSHIP,
}

enum class DreamEpistemicOrigin {
    EXPLICIT,
    OBSERVED,
    INFERRED,
    SELF_REFLECTED,
}

enum class DreamContentType {
    IDENTITY,
    PREFERENCE,
    TRAIT,
    HABIT,
    GOAL,
    PROJECT_STATE,
    PLAN,
    CONSTRAINT,
    RELATIONSHIP_NORM,
    SHARED_HISTORY,
    COMMITMENT,
    OTHER,
}

enum class DreamSupportType {
    SUPPORTS,
    CONTRADICTS,
    SUPERSEDES,
    CONTEXT,
}

enum class DreamClaimMutationReason {
    MODEL_PROPOSAL,
    AUTHORITY_INVALIDATED,
    AUTHORITY_EXPIRED,
    SUPERSEDED_BY_PROPOSAL,
    PRIVACY_SCRUB,
    USER_REJECTED,
    USER_CORRECTION,
}

data class DreamSynthesisFence(
    val scopeId: DreamScopeId,
    val runId: String,
    val leaseOwner: String,
    val baseMemoryEpoch: Long,
    /** Last authority epoch already incorporated into the active Dream state at begin-time. */
    val baseLastAppliedMemoryEpoch: Long,
    val baseDreamRevision: Long,
    val expectedActiveSnapshotId: String?,
    val frozenNowEpochMs: Long,
    /** IANA zone frozen with the run; source reading and temporal parsing reuse it exactly. */
    val sourceTimezoneId: String,
    val mode: DreamSynthesisMode,
) {
    /** Pair-Dream semantic names; persisted legacy run columns keep their old names for migration simplicity. */
    val baseExperienceEpoch: Long get() = baseMemoryEpoch
    val baseAppliedExperienceEpoch: Long get() = baseLastAppliedMemoryEpoch

    init {
        requireCanonicalDreamRunId(runId)
        requireDreamLeaseOwner(leaseOwner)
        require(baseMemoryEpoch >= 0L)
        require(baseLastAppliedMemoryEpoch in 0L..baseMemoryEpoch)
        require(baseDreamRevision >= 0L)
        expectedActiveSnapshotId?.let(::requireDreamStableId)
        require(frozenNowEpochMs >= 0L)
        require(strictZoneOrNull(sourceTimezoneId) != null) { "sourceTimezoneId must be a strict IANA zone" }
    }
}

/** Exact source identity used to bind a Memory revision to its original authority. */
data class DreamAuthoritySource(
    val conversationId: String,
    val messageId: String,
    val role: MemorySourceRole,
    val sourceKind: MemorySourceKind,
    val consumedTextDigest: DreamSha256,
    val evidenceGroupId: String,
) {
    init {
        requireDreamBoundedText(conversationId, 512, "conversationId")
        requireDreamBoundedText(messageId, 512, "messageId")
        requireDreamBoundedText(evidenceGroupId, 512, "evidenceGroupId")
    }
}

/**
 * Schema-independent view of one authoritative Memory row and its exact revision sources.
 * [authorityFingerprint] is re-computed from this object; `MemoryEntity.contentHash` is not enough.
 */
data class DreamAuthorityMemory(
    val scopeId: DreamScopeId,
    val memoryId: String,
    val revision: Long,
    val title: String?,
    val content: String,
    val kind: MemoryKind,
    val attribution: MemoryAttribution,
    val truthStatus: MemoryTruthStatus,
    val lifecycleStatus: MemoryLifecycleStatus,
    val approvalSource: MemoryApprovalSource,
    val tags: List<String>,
    val createdAtEpochMs: Long,
    val updatedAtEpochMs: Long,
    val occurredAtEpochMs: Long?,
    val expiresAtEpochMs: Long?,
    val originAssistantId: String?,
    val participants: List<String>,
    val outcome: String?,
    val sources: List<DreamAuthoritySource>,
    val tombstoned: Boolean = false,
) {
    init {
        requireDreamBoundedText(memoryId, 512, "memoryId")
        require(revision > 0L)
        require(content.isNotBlank() || tombstoned)
        require(content.length <= 64_000)
        require(title == null || title.length <= 4_096)
        require(tags.size <= 256 && tags.all { it.length <= 512 })
        require(participants.size <= 256 && participants.all { it.length <= 512 })
        require(outcome == null || outcome.length <= 8_192)
        require(createdAtEpochMs >= 0L && updatedAtEpochMs >= createdAtEpochMs)
        require(occurredAtEpochMs == null || occurredAtEpochMs >= 0L)
        require(expiresAtEpochMs == null || expiresAtEpochMs >= 0L)
        originAssistantId?.let { requireDreamBoundedText(it, 512, "originAssistantId") }
        require(sources.size <= 4_096)
        requireDreamValidUnicode(title, content, outcome, originAssistantId, *tags.toTypedArray(), *participants.toTypedArray())
    }

    fun isUsableAt(frozenNowEpochMs: Long): Boolean {
        require(frozenNowEpochMs >= 0L)
        return !tombstoned &&
            lifecycleStatus == MemoryLifecycleStatus.ACTIVE &&
            truthStatus == MemoryTruthStatus.CONFIRMED &&
            (expiresAtEpochMs == null || expiresAtEpochMs > frozenNowEpochMs)
    }

    /**
     * Automatically accepted or legacy authority must be rebound to its complete original source
     * before synthesis. Explicit MANUAL_UI/USER_REVIEWED corrections are themselves authoritative
     * Memory revisions and may intentionally have no conversation source identity.
     */
    fun requiresExactSourceRereadForSynthesis(): Boolean = approvalSource !in setOf(
        MemoryApprovalSource.MANUAL_UI,
        MemoryApprovalSource.USER_REVIEWED,
    )
}

data class DreamAuthorityPin(
    val scopeId: DreamScopeId,
    val memoryId: String,
    val expectedRevision: Long,
    val expectedAuthorityFingerprint: DreamSha256,
    val expectedSourceManifestHash: DreamSha256,
) {
    init {
        requireDreamBoundedText(memoryId, 512, "memoryId")
        require(expectedRevision > 0L)
    }
}

data class DreamClaimSourcePin(
    val authority: DreamAuthorityPin,
    val supportType: DreamSupportType,
    /** True only for an exact Memory authority source; claim-to-claim references are never direct. */
    val directAuthority: Boolean,
)

data class DreamClaimHead(
    val claimId: String,
    val scopeId: DreamScopeId,
    val revision: Long,
    val claimKey: String,
    val storageClass: DreamStorageClass,
    val epistemicType: DreamEpistemicType,
    val state: DreamClaimState,
    val title: String,
    val statement: String,
    val confidencePermille: Int,
    val temporalState: TemporalState,
    val validFromEpochMs: Long?,
    val validToEpochMs: Long?,
    val versionHash: DreamSha256,
    val sources: List<DreamClaimSourcePin>,
    val subjectKind: DreamSubjectKind = DreamSubjectKind.USER,
    val profileSection: String = "general",
    val epistemicOrigin: DreamEpistemicOrigin = DreamEpistemicOrigin.INFERRED,
    val contentType: DreamContentType = DreamContentType.OTHER,
) {
    init {
        requireDreamStableId(claimId)
        require(revision > 0L)
        requireDreamBoundedText(claimKey, 512, "claimKey")
        require(confidencePermille in 0..1_000)
        require(validFromEpochMs == null || validFromEpochMs >= 0L)
        require(validToEpochMs == null || validToEpochMs >= 0L)
        require(validFromEpochMs == null || validToEpochMs == null || validToEpochMs > validFromEpochMs)
        require(sources.size <= 4_096)
        require(profileSection.matches(Regex("^[a-z0-9][a-z0-9._-]{0,63}$")))
        if (state == DreamClaimState.TOMBSTONED) {
            require(title.isEmpty() && statement.isEmpty() && sources.isEmpty()) {
                "Tombstoned claims must not retain user content or provenance"
            }
        } else {
            require(title.isNotBlank() && statement.isNotBlank())
            require(title.length <= 4_096 && statement.length <= 32_000)
        }
        requireDreamValidUnicode(title, statement)
    }
}

data class DreamValidatedClaimVersion(
    val claimId: String,
    val expectedPreviousRevision: Long?,
    val nextRevision: Long,
    val claimKey: String,
    val storageClass: DreamStorageClass,
    val epistemicType: DreamEpistemicType,
    val nextState: DreamClaimState,
    val title: String,
    val statement: String,
    val confidencePermille: Int,
    val temporalState: TemporalState,
    val validFromEpochMs: Long?,
    val validToEpochMs: Long?,
    val sources: List<DreamClaimSourcePin>,
    val reason: DreamClaimMutationReason,
    val subjectKind: DreamSubjectKind = DreamSubjectKind.USER,
    val profileSection: String = "general",
    val epistemicOrigin: DreamEpistemicOrigin = DreamEpistemicOrigin.INFERRED,
    val contentType: DreamContentType = DreamContentType.OTHER,
) {
    init {
        requireDreamStableId(claimId)
        require(expectedPreviousRevision == null || expectedPreviousRevision > 0L)
        require(nextRevision == (expectedPreviousRevision?.plus(1L) ?: 1L))
        requireDreamBoundedText(claimKey, 512, "claimKey")
        require(title.isNotBlank() && title.length <= 4_096)
        require(statement.isNotBlank() && statement.length <= 32_000)
        require(confidencePermille in 0..1_000)
        require(nextState in setOf(
            DreamClaimState.PENDING_REVIEW,
            DreamClaimState.ACTIVE_CONTEXTUAL,
            DreamClaimState.DIRTY,
            DreamClaimState.STALE,
            DreamClaimState.SUPERSEDED,
            DreamClaimState.REJECTED,
            DreamClaimState.INVALID,
        ))
        require(sources.any { it.directAuthority }) { "Every synthesized claim needs direct authority provenance" }
        require(profileSection.matches(Regex("^[a-z0-9][a-z0-9._-]{0,63}$")))
        requireDreamValidUnicode(title, statement)
    }
}

data class DreamValidatedClaimTransition(
    val expectedRevision: Long,
    val nextVersion: DreamValidatedClaimVersion,
) {
    val claimId: String
        get() = nextVersion.claimId

    val nextState: DreamClaimState
        get() = nextVersion.nextState

    val reason: DreamClaimMutationReason
        get() = nextVersion.reason

    init {
        require(expectedRevision > 0L)
        require(nextVersion.expectedPreviousRevision == expectedRevision)
        require(nextVersion.nextRevision == expectedRevision + 1L)
        require(nextVersion.nextState in setOf(
            DreamClaimState.DIRTY,
            DreamClaimState.STALE,
            DreamClaimState.SUPERSEDED,
            DreamClaimState.REJECTED,
            DreamClaimState.INVALID,
        ))
    }
}

data class DreamValidatedPlan(
    val fence: DreamSynthesisFence,
    val proposalNonce: DreamProposalNonce,
    val upserts: List<DreamValidatedClaimVersion>,
    val transitions: List<DreamValidatedClaimTransition>,
    val resultingClaims: List<DreamClaimHead>,
    /**
     * Exact, host-resolved union of authority pins referenced by model operations.
     *
     * This deliberately includes evidence attached to an INVALIDATE_CLAIM operation even though
     * that evidence is not copied into the target Claim's immutable next version. Commit must
     * still live-recheck it. Historical sources copied by a host deterministic transition are not
     * model evidence and therefore do not belong here.
     */
    val modelEvidencePins: List<DreamAuthorityPin>,
) {
    init {
        val changedIds = (upserts.map { it.claimId } + transitions.map { it.claimId })
        require(changedIds.size == changedIds.distinct().size) { "A proposal may mutate each claim only once" }
        require(resultingClaims.all { it.scopeId == fence.scopeId })
        require(resultingClaims.filter { it.state == DreamClaimState.ACTIVE_CONTEXTUAL }.all { claim ->
            claim.sources.isNotEmpty() && claim.sources.all { it.directAuthority }
        }) { "Every active snapshot claim must retain 100% direct authority provenance" }
        require(modelEvidencePins.all { it.scopeId == fence.scopeId })
        require(modelEvidencePins == modelEvidencePins.distinct().sortedWith(DREAM_AUTHORITY_PIN_ORDER)) {
            "Model evidence pins must be unique and canonically ordered"
        }
        val upsertPins = upserts
            .flatMap { version -> version.sources.filter { it.directAuthority }.map { it.authority } }
            .distinct()
        require(modelEvidencePins.containsAll(upsertPins)) {
            "Every model upsert authority must be included in the live model evidence set"
        }
    }
}

val DREAM_AUTHORITY_PIN_ORDER: Comparator<DreamAuthorityPin> = compareBy(
    { it.memoryId },
    { it.expectedRevision },
    { it.expectedAuthorityFingerprint.value },
    { it.expectedSourceManifestHash.value },
)

fun requireDreamStableId(value: String) {
    require(value.length == 36 && value == value.lowercase()) { "ID must be a canonical lower-case UUID" }
    val canonical = try {
        Uuid.parse(value).toString() == value
    } catch (_: Exception) {
        false
    }
    require(canonical) {
        "ID must be a canonical lower-case UUID"
    }
}

fun normalizeDreamText(value: String): String =
    Normalizer.normalize(value.replace("\r\n", "\n").replace('\r', '\n'), Normalizer.Form.NFC)

fun requireDreamBoundedText(value: String, maxLength: Int, field: String) {
    require(value.isNotBlank() && value.length <= maxLength) { "$field must be non-blank and bounded" }
    require(!value.any(Char::isISOControl)) { "$field must not contain control characters" }
    requireDreamValidUnicode(value)
}

fun requireDreamValidUnicode(vararg values: String?) {
    values.filterNotNull().forEach { value ->
        var index = 0
        while (index < value.length) {
            val char = value[index]
            when {
                char.isHighSurrogate() -> {
                    require(index + 1 < value.length && value[index + 1].isLowSurrogate()) {
                        "Unpaired high surrogate is not valid Unicode"
                    }
                    index += 2
                }

                char.isLowSurrogate() -> error("Unpaired low surrogate is not valid Unicode")
                else -> index++
            }
        }
    }
}

private val LOWER_SHA_256 = Regex("^[0-9a-f]{64}$")
private val OPAQUE_TOKEN = Regex("^[mc]_[A-Za-z0-9_-]{22}$")
private val PROPOSAL_NONCE = Regex("^p_[A-Za-z0-9_-]{43}$")
