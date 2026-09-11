package me.rerere.rikkahub.memory.dreaming.review

import java.nio.charset.StandardCharsets
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import me.rerere.rikkahub.memory.dreaming.model.DREAM_SNAPSHOT_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamCanonicalJson
import me.rerere.rikkahub.memory.dreaming.model.DreamContentType
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamEpistemicType
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamStorageClass
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId
import me.rerere.rikkahub.memory.dreaming.snapshot.DreamSnapshotSection
import me.rerere.rikkahub.memory.dreaming.temporal.TemporalState

data class DreamSnapshotDocument(
    val scopeId: DreamScopeId,
    val snapshotId: String,
    val schemaVersion: Int,
    val compilerRevision: String,
    val payloadJson: String,
    val payloadHash: DreamSha256,
    val manifestHash: DreamSha256,
    val claimCount: Int,
) {
    init {
        requireDreamStableId(snapshotId)
        require(compilerRevision.isNotBlank() && compilerRevision.length <= 64)
        require(claimCount in 0..DREAM_REVIEW_MAX_CLAIMS)
    }
}

enum class DreamSnapshotChangeType {
    ADDED,
    UPDATED,
    RETIRED,
}

data class DreamSnapshotChange(
    val type: DreamSnapshotChangeType,
    val claimId: String,
    val previousRevision: Long?,
    val currentRevision: Long?,
    val section: DreamSnapshotSection,
    val title: String,
    val confidenceChanged: Boolean,
    val temporalChanged: Boolean,
)

enum class DreamSnapshotDiffFailure {
    SCOPE_MISMATCH,
    PAYLOAD_TOO_LARGE,
    PAYLOAD_HASH_MISMATCH,
    NON_CANONICAL_PAYLOAD,
    SCHEMA_MISMATCH,
    COMPILER_MISMATCH,
    MANIFEST_HASH_MISMATCH,
    MANIFEST_INVALID,
    FRAGMENT_INVALID,
}

sealed interface DreamSnapshotDiffResult {
    data class Available(val changes: List<DreamSnapshotChange>) : DreamSnapshotDiffResult
    data class Unavailable(val failure: DreamSnapshotDiffFailure) : DreamSnapshotDiffResult
}

/** Strict parser/differ: malformed snapshots are never partially displayed as a trusted diff. */
object DreamSnapshotDiff {
    fun compare(
        previous: DreamSnapshotDocument?,
        current: DreamSnapshotDocument,
    ): DreamSnapshotDiffResult {
        if (previous != null && previous.scopeId != current.scopeId) {
            return DreamSnapshotDiffResult.Unavailable(DreamSnapshotDiffFailure.SCOPE_MISMATCH)
        }
        val old = previous?.let { parse(it) }
        if (old is ParsedResult.Failure) return DreamSnapshotDiffResult.Unavailable(old.failure)
        val new = parse(current)
        if (new is ParsedResult.Failure) return DreamSnapshotDiffResult.Unavailable(new.failure)
        val oldClaims = (old as? ParsedResult.Success)?.claims.orEmpty().associateBy(ParsedClaim::claimId)
        val newClaims = (new as ParsedResult.Success).claims.associateBy(ParsedClaim::claimId)
        val changes = buildList {
            new.claims.forEach { now ->
                val before = oldClaims[now.claimId]
                when {
                    before == null -> add(now.change(DreamSnapshotChangeType.ADDED, null))
                    before.revision != now.revision || before.fragmentHash != now.fragmentHash -> {
                        add(now.change(DreamSnapshotChangeType.UPDATED, before))
                    }
                }
            }
            (old as? ParsedResult.Success)?.claims.orEmpty().forEach { before ->
                if (before.claimId !in newClaims) {
                    add(before.change(DreamSnapshotChangeType.RETIRED, before))
                }
            }
        }
        return DreamSnapshotDiffResult.Available(changes)
    }

    private fun parse(document: DreamSnapshotDocument): ParsedResult {
        val bytes = document.payloadJson.toByteArray(StandardCharsets.UTF_8)
        if (bytes.size > MAX_REVIEW_SNAPSHOT_BYTES) {
            return ParsedResult.Failure(DreamSnapshotDiffFailure.PAYLOAD_TOO_LARGE)
        }
        if (DreamCanonicalJson.sha256(bytes) != document.payloadHash) {
            return ParsedResult.Failure(DreamSnapshotDiffFailure.PAYLOAD_HASH_MISMATCH)
        }
        val root = try {
            JSON.parseToJsonElement(document.payloadJson) as? JsonObject
        } catch (_: Exception) {
            null
        } ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.NON_CANONICAL_PAYLOAD)
        if (root.keys != ROOT_KEYS || DreamCanonicalJson.encode(root) != document.payloadJson) {
            return ParsedResult.Failure(DreamSnapshotDiffFailure.NON_CANONICAL_PAYLOAD)
        }
        if (document.schemaVersion != DREAM_SNAPSHOT_SCHEMA_VERSION ||
            root.numberInt("schema_version") != document.schemaVersion
        ) {
            return ParsedResult.Failure(DreamSnapshotDiffFailure.SCHEMA_MISMATCH)
        }
        if (root.string("compiler_revision") != document.compilerRevision) {
            return ParsedResult.Failure(DreamSnapshotDiffFailure.COMPILER_MISMATCH)
        }
        val manifest = root["manifest"] as? JsonArray
            ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
        if (manifest.size != document.claimCount || DreamCanonicalJson.sha256(manifest) != document.manifestHash) {
            return ParsedResult.Failure(
                if (manifest.size != document.claimCount) DreamSnapshotDiffFailure.MANIFEST_INVALID
                else DreamSnapshotDiffFailure.MANIFEST_HASH_MISMATCH,
            )
        }
        val sections = root["sections"] as? JsonObject
            ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
        if (sections.keys !in COMPATIBLE_SECTION_KEYS) {
            return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
        }
        val references = hashSetOf<Pair<String, Int>>()
        val claimIds = hashSetOf<String>()
        val parsed = mutableListOf<ParsedClaim>()
        var previousPosition: Pair<Int, Int>? = null
        manifest.forEach { rawEntry ->
            val entry = rawEntry as? JsonObject
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            if (entry.keys != MANIFEST_KEYS) {
                return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            }
            val claimId = entry.string("claim_id")
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            try {
                requireDreamStableId(claimId)
            } catch (_: Exception) {
                return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            }
            if (!claimIds.add(claimId)) {
                return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            }
            val revision = entry.numberLong("claim_revision")?.takeIf { it > 0L }
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            val sectionName = entry.string("section")
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            val section = DreamSnapshotSection.entries.singleOrNull { it.wireName == sectionName }
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            val ordinal = entry.numberInt("ordinal")?.takeIf { it >= 0 }
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            val position = section.order to ordinal
            if (previousPosition != null && comparePosition(previousPosition!!, position) >= 0) {
                return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            }
            previousPosition = position
            if (!references.add(sectionName to ordinal)) {
                return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            }
            val expectedFragmentHash = try {
                DreamSha256(entry.string("fragment_hash") ?: return ParsedResult.Failure(
                    DreamSnapshotDiffFailure.MANIFEST_INVALID,
                ))
            } catch (_: Exception) {
                return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
            }
            val fragment = (sections[sectionName] as? JsonArray)?.getOrNull(ordinal) as? JsonObject
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.FRAGMENT_INVALID)
            val compatibleFragmentKeys = if (DreamPairScopeId.parseOrNull(document.scopeId.value) != null) {
                PAIR_FRAGMENT_KEYS
            } else {
                setOf(LEGACY_FRAGMENT_KEYS)
            }
            if (fragment.keys !in compatibleFragmentKeys || DreamCanonicalJson.sha256(fragment) != expectedFragmentHash) {
                return ParsedResult.Failure(DreamSnapshotDiffFailure.FRAGMENT_INVALID)
            }
            val parsedFragment = parseFragment(claimId, revision, section, expectedFragmentHash, fragment)
                ?: return ParsedResult.Failure(DreamSnapshotDiffFailure.FRAGMENT_INVALID)
            parsed += parsedFragment
        }
        val fragmentCount = sections.values.sumOf { (it as? JsonArray)?.size ?: return ParsedResult.Failure(
            DreamSnapshotDiffFailure.MANIFEST_INVALID,
        ) }
        if (fragmentCount != manifest.size || references.size != fragmentCount) {
            return ParsedResult.Failure(DreamSnapshotDiffFailure.MANIFEST_INVALID)
        }
        return ParsedResult.Success(parsed)
    }

    private fun parseFragment(
        claimId: String,
        revision: Long,
        section: DreamSnapshotSection,
        fragmentHash: DreamSha256,
        fragment: JsonObject,
    ): ParsedClaim? {
        val title = fragment.string("title")?.takeIf { it.isNotBlank() } ?: return null
        val confidence = fragment.numberInt("confidence_permille")
            ?.takeIf { it in 0..1_000 } ?: return null
        val temporalState = fragment.string("temporal_state")
            ?.takeIf { raw -> TemporalState.entries.any { it.name == raw } } ?: return null
        val validFrom = fragment.nullableLong("valid_from_epoch_ms") ?: return null
        val validTo = fragment.nullableLong("valid_to_epoch_ms") ?: return null
        if (validFrom.value != null && validFrom.value < 0L) return null
        if (validTo.value != null && validTo.value < 0L) return null
        if (validFrom.value != null && validTo.value != null && validTo.value <= validFrom.value) return null
        if (fragment.string("claim_key").isNullOrBlank() || fragment.string("statement").isNullOrBlank() ||
            fragment.string("epistemic_type")?.let { raw -> DreamEpistemicType.entries.none { it.name == raw } } != false ||
            fragment.string("storage_class")?.let { raw -> DreamStorageClass.entries.none { it.name == raw } } != false
        ) return null
        if (fragment.keys == PAIR_FRAGMENT_KEYS_V2) {
            if (fragment.string("subject_kind")?.let { raw -> DreamSubjectKind.entries.none { it.name == raw } } != false ||
                fragment.string("epistemic_origin")?.let { raw -> DreamEpistemicOrigin.entries.none { it.name == raw } } != false ||
                fragment.string("content_type")?.let { raw -> DreamContentType.entries.none { it.name == raw } } != false ||
                fragment.string("profile_section")?.matches(PROFILE_SECTION_PATTERN) != true
            ) return null
        }
        return ParsedClaim(
            claimId = claimId,
            revision = revision,
            section = section,
            title = title,
            confidencePermille = confidence,
            temporalState = temporalState,
            validFromEpochMs = validFrom.value,
            validToEpochMs = validTo.value,
            fragmentHash = fragmentHash,
        )
    }
}

private sealed interface ParsedResult {
    data class Success(val claims: List<ParsedClaim>) : ParsedResult
    data class Failure(val failure: DreamSnapshotDiffFailure) : ParsedResult
}

private data class ParsedClaim(
    val claimId: String,
    val revision: Long,
    val section: DreamSnapshotSection,
    val title: String,
    val confidencePermille: Int,
    val temporalState: String,
    val validFromEpochMs: Long?,
    val validToEpochMs: Long?,
    val fragmentHash: DreamSha256,
) {
    fun change(type: DreamSnapshotChangeType, previous: ParsedClaim?): DreamSnapshotChange =
        DreamSnapshotChange(
            type = type,
            claimId = claimId,
            previousRevision = if (type == DreamSnapshotChangeType.ADDED) null else previous?.revision,
            currentRevision = if (type == DreamSnapshotChangeType.RETIRED) null else revision,
            section = section,
            title = title,
            confidenceChanged = previous != null && previous.confidencePermille != confidencePermille,
            temporalChanged = previous != null && (
                previous.temporalState != temporalState ||
                    previous.validFromEpochMs != validFromEpochMs ||
                    previous.validToEpochMs != validToEpochMs
                ),
        )
}

private data class NullableLong(val value: Long?)

private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)
    ?.takeIf(JsonPrimitive::isString)
    ?.contentOrNull

private fun JsonObject.numberInt(key: String): Int? = (this[key] as? JsonPrimitive)
    ?.takeUnless(JsonPrimitive::isString)
    ?.intOrNull

private fun JsonObject.numberLong(key: String): Long? = (this[key] as? JsonPrimitive)
    ?.takeUnless(JsonPrimitive::isString)
    ?.longOrNull

private fun JsonObject.nullableLong(key: String): NullableLong? {
    val value: JsonElement = this[key] ?: return null
    if (value === JsonNull) return NullableLong(null)
    return (value as? JsonPrimitive)
        ?.takeUnless(JsonPrimitive::isString)
        ?.longOrNull
        ?.let(::NullableLong)
}

private fun comparePosition(left: Pair<Int, Int>, right: Pair<Int, Int>): Int =
    compareValuesBy(left, right, { it.first }, { it.second })

private val JSON = Json { isLenient = false; ignoreUnknownKeys = false }
private const val MAX_REVIEW_SNAPSHOT_BYTES = 2 * 1_024 * 1_024
private val ROOT_KEYS = setOf("compiler_revision", "manifest", "schema_version", "sections")
private val SECTION_KEYS_V2 = DreamSnapshotSection.entries.mapTo(linkedSetOf(), DreamSnapshotSection::wireName)
private val SECTION_KEYS_V1 = linkedSetOf(
    DreamSnapshotSection.PROFILE.wireName,
    DreamSnapshotSection.CURRENT_PROJECTS.wireName,
    DreamSnapshotSection.ACTIVE_PLANS.wireName,
    DreamSnapshotSection.ACTIVE_CONSTRAINTS.wireName,
    DreamSnapshotSection.OTHER_CONTEXT.wireName,
)
private val COMPATIBLE_SECTION_KEYS = setOf(SECTION_KEYS_V1, SECTION_KEYS_V2)
private val MANIFEST_KEYS = setOf("claim_id", "claim_revision", "fragment_hash", "ordinal", "section")
private val LEGACY_FRAGMENT_KEYS = setOf(
    "claim_key",
    "confidence_permille",
    "epistemic_type",
    "statement",
    "storage_class",
    "temporal_state",
    "title",
    "valid_from_epoch_ms",
    "valid_to_epoch_ms",
)
private val PAIR_FRAGMENT_KEYS_V2 = LEGACY_FRAGMENT_KEYS + setOf(
    "content_type",
    "epistemic_origin",
    "profile_section",
    "subject_kind",
)
private val PAIR_FRAGMENT_KEYS = setOf(LEGACY_FRAGMENT_KEYS, PAIR_FRAGMENT_KEYS_V2)
private val PROFILE_SECTION_PATTERN = Regex("^[a-z0-9][a-z0-9._-]{0,63}$")
