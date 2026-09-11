package me.rerere.rikkahub.memory.dreaming.snapshot

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.longOrNull
import me.rerere.rikkahub.memory.dreaming.model.DREAM_SNAPSHOT_SCHEMA_VERSION
import me.rerere.rikkahub.memory.dreaming.model.DreamCanonicalJson
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.model.DreamSubjectKind
import me.rerere.rikkahub.memory.dreaming.model.requireDreamStableId

/**
 * Narrow compatibility detector for Pair snapshots written by the historical validator-head bug.
 *
 * The buggy path persisted the complete [DreamValidatedClaimVersion] into Room, but projected the
 * same version into `plan.resultingClaims` without the four Pair-only metadata fields. Snapshot
 * compilation therefore saw the default USER/general/INFERRED/OTHER metadata while the immutable
 * ClaimVersion/head rows stored the correct values. No legacy claim content, revision or evidence
 * changed; only Pair classification metadata and the derived section/ordinal/hash can differ.
 *
 * This detector deliberately rejects every broader mismatch. It is used only to keep already
 * committed v1 Pair portraits readable until a subsequent fixed synthesis writes a v2 snapshot.
 */
object DreamPairSnapshotCompatibility {
    fun isLegacyV1MetadataProjectionDrift(
        scopeId: DreamScopeId,
        compilerRevision: String,
        storedPayloadJson: String,
        expectedPayloadJson: String,
    ): Boolean {
        if (DreamPairScopeId.parseOrNull(scopeId.value) == null ||
            compilerRevision != LEGACY_BUGGY_PAIR_COMPILER_REVISION ||
            storedPayloadJson == expectedPayloadJson
        ) {
            return false
        }
        val stored = parse(storedPayloadJson, compilerRevision) ?: return false
        val expected = parse(expectedPayloadJson, compilerRevision) ?: return false
        if (stored.keys != expected.keys) return false

        var metadataChanged = false
        for (claimId in stored.keys) {
            val before = stored.getValue(claimId)
            val after = expected.getValue(claimId)
            if (before.revision != after.revision) return false
            for (key in LEGACY_FRAGMENT_KEYS) {
                if (before.fragment[key] != after.fragment[key]) return false
            }
            val changedFields = PAIR_METADATA_KEYS.filterTo(linkedSetOf()) { key ->
                before.fragment[key] != after.fragment[key]
            }
            if (changedFields.isNotEmpty()) metadataChanged = true
            if (before.section != sectionFor(before.fragment) || after.section != sectionFor(after.fragment)) {
                return false
            }
        }
        return metadataChanged
    }

    private fun parse(payloadJson: String, compilerRevision: String): Map<String, CompatClaim>? {
        val root = runCatching { JSON.parseToJsonElement(payloadJson) as? JsonObject }.getOrNull()
            ?: return null
        if (root.keys != ROOT_KEYS || DreamCanonicalJson.encode(root) != payloadJson ||
            root.string("compiler_revision") != compilerRevision ||
            root.int("schema_version") != DREAM_SNAPSHOT_SCHEMA_VERSION
        ) {
            return null
        }
        val manifest = root["manifest"] as? JsonArray ?: return null
        val sections = root["sections"] as? JsonObject ?: return null
        if (sections.keys != FULL_SECTION_KEYS) return null

        val result = linkedMapOf<String, CompatClaim>()
        val referencedPositions = hashSetOf<Pair<String, Int>>()
        for (raw in manifest) {
            val entry = raw as? JsonObject ?: return null
            if (entry.keys != MANIFEST_KEYS) return null
            val claimId = entry.string("claim_id") ?: return null
            if (runCatching { requireDreamStableId(claimId) }.isFailure || claimId in result) return null
            val revision = entry.long("claim_revision")?.takeIf { it > 0L } ?: return null
            val section = entry.string("section")?.takeIf(FULL_SECTION_KEYS::contains) ?: return null
            val ordinal = entry.int("ordinal")?.takeIf { it >= 0 } ?: return null
            if (!referencedPositions.add(section to ordinal)) return null
            val expectedHash = runCatching {
                DreamSha256(entry.string("fragment_hash") ?: return null)
            }.getOrNull() ?: return null
            val fragment = ((sections[section] as? JsonArray)?.getOrNull(ordinal) as? JsonObject)
                ?: return null
            if (fragment.keys != PAIR_FRAGMENT_KEYS || DreamCanonicalJson.sha256(fragment) != expectedHash) {
                return null
            }
            result[claimId] = CompatClaim(revision, section, fragment)
        }
        val fragmentCount = sections.values.sumOf { value ->
            (value as? JsonArray)?.size ?: return null
        }
        if (fragmentCount != manifest.size || referencedPositions.size != fragmentCount) return null
        return result
    }

    private fun sectionFor(fragment: JsonObject): String? = when (fragment.string("subject_kind")) {
        DreamSubjectKind.USER.name -> DreamSnapshotSection.ABOUT_USER.wireName
        DreamSubjectKind.ASSISTANT.name -> DreamSnapshotSection.ABOUT_ASSISTANT.wireName
        DreamSubjectKind.RELATIONSHIP.name -> DreamSnapshotSection.ABOUT_RELATIONSHIP.wireName
        else -> null
    }
}

private data class CompatClaim(
    val revision: Long,
    val section: String,
    val fragment: JsonObject,
)

private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)
    ?.takeIf(JsonPrimitive::isString)
    ?.contentOrNull

private fun JsonObject.int(key: String): Int? = (this[key] as? JsonPrimitive)
    ?.takeUnless(JsonPrimitive::isString)
    ?.intOrNull

private fun JsonObject.long(key: String): Long? = (this[key] as? JsonPrimitive)
    ?.takeUnless(JsonPrimitive::isString)
    ?.longOrNull

private val JSON = Json { isLenient = false; ignoreUnknownKeys = false }
private const val LEGACY_BUGGY_PAIR_COMPILER_REVISION = "dream-snapshot-compiler-v1"
private val ROOT_KEYS = setOf("compiler_revision", "manifest", "schema_version", "sections")
private val MANIFEST_KEYS = setOf("claim_id", "claim_revision", "fragment_hash", "ordinal", "section")
private val FULL_SECTION_KEYS = DreamSnapshotSection.entries.mapTo(linkedSetOf(), DreamSnapshotSection::wireName)
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
private val PAIR_METADATA_KEYS = setOf(
    "content_type",
    "epistemic_origin",
    "profile_section",
    "subject_kind",
)
private val PAIR_FRAGMENT_KEYS = LEGACY_FRAGMENT_KEYS + PAIR_METADATA_KEYS
