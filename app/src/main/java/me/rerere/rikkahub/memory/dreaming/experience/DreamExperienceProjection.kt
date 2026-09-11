package me.rerere.rikkahub.memory.dreaming.experience

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import me.rerere.rikkahub.data.db.entity.DreamExperienceEntity
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryAttribution
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryLifecycleStatus
import me.rerere.rikkahub.memory.MemorySourceKind
import me.rerere.rikkahub.memory.MemorySourceRole
import me.rerere.rikkahub.memory.MemoryTruthStatus
import me.rerere.rikkahub.memory.dreaming.input.DreamInputCandidate
import me.rerere.rikkahub.memory.dreaming.input.DreamInputCandidateOrigin
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityFingerprintV1
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityMemory
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthorityPin
import me.rerere.rikkahub.memory.dreaming.model.DreamAuthoritySource
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamSha256
import me.rerere.rikkahub.memory.dreaming.source.DreamSourceLocator

/**
 * Compatibility projection into the established Dream input builder. The source object is an
 * Experience row, not a MemoryEntity; Memory-shaped names remain only in the legacy model shell.
 */
fun DreamExperienceEntity.toDreamInputCandidate(
    scopeId: DreamScopeId,
    origin: DreamInputCandidateOrigin,
    json: Json,
): DreamInputCandidate? {
    return try {
        if (status == "DISCARDED") return null
        val sources = if (sourceKind == "CONVERSATION") {
            runCatching { json.decodeFromString<List<DreamExperienceSourceManifestEntry>>(sourceManifestJson) }
                .getOrNull()
                ?.mapNotNull { it.toAuthoritySourceOrNull() }
                ?: return null
        } else {
            emptyList()
        }
        val authority = DreamAuthorityMemory(
            scopeId = scopeId,
            memoryId = experienceId,
            revision = experienceEpoch,
            title = experienceKind.replace('_', ' ').lowercase().replaceFirstChar(Char::uppercase),
            content = summary,
            kind = experienceKind.toMemoryKind(),
            attribution = actor.toAttribution(),
            truthStatus = MemoryTruthStatus.CONFIRMED,
            lifecycleStatus = MemoryLifecycleStatus.ACTIVE,
            approvalSource = if (sources.isEmpty()) MemoryApprovalSource.USER_REVIEWED else MemoryApprovalSource.AUTO_SAFE,
            tags = listOf("dream_experience", experienceKind.lowercase()),
            createdAtEpochMs = occurredAtMs,
            updatedAtEpochMs = ingestedAtMs,
            occurredAtEpochMs = occurredAtMs,
            expiresAtEpochMs = null,
            originAssistantId = null,
            participants = when (actor) {
                "USER" -> listOf("USER")
                "ASSISTANT" -> listOf("ASSISTANT")
                else -> listOf("USER", "ASSISTANT")
            },
            outcome = null,
            sources = sources,
            tombstoned = false,
        )
        val pin = DreamAuthorityPin(
            scopeId = scopeId,
            memoryId = experienceId,
            expectedRevision = experienceEpoch,
            expectedAuthorityFingerprint = DreamAuthorityFingerprintV1.compute(authority),
            expectedSourceManifestHash = DreamAuthorityFingerprintV1.sourceManifestHash(sources),
        )
        DreamInputCandidate(
            origin = origin,
            memory = authority,
            pin = pin,
            sourceLocators = sources.map { source ->
                DreamSourceLocator(
                    scopeId = scopeId,
                    conversationId = source.conversationId,
                    messageId = source.messageId,
                    role = source.role,
                    sourceKind = source.sourceKind,
                    expectedConsumedTextDigest = source.consumedTextDigest,
                    evidenceGroupId = source.evidenceGroupId,
                )
            },
            requireSourceReread = sources.isNotEmpty(),
        )
    } catch (_: Exception) {
        // One legacy/corrupt Experience must not abort the complete Pair Dream seed. The bounded
        // synthesis query keeps enough reserve rows for malformed candidates to be skipped safely.
        null
    }
}

private fun DreamExperienceSourceManifestEntry.toAuthoritySourceOrNull(): DreamAuthoritySource? = try {
    DreamAuthoritySource(
        conversationId = conversationId,
        messageId = messageId,
        role = enumValueOf<MemorySourceRole>(role),
        sourceKind = enumValueOf<MemorySourceKind>(sourceKind),
        consumedTextDigest = DreamSha256(consumedTextDigest),
        evidenceGroupId = evidenceGroupId,
    )
} catch (_: Exception) {
    null
}

private fun String.toAttribution(): MemoryAttribution = when (uppercase()) {
    "USER" -> MemoryAttribution.USER
    "ASSISTANT" -> MemoryAttribution.ASSISTANT
    "SHARED" -> MemoryAttribution.SHARED
    else -> MemoryAttribution.UNKNOWN
}

private fun String.toMemoryKind(): MemoryKind = when (uppercase()) {
    "CHAT_EPISODE" -> MemoryKind.EPISODE
    else -> runCatching { enumValueOf<MemoryKind>(uppercase()) }.getOrDefault(MemoryKind.OTHER)
}
