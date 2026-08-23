package me.rerere.rikkahub.memory.dreaming.model

import kotlin.uuid.Uuid

/** Stable Dream ownership for one local user and one assistant. */
data class DreamPairScope(
    val id: DreamPairScopeId,
    val userKey: String,
    val assistantId: Uuid,
) {
    companion object {
        const val LOCAL_USER_KEY: String = "local-user"

        fun forAssistant(assistantId: Uuid): DreamPairScope = DreamPairScope(
            id = DreamPairScopeId.forAssistant(assistantId),
            userKey = LOCAL_USER_KEY,
            assistantId = assistantId,
        )
    }
}

@JvmInline
value class DreamPairScopeId private constructor(val value: String) : Comparable<DreamPairScopeId> {
    override fun compareTo(other: DreamPairScopeId): Int = value.compareTo(other.value)

    override fun toString(): String = value

    companion object {
        private const val PREFIX = "pair:${DreamPairScope.LOCAL_USER_KEY}:"
        private val canonicalUuidPattern = Regex(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        )

        fun forAssistant(assistantId: Uuid): DreamPairScopeId =
            DreamPairScopeId(PREFIX + assistantId.toString())

        fun parseOrNull(raw: String?): DreamPairScopeId? {
            if (raw == null || !raw.startsWith(PREFIX)) return null
            val uuidText = raw.removePrefix(PREFIX)
            if (!canonicalUuidPattern.matches(uuidText)) return null
            val parsed = runCatching { Uuid.parse(uuidText) }.getOrNull() ?: return null
            return raw.takeIf { parsed.toString() == uuidText }?.let(::DreamPairScopeId)
        }

        fun requireCanonical(raw: String): DreamPairScopeId =
            requireNotNull(parseOrNull(raw)) { "Invalid Dream pair scope: $raw" }
    }
}
