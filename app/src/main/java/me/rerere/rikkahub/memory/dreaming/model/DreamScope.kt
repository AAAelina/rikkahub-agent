package me.rerere.rikkahub.memory.dreaming.model

import kotlin.uuid.Uuid

/**
 * Canonical identifier stored by the existing Dream run/claim/snapshot tables.
 *
 * Pair-owned Dream uses `pair:local-user:<assistant-uuid>`. Legacy private/global values remain
 * readable during migration so existing Dream history can still be shown and retired cleanly.
 */
@JvmInline
value class DreamScopeId private constructor(val value: String) : Comparable<DreamScopeId> {
    val isGlobal: Boolean
        get() = value == GLOBAL_VALUE

    override fun compareTo(other: DreamScopeId): Int = value.compareTo(other.value)

    override fun toString(): String = value

    companion object {
        const val GLOBAL_VALUE: String = "__global__"
        private const val PAIR_PREFIX: String = "pair:${DreamPairScope.LOCAL_USER_KEY}:"

        private val canonicalUuidPattern = Regex(
            "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        )

        val Global: DreamScopeId = DreamScopeId(GLOBAL_VALUE)

        /** Returns null for whitespace, aliases, upper-case UUIDs, and non-canonical UUID text. */
        fun parseOrNull(raw: String?): DreamScopeId? {
            if (raw == GLOBAL_VALUE) return Global
            if (raw == null) return null
            if (raw.startsWith(PAIR_PREFIX)) {
                val pairId = DreamPairScopeId.parseOrNull(raw) ?: return null
                return DreamScopeId(pairId.value)
            }
            if (!canonicalUuidPattern.matches(raw)) return null
            val parsed = runCatching { Uuid.parse(raw) }.getOrNull() ?: return null
            return raw.takeIf { parsed.toString() == it }?.let(::DreamScopeId)
        }

        fun requireCanonical(raw: String): DreamScopeId =
            requireNotNull(parseOrNull(raw)) {
                "Dream scope must be a canonical pair scope, lower-case UUID, or $GLOBAL_VALUE"
            }

        fun privateScope(assistantId: Uuid): DreamScopeId = DreamScopeId(assistantId.toString())

        fun pairScope(assistantId: Uuid): DreamScopeId =
            DreamScopeId(DreamPairScopeId.forAssistant(assistantId).value)
    }
}
