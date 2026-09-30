package me.rerere.rikkahub.data.scope

import java.security.MessageDigest
import kotlin.uuid.Uuid

private const val MAX_AUTHORITY_SUBJECT_ID_CHARS = 160
private const val MAX_CANONICAL_FIELDS = 64
private const val MAX_CANONICAL_FIELD_BYTES = 4_096
private val HEX = "0123456789abcdef".toCharArray()

/** Persisted by name in execution records; keep the constant names stable. */
enum class AgentScopeKind {
    ASSISTANT,
    AUTHORITY_SUBJECT,
}

/** Owner of an execution: either an Assistant or an authority subject. */
sealed interface AgentScope {
    val kind: AgentScopeKind
    val storageId: String

    data class Assistant(val assistantId: Uuid) : AgentScope {
        override val kind: AgentScopeKind = AgentScopeKind.ASSISTANT
        override val storageId: String = assistantId.toString()

        override fun toString(): String = "AgentScope.Assistant(<redacted>)"
    }

    data class AuthoritySubject(val authoritySubjectId: String) : AgentScope {
        init {
            require(isSafeScopeIdentifier(authoritySubjectId, MAX_AUTHORITY_SUBJECT_ID_CHARS)) {
                "Invalid authority subject identifier"
            }
        }

        override val kind: AgentScopeKind = AgentScopeKind.AUTHORITY_SUBJECT
        override val storageId: String = authoritySubjectId

        override fun toString(): String = "AgentScope.AuthoritySubject(<redacted>)"
    }

    companion object {
        fun parseOrNull(kind: String, id: String): AgentScope? = when (kind) {
            AgentScopeKind.ASSISTANT.name -> runCatching { Assistant(Uuid.parse(id)) }.getOrNull()
            AgentScopeKind.AUTHORITY_SUBJECT.name ->
                runCatching { AuthoritySubject(id) }.getOrNull()
            else -> null
        }
    }
}

/** Versioned, length-prefixed SHA-256; field boundaries cannot collide by string concatenation. */
object CanonicalId {
    fun digest(domainVersion: String, fields: List<String?>): String {
        require(isCanonicalDomain(domainVersion)) { "Invalid canonical ID domain" }
        require(fields.size <= MAX_CANONICAL_FIELDS) { "Too many canonical ID fields" }

        val digest = MessageDigest.getInstance("SHA-256")
        digest.updateLengthPrefixed(domainVersion.encodeToByteArray())
        fields.forEach { field ->
            if (field == null) {
                digest.updateInt(-1)
            } else {
                val bytes = field.encodeToByteArray()
                require(bytes.size <= MAX_CANONICAL_FIELD_BYTES) { "Canonical ID field too large" }
                digest.updateLengthPrefixed(bytes)
            }
        }
        return digest.digest().toLowerHex()
    }
}

private fun MessageDigest.updateLengthPrefixed(bytes: ByteArray) {
    updateInt(bytes.size)
    update(bytes)
}

private fun MessageDigest.updateInt(value: Int) {
    update((value ushr 24).toByte())
    update((value ushr 16).toByte())
    update((value ushr 8).toByte())
    update(value.toByte())
}

private fun ByteArray.toLowerHex(): String = buildString(size * 2) {
    this@toLowerHex.forEach { byte ->
        val value = byte.toInt() and 0xff
        append(HEX[value ushr 4])
        append(HEX[value and 0x0f])
    }
}

private fun isCanonicalDomain(value: String): Boolean =
    value.isNotEmpty() && value.length <= 64 && value.matches(Regex("[a-z][a-z0-9-]*-v[1-9][0-9]*"))

private fun isSafeScopeIdentifier(value: String, maxChars: Int): Boolean =
    value.isNotEmpty() &&
        value.length <= maxChars &&
        value.all { char ->
            char in 'a'..'z' ||
                char in 'A'..'Z' ||
                char in '0'..'9' ||
                char == '-' ||
                char == '_' ||
                char == '.' ||
                char == ':' ||
                char == '@'
        }
