package me.rerere.rikkahub.memory.dreaming.synthesis

import java.nio.charset.StandardCharsets
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.longOrNull
import me.rerere.rikkahub.memory.dreaming.model.DreamOpaqueToken
import me.rerere.rikkahub.memory.dreaming.model.DreamOpaqueTokenKind
import me.rerere.rikkahub.memory.dreaming.model.DreamProposalNonce
import me.rerere.rikkahub.memory.dreaming.model.requireDreamValidUnicode

enum class DreamProposalParseFailure {
    EMPTY,
    TOO_LARGE,
    INVALID_UNICODE,
    INVALID_JSON,
    DUPLICATE_KEY,
    ROOT_NOT_OBJECT,
    UNKNOWN_FIELD,
    MISSING_FIELD,
    WRONG_TYPE,
    UNKNOWN_ENUM,
    INVALID_VALUE,
    TOO_MANY_OPERATIONS,
}

sealed interface DreamProposalParseResult {
    data class Parsed(val proposal: DreamProposalEnvelope) : DreamProposalParseResult
    data class Rejected(val failure: DreamProposalParseFailure) : DreamProposalParseResult
}

/** Strict, whole-document pair-Dream proposal parser. It never searches for a JSON substring. */
object DreamProposalParser {
    private val json = Json {
        isLenient = false
        ignoreUnknownKeys = false
        coerceInputValues = false
        allowStructuredMapKeys = false
    }

    fun parse(raw: String): DreamProposalParseResult {
        if (raw.isBlank()) return DreamProposalParseResult.Rejected(DreamProposalParseFailure.EMPTY)
        if (raw.toByteArray(StandardCharsets.UTF_8).size > MAX_DREAM_PROPOSAL_UTF8_BYTES) {
            return DreamProposalParseResult.Rejected(DreamProposalParseFailure.TOO_LARGE)
        }
        val document = unwrapSingleDreamJsonDocument(raw)
            ?: return DreamProposalParseResult.Rejected(DreamProposalParseFailure.INVALID_JSON)
        try {
            requireDreamValidUnicode(document)
        } catch (_: IllegalArgumentException) {
            return DreamProposalParseResult.Rejected(DreamProposalParseFailure.INVALID_UNICODE)
        }
        val duplicateScan = StrictJsonKeyScanner.scan(document)
        if (duplicateScan == StrictJsonKeyScanner.Result.DUPLICATE) {
            return DreamProposalParseResult.Rejected(DreamProposalParseFailure.DUPLICATE_KEY)
        }
        if (duplicateScan == StrictJsonKeyScanner.Result.INVALID) {
            return DreamProposalParseResult.Rejected(DreamProposalParseFailure.INVALID_JSON)
        }
        val root = try {
            json.parseToJsonElement(document)
        } catch (_: Exception) {
            return DreamProposalParseResult.Rejected(DreamProposalParseFailure.INVALID_JSON)
        }
        val objectRoot = root as? JsonObject
            ?: return DreamProposalParseResult.Rejected(DreamProposalParseFailure.ROOT_NOT_OBJECT)
        return try {
            DreamProposalParseResult.Parsed(parseEnvelope(objectRoot))
        } catch (error: ProposalParseException) {
            DreamProposalParseResult.Rejected(error.failure)
        } catch (_: Exception) {
            DreamProposalParseResult.Rejected(DreamProposalParseFailure.INVALID_VALUE)
        }
    }

    private fun parseEnvelope(root: JsonObject): DreamProposalEnvelope {
        root.requireExactKeys(
            required = setOf(
                "schema_version",
                "proposal_nonce",
                "base_experience_epoch",
                "base_dream_revision",
                "mode",
                "operations",
            ),
        )
        val operationsArray = root.requiredArray("operations")
        if (operationsArray.size > MAX_DREAM_PROPOSAL_OPERATIONS) {
            fail(DreamProposalParseFailure.TOO_MANY_OPERATIONS)
        }
        return DreamProposalEnvelope(
            schemaVersion = root.requiredLong("schema_version").toIntExact(),
            proposalNonce = constructOrFail { DreamProposalNonce(root.requiredString("proposal_nonce")) },
            baseMemoryEpoch = root.requiredLong("base_experience_epoch").nonNegative(),
            baseDreamRevision = root.requiredLong("base_dream_revision").nonNegative(),
            mode = root.requiredEnum("mode"),
            operations = operationsArray.map { parseOperation(it.requiredObject()) },
        )
    }

    private fun parseOperation(value: JsonObject): DreamProposalOperation = when (value.requiredString("op")) {
        "UPSERT_CLAIM" -> {
            value.requireExactKeys(
                required = setOf("op", "target_claim_token", "expected_claim_revision", "claim"),
            )
            val target = value.nullableString("target_claim_token")?.let {
                constructOrFail { DreamOpaqueToken(it) }.requireKind(DreamOpaqueTokenKind.CLAIM)
            }
            val revision = value.nullableLong("expected_claim_revision")
            constructOrFail {
                DreamProposalOperation.UpsertClaim(target, revision, parseClaim(value.requiredObject("claim")))
            }
        }

        "SUPERSEDE_CLAIM" -> {
            value.requireExactKeys(
                required = setOf("op", "target_claim_token", "expected_claim_revision", "replacement"),
            )
            constructOrFail {
                DreamProposalOperation.SupersedeClaim(
                    targetClaimToken = DreamOpaqueToken(value.requiredString("target_claim_token"))
                        .requireKind(DreamOpaqueTokenKind.CLAIM),
                    expectedClaimRevision = value.requiredLong("expected_claim_revision"),
                    replacement = parseClaim(value.requiredObject("replacement")),
                )
            }
        }

        "INVALIDATE_CLAIM" -> {
            value.requireExactKeys(
                required = setOf("op", "target_claim_token", "expected_claim_revision", "reason", "evidence"),
            )
            constructOrFail {
                DreamProposalOperation.InvalidateClaim(
                    targetClaimToken = DreamOpaqueToken(value.requiredString("target_claim_token"))
                        .requireKind(DreamOpaqueTokenKind.CLAIM),
                    expectedClaimRevision = value.requiredLong("expected_claim_revision"),
                    reason = value.requiredEnum("reason"),
                    evidence = value.requiredArray("evidence").map { parseEvidence(it.requiredObject()) },
                )
            }
        }

        "NO_OP" -> {
            value.requireExactKeys(required = setOf("op"))
            DreamProposalOperation.NoOp
        }

        else -> fail(DreamProposalParseFailure.UNKNOWN_ENUM)
    }

    private fun parseClaim(value: JsonObject): DreamProposedClaim {
        value.requireExactKeys(
            required = setOf(
                "claim_key_hint",
                "subject_kind",
                "profile_section",
                "epistemic_origin",
                "content_type",
                "title",
                "statement",
                "temporal_expression",
                "evidence",
            ),
        )
        val evidence = value.requiredArray("evidence")
        if (evidence.size > MAX_DREAM_EVIDENCE_PER_OPERATION) {
            fail(DreamProposalParseFailure.INVALID_VALUE)
        }
        return constructOrFail {
            DreamProposedClaim(
                claimKeyHint = value.requiredString("claim_key_hint"),
                subjectKind = value.requiredEnum("subject_kind"),
                profileSection = value.requiredString("profile_section"),
                epistemicOrigin = value.requiredEnum("epistemic_origin"),
                contentType = value.requiredEnum("content_type"),
                title = value.requiredString("title"),
                statement = value.requiredString("statement"),
                temporalExpression = value.nullableString("temporal_expression"),
                evidence = evidence.map { parseEvidence(it.requiredObject()) },
            )
        }
    }

    private fun parseEvidence(value: JsonObject): DreamProposedEvidence {
        value.requireExactKeys(required = setOf("experience_token", "expected_epoch", "support_type"))
        return constructOrFail {
            DreamProposedEvidence(
                experienceToken = DreamOpaqueToken(value.requiredString("experience_token"))
                    .requireKind(DreamOpaqueTokenKind.MEMORY),
                expectedEpoch = value.requiredLong("expected_epoch"),
                supportType = value.requiredEnum("support_type"),
            )
        }
    }

    private fun JsonObject.requireExactKeys(required: Set<String>) {
        if (!keys.containsAll(required)) fail(DreamProposalParseFailure.MISSING_FIELD)
        if (keys.any { it !in required }) fail(DreamProposalParseFailure.UNKNOWN_FIELD)
    }

    private fun JsonObject.requiredString(key: String): String {
        val primitive = this[key] as? JsonPrimitive ?: fail(DreamProposalParseFailure.WRONG_TYPE)
        if (!primitive.isString) fail(DreamProposalParseFailure.WRONG_TYPE)
        return primitive.content
    }

    private fun JsonObject.nullableString(key: String): String? {
        val value = this[key] ?: fail(DreamProposalParseFailure.MISSING_FIELD)
        if (value is JsonNull) return null
        val primitive = value as? JsonPrimitive ?: fail(DreamProposalParseFailure.WRONG_TYPE)
        if (!primitive.isString) fail(DreamProposalParseFailure.WRONG_TYPE)
        return primitive.content
    }

    private fun JsonObject.requiredLong(key: String): Long {
        val primitive = this[key] as? JsonPrimitive ?: fail(DreamProposalParseFailure.WRONG_TYPE)
        if (primitive.isString) fail(DreamProposalParseFailure.WRONG_TYPE)
        return primitive.longOrNull ?: fail(DreamProposalParseFailure.WRONG_TYPE)
    }

    private fun JsonObject.nullableLong(key: String): Long? {
        val value = this[key] ?: fail(DreamProposalParseFailure.MISSING_FIELD)
        if (value is JsonNull) return null
        val primitive = value as? JsonPrimitive ?: fail(DreamProposalParseFailure.WRONG_TYPE)
        if (primitive.isString) fail(DreamProposalParseFailure.WRONG_TYPE)
        return primitive.longOrNull ?: fail(DreamProposalParseFailure.WRONG_TYPE)
    }

    private fun JsonObject.requiredArray(key: String): JsonArray =
        this[key] as? JsonArray ?: fail(DreamProposalParseFailure.WRONG_TYPE)

    private fun JsonObject.requiredObject(key: String): JsonObject =
        this[key] as? JsonObject ?: fail(DreamProposalParseFailure.WRONG_TYPE)

    private fun JsonElement.requiredObject(): JsonObject =
        this as? JsonObject ?: fail(DreamProposalParseFailure.WRONG_TYPE)

    private inline fun <reified T : Enum<T>> JsonObject.requiredEnum(key: String): T =
        enumValues<T>().singleOrNull { it.name == requiredString(key) }
            ?: fail(DreamProposalParseFailure.UNKNOWN_ENUM)

    private fun DreamOpaqueToken.requireKind(expected: DreamOpaqueTokenKind): DreamOpaqueToken = apply {
        if (kind != expected) fail(DreamProposalParseFailure.INVALID_VALUE)
    }

    private fun Long.nonNegative(): Long = apply {
        if (this < 0L) fail(DreamProposalParseFailure.INVALID_VALUE)
    }

    private fun Long.toIntExact(): Int {
        if (this !in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong()) {
            fail(DreamProposalParseFailure.INVALID_VALUE)
        }
        return toInt()
    }

    private inline fun <T> constructOrFail(block: () -> T): T = try {
        block()
    } catch (error: ProposalParseException) {
        throw error
    } catch (_: Exception) {
        fail(DreamProposalParseFailure.INVALID_VALUE)
    }

    private fun fail(failure: DreamProposalParseFailure): Nothing = throw ProposalParseException(failure)

    private class ProposalParseException(val failure: DreamProposalParseFailure) : RuntimeException()
}

/** Allows only one transport fence; prose and nested/trailing fences remain invalid. */
private fun unwrapSingleDreamJsonDocument(raw: String): String? {
    val trimmed = raw.trim()
    if (!trimmed.startsWith("```")) return trimmed
    val firstLineEnd = trimmed.indexOf('\n')
    if (firstLineEnd < 0) return null
    val header = trimmed.substring(0, firstLineEnd).trimEnd('\r')
    if (header != "```" && !header.equals("```json", ignoreCase = true)) return null
    val closingStart = trimmed.lastIndexOf("```")
    if (closingStart <= firstLineEnd || closingStart + 3 != trimmed.length) return null
    val body = trimmed.substring(firstLineEnd + 1, closingStart).trim()
    return body.takeIf { it.isNotEmpty() && !it.contains("```") }
}

/** Detects semantic duplicate keys, including `"a"` versus `"\u0061"`, before Json decoding. */
private object StrictJsonKeyScanner {
    enum class Result { VALID, DUPLICATE, INVALID }

    fun scan(raw: String): Result = try {
        Cursor(raw).parseDocument()
        Result.VALID
    } catch (_: DuplicateKey) {
        Result.DUPLICATE
    } catch (_: InvalidJson) {
        Result.INVALID
    }

    private class Cursor(private val raw: String) {
        private var index = 0

        fun parseDocument() {
            whitespace()
            value()
            whitespace()
            if (index != raw.length) invalid()
        }

        private fun value() {
            if (index >= raw.length) invalid()
            when (raw[index]) {
                '{' -> objectValue()
                '[' -> arrayValue()
                '"' -> stringValue()
                't' -> literal("true")
                'f' -> literal("false")
                'n' -> literal("null")
                '-', in '0'..'9' -> number()
                else -> invalid()
            }
        }

        private fun objectValue() {
            index++
            whitespace()
            val keys = hashSetOf<String>()
            if (take('}')) return
            while (true) {
                if (index >= raw.length || raw[index] != '"') invalid()
                val key = stringValue()
                if (!keys.add(key)) throw DuplicateKey()
                whitespace()
                if (!take(':')) invalid()
                whitespace()
                value()
                whitespace()
                if (take('}')) return
                if (!take(',')) invalid()
                whitespace()
            }
        }

        private fun arrayValue() {
            index++
            whitespace()
            if (take(']')) return
            while (true) {
                value()
                whitespace()
                if (take(']')) return
                if (!take(',')) invalid()
                whitespace()
            }
        }

        private fun stringValue(): String {
            if (!take('"')) invalid()
            val result = StringBuilder()
            while (index < raw.length) {
                val char = raw[index++]
                when {
                    char == '"' -> return result.toString()
                    char == '\\' -> escaped(result)
                    char.code < 0x20 -> invalid()
                    char.isHighSurrogate() -> {
                        if (index >= raw.length || !raw[index].isLowSurrogate()) invalid()
                        result.append(char).append(raw[index++])
                    }
                    char.isLowSurrogate() -> invalid()
                    else -> result.append(char)
                }
            }
            invalid()
        }

        private fun escaped(result: StringBuilder) {
            if (index >= raw.length) invalid()
            when (val escape = raw[index++]) {
                '"', '\\', '/' -> result.append(escape)
                'b' -> result.append('\b')
                'f' -> result.append('\u000c')
                'n' -> result.append('\n')
                'r' -> result.append('\r')
                't' -> result.append('\t')
                'u' -> {
                    val first = unicodeUnit()
                    if (first.isHighSurrogate()) {
                        if (index + 1 >= raw.length || raw[index] != '\\' || raw[index + 1] != 'u') invalid()
                        index += 2
                        val second = unicodeUnit()
                        if (!second.isLowSurrogate()) invalid()
                        result.append(first).append(second)
                    } else {
                        if (first.isLowSurrogate()) invalid()
                        result.append(first)
                    }
                }
                else -> invalid()
            }
        }

        private fun unicodeUnit(): Char {
            if (index + 4 > raw.length) invalid()
            val value = raw.substring(index, index + 4).toIntOrNull(16) ?: invalid()
            index += 4
            return value.toChar()
        }

        private fun number() {
            take('-')
            if (take('0')) {
                if (index < raw.length && raw[index].isDigit()) invalid()
            } else {
                digits(required = true)
            }
            if (take('.')) digits(required = true)
            if (take('e') || take('E')) {
                take('+') || take('-')
                digits(required = true)
            }
        }

        private fun digits(required: Boolean) {
            val start = index
            while (index < raw.length && raw[index].isDigit()) index++
            if (required && start == index) invalid()
        }

        private fun literal(value: String) {
            if (!raw.startsWith(value, index)) invalid()
            index += value.length
        }

        private fun whitespace() {
            while (index < raw.length && raw[index] in charArrayOf(' ', '\t', '\r', '\n')) index++
        }

        private fun take(expected: Char): Boolean =
            if (index < raw.length && raw[index] == expected) {
                index++
                true
            } else {
                false
            }

        private fun invalid(): Nothing = throw InvalidJson()
    }

    private class DuplicateKey : RuntimeException()
    private class InvalidJson : RuntimeException()
}
