package me.rerere.rikkahub.memory.dreaming.runtime

import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamPairScopeId
import me.rerere.rikkahub.memory.dreaming.model.DreamingFeatureFlags

const val DREAMING_PREFERENCES_SCHEMA_VERSION = 1
const val MAX_DREAMING_PREFERENCES_JSON_CHARS = 256 * 1024
const val MAX_DREAMING_SCOPE_PREFERENCES = 1_024

const val MAX_DREAMING_DAILY_RUN_LIMIT = 64
const val MAX_DREAMING_DAILY_INPUT_TOKEN_LIMIT = 2_000_000L
const val MAX_DREAMING_DAILY_OUTPUT_TOKEN_LIMIT = 500_000L
const val MAX_DREAMING_RETRY_LIMIT = 5
const val MIN_DREAMING_IDLE_THRESHOLD_MINUTES = 5
const val MAX_DREAMING_IDLE_THRESHOLD_MINUTES = 1_440

/** Network constraint applied to every background synthesis request. */
@Serializable
enum class DreamNetworkPolicy {
    CONNECTED,
    UNMETERED,
}

/**
 * One app-global cost policy. It is deliberately not copied into each scope: every private scope
 * and the global scope consume the same UTC-day allowance.
 *
 * A nullable token limit means that dimension is not capped. A non-null zero is an enabled cap
 * that denies every model call; it never means "unlimited".
 */
@Serializable
data class DreamingCostPolicy(
    val networkPolicy: DreamNetworkPolicy = DreamNetworkPolicy.UNMETERED,
    val requireBatteryNotLow: Boolean = true,
    val requireCharging: Boolean = true,
    val dailyRunLimit: Int = 4,
    val dailyInputTokenLimit: Long? = 100_000L,
    val dailyOutputTokenLimit: Long? = 20_000L,
    /** Number of retries after the initial attempt. */
    val retryLimit: Int = 2,
    val idleThresholdMinutes: Int = 15,
) {
    fun validatedOrNull(): DreamingCostPolicy? = takeIf {
        dailyRunLimit in 0..MAX_DREAMING_DAILY_RUN_LIMIT &&
            dailyInputTokenLimit.isNullOrIn(0L..MAX_DREAMING_DAILY_INPUT_TOKEN_LIMIT) &&
            dailyOutputTokenLimit.isNullOrIn(0L..MAX_DREAMING_DAILY_OUTPUT_TOKEN_LIMIT) &&
            retryLimit in 0..MAX_DREAMING_RETRY_LIMIT &&
            idleThresholdMinutes in
            MIN_DREAMING_IDLE_THRESHOLD_MINUTES..MAX_DREAMING_IDLE_THRESHOLD_MINUTES
    }
}

/** Per-scope behavior only. Cost, network, charging, and retry policy remain app-global. */
@Serializable
data class DreamingScopePreferences(
    val generate: Boolean = false,
    val shadow: Boolean = false,
    val use: Boolean = false,
) {
    /**
     * Valid states are off, use-an-existing-snapshot, generate-only, generate-and-use, or shadow.
     * Shadow generation is intentionally mutually exclusive with runtime use.
     */
    fun validatedOrNull(): DreamingScopePreferences? = takeIf {
        (!shadow || generate) && (!shadow || !use)
    }

    fun apply(mutation: DreamingScopePreferenceMutation): DreamingScopePreferences =
        when (mutation) {
            is DreamingScopePreferenceMutation.SetGenerate -> copy(
                generate = mutation.enabled,
                shadow = shadow && mutation.enabled,
            )

            is DreamingScopePreferenceMutation.SetShadow -> if (mutation.enabled) {
                copy(generate = true, shadow = true, use = false)
            } else {
                copy(shadow = false)
            }

            is DreamingScopePreferenceMutation.SetUse -> copy(
                shadow = if (mutation.enabled) false else shadow,
                use = mutation.enabled,
            )
        }.also { checkNotNull(it.validatedOrNull()) }

    internal fun toFeatureFlags(schemaReady: Boolean): DreamingFeatureFlags =
        if (!schemaReady) {
            DreamingFeatureFlags.M1AllOff
        } else {
            DreamingFeatureFlags(
                schemaReady = true,
                generate = generate,
                shadow = shadow,
                use = use,
                deepRebuild = false,
                relationRoute = false,
            )
        }
}

/** A typed user intent avoids guessing which side of an invalid switch combination should win. */
sealed interface DreamingScopePreferenceMutation {
    data class SetGenerate(val enabled: Boolean) : DreamingScopePreferenceMutation
    data class SetShadow(val enabled: Boolean) : DreamingScopePreferenceMutation
    data class SetUse(val enabled: Boolean) : DreamingScopePreferenceMutation
}

/** Previous/current values captured by the same SettingsStore transform lock. */
data class DreamingPreferenceChange<T>(
    val previous: T,
    val current: T,
)

/** Single backed-up JSON root for every scope flag and the one app-global cost policy. */
@Serializable
data class DreamingPreferencesV1(
    val schemaVersion: Int = DREAMING_PREFERENCES_SCHEMA_VERSION,
    val scopes: Map<String, DreamingScopePreferences> = emptyMap(),
    val costPolicy: DreamingCostPolicy = DreamingCostPolicy(),
) {
    fun validatedOrNull(): DreamingPreferencesV1? {
        if (schemaVersion != DREAMING_PREFERENCES_SCHEMA_VERSION) return null
        if (scopes.size > MAX_DREAMING_SCOPE_PREFERENCES) return null
        val validatedPolicy = costPolicy.validatedOrNull() ?: return null
        val canonicalScopes = sortedMapOf<String, DreamingScopePreferences>()
        scopes.forEach { (rawScope, preferences) ->
            val scope = DreamScopeId.parseOrNull(rawScope) ?: return null
            if (scope.value != rawScope) return null
            val validated = preferences.validatedOrNull() ?: return null
            if (validated != DreamingScopePreferences()) {
                canonicalScopes[scope.value] = validated
            }
        }
        return copy(scopes = canonicalScopes, costPolicy = validatedPolicy)
    }

    fun failClosed(): DreamingPreferencesV1 = validatedOrNull() ?: DreamingPreferencesV1()

    fun forScope(scopeId: DreamScopeId): DreamingScopePreferences {
        val base = failClosed()
        base.scopes[scopeId.value]?.let { return it }
        // One-time semantic bridge from the old Memory-owned Dream scopes. New pair-specific
        // settings win immediately; otherwise preserve the user's previous assistant/global flag.
        if (DreamPairScopeId.parseOrNull(scopeId.value) != null) {
            val assistantId = scopeId.value.substringAfterLast(':')
            base.scopes[assistantId]?.let { return it }
            base.scopes[DreamScopeId.GLOBAL_VALUE]?.let { return it }
        }
        return DreamingScopePreferences()
    }

    fun withScopeMutation(
        scopeId: DreamScopeId,
        mutation: DreamingScopePreferenceMutation,
    ): DreamingPreferencesV1 = withScopePreferences(
        scopeId = scopeId,
        preferences = forScope(scopeId).apply(mutation),
    )

    fun withScopePreferences(
        scopeId: DreamScopeId,
        preferences: DreamingScopePreferences,
    ): DreamingPreferencesV1 {
        val base = failClosed()
        val validated = requireNotNull(preferences.validatedOrNull()) {
            "Illegal Dreaming scope preference combination"
        }
        val updated = base.scopes.toMutableMap()
        if (validated == DreamingScopePreferences()) {
            updated.remove(scopeId.value)
        } else {
            require(scopeId.value in updated || updated.size < MAX_DREAMING_SCOPE_PREFERENCES) {
                "Too many Dreaming scope preference entries"
            }
            updated[scopeId.value] = validated
        }
        return base.copy(scopes = updated.toSortedMap())
    }

    fun withCostPolicy(policy: DreamingCostPolicy): DreamingPreferencesV1 =
        failClosed().copy(
            costPolicy = requireNotNull(policy.validatedOrNull()) {
                "Dreaming cost policy is outside its supported bounds"
            },
        )
}

private val strictDreamingPreferencesJson = Json {
    encodeDefaults = true
    ignoreUnknownKeys = false
    coerceInputValues = false
    isLenient = false
}

/**
 * Missing, malformed, future-version, structurally incomplete, or semantically invalid data is
 * always all-off with conservative defaults. A present V1 blob must contain the complete V1
 * shape; default constructor values are for a missing key, not for repairing a partial payload.
 */
fun decodeDreamingPreferencesOrDefault(raw: String?): DreamingPreferencesV1 {
    if (raw.isNullOrBlank() || raw.length > MAX_DREAMING_PREFERENCES_JSON_CHARS) {
        return DreamingPreferencesV1()
    }
    return try {
        val root = strictDreamingPreferencesJson.parseToJsonElement(raw) as? JsonObject
            ?: return DreamingPreferencesV1()
        if (root.keys != DREAMING_ROOT_KEYS) return DreamingPreferencesV1()
        val cost = root["costPolicy"] as? JsonObject ?: return DreamingPreferencesV1()
        if (cost.keys != DREAMING_COST_POLICY_KEYS) return DreamingPreferencesV1()
        val scopes = root["scopes"] as? JsonObject ?: return DreamingPreferencesV1()
        if (scopes.values.any { value ->
                val entry = value as? JsonObject ?: return@any true
                entry.keys != DREAMING_SCOPE_KEYS
            }
        ) {
            return DreamingPreferencesV1()
        }
        strictDreamingPreferencesJson.decodeFromString<DreamingPreferencesV1>(raw)
            .validatedOrNull()
            ?: DreamingPreferencesV1()
    } catch (_: Exception) {
        DreamingPreferencesV1()
    }
}

/** Invalid in-memory values are never persisted; they collapse to the same all-off safe root. */
fun encodeDreamingPreferencesFailClosed(preferences: DreamingPreferencesV1): String =
    strictDreamingPreferencesJson.encodeToString(preferences.failClosed())

private val DREAMING_ROOT_KEYS = setOf("schemaVersion", "scopes", "costPolicy")
private val DREAMING_SCOPE_KEYS = setOf("generate", "shadow", "use")
private val DREAMING_COST_POLICY_KEYS = setOf(
    "networkPolicy",
    "requireBatteryNotLow",
    "requireCharging",
    "dailyRunLimit",
    "dailyInputTokenLimit",
    "dailyOutputTokenLimit",
    "retryLimit",
    "idleThresholdMinutes",
)

private fun Long?.isNullOrIn(range: LongRange): Boolean = this == null || this in range
