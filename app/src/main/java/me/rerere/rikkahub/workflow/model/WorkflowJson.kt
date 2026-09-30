package me.rerere.rikkahub.workflow.model

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.intOrNull
import kotlinx.serialization.json.booleanOrNull
import me.rerere.ai.core.Tool
import me.rerere.rikkahub.data.capability.CapabilityKey
import me.rerere.rikkahub.toolcatalog.ToolCatalogSnapshot

/**
 * Phase 12 鈥?strict JSON schema validator + parser/serializer.
 *
 * Wire shape (LLM-facing):
 * ```
 * { "trigger": { "type": "wifi_connected", "params": { "ssid": "X" } },
 *   "conditions": [ { "type": "time_after_sunset", "params": { } } ],
 *   "actions": [ { "tool": "...", "args": { ... } } ] }
 * ```
 *
 * kotlinx polymorphic-sealed default is `{ "type": "...", <inline-fields> }`. We bridge by
 * flattening the `params` object into the same JSON object as `type` before calling the
 * polymorphic decoder. That way the LLM sees a tidy nested schema while we get static
 * typing on the Kotlin side. Errors return [ParseResult.Err] with stable codes the tools
 * pass straight back as their error envelopes.
 */
object WorkflowJson {

    private val strictRootKeys = setOf(
        "id",
        "name",
        "description",
        "enabled",
        "trigger",
        "conditions",
        "actions",
        "cooldown_seconds",
        "max_runs_per_day",
        "created_at_ms",
        "updated_at_ms",
        "authoring_assistant_id",
        // Server-owned when workflow_create/workflow_update persist a definition. It remains a
        // known key so canonical stored JSON can also pass through tooling without being
        // mistaken for a schema extension; callers still overwrite it from reviewed actions.
        "capability_snapshot",
        "origin",
        "source_candidate_id",
        "source_artifact_hash",
        "grant_digest",
        "authority_subject_id",
    )

    private val strictActionKeys = setOf(
        "tool",
        "args",
        "timeout_seconds",
        "tool_schema_fingerprint",
    )

    @OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)
    private val strict: Json = Json {
        ignoreUnknownKeys = false
        coerceInputValues = false
        explicitNulls = false
        encodeDefaults = true
        // The wire shape uses snake_case (LLM-facing); Kotlin sealed-class fields are
        // camelCase. The naming strategy is applied to all properties of @Serializable
        // classes 鈥?both encode and decode 鈥?so `thresholdPercent` becomes
        // `threshold_percent` in JSON.
        namingStrategy = kotlinx.serialization.json.JsonNamingStrategy.SnakeCase
    }

    sealed class ParseResult {
        data class Ok(val definition: WorkflowDefinition) : ParseResult()
        data class Err(val error: String, val detail: String) : ParseResult() {
            fun withIndex(idx: Int, kind: String): Err = Err(error, "$kind[$idx]: $detail")
        }
    }

    /**
     * Parse a workflow definition from the LLM's JSON. Returns Err for any structural problem
     * with a stable error code. The caller is expected to surface the error verbatim so the
     * LLM can repair its emission.
     */
    fun parse(rawJson: String, knownToolNames: Set<String>): ParseResult =
        parseInternal(rawJson, knownToolNames, emptyMap())

    /** Production authoring path: validates args against exact Tool definitions and stamps them. */
    fun parse(rawJson: String, knownTools: Collection<Tool>): ParseResult {
        val definitions = knownTools.distinctBy(Tool::name)
        val catalog = ToolCatalogSnapshot.fromDefinitions(definitions)
        val byName = definitions.associateBy(Tool::name)
        return parseInternal(
            rawJson = rawJson,
            knownToolNames = byName.keys,
            toolDefinitions = byName.mapValues { (name, tool) ->
                ToolDefinition(tool, catalog.entry(name)?.schemaFingerprint)
            },
        )
    }

    private data class ToolDefinition(val tool: Tool, val schemaFingerprint: String?)

    private fun parseInternal(
        rawJson: String,
        knownToolNames: Set<String>,
        toolDefinitions: Map<String, ToolDefinition>,
    ): ParseResult {
        val element: JsonElement = runCatching { Json.parseToJsonElement(rawJson) }.getOrElse {
            return ParseResult.Err("invalid_json", it.message ?: "JSON parse failed")
        }
        val obj = element as? JsonObject
            ?: return ParseResult.Err("not_an_object", "definition must be a JSON object")

        val unknownRootKeys = obj.keys - strictRootKeys
        if (unknownRootKeys.isNotEmpty()) {
            return ParseResult.Err(
                "unknown_root_key",
                "definition contains unknown key(s): ${unknownRootKeys.sorted().joinToString()}",
            )
        }

        val name = (obj["name"] as? JsonPrimitive)?.takeIf { it.isString }?.contentOrNull
            ?: return ParseResult.Err("missing_name", "name is required")
        if (name.isBlank()) return ParseResult.Err("invalid_name", "name must be non-blank")
        if (name.length > WorkflowConstants.MAX_NAME_LENGTH) {
            return ParseResult.Err("invalid_name",
                "name must be 鈮?${WorkflowConstants.MAX_NAME_LENGTH} chars")
        }

        val description = when (val element = obj["description"]) {
            null, kotlinx.serialization.json.JsonNull -> null
            is JsonPrimitive -> element.takeIf { it.isString }?.contentOrNull
                ?: return ParseResult.Err("invalid_description", "description must be a string")
            else -> return ParseResult.Err("invalid_description", "description must be a string")
        }?.take(WorkflowConstants.MAX_DESCRIPTION_LENGTH)
        val enabled = when (val element = obj["enabled"]) {
            null -> true
            is JsonPrimitive -> element.booleanOrNull
                ?: return ParseResult.Err("invalid_enabled", "enabled must be a boolean")
            else -> return ParseResult.Err("invalid_enabled", "enabled must be a boolean")
        }

        val triggerObj = obj["trigger"] as? JsonObject
            ?: return ParseResult.Err("missing_trigger", "trigger object is required")
        val trigger = when (val r = decodeTrigger(triggerObj)) {
            is DecodeOk -> r.value as TriggerSpec
            is DecodeErr -> return r.err
        }

        val conditionsArr = when (val element = obj["conditions"]) {
            null -> JsonArray(emptyList())
            is JsonArray -> element
            else -> return ParseResult.Err("bad_conditions_shape", "conditions must be an array")
        }
        val conditions = mutableListOf<ConditionSpec>()
        for ((idx, el) in conditionsArr.withIndex()) {
            val condObj = el as? JsonObject
                ?: return ParseResult.Err("bad_condition_shape", "condition $idx is not an object")
            val cond = when (val r = decodeCondition(condObj)) {
                is DecodeOk -> r.value as ConditionSpec
                is DecodeErr -> return r.err.withIndex(idx, "condition")
            }
            conditions += cond
        }

        val actionsArr = obj["actions"] as? JsonArray
            ?: return ParseResult.Err("missing_actions", "actions array is required")
        if (actionsArr.isEmpty()) {
            return ParseResult.Err("empty_actions", "actions must be non-empty")
        }
        if (actionsArr.size > WorkflowConstants.MAX_ACTIONS) {
            return ParseResult.Err("too_many_actions",
                "actions must be 鈮?${WorkflowConstants.MAX_ACTIONS}")
        }
        val actions = mutableListOf<WorkflowAction>()
        for ((idx, el) in actionsArr.withIndex()) {
            val ao = el as? JsonObject
                ?: return ParseResult.Err("bad_action_shape", "action $idx is not an object")
            val unknownActionKeys = ao.keys - strictActionKeys
            if (unknownActionKeys.isNotEmpty()) {
                return ParseResult.Err(
                    "unknown_action_key",
                    "action $idx contains unknown key(s): ${unknownActionKeys.sorted().joinToString()}",
                )
            }
            val toolName = (ao["tool"] as? JsonPrimitive)?.contentOrNull
                ?: return ParseResult.Err("missing_tool", "action $idx missing 'tool'")
            // Stored definitions use parseStored(); an empty current surface grants nothing.
            if (toolName !in knownToolNames) {
                return ParseResult.Err("unknown_tool",
                    "action $idx tool '$toolName' is not registered for this assistant")
            }
            // Forbid workflow chaining via workflow_run as an action. The spec explicitly
            // lists "Workflow chaining (one workflow triggering another)" as out-of-scope
            // for v1 鈥?without this guard, a malicious or hallucinated workflow definition
            // could trigger an unbounded chain across distinct workflow ids that the
            // per-workflow Mutex doesn't catch.
            if (toolName == "workflow_run") {
                return ParseResult.Err("workflow_chaining_disabled",
                    "action $idx: workflow_run cannot be used as a workflow action (chaining is out-of-scope in v1)")
            }
            val args = when (val argsElement = ao["args"]) {
                null -> buildJsonObject { }
                is JsonObject -> argsElement
                else -> return ParseResult.Err(
                    "bad_action_args",
                    "action $idx args must be a JSON object",
                )
            }
            toolDefinitions[toolName]?.let { definition ->
                WorkflowInputSchemaValidator.validate(args, definition.tool.parameters())?.let { error ->
                    return ParseResult.Err(
                        "invalid_action_args",
                        "action $idx ${error.path}: ${error.detail}",
                    )
                }
                if (definition.schemaFingerprint?.let(WorkflowToolSchemaSnapshot::isCanonical) != true) {
                    return ParseResult.Err(
                        "missing_tool_schema_fingerprint",
                        "action $idx tool '$toolName' has no canonical schema fingerprint",
                    )
                }
            }
            val timeout = when (val element = ao["timeout_seconds"]) {
                null -> 60
                is JsonPrimitive -> element.intOrNull
                    ?: return ParseResult.Err("invalid_timeout", "action $idx timeout_seconds must be an integer")
                else -> return ParseResult.Err("invalid_timeout", "action $idx timeout_seconds must be an integer")
            }
            if (timeout < WorkflowConstants.MIN_ACTION_TIMEOUT_S
                || timeout > WorkflowConstants.MAX_ACTION_TIMEOUT_S) {
                return ParseResult.Err("invalid_timeout",
                    "action $idx timeout_seconds must be ${WorkflowConstants.MIN_ACTION_TIMEOUT_S}..${WorkflowConstants.MAX_ACTION_TIMEOUT_S}")
            }
            actions += WorkflowAction(
                tool = toolName,
                args = args,
                timeoutSeconds = timeout,
                toolSchemaFingerprint = toolDefinitions[toolName]?.schemaFingerprint,
            )
        }

        val cooldown = when (val element = obj["cooldown_seconds"]) {
            null -> 0
            is JsonPrimitive -> element.intOrNull
                ?: return ParseResult.Err("invalid_cooldown", "cooldown_seconds must be an integer")
            else -> return ParseResult.Err("invalid_cooldown", "cooldown_seconds must be an integer")
        }
        if (cooldown < 0 || cooldown > WorkflowConstants.MAX_COOLDOWN_S) {
            return ParseResult.Err("invalid_cooldown",
                "cooldown_seconds must be 0..${WorkflowConstants.MAX_COOLDOWN_S}")
        }

        val maxRunsPerDay = when (val element = obj["max_runs_per_day"]) {
            null, kotlinx.serialization.json.JsonNull -> null
            is JsonPrimitive -> element.intOrNull
                ?: return ParseResult.Err("invalid_daily_cap", "max_runs_per_day must be an integer")
            else -> return ParseResult.Err("invalid_daily_cap", "max_runs_per_day must be an integer")
        }
        if (maxRunsPerDay != null && (maxRunsPerDay < WorkflowConstants.MAX_RUNS_PER_DAY_FLOOR
                    || maxRunsPerDay > WorkflowConstants.MAX_RUNS_PER_DAY_CEIL)) {
            return ParseResult.Err("invalid_daily_cap",
                "max_runs_per_day must be ${WorkflowConstants.MAX_RUNS_PER_DAY_FLOOR}..${WorkflowConstants.MAX_RUNS_PER_DAY_CEIL}")
        }

        sanityCheckTrigger(trigger)?.let { return it }
        for ((idx, c) in conditions.withIndex()) {
            sanityCheckCondition(c)?.let { return it.withIndex(idx, "condition") }
        }

        val id = (obj["id"] as? JsonPrimitive)?.takeIf { it.isString }?.contentOrNull
            ?.takeIf { it.isNotBlank() }
            ?: kotlin.uuid.Uuid.random().toString()

        val now = System.currentTimeMillis()
        return ParseResult.Ok(WorkflowDefinition(
            id = id,
            name = name.trim(),
            description = description?.trim(),
            enabled = enabled,
            trigger = trigger,
            conditions = conditions,
            actions = actions,
            cooldownSeconds = cooldown,
            maxRunsPerDay = maxRunsPerDay,
            createdAtMs = (obj["created_at_ms"] as? JsonPrimitive)?.contentOrNull?.toLongOrNull() ?: now,
            updatedAtMs = now,
            authoringAssistantId = (obj["authoring_assistant_id"] as? JsonPrimitive)
                ?.takeIf { it.isString }?.contentOrNull
                ?.takeIf { it.isNotBlank() },
        ))
    }

    /** Serialize a definition back to canonical wire JSON. */
    fun encode(definition: WorkflowDefinition): String {
        val obj = buildJsonObject {
            put("id", JsonPrimitive(definition.id))
            put("name", JsonPrimitive(definition.name))
            if (definition.description != null) put("description", JsonPrimitive(definition.description))
            put("enabled", JsonPrimitive(definition.enabled))
            put("trigger", encodeTrigger(definition.trigger))
            put("conditions", buildJsonArray {
                for (c in definition.conditions) add(encodeCondition(c))
            })
            put("actions", buildJsonArray {
                for (a in definition.actions) {
                    add(buildJsonObject {
                        put("tool", JsonPrimitive(a.tool))
                        put("args", a.args)
                        put("timeout_seconds", JsonPrimitive(a.timeoutSeconds))
                        a.toolSchemaFingerprint?.let { fingerprint ->
                            put("tool_schema_fingerprint", JsonPrimitive(fingerprint))
                        }
                    })
                }
            })
            put("cooldown_seconds", JsonPrimitive(definition.cooldownSeconds))
            if (definition.maxRunsPerDay != null) {
                put("max_runs_per_day", JsonPrimitive(definition.maxRunsPerDay))
            }
            put("created_at_ms", JsonPrimitive(definition.createdAtMs.toString()))
            put("updated_at_ms", JsonPrimitive(definition.updatedAtMs.toString()))
            if (definition.authoringAssistantId != null) {
                put("authoring_assistant_id", JsonPrimitive(definition.authoringAssistantId))
            }
            // An empty in-memory snapshot still represents a legacy definition loaded before
            // this field existed (for example when the user only toggles that row). Preserve
            // that compatibility state by omitting the field. New learned artifacts must use
            // encodeForLearned(), which rejects the same empty value instead of encoding it as
            // legacy.
            if (definition.capabilitySnapshot.isNotEmpty()) {
                put("capability_snapshot", buildJsonArray {
                    definition.capabilitySnapshot.toSortedSet().forEach { capability ->
                        add(JsonPrimitive(capability))
                    }
                })
            }
            put("origin", JsonPrimitive(definition.origin.name))
            if (definition.origin == WorkflowOrigin.LEARNED) {
                definition.sourceCandidateId?.let { put("source_candidate_id", JsonPrimitive(it)) }
                definition.sourceArtifactHash?.let { put("source_artifact_hash", JsonPrimitive(it)) }
                definition.grantDigest?.let { put("grant_digest", JsonPrimitive(it)) }
                // Presence is authoritative. Explicit null is Assistant scope; absence is an
                // old/corrupt artifact whose exact scope is unavailable and must be quarantined.
                put(
                    "authority_subject_id",
                    definition.authoritySubjectId?.let(::JsonPrimitive)
                        ?: kotlinx.serialization.json.JsonNull,
                )
            }
        }
        return obj.toString()
    }

    /** Canonical learned-artifact encoder. Null means the capability snapshot failed closed. */
    fun encodeForLearned(definition: WorkflowDefinition): String? =
        definition.takeIf { candidate ->
            candidate.origin == WorkflowOrigin.LEARNED &&
                candidate.authoringAssistantId?.isNotBlank() == true &&
                candidate.sourceCandidateId?.isNotBlank() == true &&
                candidate.sourceArtifactHash?.isNotBlank() == true &&
                candidate.grantDigest?.isNotBlank() == true &&
                WorkflowCapabilitySnapshot.parsePersistedForLearnedExecution(
                    candidate.capabilitySnapshot,
                ) != null &&
                WorkflowToolSchemaSnapshot.isComplete(candidate.actions)
        }?.let(::encode)

    /** Whether a stored row predates persisted workflow capability snapshots. */
    enum class CapabilitySnapshotStorage {
        PERSISTED,
        LEGACY_MISSING,
    }

    enum class ToolSchemaSnapshotStorage {
        PERSISTED,
        LEGACY_MISSING,
    }

    enum class LearnedScopeStorage {
        PERSISTED,
        LEGACY_MISSING,
    }

    /**
     * Stored-read result with explicit compatibility metadata. This prevents callers from
     * treating an old row with no field as equivalent to a new-format row that persisted an
     * empty snapshot. Only the former is eligible for the narrowly-scoped legacy policy.
     */
    data class StoredDefinition(
        val definition: WorkflowDefinition,
        val capabilitySnapshotStorage: CapabilitySnapshotStorage,
        val toolSchemaSnapshotStorage: ToolSchemaSnapshotStorage,
        val learnedScopeStorage: LearnedScopeStorage,
    ) {
        fun learnedExecutionCapabilitiesOrNull(): Set<CapabilityKey>? =
            if (capabilitySnapshotStorage == CapabilitySnapshotStorage.PERSISTED) {
                WorkflowCapabilitySnapshot.parsePersistedForLearnedExecution(
                    definition.capabilitySnapshot,
                )
            } else {
                null
            }

        fun learnedToolSchemasOrNull(): List<String>? =
            if (
                toolSchemaSnapshotStorage == ToolSchemaSnapshotStorage.PERSISTED &&
                WorkflowToolSchemaSnapshot.isComplete(definition.actions)
            ) {
                definition.actions.mapNotNull(WorkflowAction::toolSchemaFingerprint)
            } else {
                null
            }
    }

    /**
     * Round-trip parse 鈥?used when reading [me.rerere.rikkahub.workflow.db.WorkflowEntity.definitionJson]
     * back into a domain object.
     *
     * Compatibility-oriented: length / range / sanity checks are skipped on the read path
     * because the stored blob was already validated at write time. Structural corruption and
     * invalid current-format capability snapshots still fail closed.
     * If we ever tighten constraints later (e.g. lower MAX_ACTIONS), existing rows must
     * still be loadable so the user can see them, edit them down, or delete them; rejecting
     * silently would hide them from the Settings page and tear down their triggers without
     * explanation. Tool-name validation is also skipped 鈥?a row whose action references a
     * tool the user later disabled stays visible; the runner reports it at fire time.
     */
    fun parseStored(definitionJson: String): WorkflowDefinition? {
        val stored = parseStoredWithCompatibility(definitionJson) ?: return null
        if (stored.definition.origin == WorkflowOrigin.LEARNED) {
            if (stored.learnedExecutionCapabilitiesOrNull() == null) return null
            if (stored.learnedToolSchemasOrNull() == null) return null
            if (stored.definition.authoringAssistantId.isNullOrBlank()) return null
            if (stored.definition.sourceCandidateId.isNullOrBlank()) return null
            if (stored.definition.sourceArtifactHash.isNullOrBlank()) return null
            if (stored.definition.grantDigest.isNullOrBlank()) return null
            if (stored.learnedScopeStorage != LearnedScopeStorage.PERSISTED) return null
        } else if (
            stored.capabilitySnapshotStorage == CapabilitySnapshotStorage.PERSISTED &&
            stored.definition.capabilitySnapshot.any { raw ->
                runCatching { CapabilityKey.of(raw) }.getOrNull()?.value != raw
            }
        ) {
            return null
        }
        return stored.definition
    }

    fun parseStoredWithCompatibility(definitionJson: String): StoredDefinition? {
        val element: JsonElement = runCatching { Json.parseToJsonElement(definitionJson) }.getOrNull() ?: return null
        val obj = element as? JsonObject ?: return null
        if ((obj.keys - strictRootKeys).isNotEmpty()) return null
        val id = obj.stringValue("id")?.takeIf(String::isNotBlank) ?: return null
        val name = obj.stringValue("name")?.takeIf(String::isNotBlank) ?: return null
        val triggerObj = obj["trigger"] as? JsonObject ?: return null
        val triggerSpec = (decodeTrigger(triggerObj) as? DecodeOk)?.value as? TriggerSpec ?: return null
        val conditionsElement = obj["conditions"]
        val conditionArray = when (conditionsElement) {
            null -> JsonArray(emptyList())
            is JsonArray -> conditionsElement
            else -> return null
        }
        val conditions = conditionArray.map { el ->
            val condition = el as? JsonObject ?: return null
            (decodeCondition(condition) as? DecodeOk)?.value as? ConditionSpec ?: return null
        }
        val actionArray = obj["actions"] as? JsonArray ?: return null
        if (actionArray.isEmpty()) return null
        var fingerprintsPresent: Boolean? = null
        val actions = actionArray.map { el ->
            val ao = el as? JsonObject ?: return null
            if ((ao.keys - strictActionKeys).isNotEmpty()) return null
            val toolName = ao.stringValue("tool")?.takeIf(String::isNotBlank) ?: return null
            if (toolName == "workflow_run") return null
            val args = ao["args"] as? JsonObject ?: return null
            val timeout = when (val timeoutElement = ao["timeout_seconds"]) {
                null -> 60
                is JsonPrimitive -> timeoutElement.intOrNull ?: return null
                else -> return null
            }
            if (timeout !in WorkflowConstants.MIN_ACTION_TIMEOUT_S..WorkflowConstants.MAX_ACTION_TIMEOUT_S) {
                return null
            }
            val fingerprintElement = ao["tool_schema_fingerprint"]
            val fingerprint = when (fingerprintElement) {
                null -> null
                is JsonPrimitive -> fingerprintElement.contentOrNull
                    ?.takeIf(WorkflowToolSchemaSnapshot::isCanonical) ?: return null
                else -> return null
            }
            val present = fingerprintElement != null
            if (fingerprintsPresent != null && fingerprintsPresent != present) return null
            fingerprintsPresent = present
            WorkflowAction(
                tool = toolName,
                args = args,
                timeoutSeconds = timeout,
                toolSchemaFingerprint = fingerprint,
            )
        }
        val cooldown = obj.optionalInt("cooldown_seconds", 0) ?: return null
        val maxRunsPerDay = obj.optionalNullableInt("max_runs_per_day") ?: return null
        val snapshotElement = obj["capability_snapshot"]
        val snapshotStorage = if (snapshotElement == null) {
            CapabilitySnapshotStorage.LEGACY_MISSING
        } else {
            CapabilitySnapshotStorage.PERSISTED
        }
        val capabilityList = when (snapshotElement) {
            null -> emptyList()
            is JsonArray -> snapshotElement.map { item ->
                (item as? JsonPrimitive)?.contentOrNull?.takeIf(String::isNotBlank) ?: return null
            }
            else -> return null
        }
        if (capabilityList.distinct().sorted() != capabilityList) return null
        val capabilitySnapshot = capabilityList.toSet()
        val enabled = when (val enabledElement = obj["enabled"]) {
            null -> true
            is JsonPrimitive -> enabledElement.booleanOrNull ?: return null
            else -> return null
        }
        val originValue = obj.optionalString("origin") ?: return null
        val origin = when (val rawOrigin = originValue.value) {
            null -> WorkflowOrigin.USER
            else -> runCatching { WorkflowOrigin.valueOf(rawOrigin) }.getOrNull() ?: return null
        }
        val learnedScopeStorage = if ("authority_subject_id" in obj) {
            LearnedScopeStorage.PERSISTED
        } else {
            LearnedScopeStorage.LEGACY_MISSING
        }
        val authoritySubjectId = (obj.optionalString("authority_subject_id") ?: return null).value
            ?.takeIf(String::isNotBlank)
        if (origin == WorkflowOrigin.LEARNED && authoritySubjectId != null) {
            val valid = runCatching {
                me.rerere.rikkahub.data.scope.AgentScope.AuthoritySubject(
                    authoritySubjectId,
                )
            }.isSuccess
            if (!valid) return null
        }
        val now = System.currentTimeMillis()
        val definition = WorkflowDefinition(
            id = id,
            name = name,
            description = (obj.optionalString("description") ?: return null).value,
            enabled = enabled,
            trigger = triggerSpec,
            conditions = conditions,
            actions = actions,
            cooldownSeconds = cooldown,
            maxRunsPerDay = maxRunsPerDay.value,
            createdAtMs = obj.optionalLong("created_at_ms", now) ?: return null,
            updatedAtMs = obj.optionalLong("updated_at_ms", now) ?: return null,
            authoringAssistantId = obj.optionalString("authoring_assistant_id")?.value
                ?.takeIf(String::isNotBlank),
            capabilitySnapshot = capabilitySnapshot,
            origin = origin,
            sourceCandidateId = obj.optionalString("source_candidate_id")?.value
                ?.takeIf(String::isNotBlank),
            sourceArtifactHash = obj.optionalString("source_artifact_hash")?.value
                ?.takeIf(String::isNotBlank),
            grantDigest = obj.optionalString("grant_digest")?.value
                ?.takeIf(String::isNotBlank),
            authoritySubjectId = authoritySubjectId,
        )
        return StoredDefinition(
            definition = definition,
            capabilitySnapshotStorage = snapshotStorage,
            toolSchemaSnapshotStorage = if (fingerprintsPresent == true) {
                ToolSchemaSnapshotStorage.PERSISTED
            } else {
                ToolSchemaSnapshotStorage.LEGACY_MISSING
            },
            learnedScopeStorage = learnedScopeStorage,
        )
    }

    // -- internal decode helpers -------------------------------------------------------

    private sealed class DecodeOutcome
    private data class DecodeOk(val value: Any) : DecodeOutcome()
    private data class DecodeErr(val err: ParseResult.Err) : DecodeOutcome()

    private data class OptionalValue<T>(val value: T?)

    private fun JsonObject.stringValue(key: String): String? {
        val primitive = this[key] as? JsonPrimitive ?: return null
        return primitive.takeIf { it.isString }?.contentOrNull
    }

    /** Null return = malformed; OptionalValue(null) = absent/explicit null. */
    private fun JsonObject.optionalString(key: String): OptionalValue<String>? {
        return when (val element = this[key]) {
            null, kotlinx.serialization.json.JsonNull -> OptionalValue(null)
            is JsonPrimitive -> element.takeIf { it.isString }?.contentOrNull
                ?.let(::OptionalValue)
            else -> null
        }
    }

    private fun JsonObject.optionalInt(key: String, default: Int): Int? =
        when (val element = this[key]) {
            null -> default
            is JsonPrimitive -> element.intOrNull
            else -> null
        }

    private fun JsonObject.optionalNullableInt(key: String): OptionalValue<Int>? =
        when (val element = this[key]) {
            null, kotlinx.serialization.json.JsonNull -> OptionalValue(null)
            is JsonPrimitive -> element.intOrNull?.let(::OptionalValue)
            else -> null
        }

    private fun JsonObject.optionalLong(key: String, default: Long): Long? =
        when (val element = this[key]) {
            null -> default
            is JsonPrimitive -> element.contentOrNull?.toLongOrNull()
            else -> null
        }

    private fun decodeTrigger(triggerObj: JsonObject, json: Json = strict): DecodeOutcome {
        val type = (triggerObj["type"] as? JsonPrimitive)?.takeIf { it.isString }?.contentOrNull
            ?: return DecodeErr(ParseResult.Err("missing_trigger_type", "trigger.type is required"))
        // Accept either nested {type, params:{...}} OR flat {type, ...keys}. The nested
        // form is canonical and what encodeTrigger emits, but most LLMs default to the
        // flat shape (it's the natural JSON for a single object). Without this leniency
        // any trigger with required params (time_cron's time_of_day, geofence_*'s lat/lng,
        // battery_*'s threshold_percent) gets rejected with a confusing serializer error.
        val paramsElement = triggerObj["params"]
        if (paramsElement != null && paramsElement !is JsonObject) {
            return DecodeErr(ParseResult.Err("bad_trigger_params", "trigger.params must be an object"))
        }
        val nestedParams = paramsElement as? JsonObject
        if (nestedParams != null && (triggerObj.keys - setOf("type", "params")).isNotEmpty()) {
            return DecodeErr(ParseResult.Err("unknown_trigger_key", "trigger contains unknown key(s)"))
        }
        val flat = buildJsonObject {
            put("type", JsonPrimitive(type))
            if (nestedParams != null) {
                for ((k, v) in nestedParams) put(k, v)
            } else {
                // Flat form: every key other than "type" is a param.
                for ((k, v) in triggerObj) if (k != "type") put(k, v)
            }
        }
        return runCatching {
            DecodeOk(json.decodeFromJsonElement(TriggerSpec.serializer(), flat))
        }.getOrElse {
            DecodeErr(ParseResult.Err("unknown_trigger_type",
                "trigger.type='$type' not recognised or params malformed: ${it.message}"))
        }
    }

    private fun encodeTrigger(t: TriggerSpec): JsonObject {
        val flat = strict.encodeToJsonElement(TriggerSpec.serializer(), t).jsonObject
        val type = flat["type"]?.jsonPrimitive?.contentOrNull ?: "unknown"
        val params = buildJsonObject { for ((k, v) in flat) if (k != "type") put(k, v) }
        return buildJsonObject {
            put("type", JsonPrimitive(type))
            put("params", params)
        }
    }

    private fun decodeCondition(condObj: JsonObject, json: Json = strict): DecodeOutcome {
        val type = (condObj["type"] as? JsonPrimitive)?.takeIf { it.isString }?.contentOrNull
            ?: return DecodeErr(ParseResult.Err("missing_condition_type", "condition.type is required"))
        // Accept either nested {type, params:{...}} OR flat {type, ...keys}. See the
        // matching comment in decodeTrigger for the rationale.
        val paramsElement = condObj["params"]
        if (paramsElement != null && paramsElement !is JsonObject) {
            return DecodeErr(ParseResult.Err("bad_condition_params", "condition.params must be an object"))
        }
        val nestedParams = paramsElement as? JsonObject
        if (nestedParams != null && (condObj.keys - setOf("type", "params")).isNotEmpty()) {
            return DecodeErr(ParseResult.Err("unknown_condition_key", "condition contains unknown key(s)"))
        }
        val flat = buildJsonObject {
            put("type", JsonPrimitive(type))
            if (nestedParams != null) {
                for ((k, v) in nestedParams) put(k, v)
            } else {
                for ((k, v) in condObj) if (k != "type") put(k, v)
            }
        }
        return runCatching {
            DecodeOk(json.decodeFromJsonElement(ConditionSpec.serializer(), flat))
        }.getOrElse {
            DecodeErr(ParseResult.Err("unknown_condition_type",
                "condition.type='$type' not recognised or params malformed: ${it.message}"))
        }
    }

    private fun encodeCondition(c: ConditionSpec): JsonObject {
        val flat = strict.encodeToJsonElement(ConditionSpec.serializer(), c).jsonObject
        val type = flat["type"]?.jsonPrimitive?.contentOrNull ?: "unknown"
        val params = buildJsonObject { for ((k, v) in flat) if (k != "type") put(k, v) }
        return buildJsonObject {
            put("type", JsonPrimitive(type))
            put("params", params)
        }
    }

    private fun sanityCheckTrigger(t: TriggerSpec): ParseResult.Err? = when (t) {
        is TriggerSpec.TimeCron -> when {
            t.cron.isNullOrBlank() && t.timeOfDay.isNullOrBlank() ->
                ParseResult.Err("invalid_trigger", "time_cron requires either cron or time_of_day")
            !t.cron.isNullOrBlank() && !t.timeOfDay.isNullOrBlank() ->
                ParseResult.Err("invalid_trigger", "time_cron: cron and time_of_day are mutually exclusive")
            !t.timeOfDay.isNullOrBlank() && !validHHmm(t.timeOfDay) ->
                ParseResult.Err("invalid_trigger", "time_cron.time_of_day must be HH:mm 24h")
            t.daysOfWeek.any { it !in 1..7 } ->
                ParseResult.Err("invalid_trigger", "time_cron.days_of_week values must be 1..7 (ISO, 1=Mon)")
            // Reject unparseable cron up front so the LLM gets a repair signal at create
            // time instead of a workflow that silently fires hourly. Valid means either
            // the trigger family's own subset (@hourly/@daily/@weekly/@every Nx) or a
            // 5-field expression the shared scheduled-jobs parser accepts.
            !t.cron.isNullOrBlank()
                && me.rerere.rikkahub.workflow.trigger.TimeCronTriggerFamily.derivePeriodMs(t) == null
                && me.rerere.rikkahub.service.CronExpressionParser.parse(t.cron.trim()).isFailure ->
                ParseResult.Err("invalid_trigger",
                    "time_cron.cron is not a valid cron expression (5-field UNIX dialect, @hourly/@daily/@weekly, or @every Ns/Nm/Nh)")
            else -> null
        }
        is TriggerSpec.BatteryBelow -> if (t.thresholdPercent !in 1..100)
            ParseResult.Err("invalid_trigger", "battery_below.threshold_percent must be 1..100") else null
        is TriggerSpec.BatteryAbove -> if (t.thresholdPercent !in 1..100)
            ParseResult.Err("invalid_trigger", "battery_above.threshold_percent must be 1..100") else null
        is TriggerSpec.GeofenceEnter -> validateGeofence(t.lat, t.lng, t.radiusM)
        is TriggerSpec.GeofenceExit -> validateGeofence(t.lat, t.lng, t.radiusM)
        is TriggerSpec.AppLaunched -> if (t.packageName.isBlank())
            ParseResult.Err("invalid_trigger", "app_launched.package_name must be non-blank") else null
        is TriggerSpec.AppClosed -> if (t.packageName.isBlank())
            ParseResult.Err("invalid_trigger", "app_closed.package_name must be non-blank") else null
        is TriggerSpec.NotificationReceived -> when {
            // At least one filter 鈥?otherwise the workflow fires on every notification.
            t.packageName.isNullOrBlank() && t.titleContains.isNullOrBlank()
                && t.textContains.isNullOrBlank() && t.titleMatches.isNullOrBlank()
                && t.textMatches.isNullOrBlank() ->
                ParseResult.Err("invalid_trigger",
                    "notification_received requires at least one filter (package_name, title_contains, text_contains, title_matches, or text_matches)")
            // Reject uncompilable regex up front so the LLM gets a clear repair signal
            // instead of a workflow that silently never matches.
            !t.titleMatches.isNullOrBlank() && !isValidRegex(t.titleMatches) ->
                ParseResult.Err("invalid_trigger", "notification_received.title_matches is not a valid regex")
            !t.textMatches.isNullOrBlank() && !isValidRegex(t.textMatches) ->
                ParseResult.Err("invalid_trigger", "notification_received.text_matches is not a valid regex")
            else -> null
        }
        else -> null
    }

    private fun isValidRegex(pattern: String): Boolean =
        runCatching { java.util.regex.Pattern.compile(pattern) }.isSuccess

    private fun validateGeofence(lat: Double, lng: Double, radiusM: Int): ParseResult.Err? {
        if (lat !in -90.0..90.0) return ParseResult.Err("invalid_trigger", "geofence.lat must be -90..90")
        if (lng !in -180.0..180.0) return ParseResult.Err("invalid_trigger", "geofence.lng must be -180..180")
        if (radiusM !in WorkflowConstants.MIN_GEOFENCE_RADIUS_M..WorkflowConstants.MAX_GEOFENCE_RADIUS_M) {
            return ParseResult.Err("invalid_trigger",
                "geofence.radius_m must be ${WorkflowConstants.MIN_GEOFENCE_RADIUS_M}..${WorkflowConstants.MAX_GEOFENCE_RADIUS_M}")
        }
        return null
    }

    private fun sanityCheckCondition(c: ConditionSpec): ParseResult.Err? = when (c) {
        is ConditionSpec.TimeBetween -> when {
            !validHHmm(c.start) -> ParseResult.Err("invalid_condition", "time_between.start must be HH:mm 24h")
            !validHHmm(c.end) -> ParseResult.Err("invalid_condition", "time_between.end must be HH:mm 24h")
            else -> null
        }
        is ConditionSpec.TimeAfterSunset -> if (c.offsetMinutes !in -720..720)
            ParseResult.Err("invalid_condition", "time_after_sunset.offset_minutes must be -720..720") else null
        is ConditionSpec.TimeBeforeSunrise -> if (c.offsetMinutes !in -720..720)
            ParseResult.Err("invalid_condition", "time_before_sunrise.offset_minutes must be -720..720") else null
        is ConditionSpec.DayOfWeekIn -> if (c.days.any { it !in 1..7 })
            ParseResult.Err("invalid_condition", "day_of_week_in.days values must be 1..7 (ISO, 1=Mon)") else null
        is ConditionSpec.WifiSsidIs -> if (c.ssid.isBlank())
            ParseResult.Err("invalid_condition", "wifi_ssid_is.ssid must be non-blank") else null
        is ConditionSpec.WifiSsidIn -> if (c.ssids.isEmpty() || c.ssids.any { it.isBlank() })
            ParseResult.Err("invalid_condition", "wifi_ssid_in.ssids must be non-empty and non-blank") else null
        is ConditionSpec.BatteryAbove -> if (c.percent !in 1..100)
            ParseResult.Err("invalid_condition", "battery_above.percent must be 1..100") else null
        is ConditionSpec.BatteryBelow -> if (c.percent !in 1..100)
            ParseResult.Err("invalid_condition", "battery_below.percent must be 1..100") else null
        is ConditionSpec.ForegroundAppIs -> if (c.packageName.isBlank())
            ParseResult.Err("invalid_condition", "foreground_app_is.package_name must be non-blank") else null
        is ConditionSpec.ForegroundAppIn -> if (c.packageNames.isEmpty() || c.packageNames.any { it.isBlank() })
            ParseResult.Err("invalid_condition", "foreground_app_in.package_names must be non-empty and non-blank") else null
        else -> null
    }

    private fun validHHmm(s: String): Boolean {
        val parts = s.split(":")
        if (parts.size != 2) return false
        val h = parts[0].toIntOrNull() ?: return false
        val m = parts[1].toIntOrNull() ?: return false
        return h in 0..23 && m in 0..59
    }
}
