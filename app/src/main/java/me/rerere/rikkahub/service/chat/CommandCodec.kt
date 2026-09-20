package me.rerere.rikkahub.service.chat

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.add
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import me.rerere.rikkahub.memory.MemorySourceVersion
import kotlin.uuid.Uuid

object CommandCodec {
    // Durable command payloads are authority-bearing state. Persist defaults explicitly so fields
    // with dynamic defaults (for example RawUserContent.createdAt) can never be re-sampled while
    // restoring a queued command.
    private val json = Json {
        ignoreUnknownKeys = true
        encodeDefaults = true
    }
    private const val DURABLE_ORIGIN_FIELD = "_commandOrigin"

    fun encodeDurable(command: ChatCommand, origin: CommandOrigin): Pair<String, String> {
        val (type, payload) = encode(command)
        val root = json.parseToJsonElement(payload).jsonObject
        return type to buildJsonObject {
            root.forEach { (key, value) -> put(key, value) }
            put(DURABLE_ORIGIN_FIELD, origin.name)
        }.toString()
    }

    fun decodeDurableOrigin(payload: String): CommandOrigin = runCatching {
        val value = json.parseToJsonElement(payload).jsonObject[DURABLE_ORIGIN_FIELD]
            ?.jsonPrimitive?.content
        value?.let(CommandOrigin::valueOf) ?: CommandOrigin.INTERNAL
    }.getOrDefault(CommandOrigin.INTERNAL)

    fun encode(command: ChatCommand): Pair<String, String> = when (command) {
        is MutateMessageCommand -> "mutate_message" to json.encodeToString(MutateMessageCommand.serializer(), command)
        is PetDialogueCommand -> error("pet_dialogue_command_is_memory_only")
        is SendMessageCommand -> "send_message" to buildJsonObject {
            put("content", json.encodeToString(RawUserContent.serializer(), command.content))
            command.assistantIdSnapshot?.let { put("assistantIdSnapshot", it.toString()) }
            command.modelIdSnapshot?.let { put("modelIdSnapshot", it) }
            command.quickCaptureSessionId?.let { put("quickCaptureSessionId", it.toString()) }
        }.toString()
        is StopCommand -> "stop" to buildJsonObject { put("pauseQueue", command.pauseQueue) }.toString()
        is InterruptCommand -> "interrupt" to buildJsonObject {
            put("content", json.encodeToString(RawUserContent.serializer(), command.replacement.content))
        }.toString()
        is InterruptRegenerateCommand -> "interrupt_regenerate" to buildJsonObject {
            put("targetMessageId", command.regeneration.targetMessageId.toString())
            put("expectedTargetVersion", command.regeneration.expectedTargetVersion)
            put("expectedBranchHeadMessageId", command.regeneration.expectedBranchHeadMessageId.toString())
            put("policy", command.regeneration.policy.name)
            command.regeneration.baselineAssistantScopeId?.trim()?.takeIf(String::isNotEmpty)
                ?.let { put("baselineAssistantScopeId", it) }
            put("baselineSelectedMessageIds", buildJsonArray {
                command.regeneration.baselineSelectedMessageIds.asSequence()
                    .map(String::trim)
                    .filter(String::isNotEmpty)
                    .distinct()
                    .sorted()
                    .forEach { messageId -> add(messageId) }
            })
            put(
                "baselineSelectedSourceVersions",
                encodeSourceVersions(command.regeneration.baselineSelectedSourceVersions),
            )
        }.toString()
        is ToolApprovalCommand -> "tool_approval" to buildJsonObject {
            put("toolCallId", command.toolCallId)
            put("decision", when (val decision = command.decision) {
                ToolDecision.Approved -> buildJsonObject { put("kind", "approved") }
                is ToolDecision.Denied -> buildJsonObject { put("kind", "denied"); put("reason", decision.reason) }
                is ToolDecision.Answered -> buildJsonObject { put("kind", "answered"); put("answer", decision.answer) }
            })
            command.toolName?.let { put("toolName", it) }
            put("scope", command.scope)
            command.approvalId?.let { put("approvalId", it) }
            command.executionId?.let { put("executionId", it) }
            command.expectedStateVersion?.let { put("expectedStateVersion", it) }
            command.resolutionRequestId?.let { put("resolutionRequestId", it) }
        }.toString()
        is SteerCommand -> "steer" to buildJsonObject {
            put("text", command.text)
            put("scope", command.scope.name)
            put("applyPolicy", command.applyPolicy.name)
            put("historyMode", command.historyMode.name)
        }.toString()
        is CancelCurrentToolCommand -> "cancel_current_tool" to buildJsonObject {
            put("toolCallId", command.toolCallId)
        }.toString()
        is RegenerateCommand -> "regenerate" to buildJsonObject {
            put("targetMessageId", command.targetMessageId.toString())
            put("expectedTargetVersion", command.expectedTargetVersion)
            put("expectedBranchHeadMessageId", command.expectedBranchHeadMessageId.toString())
            put("policy", command.policy.name)
            command.baselineAssistantScopeId?.trim()?.takeIf(String::isNotEmpty)
                ?.let { put("baselineAssistantScopeId", it) }
            put("baselineSelectedMessageIds", buildJsonArray {
                command.baselineSelectedMessageIds.asSequence()
                    .map(String::trim)
                    .filter(String::isNotEmpty)
                    .distinct()
                    .sorted()
                    .forEach { messageId -> add(messageId) }
            })
            put(
                "baselineSelectedSourceVersions",
                encodeSourceVersions(command.baselineSelectedSourceVersions),
            )
        }.toString()
        ResumeAfterApprovalCommand -> "resume_after_approval" to "{}"
        is ResumeQueueCommand -> "resume_queue" to buildJsonObject {
            put("startNextImmediately", command.startNextImmediately)
        }.toString()
        is ClearPendingQueueCommand -> "clear_queue" to buildJsonObject { put("reason", command.reason) }.toString()
        is CancelQueuedCommand -> "cancel_queued" to buildJsonObject { put("targetCommandId", command.targetCommandId.toString()) }.toString()
        is CancelSteeringCommand -> "cancel_steering" to buildJsonObject { put("targetCommandId", command.targetCommandId.toString()) }.toString()
        is UpdateQueuedMessageCommand -> "update_queued_message" to buildJsonObject {
            put("targetCommandId", command.targetCommandId.toString())
            put("content", json.encodeToString(RawUserContent.serializer(), command.content))
        }.toString()
        is PromoteQueuedMessageToSteeringCommand -> "promote_queued_to_steering" to buildJsonObject {
            put("targetCommandId", command.targetCommandId.toString())
            put("scope", command.scope.name)
            put("historyMode", command.historyMode.name)
        }.toString()
    }

    fun decode(type: String, payload: String): ChatCommand? = runCatching {
        val root = json.parseToJsonElement(payload).jsonObject
        when (type) {
            "send_message" -> {
                val rawContent = root["content"]?.jsonPrimitive?.content
                    ?: return@runCatching null
                SendMessageCommand(
                    content = json.decodeFromString(RawUserContent.serializer(), rawContent),
                    assistantIdSnapshot = root["assistantIdSnapshot"]?.jsonPrimitive?.content?.let { Uuid.parse(it) },
                    modelIdSnapshot = root["modelIdSnapshot"]?.jsonPrimitive?.content,
                    quickCaptureSessionId = root["quickCaptureSessionId"]?.jsonPrimitive?.content
                        ?.let { Uuid.parse(it) },
                )
            }
            "interrupt" -> InterruptCommand(
                SendMessageCommand(
                    root["content"]?.jsonPrimitive?.content?.let {
                        json.decodeFromString(RawUserContent.serializer(), it)
                    } ?: json.decodeFromString(RawUserContent.serializer(), payload)
                )
            )
            "interrupt_regenerate" -> InterruptRegenerateCommand(
                RegenerateCommand(
                    targetMessageId = Uuid.parse(
                        root["targetMessageId"]?.jsonPrimitive?.content ?: return@runCatching null
                    ),
                    expectedTargetVersion = root["expectedTargetVersion"]?.jsonPrimitive?.content
                        ?.toLongOrNull() ?: return@runCatching null,
                    expectedBranchHeadMessageId = Uuid.parse(
                        root["expectedBranchHeadMessageId"]?.jsonPrimitive?.content
                            ?: return@runCatching null
                    ),
                    policy = RegeneratePolicy.valueOf(
                        root["policy"]?.jsonPrimitive?.content ?: RegeneratePolicy.INTERRUPT_CURRENT.name
                    ),
                    baselineAssistantScopeId = root["baselineAssistantScopeId"]?.jsonPrimitive
                        ?.content?.trim()?.takeIf(String::isNotEmpty),
                    baselineSelectedMessageIds = root["baselineSelectedMessageIds"]?.jsonArray
                        ?.mapNotNull { element ->
                            element.jsonPrimitive.content.trim().takeIf(String::isNotEmpty)
                        }
                        ?.distinct()
                        .orEmpty(),
                    baselineSelectedSourceVersions = decodeSourceVersions(
                        root["baselineSelectedSourceVersions"]?.jsonArray,
                    ),
                )
            )
            "mutate_message" -> json.decodeFromString(MutateMessageCommand.serializer(), payload)
            "stop" -> StopCommand(root["pauseQueue"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() ?: true)
            "tool_approval" -> {
                val decision = root["decision"]?.jsonObject ?: return@runCatching null
                val decodedDecision = when (decision["kind"]?.jsonPrimitive?.content) {
                    "approved" -> ToolDecision.Approved
                    "denied" -> ToolDecision.Denied(decision["reason"]?.jsonPrimitive?.content.orEmpty())
                    "answered" -> ToolDecision.Answered(decision["answer"]?.jsonPrimitive?.content.orEmpty())
                    else -> return@runCatching null
                }
                ToolApprovalCommand(
                    toolCallId = root["toolCallId"]?.jsonPrimitive?.content ?: return@runCatching null,
                    decision = decodedDecision,
                    toolName = root["toolName"]?.jsonPrimitive?.content,
                    scope = root["scope"]?.jsonPrimitive?.content ?: "Once",
                    approvalId = root["approvalId"]?.jsonPrimitive?.content,
                    executionId = root["executionId"]?.jsonPrimitive?.content,
                    expectedStateVersion = root["expectedStateVersion"]?.jsonPrimitive?.content
                        ?.toLongOrNull(),
                    resolutionRequestId = root["resolutionRequestId"]?.jsonPrimitive?.content,
                )
            }
            "steer" -> SteerCommand(
                text = root["text"]?.jsonPrimitive?.content ?: return@runCatching null,
                scope = SteeringScope.valueOf(root["scope"]?.jsonPrimitive?.content ?: SteeringScope.REMAINDER_OF_RUN.name),
                applyPolicy = SteeringApplyPolicy.valueOf(root["applyPolicy"]?.jsonPrimitive?.content ?: SteeringApplyPolicy.AFTER_CHECKPOINT.name),
                historyMode = SteeringHistoryMode.valueOf(
                    root["historyMode"]?.jsonPrimitive?.content ?: SteeringHistoryMode.TRANSIENT.name
                ),
            )
            "cancel_current_tool" -> CancelCurrentToolCommand(
                toolCallId = root["toolCallId"]?.jsonPrimitive?.content ?: return@runCatching null,
            )
            "regenerate" -> RegenerateCommand(
                targetMessageId = Uuid.parse(root["targetMessageId"]?.jsonPrimitive?.content ?: return@runCatching null),
                expectedTargetVersion = root["expectedTargetVersion"]?.jsonPrimitive?.content?.toLongOrNull() ?: return@runCatching null,
                expectedBranchHeadMessageId = Uuid.parse(root["expectedBranchHeadMessageId"]?.jsonPrimitive?.content ?: return@runCatching null),
                policy = RegeneratePolicy.valueOf(root["policy"]?.jsonPrimitive?.content ?: RegeneratePolicy.INTERRUPT_CURRENT.name),
                baselineAssistantScopeId = root["baselineAssistantScopeId"]?.jsonPrimitive
                    ?.content?.trim()?.takeIf(String::isNotEmpty),
                baselineSelectedMessageIds = root["baselineSelectedMessageIds"]?.jsonArray
                    ?.mapNotNull { element ->
                        element.jsonPrimitive.content.trim().takeIf(String::isNotEmpty)
                    }
                    ?.distinct()
                    .orEmpty(),
                baselineSelectedSourceVersions = decodeSourceVersions(
                    root["baselineSelectedSourceVersions"]?.jsonArray,
                ),
            )
            "resume_after_approval" -> ResumeAfterApprovalCommand
            "resume_queue" -> ResumeQueueCommand(root["startNextImmediately"]?.jsonPrimitive?.content?.toBooleanStrictOrNull() ?: true)
            "clear_queue" -> ClearPendingQueueCommand(root["reason"]?.jsonPrimitive?.content ?: "Cleared by user")
            "cancel_queued" -> CancelQueuedCommand(Uuid.parse(root["targetCommandId"]?.jsonPrimitive?.content ?: return@runCatching null))
            "cancel_steering" -> CancelSteeringCommand(Uuid.parse(root["targetCommandId"]?.jsonPrimitive?.content ?: return@runCatching null))
            "update_queued_message" -> UpdateQueuedMessageCommand(
                targetCommandId = Uuid.parse(root["targetCommandId"]?.jsonPrimitive?.content ?: return@runCatching null),
                content = root["content"]?.jsonPrimitive?.content?.let {
                    json.decodeFromString(RawUserContent.serializer(), it)
                } ?: return@runCatching null,
            )
            "promote_queued_to_steering" -> PromoteQueuedMessageToSteeringCommand(
                targetCommandId = Uuid.parse(root["targetCommandId"]?.jsonPrimitive?.content ?: return@runCatching null),
                scope = SteeringScope.valueOf(
                    root["scope"]?.jsonPrimitive?.content ?: SteeringScope.REMAINDER_OF_RUN.name
                ),
                historyMode = SteeringHistoryMode.valueOf(
                    root["historyMode"]?.jsonPrimitive?.content ?: SteeringHistoryMode.TRANSIENT.name
                ),
            )
            else -> null
        }
    }.getOrNull()

    private fun encodeSourceVersions(
        versions: Collection<MemorySourceVersion>,
    ) = buildJsonArray {
        normalizeSourceVersions(versions).forEach { version ->
            add(buildJsonObject {
                put("messageId", version.messageId)
                put("consumedTextDigest", version.consumedTextDigest)
            })
        }
    }

    private fun decodeSourceVersions(
        elements: kotlinx.serialization.json.JsonArray?,
    ): List<MemorySourceVersion> = normalizeSourceVersions(
        elements.orEmpty().mapNotNull { element ->
            runCatching {
                val source = element.jsonObject
                MemorySourceVersion(
                    messageId = source["messageId"]?.jsonPrimitive?.content.orEmpty(),
                    consumedTextDigest = source["consumedTextDigest"]
                        ?.jsonPrimitive?.content.orEmpty(),
                )
            }.getOrNull()
        },
    )

    private fun normalizeSourceVersions(
        versions: Collection<MemorySourceVersion>,
    ): List<MemorySourceVersion> = versions.asSequence()
        .map { version ->
            MemorySourceVersion(
                messageId = version.messageId.trim(),
                consumedTextDigest = version.consumedTextDigest.trim().lowercase(),
            )
        }
        .filter { version ->
            version.messageId.isNotEmpty() &&
                version.consumedTextDigest.matches(SHA_256_HEX)
        }
        .distinct()
        .sortedWith(compareBy(MemorySourceVersion::messageId, MemorySourceVersion::consumedTextDigest))
        .toList()

    private val SHA_256_HEX = Regex("[0-9a-f]{64}")
}
