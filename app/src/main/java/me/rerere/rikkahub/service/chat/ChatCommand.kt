package me.rerere.rikkahub.service.chat

import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.TimeZone
import kotlinx.datetime.toLocalDateTime
import kotlinx.serialization.Serializable
import me.rerere.ai.core.MessageRole
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessageAnnotation
import me.rerere.ai.ui.UIMessagePart
import me.rerere.rikkahub.memory.MemorySourceVersion
import kotlin.time.Clock
import kotlin.uuid.Uuid

sealed interface ChatCommand
sealed interface EmergencyCommand : ChatCommand
sealed interface ControlCommand : ChatCommand
sealed interface NormalCommand : ChatCommand

data class CommandDependency(
    val commandId: Uuid,
    val requiredOutcome: RequiredOutcome = RequiredOutcome.COMPLETED,
)

enum class RequiredOutcome { COMPLETED, NOT_FAILED }

enum class CommandOrigin {
    APP_UI,
    SYSTEM_ASSISTANT,
    SYSTEM_ASSISTANT_KEYGUARD,
    QUICK_CAPTURE,
    TELEGRAM,
    WEB_API,
    CRON,
    PET_INTERACTION,
    PET_HANDOFF_CONFIRMED,
    PET_HANDOFF_AUTO,
    INTERNAL,
}

/** In-memory only: never encoded into the durable ordinary-chat command queue. */
data class PetDialogueCommand(
    val assistantId: Uuid,
    val privilegedConversationId: Uuid,
    val input: String,
) : ChatCommand

@Serializable
data class RawUserContent(
    val parts: List<UIMessagePart>,
    val answer: Boolean = true,
    val annotations: List<UIMessageAnnotation> = emptyList(),
    /**
     * Frozen once at submission and persisted with the durable command. Admission and execution
     * must build the branch anchor with this same value; sampling the clock twice changes the
     * authority payload digest even when the user's content is byte-for-byte identical.
     */
    val createdAt: LocalDateTime = Clock.System.now()
        .toLocalDateTime(TimeZone.currentSystemDefault()),
)

internal fun RawUserContent.toAnchoredUserMessage(
    messageId: Uuid,
    effectiveParts: List<UIMessagePart> = parts,
): UIMessage = UIMessage(
    id = messageId,
    role = MessageRole.USER,
    parts = effectiveParts,
    annotations = annotations,
    createdAt = createdAt,
)

data class StopCommand(
    val pauseQueue: Boolean = true,
) : EmergencyCommand

data class InterruptCommand(
    val replacement: SendMessageCommand,
) : EmergencyCommand

data class InterruptRegenerateCommand(
    val regeneration: RegenerateCommand,
) : EmergencyCommand

data class ToolApprovalCommand(
    val toolCallId: String,
    val decision: ToolDecision,
    val toolName: String? = null,
    val scope: String = "Once",
    /** Exact immutable approval projection identity. Null only for legacy denial payloads. */
    val approvalId: String? = null,
    /** Exact execution ledger identity bound to [approvalId]. */
    val executionId: String? = null,
    val expectedStateVersion: Long? = null,
    val resolutionRequestId: String? = null,
) : ControlCommand

data class SteerCommand(
    val text: String,
    val scope: SteeringScope = SteeringScope.REMAINDER_OF_RUN,
    val applyPolicy: SteeringApplyPolicy = SteeringApplyPolicy.AFTER_CHECKPOINT,
    val historyMode: SteeringHistoryMode = SteeringHistoryMode.TRANSIENT,
) : ControlCommand

data class CancelCurrentToolCommand(
    val toolCallId: String,
) : ControlCommand

data class SendMessageCommand(
    val content: RawUserContent,
    val assistantIdSnapshot: Uuid? = null,
    val modelIdSnapshot: String? = null,
    /** Present only for an already-authorized in-process QuickCapture submission. */
    val quickCaptureSessionId: Uuid? = null,
) : NormalCommand

data class RegenerateCommand(
    val targetMessageId: Uuid,
    val expectedTargetVersion: Long,
    val expectedBranchHeadMessageId: Uuid,
    val policy: RegeneratePolicy = RegeneratePolicy.INTERRUPT_CURRENT,
    /**
     * Durable source baseline captured when the command is admitted. IDs keep old pending rows
     * decodable; versions let the successful final commit distinguish an edit from a deletion.
     */
    val baselineAssistantScopeId: String? = null,
    val baselineSelectedMessageIds: List<String> = emptyList(),
    val baselineSelectedSourceVersions: List<MemorySourceVersion> = emptyList(),
) : NormalCommand

data class ResumeQueueCommand(
    val startNextImmediately: Boolean = true,
) : NormalCommand

/** Internal continuation after the final pending tool approval is committed. */
data object ResumeAfterApprovalCommand : ControlCommand

data class ClearPendingQueueCommand(
    val reason: String = "Cleared by user",
) : NormalCommand

data class CancelQueuedCommand(
    val targetCommandId: Uuid,
) : NormalCommand

/** Permanently removes one guidance item from the active run or its queue fallback. */
data class CancelSteeringCommand(
    val targetCommandId: Uuid,
) : NormalCommand

data class UpdateQueuedMessageCommand(
    val targetCommandId: Uuid,
    val content: RawUserContent,
) : NormalCommand

data class PromoteQueuedMessageToSteeringCommand(
    val targetCommandId: Uuid,
    val scope: SteeringScope = SteeringScope.REMAINDER_OF_RUN,
    val historyMode: SteeringHistoryMode = SteeringHistoryMode.TRANSIENT,
) : NormalCommand

enum class RegeneratePolicy {
    INTERRUPT_CURRENT,
    REJECT_IF_BUSY,
    QUEUE_WITH_BRANCH_CHECK,
}

enum class SteeringScope { NEXT_MODEL_CALL, REMAINDER_OF_RUN }
enum class SteeringApplyPolicy { AFTER_CHECKPOINT, CANCEL_CURRENT_TOOL }
enum class SteeringHistoryMode { TRANSIENT, PERSISTENT }

/** UI-facing snapshot of guidance attached to the current active run. */
data class SteeringUiEntry(
    val commandId: Uuid,
    val runId: Uuid,
    val text: String,
    val state: me.rerere.rikkahub.data.ai.SteeringState,
    val historyMode: SteeringHistoryMode,
    val editable: Boolean,
)

sealed interface ToolDecision {
    data object Approved : ToolDecision
    data class Denied(val reason: String) : ToolDecision
    data class Answered(val answer: String) : ToolDecision
}

/** Identity-only UI graph intent, serialized with generation by ConversationRuntime. */
@Serializable
data class MutateMessageCommand(
    val nodeId: Uuid,
    val messageId: Uuid,
    val replacementParts: List<UIMessagePart>? = null,
    val newMessageId: Uuid = Uuid.random(),
) : NormalCommand
