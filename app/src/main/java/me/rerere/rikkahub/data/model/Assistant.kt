package me.rerere.rikkahub.data.model

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import me.rerere.ai.core.MessageRole
import me.rerere.ai.provider.CustomBody
import me.rerere.ai.provider.CustomHeader
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.core.ReasoningLevel
import me.rerere.rikkahub.data.ai.tools.LocalToolOption
import me.rerere.rikkahub.memory.MemoryAutoSaveMode
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryCaptureOrigin
import me.rerere.rikkahub.memory.MemoryKind
import kotlin.uuid.Uuid

@Serializable
data class Assistant(
    val id: Uuid = Uuid.random(),
    val chatModelId: Uuid? = null, // 如果为null, 使用全局默认模型
    val name: String = "",
    val avatar: Avatar = Avatar.Dummy,
    val useAssistantAvatar: Boolean = false, // 使用助手头像替代模型头像
    val tags: List<Uuid> = emptyList(),
    val systemPrompt: String = "",
    val temperature: Float? = null,
    val topP: Float? = null,
    val contextMessageSize: Int = 0,
    val streamOutput: Boolean = true,
    val enableWebSearch: Boolean = false,
    val enableMemory: Boolean = false,
    val memoryAutoSaveMode: MemoryAutoSaveMode = MemoryAutoSaveMode.OFF,
    val memoryCaptureOrigins: Set<MemoryCaptureOrigin> = setOf(
        MemoryCaptureOrigin.APP_UI,
        MemoryCaptureOrigin.SYSTEM_ASSISTANT,
    ),
    val memoryIdleDelayMinutes: Int = 10,
    val memoryImmediateCaptureThreshold: Int = 5,
    /** Number of consecutive completed turns that one extraction may combine. */
    val memoryConversationContextTurns: Int = 12,
    val memoryNarrativeEventsEnabled: Boolean = false,
    val memoryInsightsTheoriesEnabled: Boolean = false,
    /** Readable names used for the person and conversation partner in narrative memories. */
    val memoryNarrativeUserName: String = "",
    val memoryNarrativeCompanionName: String = "",
    val useGlobalMemory: Boolean = false, // 使用全局共享记忆而非助手隔离记忆
    val enableRecentChatsReference: Boolean = false,
    val autoContextEnabled: Boolean = false,
    val autoContextForegroundWindow: Boolean = true,
    val autoContextUiTree: Boolean = true,
    val autoContextDeviceStatus: Boolean = true,
    val autoContextOcrFallback: Boolean = false,
    val autoContextUsageStats: Boolean = false,
    val autoContextNotifications: Boolean = false,
    val autoContextMaxChars: Int = 6000,
    val enabledPluginIds: Set<String> = emptySet(),
    val messageTemplate: String = "{{ message }}",
    val presetMessages: List<UIMessage> = emptyList(),
    val quickMessageIds: Set<Uuid> = emptySet(),
    val regexes: List<AssistantRegex> = emptyList(),
    val reasoningLevel: ReasoningLevel = ReasoningLevel.AUTO,
    val maxTokens: Int? = null,
    /**
     * Maximum model/tool-loop steps for one interactive turn. Null keeps the role-aware default:
     * 64 for the active local second user and 32 for ordinary assistants.
     */
    val generationMaxSteps: Int? = null,
    /**
     * Optional wall-clock budget for one interactive model/tool turn. Null keeps the
     * role-aware policy: the active local second user gets a 60 minute budget while ordinary
     * assistants continue using the global runtime budget.
     */
    val generationTurnBudgetMinutes: Int? = null,
    val customHeaders: List<CustomHeader> = emptyList(),
    val customBodies: List<CustomBody> = emptyList(),
    val mcpServers: Set<Uuid> = emptySet(),
    val localTools: List<LocalToolOption> = listOf(LocalToolOption.TimeInfo),
    val workspaceId: Uuid? = null,
    val background: String? = null, // 聊天页背景图地址(本地文件 URI 或网络 URL), 为 null 时无背景
    val backgroundOpacity: Float = 1.0f, // 背景图不透明度(0~1)
    val useGradientBackground: Boolean = false, // 开启后聊天页使用动态渐变背景
    val modeInjectionIds: Set<Uuid> = emptySet(),      // 关联的模式注入 ID
    val lorebookIds: Set<Uuid> = emptySet(),            // 关联的 Lorebook ID
    val enabledSkills: Set<String> = emptySet(),        // 启用的 skill 名称列表
    val enableTimeReminder: Boolean = false,            // 为用户消息附加稳定的发送时间
    // Phase 11 — Sub-agents settings. Defaults to "inherit from main" (null model id +
    // empty system prompt → built-in focused-sub-agent prompt). Each assistant has its
    // own concurrency cap; we hard-cap globally at 16 across all assistants in the engine.
    val subAgentModelId: Uuid? = null,
    val subAgentSystemPrompt: String = "",
    val maxConcurrentSubAgents: Int = 3,
    // Phase 15 — Per-task token budget. Both null = no budget enforcement. The LLM
    // checks via `check_token_usage`; auto-stop integration into GenerationHandler is
    // Phase 15.5 follow-up.
    val tokenBudgetSoftCap: Int? = null,
    val tokenBudgetHardCap: Int? = null,
    // Phase 16 — Fast-path router. Off by default per spec. When ON, ChatService runs
    // FastPathRouter.route() on the user's message before firing the LLM; matched intents
    // execute the matching tool directly and skip the LLM. Conservative matching — falls
    // through to the LLM whenever in doubt. Per-tool HARDLINE / approval still apply at
    // the dispatch level; v1 only matches read-only tools so approval is a non-issue.
    val fastPathRouterEnabled: Boolean = false,
    val allowConversationSystemPrompt: Boolean = false, // 允许对话单独重写 system prompt
    val allowConversationPromptInjection: Boolean = false, // 允许对话单独绑定提示词注入
    // P2: 不受限模式 — 开启后 ToolExecutionGate 只检查紧急停止，跳过所有其他安全门
    // Legacy migration marker. It remains readable for existing exports, but it no longer
    // grants runtime bypasses. A user must confirm the selected local second-user session.
    val unrestricted: Boolean = false,
    // A user-selected conversation that runs as the high-autonomy "second user". Keeping
    // this on Assistant (DataStore JSON) avoids a Room migration and keeps old settings
    // readable because both fields have defaults.
    val privilegedConversationId: Uuid? = null,
    val privilegedIdentityName: String = "第二用户",
    /** Local UI confirmation for the selected second-user conversation. Reset on reassignment. */
    val secondUserPolicyConfirmed: Boolean = false,
    /** Explicit opt-in for temporary read-only access to other local conversation histories. */
    val allowConversationHistoryRead: Boolean = false,
    /** Optional P1 pet sidecar settings. Defaults keep existing assistants and exports unchanged. */
    val petEnabled: Boolean = false,
    val petPackageId: String? = null,
    val petSupplement: String? = null,
    val petHandoffMode: String = "CONFIRM",
    val petBootRestoreEnabled: Boolean = false,
    val petHeadBoundary: Float = 0.34f,
    val petBodyBoundary: Float = 0.76f,
    /** Visual scale relative to the native 192x208dp Codex Pet frame. */
    val petScale: Float = 1.0f,
    /** Atlas playback rate. A calm default avoids rapidly looping short idle animations. */
    val petAnimationFps: Int = 6,
    /** Optional local idle variety; global selection remains disabled by default. */
    val petIdlePoolEnabled: Boolean = false,
)

@Serializable
data class QuickMessage(
    val id: Uuid = Uuid.random(),
    val title: String = "",
    val content: String = "",
)

@Serializable
data class AssistantMemory(
    val id: Int,
    val content: String = "",
    @Transient
    val title: String? = null,
    @Transient
    val kind: MemoryKind = MemoryKind.OTHER,
    @Transient
    val approvalSource: MemoryApprovalSource = MemoryApprovalSource.LEGACY,
    /** Runtime authorization identity for UI mutations; never serialized into prompts/tools. */
    @Transient
    val scopeId: String? = null,
    /** Runtime CAS value paired with [scopeId]. */
    @Transient
    val revision: Int? = null,
)

@Serializable
enum class AssistantAffectScope {
    USER,
    ASSISTANT,
}

@Serializable
data class AssistantRegex(
    val id: Uuid,
    val name: String = "",
    val enabled: Boolean = true,
    val findRegex: String = "", // 正则表达式
    val replaceString: String = "", // 替换字符串
    val affectingScope: Set<AssistantAffectScope> = setOf(),
    val visualOnly: Boolean = false, // 是否仅在视觉上影响
)

fun String.replaceRegexes(
    assistant: Assistant?,
    scope: AssistantAffectScope,
    visual: Boolean = false
): String {
    if (assistant == null) return this
    if (assistant.regexes.isEmpty()) return this
    return assistant.regexes.fold(this) { acc, regex ->
        if (regex.enabled && regex.visualOnly == visual && regex.affectingScope.contains(scope)) {
            val compiled = compiledAssistantRegex(regex.findRegex).getOrNull()
                ?: return@fold acc
            runCatching {
                acc.replace(
                    regex = compiled,
                    replacement = regex.replaceString,
                )
            }.getOrElse {
                // 如果正则表达式格式错误，返回原字符串
                acc
            }
        } else {
            acc
        }
    }
}

/**
 * 注入位置
 */
@Serializable
enum class InjectionPosition {
    @SerialName("before_system_prompt")
    BEFORE_SYSTEM_PROMPT,   // 系统提示词之前

    @SerialName("after_system_prompt")
    AFTER_SYSTEM_PROMPT,    // 系统提示词之后（最常用）

    @SerialName("top_of_chat")
    TOP_OF_CHAT,            // 对话最开头（第一条用户消息之前）

    @SerialName("bottom_of_chat")
    BOTTOM_OF_CHAT,         // 最新消息之前（当前用户输入之前）

    @SerialName("at_depth")
    AT_DEPTH,               // 在指定深度位置插入（从最新消息往前数）
}

/**
 * 提示词注入
 *
 * - ModeInjection: 基于模式开关的注入（如学习模式）
 * - RegexInjection: 基于正则匹配的注入（Lorebook）
 */
@Serializable
sealed class PromptInjection {
    abstract val id: Uuid
    abstract val name: String
    abstract val enabled: Boolean
    abstract val priority: Int
    abstract val position: InjectionPosition
    abstract val content: String
    abstract val injectDepth: Int  // 当 position 为 AT_DEPTH 时使用，表示从最新消息往前数的位置
    abstract val role: MessageRole  // 注入角色：USER 或 ASSISTANT

    /**
     * 模式注入 - 基于开关状态触发
     */
    @Serializable
    @SerialName("mode")
    data class ModeInjection(
        override val id: Uuid = Uuid.random(),
        override val name: String = "",
        override val enabled: Boolean = true,
        override val priority: Int = 0,
        override val position: InjectionPosition = InjectionPosition.AFTER_SYSTEM_PROMPT,
        override val content: String = "",
        override val injectDepth: Int = 4,
        override val role: MessageRole = MessageRole.USER,
    ) : PromptInjection()

    /**
     * 正则注入 - 基于内容匹配触发（世界书）
     */
    @Serializable
    @SerialName("regex")
    data class RegexInjection(
        override val id: Uuid = Uuid.random(),
        override val name: String = "",
        override val enabled: Boolean = true,
        override val priority: Int = 0,
        override val position: InjectionPosition = InjectionPosition.AFTER_SYSTEM_PROMPT,
        override val content: String = "",
        override val injectDepth: Int = 4,
        override val role: MessageRole = MessageRole.USER,
        val keywords: List<String> = emptyList(),  // 触发关键词
        val useRegex: Boolean = false,             // 是否使用正则匹配
        val caseSensitive: Boolean = false,        // 大小写敏感
        val scanDepth: Int = 4,                    // 扫描最近N条消息
        val constantActive: Boolean = false,       // 常驻激活（无需匹配）
    ) : PromptInjection()
}

/**
 * Lorebook - 组织管理多个 RegexInjection
 */
@Serializable
data class Lorebook(
    val id: Uuid = Uuid.random(),
    val name: String = "",
    val description: String = "",
    val enabled: Boolean = true,
    val entries: List<PromptInjection.RegexInjection> = emptyList(),
)

/**
 * 检查 RegexInjection 是否被触发
 *
 * @param context 要扫描的上下文文本
 * @return 是否触发
 */
fun PromptInjection.RegexInjection.isTriggered(context: String): Boolean {
    if (!enabled) return false
    if (constantActive) return true
    if (keywords.isEmpty()) return false

    return keywords.any { keyword ->
        if (useRegex) {
            try {
                val options = if (caseSensitive) emptySet() else setOf(RegexOption.IGNORE_CASE)
                Regex(keyword, options).containsMatchIn(context)
            } catch (e: Exception) {
                false
            }
        } else {
            if (caseSensitive) {
                context.contains(keyword)
            } else {
                context.contains(keyword, ignoreCase = true)
            }
        }
    }
}

/**
 * 从消息列表中提取用于匹配的上下文文本
 *
 * @param messages 消息列表
 * @param scanDepth 扫描深度（最近N条消息）
 * @return 拼接的文本内容
 */
fun extractContextForMatching(
    messages: List<UIMessage>,
    scanDepth: Int
): String {
    return messages
        .takeLast(scanDepth)
        .joinToString("\n") { it.toText() }
}

/**
 * 获取所有被触发的注入，按优先级排序
 *
 * @param injections 所有注入规则
 * @param context 上下文文本
 * @return 被触发的注入列表，按优先级降序排列
 */
fun getTriggeredInjections(
    injections: List<PromptInjection.RegexInjection>,
    context: String
): List<PromptInjection.RegexInjection> {
    return injections
        .filter { it.isTriggered(context) }
        .sortedByDescending { it.priority }
}
