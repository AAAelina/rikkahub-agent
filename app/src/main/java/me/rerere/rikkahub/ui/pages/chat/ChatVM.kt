package me.rerere.rikkahub.ui.pages.chat

import android.app.Application
import android.content.Context
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.tooling.preview.Preview
import androidx.core.net.toUri
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import me.rerere.rikkahub.data.ai.SteeringState
import me.rerere.rikkahub.service.chat.CommandOrigin
import me.rerere.rikkahub.service.chat.QueueStatus
import me.rerere.rikkahub.service.chat.QueuedMessageUiEntry
import me.rerere.rikkahub.service.chat.RawUserContent
import me.rerere.rikkahub.service.chat.RuntimeState
import me.rerere.rikkahub.service.chat.SteeringHistoryMode
import me.rerere.rikkahub.service.chat.SteeringScope
import me.rerere.rikkahub.service.chat.SteeringUiEntry
import me.rerere.rikkahub.service.chat.SubmitResult
import me.rerere.ai.provider.Model
import me.rerere.ai.ui.UIMessage
import me.rerere.ai.ui.UIMessagePart
import me.rerere.ai.ui.isEmptyInputMessage
import me.rerere.rikkahub.R
import me.rerere.rikkahub.data.authority.reward.RewardFeedbackAuthorityRepository
import me.rerere.rikkahub.data.authority.reward.RewardFeedbackVerdict
import me.rerere.rikkahub.data.authority.reward.RewardFeedbackWriteResult
import me.rerere.rikkahub.assistant.SecondUserAuthorityRegistry
import me.rerere.rikkahub.data.datastore.Settings
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.datastore.getAssistantById
import me.rerere.rikkahub.data.datastore.getChatModelForAssistant
import me.rerere.rikkahub.data.datastore.getCurrentAssistant
import me.rerere.rikkahub.data.files.FilesManager
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.data.model.Avatar
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.model.MessageNode
import me.rerere.rikkahub.data.model.NodeFavoriteTarget
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.data.repository.ConversationDeletionResult
import me.rerere.rikkahub.data.repository.FavoriteRepository
import me.rerere.rikkahub.service.ChatError
import me.rerere.rikkahub.service.ChatService
import me.rerere.rikkahub.ui.hooks.writeStringPreference
import me.rerere.rikkahub.ui.hooks.ChatInputState
import me.rerere.rikkahub.utils.UiState
import me.rerere.rikkahub.utils.UpdateChecker
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong
import kotlin.uuid.Uuid

private const val TAG = "ChatVM"

class ChatVM(
    id: String,
    private val context: Application,
    private val settingsStore: SettingsStore,
    private val conversationRepo: ConversationRepository,
    private val chatService: ChatService,
    val updateChecker: UpdateChecker,
    private val filesManager: FilesManager,
    private val favoriteRepository: FavoriteRepository,
    private val rewardFeedbackAuthorityRepository: RewardFeedbackAuthorityRepository,
) : ViewModel() {
    private val assistantSelectionMutex = Mutex()
    private val assistantSelectionSequence = AtomicLong(0)
    private val _conversationId: Uuid = Uuid.parse(id)
    val conversation: StateFlow<Conversation> = chatService.getConversationFlow(_conversationId)
    var chatListInitialized by mutableStateOf(false) // 聊天列表是否已经滚动到底部

    // 聊天输入状态 - 保存在 ViewModel 中避免 TransactionTooLargeException
    val inputState = ChatInputState()

    // 异步任务 (从ChatService获取，响应式)
    val conversationJob: StateFlow<Job?> =
        chatService
            .getGenerationJobStateFlow(_conversationId)
            .stateIn(viewModelScope, SharingStarted.Eagerly, null)

    val processingStatus: StateFlow<String?> =
        chatService
            .getProcessingStatusFlow(_conversationId)

    val runtimeState: StateFlow<RuntimeState> =
        chatService.getRuntimeStateFlow(_conversationId)
            .stateIn(viewModelScope, SharingStarted.Eagerly, RuntimeState.Hydrating)

    val queueStatus: StateFlow<QueueStatus> =
        chatService.getQueueStatusFlow(_conversationId)
            .stateIn(viewModelScope, SharingStarted.Eagerly, QueueStatus(false, 0, null))

    val queuedMessages: StateFlow<List<QueuedMessageUiEntry>> =
        chatService.getQueuedMessagesFlow(_conversationId)
            .stateIn(viewModelScope, SharingStarted.Eagerly, emptyList())

    val steeringStatus: StateFlow<Map<Uuid, SteeringState>> =
        chatService.getSteeringStatusFlow(_conversationId)
            .stateIn(viewModelScope, SharingStarted.Eagerly, emptyMap())

    val steeringEntries: StateFlow<Map<Uuid, SteeringUiEntry>> =
        chatService.getSteeringEntriesFlow(_conversationId)
            .stateIn(viewModelScope, SharingStarted.Eagerly, emptyMap())

    val conversationJobs = chatService
        .getConversationJobs()
        .stateIn(viewModelScope, SharingStarted.Eagerly, emptyMap())

    init {
        chatService.onConversationVisible(_conversationId)
        // 添加对话引用
        chatService.addConversationReference(_conversationId)

        // 初始化对话
        viewModelScope.launch {
            chatService.initializeConversation(_conversationId)
        }

        // 记住对话ID, 方便下次启动恢复
        context.writeStringPreference("lastConversationId", _conversationId.toString())
    }

    override fun onCleared() {
        super.onCleared()
        // 移除对话引用
        chatService.removeConversationReference(_conversationId)
    }

    // 用户设置
    val settings: StateFlow<Settings> =
        settingsStore.settingsFlow.stateIn(viewModelScope, SharingStarted.Eagerly, Settings.dummy())

    /** UI visibility only; the repository repeats the exact command/source/scope gate in-transaction. */
    val rewardFeedbackAvailable: StateFlow<Boolean> = combine(settings, conversation) {
            current,
            currentConversation,
        ->
        if (current.init) return@combine false
        val preferences = current.learningPreferences.failClosed()
        if (!preferences.capture || !preferences.jobs || !preferences.handoff) return@combine false
        val exactAssistant = current.assistants.singleOrNull {
            it.id == currentConversation.assistantId
        } ?: return@combine false
        if (exactAssistant.learningCaptureEnabled) return@combine true
        val authority = SecondUserAuthorityRegistry.current() ?: return@combine false
        authority.assistantId == exactAssistant.id &&
            exactAssistant.authoritySubjectLearningCaptureEnabled
    }.stateIn(viewModelScope, SharingStarted.Eagerly, false)

    // 网络搜索
    val enableWebSearch = combine(settings, conversation) { settings, conversation ->
        val assistant = settings.getAssistantById(conversation.assistantId)
            ?: settings.getCurrentAssistant()
        assistant.enableWebSearch
    }.stateIn(viewModelScope, SharingStarted.Eagerly, false)

    // 当前模型
    val currentChatModel = combine(settings, conversation) { settings, conversation ->
        settings.getChatModelForAssistant(conversation.assistantId)
    }.stateIn(viewModelScope, SharingStarted.Lazily, null)

    // 错误状态
    val errors: StateFlow<List<ChatError>> = chatService.errors

    fun dismissError(id: Uuid) = chatService.dismissError(id)

    fun clearAllErrors() = chatService.clearAllErrors()

    suspend fun recordHelpfulFeedback(messageId: Uuid): RewardFeedbackWriteResult =
        rewardFeedbackAuthorityRepository.record(
            targetAssistantMessageId = messageId.toString(),
            verdict = RewardFeedbackVerdict.HELPFUL,
        )

    suspend fun recordNotHelpfulFeedback(messageId: Uuid): RewardFeedbackWriteResult =
        rewardFeedbackAuthorityRepository.record(
            targetAssistantMessageId = messageId.toString(),
            verdict = RewardFeedbackVerdict.NOT_HELPFUL,
        )

    // 生成完成
    val generationDoneFlow: SharedFlow<Uuid> = chatService.generationDoneFlow

    // MCP管理器
    val mcpManager = chatService.mcpManager

    // 更新设置
    fun updateSettings(newSettings: Settings) {
        viewModelScope.launch {
            val oldSettings = settings.value
            // 检查用户头像是否有变化，如果有则删除旧头像
            checkUserAvatarDelete(oldSettings, newSettings)
            settingsStore.update(newSettings)
        }
    }

    /**
     * Persist an explicit assistant selection before resolving its destination conversation.
     * A newer picker action supersedes an older in-flight action and is the only one allowed to
     * navigate, so rapid taps cannot open a conversation belonging to the previous assistant.
     */
    suspend fun selectAssistantAndResolveConversation(
        newSettings: Settings,
        createNewConversation: Boolean,
    ): Uuid? {
        val sequence = assistantSelectionSequence.incrementAndGet()
        return assistantSelectionMutex.withLock {
            if (sequence != assistantSelectionSequence.get()) return@withLock null
            checkUserAvatarDelete(settings.value, newSettings)
            settingsStore.update(newSettings)
            if (sequence != assistantSelectionSequence.get()) return@withLock null
            val protectedConversationId = newSettings.secondUserAuthority
                .normalized()
                .takeIf { it.assistantId == newSettings.assistantId }
                ?.conversationId
            if (protectedConversationId != null) {
                return@withLock protectedConversationId
            }
            if (createNewConversation) {
                Uuid.random()
            } else {
                conversationRepo.getConversationsOfAssistant(newSettings.assistantId)
                    .first()
                    .firstOrNull()
                    ?.id
                    ?: Uuid.random()
            }
        }
    }

    fun updateWebSearch(enabled: Boolean) {
        val conversationAssistantId = conversation.value.assistantId
        val assistantId = settings.value.getAssistantById(conversationAssistantId)?.id
            ?: settings.value.getCurrentAssistant().id
        viewModelScope.launch {
            settingsStore.updateAssistantWebSearch(assistantId, enabled)
        }
    }

    suspend fun captureMemorySelection(selectedNodeIds: Set<Uuid>) =
        chatService.captureMemorySelection(_conversationId, selectedNodeIds)

    // 检查用户头像删除
    private fun checkUserAvatarDelete(oldSettings: Settings, newSettings: Settings) {
        val oldAvatar = oldSettings.displaySetting.userAvatar
        val newAvatar = newSettings.displaySetting.userAvatar

        if (oldAvatar is Avatar.Image && oldAvatar != newAvatar) {
            filesManager.deleteChatFiles(listOf(oldAvatar.url.toUri()))
        }
    }

    // 设置聊天模型
    fun setChatModel(assistant: Assistant, model: Model) {
        viewModelScope.launch {
            settingsStore.update { settings ->
                settings.copy(
                    assistants = settings.assistants.map {
                        if (it.id == assistant.id) {
                            it.copy(
                                chatModelId = model.id
                            )
                        } else {
                            it
                        }
                    })
            }
        }
    }

    // Update checker
    val updateState =
        updateChecker.checkUpdate().stateIn(viewModelScope, SharingStarted.Eagerly, UiState.Loading)

    /**
     * 处理消息发送
     *
     * @param content 消息内容
     * @param answer 是否触发消息生成，如果为false，则仅添加消息到消息列表中
     */
    fun handleMessageSend(content: List<UIMessagePart>, answer: Boolean = true) {
        if (content.isEmptyInputMessage()) return
        val agentTimingSubmission = chatService.beginAgentTimingSubmission(_conversationId)

        viewModelScope.launch {
            reportSubmitResult(
                chatService.submitUserMessage(
                    conversationId = _conversationId,
                    content = content,
                    answer = answer,
                    origin = CommandOrigin.APP_UI,
                    agentTimingSubmission = agentTimingSubmission,
                )
            )
        }
    }

    fun handleSteer(
        text: String,
        scope: SteeringScope = SteeringScope.REMAINDER_OF_RUN,
        historyMode: SteeringHistoryMode = SteeringHistoryMode.TRANSIENT,
    ) {
        viewModelScope.launch {
            reportSubmitResult(
                chatService.submitSteer(
                    conversationId = _conversationId,
                    text = text,
                    scope = scope,
                    origin = CommandOrigin.APP_UI,
                    historyMode = historyMode,
                )
            )
        }
    }

    fun toggleSteeringHistoryMode(commandId: Uuid) {
        val entry = steeringEntries.value[commandId] ?: return
        if (!entry.editable) return
        val next = when (entry.historyMode) {
            SteeringHistoryMode.TRANSIENT -> SteeringHistoryMode.PERSISTENT
            SteeringHistoryMode.PERSISTENT -> SteeringHistoryMode.TRANSIENT
        }
        chatService.updateSteeringHistoryMode(_conversationId, commandId, next)
    }

    fun cancelSteering(commandId: Uuid) {
        viewModelScope.launch {
            reportSubmitResult(chatService.cancelSteering(_conversationId, commandId))
        }
    }

    fun handleInterrupt(content: List<UIMessagePart>, answer: Boolean = true) {
        if (content.isEmptyInputMessage()) return
        val agentTimingSubmission = chatService.beginAgentTimingSubmission(_conversationId)
        reportSubmitResult(
            chatService.submitInterrupt(
                _conversationId,
                content,
                answer,
                CommandOrigin.APP_UI,
                agentTimingSubmission,
            )
        )
    }

    fun resumeQueue() {
        viewModelScope.launch {
            reportSubmitResult(chatService.resumeQueue(_conversationId, CommandOrigin.APP_UI))
        }
    }

    fun clearQueue() {
        viewModelScope.launch {
            reportSubmitResult(chatService.clearPendingQueue(_conversationId))
        }
    }

    fun beginEditQueuedMessage(entry: QueuedMessageUiEntry) {
        inputState.editingMessage = null
        inputState.editingQueuedCommand = entry.commandId
        inputState.setContents(entry.content.parts)
    }

    fun updateQueuedMessage(commandId: Uuid, parts: List<UIMessagePart>) {
        if (parts.isEmptyInputMessage()) return
        viewModelScope.launch {
            reportSubmitResult(
                chatService.updateQueuedMessage(
                    conversationId = _conversationId,
                    commandId = commandId,
                    content = RawUserContent(
                        parts = parts,
                        answer = queuedMessages.value
                            .firstOrNull { it.commandId == commandId }
                            ?.content
                            ?.answer
                            ?: true,
                    ),
                )
            )
        }
    }

    fun promoteQueuedMessage(commandId: Uuid) {
        viewModelScope.launch {
            reportSubmitResult(
                chatService.promoteQueuedMessageToSteering(_conversationId, commandId)
            )
        }
    }

    fun cancelQueuedMessage(commandId: Uuid) {
        viewModelScope.launch {
            reportSubmitResult(chatService.cancelQueuedCommand(_conversationId, commandId))
        }
    }

    private fun reportSubmitResult(result: SubmitResult) {
        when (result) {
            is SubmitResult.Accepted -> Unit
            is SubmitResult.QueueFull -> chatService.addError(
                IllegalStateException("现在等着处理的消息已满（最多 ${result.limit} 条），请稍后再发"),
                conversationId = _conversationId,
            )
            is SubmitResult.RuntimeUnavailable -> chatService.addError(
                IllegalStateException("这次对话暂时没有准备好：${result.reason}"),
                conversationId = _conversationId,
            )
            is SubmitResult.Rejected -> chatService.addError(
                IllegalStateException("这条消息暂时没能接收：${result.reason}"),
                conversationId = _conversationId,
            )
        }
    }

    fun handleMessageEdit(parts: List<UIMessagePart>, messageId: Uuid) {
        if (parts.isEmptyInputMessage()) return

        viewModelScope.launch {
            chatService.editMessage(_conversationId, messageId, parts)
        }
    }

    fun handleCompressContext(additionalPrompt: String, targetTokens: Int, keepRecentMessages: Int): Job {
        return viewModelScope.launch {
            chatService.compressConversation(
                _conversationId,
                conversation.value,
                additionalPrompt,
                targetTokens,
                keepRecentMessages
            ).onFailure {
                chatService.addError(it, title = context.getString(R.string.error_title_compress_conversation))
            }
        }
    }

    suspend fun forkMessage(message: UIMessage): Conversation {
        return chatService.forkConversationAtMessage(_conversationId, message.id)
    }

    fun deleteMessage(message: UIMessage) {
        viewModelScope.launch {
            chatService.deleteMessage(_conversationId, message)
        }
    }

    fun showDeleteBlockedWhileGeneratingError() {
        chatService.addError(
            error = IllegalStateException(context.getString(R.string.chat_stop_generation_before_delete)),
            conversationId = _conversationId,
            title = context.getString(R.string.error_title_operation)
        )
    }

    fun regenerateAtMessage(
        message: UIMessage,
        regenerateAssistantMsg: Boolean = true
    ) {
        val agentTimingSubmission = chatService.beginAgentTimingSubmission(_conversationId)
        chatService.regenerateAtMessage(
            _conversationId,
            message,
            regenerateAssistantMsg,
            agentTimingSubmission,
        )
    }

    fun handleToolApproval(
        toolCallId: String,
        approved: Boolean,
        reason: String = "",
        scope: me.rerere.rikkahub.service.ChatService.ApprovalScope =
            me.rerere.rikkahub.service.ChatService.ApprovalScope.Once,
        toolName: String? = null,
    ) {
        chatService.handleToolApproval(
            conversationId = _conversationId,
            toolCallId = toolCallId,
            approved = approved,
            reason = reason,
            scope = scope,
            toolName = toolName,
        )
    }

    fun handleToolAnswer(
        toolCallId: String,
        answer: String,
    ) {
        chatService.handleToolApproval(_conversationId, toolCallId, approved = true, answer = answer)
    }

    fun stopGeneration() {
        viewModelScope.launch {
            chatService.stopGeneration(_conversationId)
        }
    }

    fun mutateMetadata(mutation: me.rerere.rikkahub.data.repository.ConversationMetadataMutation) {
        viewModelScope.launch { chatService.mutateConversationMetadata(_conversationId, mutation) }
    }

    fun updateTitle(title: String) =
        mutateMetadata(me.rerere.rikkahub.data.repository.ConversationMetadataMutation.Title(title))

    fun selectMessageVersion(nodeId: Uuid, messageId: Uuid) {
        viewModelScope.launch { reportSubmitResult(chatService.selectMessageVersion(_conversationId, nodeId, messageId)) }
    }

    suspend fun deleteConversation(conversation: Conversation): ConversationDeletionResult =
        conversationRepo.deleteConversation(conversation)

    fun updatePinnedStatus(conversation: Conversation) {
        viewModelScope.launch {
            conversationRepo.togglePinStatus(conversation.id)
        }
    }

    fun moveConversationToAssistant(conversation: Conversation, targetAssistantId: Uuid) {
        viewModelScope.launch {
            val conversationFull = conversationRepo.getConversationById(conversation.id) ?: return@launch
            val updatedConversation = conversationFull.copy(assistantId = targetAssistantId)
            val updateResult = conversationRepo.updateConversation(updatedConversation)
            if (updateResult !is me.rerere.rikkahub.data.repository.ConversationUpdateResult.Updated) {
                return@launch
            }
            // Drop any "Allow for this chat" grants the user gave the previous assistant.
            // The grants apply to a tool surface the new assistant may use very differently
            // (different prompt, different tool list), and the user authorised them under
            // the old persona's behaviour, not this one's. Persistent "Always Allow" grants
            // stay (they were granted globally) but ChatScope is reset.
            me.rerere.rikkahub.data.ai.tools.ToolApprovalAllowList.clearChat(conversation.id)
            if (conversation.id == _conversationId) {
                chatService.updateConversationState(_conversationId) { updatedConversation }
                settingsStore.updateAssistant(targetAssistantId)
            }
        }
    }

    fun translateMessage(message: UIMessage, targetLanguage: Locale) {
        chatService.translateMessage(_conversationId, message, targetLanguage)
    }

    fun generateTitle(conversation: Conversation, force: Boolean = false) {
        viewModelScope.launch {
            val conversationFull = conversationRepo.getConversationById(conversation.id) ?: return@launch
            chatService.generateTitle(_conversationId, conversationFull, force)
        }
    }

    fun generateSuggestion(conversation: Conversation) {
        viewModelScope.launch {
            chatService.generateSuggestion(_conversationId, conversation)
        }
    }

    fun clearTranslationField(messageId: Uuid) {
        chatService.clearTranslationField(_conversationId, messageId)
    }

    fun toggleMessageFavorite(node: MessageNode) {
        viewModelScope.launch {
            val currentlyFavorited = favoriteRepository.isNodeFavorited(_conversationId, node.id)
            if (currentlyFavorited) {
                favoriteRepository.removeNodeFavorite(_conversationId, node.id)
            } else {
                favoriteRepository.addNodeFavorite(
                    NodeFavoriteTarget(
                        conversationId = _conversationId,
                        conversationTitle = conversation.value.title,
                        nodeId = node.id,
                        node = node
                    )
                )
            }

            chatService.updateConversationState(_conversationId) { currentConversation ->
                currentConversation.copy(
                    messageNodes = currentConversation.messageNodes.map { existingNode ->
                        if (existingNode.id == node.id) {
                            existingNode.copy(isFavorite = !currentlyFavorited)
                        } else {
                            existingNode
                        }
                    }
                )
            }
        }
    }

}
