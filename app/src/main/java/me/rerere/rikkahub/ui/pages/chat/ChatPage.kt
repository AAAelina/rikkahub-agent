package me.rerere.rikkahub.ui.pages.chat

import android.net.Uri
import android.util.Log
import androidx.activity.compose.BackHandler
import androidx.activity.compose.rememberLauncherForActivityResult
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.foundation.lazy.LazyListState
import androidx.compose.foundation.lazy.rememberLazyListState
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.DrawerState
import androidx.compose.material3.DrawerValue
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.LocalContentColor
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.ModalBottomSheet
import androidx.compose.material3.ModalNavigationDrawer
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.PermanentNavigationDrawer
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SheetValue
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.material3.TopAppBarDefaults
import androidx.compose.material3.adaptive.currentWindowDpSize
import androidx.compose.material3.rememberBottomSheetState
import androidx.compose.material3.rememberDrawerState
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.saveable.rememberSaveable
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.platform.LocalContext
import androidx.compose.ui.platform.LocalSoftwareKeyboardController
import androidx.compose.ui.res.stringResource
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.compose.LifecycleEventEffect
import androidx.core.content.FileProvider
import androidx.core.net.toUri
import com.dokar.sonner.ToastType
import dev.chrisbanes.haze.hazeSource
import dev.chrisbanes.haze.rememberHazeState
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import me.rerere.ai.provider.Model
import me.rerere.ai.ui.UIMessagePart
import me.rerere.common.android.appTempFolder
import me.rerere.hugeicons.HugeIcons
import me.rerere.hugeicons.stroke.Cancel01
import me.rerere.hugeicons.stroke.Activity01
import me.rerere.hugeicons.stroke.LeftToRightListBullet
import me.rerere.hugeicons.stroke.Menu03
import me.rerere.hugeicons.stroke.MessageAdd01
import me.rerere.rikkahub.R
import me.rerere.rikkahub.Screen
import me.rerere.rikkahub.data.authority.reward.RewardFeedbackWriteResult
import me.rerere.rikkahub.data.datastore.Settings
import me.rerere.rikkahub.data.datastore.findProvider
import me.rerere.rikkahub.data.datastore.getCurrentAssistant
import me.rerere.rikkahub.data.datastore.getAssistantById
import me.rerere.rikkahub.data.datastore.getCurrentChatModel
import me.rerere.rikkahub.data.files.FilesManager
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.repository.WorkspaceRepository
import me.rerere.rikkahub.service.ChatError
import me.rerere.rikkahub.service.chat.RuntimeState
import me.rerere.rikkahub.service.chat.SteeringHistoryMode
import me.rerere.rikkahub.ui.components.ai.ChatInput
import me.rerere.rikkahub.ui.components.ai.FilesPicker
import me.rerere.rikkahub.ui.components.ai.completion.WorkspaceCompletionProvider
import me.rerere.rikkahub.ui.components.ai.useCropLauncher
import me.rerere.rikkahub.ui.components.ui.permission.PermissionCamera
import me.rerere.rikkahub.ui.components.ui.permission.PermissionManager
import me.rerere.rikkahub.ui.components.ui.permission.rememberPermissionState
import me.rerere.rikkahub.ui.context.LocalNavController
import me.rerere.rikkahub.ui.context.LocalToaster
import me.rerere.rikkahub.ui.context.Navigator
import me.rerere.rikkahub.ui.hooks.ChatInputState
import me.rerere.rikkahub.ui.hooks.EditStateContent
import me.rerere.rikkahub.ui.hooks.useEditState
import me.rerere.rikkahub.utils.base64DecodeOrOriginal
import me.rerere.rikkahub.utils.isAllowedFileType
import me.rerere.rikkahub.utils.navigateToChatPage
import org.koin.androidx.compose.koinViewModel
import org.koin.compose.koinInject
import org.koin.core.parameter.parametersOf
import java.io.File
import kotlin.uuid.Uuid

@Composable
fun ChatPage(
    id: Uuid,
    text: String?,
    files: List<Uri>,
    nodeId: Uuid? = null,
) {
    val vm: ChatVM = koinViewModel(
        parameters = {
            parametersOf(id.toString())
        }
    )
    val filesManager: FilesManager = koinInject()
    val navController = LocalNavController.current
    val scope = rememberCoroutineScope()

    val setting by vm.settings.collectAsStateWithLifecycle()
    val conversation by vm.conversation.collectAsStateWithLifecycle()
    val loadingJob by vm.conversationJob.collectAsStateWithLifecycle()
    val processingStatus by vm.processingStatus.collectAsStateWithLifecycle()
    val currentChatModel by vm.currentChatModel.collectAsStateWithLifecycle()
    val enableWebSearch by vm.enableWebSearch.collectAsStateWithLifecycle()
    val errors by vm.errors.collectAsStateWithLifecycle()

    val drawerState = rememberDrawerState(initialValue = DrawerValue.Closed)
    val softwareKeyboardController = LocalSoftwareKeyboardController.current

    // Handle back press when drawer is open
    BackHandler(enabled = drawerState.isOpen) {
        scope.launch {
            drawerState.close()
        }
    }

    // Hide keyboard when drawer is open
    LaunchedEffect(drawerState.isOpen) {
        if (drawerState.isOpen) {
            softwareKeyboardController?.hide()
        }
    }

    @Suppress("DEPRECATION")  // LocalWindowInfo replaces this in a future Compose bump
    val windowAdaptiveInfo = currentWindowDpSize()
    val isBigScreen =
        windowAdaptiveInfo.width > windowAdaptiveInfo.height && windowAdaptiveInfo.width >= 1100.dp

    val inputState = vm.inputState

    // 初始化输入状态（处理传入的 files 和 text 参数）
    LaunchedEffect(files, text) {
        if (files.isNotEmpty()) {
            val localFiles = filesManager.createChatFilesByContents(files)
            val contentTypes = files.mapNotNull { file ->
                filesManager.getFileMimeType(file)
            }
            val parts = buildList {
                localFiles.forEachIndexed { index, file ->
                    val type = contentTypes.getOrNull(index)
                    if (type?.startsWith("image/") == true) {
                        add(UIMessagePart.Image(url = file.toString()))
                    } else if (type?.startsWith("video/") == true) {
                        add(UIMessagePart.Video(url = file.toString()))
                    } else if (type?.startsWith("audio/") == true) {
                        add(UIMessagePart.Audio(url = file.toString()))
                    }
                }
            }
            inputState.messageContent = parts
        }
        text?.base64DecodeOrOriginal()?.let { decodedText ->
            if (decodedText.isNotEmpty()) {
                inputState.setMessageText(decodedText)
            }
        }
    }

    val chatListState = rememberLazyListState()
    LaunchedEffect(nodeId, conversation.messageNodes.size) {
        if (!vm.chatListInitialized && conversation.messageNodes.isNotEmpty()) {
            if (nodeId != null) {
                val index = conversation.messageNodes.indexOfFirst { it.id == nodeId }
                if (index >= 0) {
                    chatListState.scrollToItem(index)
                }
            } else {
                chatListState.requestScrollToItem(conversation.currentMessages.size + 5)
            }
            vm.chatListInitialized = true
        }
    }

    when {
        isBigScreen -> {
            PermanentNavigationDrawer(
                drawerContent = {
                    ChatDrawerContent(
                        navController = navController,
                        current = conversation,
                        vm = vm,
                        settings = setting
                    )
                }
            ) {
                ChatPageContent(
                    inputState = inputState,
                    loadingJob = loadingJob,
                    processingStatus = processingStatus,
                    setting = setting,
                    conversation = conversation,
                    drawerState = drawerState,
                    navController = navController,
                    vm = vm,
                    chatListState = chatListState,
                    enableWebSearch = enableWebSearch,
                    currentChatModel = currentChatModel,
                    bigScreen = true,
                    errors = errors,
                    onDismissError = { vm.dismissError(it) },
                    onClearAllErrors = { vm.clearAllErrors() },
                )
            }
        }

        else -> {
            ModalNavigationDrawer(
                drawerState = drawerState,
                drawerContent = {
                    ChatDrawerContent(
                        navController = navController,
                        current = conversation,
                        vm = vm,
                        settings = setting
                    )
                }
            ) {
                ChatPageContent(
                    inputState = inputState,
                    loadingJob = loadingJob,
                    processingStatus = processingStatus,
                    setting = setting,
                    conversation = conversation,
                    drawerState = drawerState,
                    navController = navController,
                    vm = vm,
                    chatListState = chatListState,
                    enableWebSearch = enableWebSearch,
                    currentChatModel = currentChatModel,
                    bigScreen = false,
                    errors = errors,
                    onDismissError = { vm.dismissError(it) },
                    onClearAllErrors = { vm.clearAllErrors() },
                )
            }
            BackHandler(drawerState.isOpen) {
                scope.launch { drawerState.close() }
            }
        }
    }
}

@Composable
private fun ChatPageContent(
    inputState: ChatInputState,
    loadingJob: Job?,
    processingStatus: String? = null,
    setting: Settings,
    bigScreen: Boolean,
    conversation: Conversation,
    drawerState: DrawerState,
    navController: Navigator,
    vm: ChatVM,
    chatListState: LazyListState,
    enableWebSearch: Boolean,
    currentChatModel: Model?,
    errors: List<ChatError>,
    onDismissError: (Uuid) -> Unit,
    onClearAllErrors: () -> Unit,
) {
    val scope = rememberCoroutineScope()
    val toaster = LocalToaster.current
    val runtimeState by vm.runtimeState.collectAsStateWithLifecycle()
    val queueStatus by vm.queueStatus.collectAsStateWithLifecycle()
    val queuedMessages by vm.queuedMessages.collectAsStateWithLifecycle()
    val steeringEntries by vm.steeringEntries.collectAsStateWithLifecycle()
    val rewardFeedbackAvailable by vm.rewardFeedbackAvailable.collectAsStateWithLifecycle()
    val context = LocalContext.current
    val workspaceRepository: WorkspaceRepository = koinInject()
    var previewMode by rememberSaveable { mutableStateOf(false) }
    val hazeState = rememberHazeState()
    val assistant = setting.getAssistantById(conversation.assistantId) ?: setting.getCurrentAssistant()
    var showFilesSheet by remember { mutableStateOf(false) }
    var showSendModeDialog by remember { mutableStateOf(false) }

    fun showRewardFeedbackResult(result: RewardFeedbackWriteResult) {
        when (result) {
            is RewardFeedbackWriteResult.Committed,
            is RewardFeedbackWriteResult.Duplicate -> toaster.show(
                context.getString(R.string.learning_feedback_recorded),
                type = ToastType.Success,
            )

            is RewardFeedbackWriteResult.Rejected -> toaster.show(
                context.getString(R.string.learning_feedback_unavailable),
                type = ToastType.Error,
            )
        }
    }

    LaunchedEffect(runtimeState) {
        me.rerere.rikkahub.pet.overlay.TrustedApprovalSurfaceVisibility.setVisible(
            runtimeState == RuntimeState.WaitingApproval,
        )
    }
    LifecycleEventEffect(Lifecycle.Event.ON_STOP) {
        me.rerere.rikkahub.pet.overlay.TrustedApprovalSurfaceVisibility.setVisible(false)
    }
    LifecycleEventEffect(Lifecycle.Event.ON_START) {
        me.rerere.rikkahub.pet.overlay.TrustedApprovalSurfaceVisibility.setVisible(
            runtimeState == RuntimeState.WaitingApproval,
        )
    }
    DisposableEffect(conversation.id) {
        onDispose {
            me.rerere.rikkahub.pet.overlay.TrustedApprovalSurfaceVisibility.setVisible(false)
        }
    }

    val completionProviders = remember(assistant.workspaceId, conversation.workspaceCwd, workspaceRepository) {
        assistant.workspaceId?.let { workspaceId ->
            listOf(
                WorkspaceCompletionProvider(
                    workspaceId = workspaceId.toString(),
                    repository = workspaceRepository,
                    currentCwd = conversation.workspaceCwd,
                )
            )
        }.orEmpty()
    }

    fun submitRunningChoice(choice: RunningSendChoice) {
        val contents = inputState.getContents()
        val text = contents.filterIsInstance<UIMessagePart.Text>()
            .joinToString("\n") { it.text }
        when (choice.toSendAction()) {
            ChatSendAction.SOFT_STEER -> {
                if (text.isBlank()) {
                    toaster.show(
                        "这类补充目前需要包含文字；附件可以选择稍后处理或立即改做。",
                        type = ToastType.Warning,
                    )
                    return
                }
                vm.handleSteer(text)
            }
            ChatSendAction.QUEUE -> vm.handleMessageSend(contents)
            ChatSendAction.INTERRUPT -> vm.handleInterrupt(contents)
            ChatSendAction.STOP -> vm.stopGeneration()
            ChatSendAction.SHOW_RUNNING_CHOICES -> return
        }
        inputState.clearInput()
        showSendModeDialog = false
    }

    TTSAutoPlay(vm = vm, setting = setting, conversation = conversation)

    Surface(
        color = MaterialTheme.colorScheme.background,
        modifier = Modifier.fillMaxSize()
    ) {
        AssistantBackground(setting = setting, modifier = Modifier.hazeSource(hazeState))
        Scaffold(
            topBar = {
                Column {
                    TopBar(
                        settings = setting,
                        conversation = conversation,
                        bigScreen = bigScreen,
                        drawerState = drawerState,
                        previewMode = previewMode,
                        onNewChat = { navigateToChatPage(navController) },
                        onClickMenu = { previewMode = !previewMode },
                        onOpenDiagnostics = {
                            navController.navigate(
                                Screen.SettingDiagnosticsForConversation(conversation.id.toString())
                            )
                        },
                        onUpdateTitle = { vm.updateTitle(it) },
                    )
                    PetDialogueCard(
                        assistant = assistant,
                        conversationId = conversation.id,
                        mainBusy = runtimeState !is RuntimeState.Idle,
                    )
                }
            },
            bottomBar = {
                Column {
                    val statusText = when (val state = runtimeState) {
                        RuntimeState.Idle -> null
                        RuntimeState.Hydrating -> "正在准备对话"
                        RuntimeState.Running -> "正在处理，发来的补充会在下一步看"
                        RuntimeState.Paused -> "当前任务已暂停"
                        RuntimeState.WaitingApproval -> "正在等你确认工具操作"
                        is RuntimeState.Cancelling -> "正在停止当前任务"
                        is RuntimeState.HydrationFailed -> "对话恢复失败"
                        is RuntimeState.Fatal -> "这次对话暂时无法继续"
                    }
                    val visibleSteering = selectVisibleSteeringEntries(steeringEntries.values)
                    if (visibleSteering.isNotEmpty()) {
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .padding(horizontal = 8.dp, vertical = 4.dp),
                            verticalArrangement = Arrangement.spacedBy(6.dp),
                        ) {
                            Text(
                                text = "我正在记着",
                                style = MaterialTheme.typography.labelMedium,
                                color = MaterialTheme.colorScheme.onSurfaceVariant,
                            )
                            visibleSteering.forEach { entry ->
                                val persistent = entry.historyMode == SteeringHistoryMode.PERSISTENT
                                val stateLabel = when (entry.state) {
                                    me.rerere.rikkahub.data.ai.SteeringState.PENDING,
                                    me.rerere.rikkahub.data.ai.SteeringState.DELIVERING -> "等我做完这一步就看"
                                    me.rerere.rikkahub.data.ai.SteeringState.APPLIED -> "已看见，可继续切换是否保留"
                                    me.rerere.rikkahub.data.ai.SteeringState.FALLBACK_QUEUED -> "没赶上这一步，已经放到下一条"
                                    me.rerere.rikkahub.data.ai.SteeringState.NOT_APPLIED_RUN_FINISHED -> "这次没来得及用上，请重新发一次"
                                    me.rerere.rikkahub.data.ai.SteeringState.REJECTED_NOT_STEERABLE -> "这条补充没能接上，请重新发一次"
                                }
                                Surface(
                                    onClick = { vm.toggleSteeringHistoryMode(entry.commandId) },
                                    enabled = entry.editable,
                                    color = if (persistent) Color(0xFFFFE39A) else Color(0xFFE4C7FF),
                                    contentColor = Color(0xFF2B2033),
                                    shape = MaterialTheme.shapes.medium,
                                ) {
                                    Column(modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp)) {
                                        Text(
                                            text = entry.text,
                                            maxLines = 2,
                                            overflow = TextOverflow.Ellipsis,
                                            style = MaterialTheme.typography.bodySmall,
                                        )
                                        Text(
                                            text = if (persistent) {
                                                "$stateLabel · 黄色：以后聊天也会记得"
                                            } else {
                                                "$stateLabel · 紫色：只在这次任务里参考"
                                            },
                                            style = MaterialTheme.typography.labelSmall,
                                        )
                                        Row(
                                            modifier = Modifier.fillMaxWidth(),
                                            horizontalArrangement = Arrangement.End,
                                        ) {
                                            TextButton(
                                                onClick = { vm.cancelSteering(entry.commandId) },
                                            ) {
                                                Text(stringResource(R.string.chat_cancel_guidance))
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (queuedMessages.isNotEmpty()) {
                        Column(
                            modifier = Modifier
                                .fillMaxWidth()
                                .heightIn(max = 280.dp)
                                .verticalScroll(rememberScrollState())
                                .padding(horizontal = 8.dp, vertical = 4.dp),
                            verticalArrangement = Arrangement.spacedBy(6.dp),
                        ) {
                            Row(
                                modifier = Modifier.fillMaxWidth(),
                                horizontalArrangement = Arrangement.SpaceBetween,
                            ) {
                                Text(
                                    text = if (queueStatus.paused) {
                                        "等这件事做完再看 · 已暂停"
                                    } else {
                                        "等这件事做完再看"
                                    },
                                    style = MaterialTheme.typography.labelMedium,
                                    color = MaterialTheme.colorScheme.onSurfaceVariant,
                                )
                                if (queueStatus.paused) {
                                    TextButton(onClick = vm::resumeQueue) {
                                        Text("恢复")
                                    }
                                }
                            }
                            queuedMessages.forEach { entry ->
                                val text = entry.content.parts
                                    .filterIsInstance<UIMessagePart.Text>()
                                    .joinToString("\n") { it.text }
                                    .trim()
                                val attachmentLabel = entry.content.parts
                                    .filterNot { it is UIMessagePart.Text }
                                    .joinToString("、") { part ->
                                        when (part) {
                                            is UIMessagePart.Image -> "图片"
                                            is UIMessagePart.Video -> "视频"
                                            is UIMessagePart.Audio -> "音频"
                                            is UIMessagePart.Document -> part.fileName.ifBlank { "文件" }
                                            else -> "附件"
                                        }
                                    }
                                Surface(
                                    color = MaterialTheme.colorScheme.surfaceVariant,
                                    contentColor = MaterialTheme.colorScheme.onSurfaceVariant,
                                    shape = MaterialTheme.shapes.medium,
                                ) {
                                    Column(
                                        modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp),
                                        verticalArrangement = Arrangement.spacedBy(4.dp),
                                    ) {
                                        Text(
                                            text = text.ifBlank { attachmentLabel.ifBlank { "空消息" } },
                                            maxLines = 2,
                                            overflow = TextOverflow.Ellipsis,
                                            style = MaterialTheme.typography.bodySmall,
                                        )
                                        if (text.isNotBlank() && attachmentLabel.isNotBlank()) {
                                            Text(
                                                text = attachmentLabel,
                                                maxLines = 1,
                                                overflow = TextOverflow.Ellipsis,
                                                style = MaterialTheme.typography.labelSmall,
                                            )
                                        }
                                        Text(
                                            text = "第 ${entry.position} 条",
                                            style = MaterialTheme.typography.labelSmall,
                                        )
                                        Row(
                                            modifier = Modifier.fillMaxWidth(),
                                            horizontalArrangement = Arrangement.End,
                                        ) {
                                            TextButton(
                                                enabled = text.isNotBlank(),
                                                onClick = { vm.promoteQueuedMessage(entry.commandId) },
                                            ) {
                                                Text("现在提醒它")
                                            }
                                            TextButton(onClick = { vm.beginEditQueuedMessage(entry) }) {
                                                Text("修改")
                                            }
                                            TextButton(onClick = { vm.cancelQueuedMessage(entry.commandId) }) {
                                                Text("不发了")
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if (statusText != null) {
                        Text(
                            text = statusText,
                            modifier = Modifier.fillMaxWidth(),
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            style = MaterialTheme.typography.labelSmall,
                        )
                    }
                    ChatInput(
                    state = inputState,
                    loading = runtimeState == RuntimeState.Running,
                    settings = setting,
                    hazeState = hazeState,
                    completionProviders = completionProviders,
                    onCancelClick = {
                        vm.stopGeneration()
                    },
                    enableSearch = enableWebSearch,
                    onToggleSearch = {
                        vm.updateWebSearch(!enableWebSearch)
                    },
                    onSendClick = {
                        if (currentChatModel == null) {
                            toaster.show(
                                context.getString(R.string.chat_select_model_first),
                                type = ToastType.Error,
                            )
                            return@ChatInput
                        }
                        if (inputState.isEditingQueuedMessage()) {
                            vm.updateQueuedMessage(
                                commandId = inputState.editingQueuedCommand!!,
                                parts = inputState.getContents(),
                            )
                        } else if (inputState.editingMessage != null) {
                            vm.handleMessageEdit(
                                parts = inputState.getContents(),
                                messageId = inputState.editingMessage!!,
                            )
                        } else {
                            val contents = inputState.getContents()
                            val text = contents.filterIsInstance<UIMessagePart.Text>()
                                .joinToString("\n") { it.text }
                            val action = resolveShortSendAction(
                                runtimeState = runtimeState,
                                hasInput = !inputState.isEmpty(),
                                hasGuidanceText = text.isNotBlank(),
                            )
                            when (action) {
                                ChatSendAction.SOFT_STEER -> vm.handleSteer(text)
                                ChatSendAction.QUEUE -> vm.handleMessageSend(contents)
                                ChatSendAction.INTERRUPT -> vm.handleInterrupt(contents)
                                ChatSendAction.STOP -> vm.stopGeneration()
                                ChatSendAction.SHOW_RUNNING_CHOICES -> showSendModeDialog = true
                            }
                            if (action != ChatSendAction.SHOW_RUNNING_CHOICES) {
                                inputState.clearInput()
                                scope.launch {
                                    chatListState.requestScrollToItem(conversation.currentMessages.size + 5)
                                }
                            }
                        }
                        if (inputState.isEditing()) {
                            inputState.clearInput()
                        }
                    },
                    onLongSendClick = {
                        if (inputState.isEditingQueuedMessage()) {
                            vm.updateQueuedMessage(
                                commandId = inputState.editingQueuedCommand!!,
                                parts = inputState.getContents(),
                            )
                            inputState.clearInput()
                        } else if (inputState.editingMessage != null) {
                            vm.handleMessageEdit(
                                parts = inputState.getContents(),
                                messageId = inputState.editingMessage!!,
                            )
                            inputState.clearInput()
                        } else {
                            val contents = inputState.getContents()
                            when (
                                resolveLongSendAction(
                                    runtimeState = runtimeState,
                                    hasInput = !inputState.isEmpty(),
                                )
                            ) {
                                ChatSendAction.SHOW_RUNNING_CHOICES -> showSendModeDialog = true
                                ChatSendAction.QUEUE -> {
                                    vm.handleMessageSend(content = contents)
                                    inputState.clearInput()
                                    scope.launch {
                                        chatListState.requestScrollToItem(conversation.currentMessages.size + 5)
                                    }
                                }
                                ChatSendAction.STOP -> vm.stopGeneration()
                                ChatSendAction.SOFT_STEER -> submitRunningChoice(
                                    RunningSendChoice.CONTINUE_WITH_GUIDANCE
                                )
                                ChatSendAction.INTERRUPT -> submitRunningChoice(
                                    RunningSendChoice.STOP_AND_REPLACE
                                )
                            }
                        }
                    },
                    onUpdateChatModel = {
                        vm.setChatModel(assistant = assistant, model = it)
                    },
                    onUpdateAssistant = {
                        vm.updateSettings(
                            setting.copy(
                                assistants = setting.assistants.map { assistant ->
                                    if (assistant.id == it.id) {
                                        it
                                    } else {
                                        assistant
                                    }
                                }
                            )
                        )
                    },
                    onUpdateSearchService = { index ->
                        vm.updateSettings(
                            setting.copy(
                                searchServiceSelected = index
                            )
                        )
                    },
                    onMoreClick = {
                        showFilesSheet = true
                    },
                )
                }
            },
            containerColor = Color.Transparent,
        ) { innerPadding ->
            ChatList(
                innerPadding = innerPadding,
                conversation = conversation,
                state = chatListState,
                loading = loadingJob != null,
                processingStatus = processingStatus,
                previewMode = previewMode,
                settings = setting,
                hazeState = hazeState,
                errors = errors,
                onDismissError = onDismissError,
                onClearAllErrors = onClearAllErrors,
                onRegenerate = {
                    vm.regenerateAtMessage(it)
                },
                onEdit = {
                    inputState.editingQueuedCommand = null
                    inputState.editingMessage = it.id
                    inputState.setContents(it.parts)
                },
                onForkMessage = {
                    scope.launch {
                        val fork = vm.forkMessage(message = it)
                        navigateToChatPage(navController, chatId = fork.id)
                    }
                },
                onDelete = {
                    if (loadingJob != null) {
                        vm.showDeleteBlockedWhileGeneratingError()
                    } else {
                        vm.deleteMessage(it)
                    }
                },
                onUpdateMessage = { nodeId, messageId ->
                    vm.selectMessageVersion(nodeId, messageId)
                },
                onHelpfulFeedback = if (rewardFeedbackAvailable) {
                    { message ->
                        scope.launch {
                            showRewardFeedbackResult(vm.recordHelpfulFeedback(message.id))
                        }
                    }
                } else null,
                onNotHelpfulFeedback = if (rewardFeedbackAvailable) {
                    { message ->
                        scope.launch {
                            showRewardFeedbackResult(vm.recordNotHelpfulFeedback(message.id))
                        }
                    }
                } else null,
                onClickSuggestion = { suggestion ->
                    inputState.editingMessage = null
                    inputState.editingQueuedCommand = null
                    inputState.setMessageText(suggestion)
                },
                onTranslate = { message, locale ->
                    vm.translateMessage(message, locale)
                },
                onClearTranslation = { message ->
                    vm.clearTranslationField(message.id)
                },
                onJumpToMessage = { index ->
                    previewMode = false
                    scope.launch {
                        chatListState.animateScrollToItem(index)
                    }
                },
                onToolApproval = { toolCallId, approved, reason, scope, toolName ->
                    vm.handleToolApproval(toolCallId, approved, reason, scope, toolName)
                },
                onToolAnswer = { toolCallId, answer ->
                    vm.handleToolAnswer(toolCallId, answer)
                },
                onToggleFavorite = { node ->
                    vm.toggleMessageFavorite(node)
                },
                onConversationSystemPromptChange = { newPrompt ->
                    vm.mutateMetadata(me.rerere.rikkahub.data.repository.ConversationMetadataMutation.SystemPrompt(newPrompt))
                },
                onAddSelectionToMemory = { selectedNodeIds ->
                    scope.launch {
                        when (vm.captureMemorySelection(selectedNodeIds)) {
                            me.rerere.rikkahub.memory.ManualMemorySelectionResult.QUEUED -> {
                                toaster.show(
                                    context.getString(R.string.memory_v2_manual_queued),
                                    type = ToastType.Success,
                                )
                            }
                            me.rerere.rikkahub.memory.ManualMemorySelectionResult.MEMORY_DISABLED -> {
                                toaster.show(
                                    context.getString(R.string.memory_v2_manual_enable_first),
                                    type = ToastType.Warning,
                                )
                                navController.navigate(
                                    Screen.AssistantMemory(conversation.assistantId.toString()),
                                )
                            }
                            me.rerere.rikkahub.memory.ManualMemorySelectionResult.NO_USER_TEXT -> {
                                toaster.show(
                                    context.getString(R.string.memory_v2_manual_requires_user),
                                    type = ToastType.Warning,
                                )
                            }
                            me.rerere.rikkahub.memory.ManualMemorySelectionResult.FAILED -> {
                                toaster.show(
                                    context.getString(R.string.memory_v2_manual_failed),
                                    type = ToastType.Error,
                                )
                            }
                        }
                    }
                },
            )
        }

        if (showSendModeDialog) {
            AlertDialog(
                onDismissRequest = { showSendModeDialog = false },
                title = { Text("这条消息怎么处理？") },
                text = {
                    Column(verticalArrangement = Arrangement.spacedBy(8.dp)) {
                        SendModeOption(
                            title = "继续做，下一步参考这条",
                            description = "不打断正在执行的工具；做完当前这一步后，AI 会按你的补充调整后续操作。",
                            color = Color(0xFFE4C7FF),
                            onClick = {
                                submitRunningChoice(RunningSendChoice.CONTINUE_WITH_GUIDANCE)
                            },
                        )
                        SendModeOption(
                            title = "做完当前任务，再处理这条",
                            description = "把它放到下一条，不改变现在这项任务。",
                            color = MaterialTheme.colorScheme.surfaceContainerHigh,
                            onClick = {
                                submitRunningChoice(RunningSendChoice.HANDLE_AFTER_CURRENT_TASK)
                            },
                        )
                        SendModeOption(
                            title = "现在停下来，改做这条",
                            description = "先停下手头这件事，收好已经做到的部分，再从这条消息重新开始。",
                            color = MaterialTheme.colorScheme.errorContainer,
                            onClick = {
                                submitRunningChoice(RunningSendChoice.STOP_AND_REPLACE)
                            },
                        )
                    }
                },
                confirmButton = {
                    TextButton(onClick = { showSendModeDialog = false }) {
                        Text("先不发送")
                    }
                },
            )
        }

        if (showFilesSheet) {
            ChatFilesPickerSheet(
                inputState = inputState,
                setting = setting,
                conversation = conversation,
                assistant = assistant,
                vm = vm,
                onDismiss = { showFilesSheet = false },
            )
        }
    }
}

@Composable
private fun SendModeOption(
    title: String,
    description: String,
    color: Color,
    onClick: () -> Unit,
) {
    Surface(
        onClick = onClick,
        color = color,
        shape = MaterialTheme.shapes.medium,
        modifier = Modifier.fillMaxWidth(),
    ) {
        Column(
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 10.dp),
            verticalArrangement = Arrangement.spacedBy(2.dp),
        ) {
            Text(title, style = MaterialTheme.typography.titleSmall)
            Text(
                description,
                style = MaterialTheme.typography.bodySmall,
                color = LocalContentColor.current.copy(alpha = 0.72f),
            )
        }
    }
}

@Composable
private fun ChatFilesPickerSheet(
    inputState: ChatInputState,
    setting: Settings,
    conversation: Conversation,
    assistant: Assistant,
    vm: ChatVM,
    onDismiss: () -> Unit,
) {
    val context = LocalContext.current
    val toaster = LocalToaster.current
    val filesManager: FilesManager = koinInject()
    var showInjectionSheet by remember { mutableStateOf(false) }
    var showCompressDialog by remember { mutableStateOf(false) }

    fun dismissAll() {
        showInjectionSheet = false
        showCompressDialog = false
        onDismiss()
    }

    val cameraPermission = rememberPermissionState(PermissionCamera)
    PermissionManager(permissionState = cameraPermission)

    var cameraOutputUri by remember { mutableStateOf<Uri?>(null) }
    var cameraOutputFile by remember { mutableStateOf<File?>(null) }
    val (_, launchCameraCrop) = useCropLauncher(
        onCroppedImageReady = { croppedUri ->
            inputState.addImages(filesManager.createChatFilesByContents(listOf(croppedUri)))
            dismissAll()
        },
        onCropError = { error ->
            toaster.show(error.message ?: context.getString(R.string.backup_page_unknown_error), type = ToastType.Error)
        },
        onCleanup = {
            cameraOutputFile?.delete()
            cameraOutputFile = null
            cameraOutputUri = null
        }
    )
    val cameraLauncher = rememberLauncherForActivityResult(ActivityResultContracts.TakePicture()) { captureSuccessful ->
        if (captureSuccessful && cameraOutputUri != null) {
            if (setting.displaySetting.skipCropImage) {
                inputState.addImages(filesManager.createChatFilesByContents(listOf(cameraOutputUri!!)))
                cameraOutputFile?.delete()
                cameraOutputFile = null
                cameraOutputUri = null
                dismissAll()
            } else {
                launchCameraCrop(cameraOutputUri!!)
            }
        } else {
            cameraOutputFile?.delete()
            cameraOutputFile = null
            cameraOutputUri = null
        }
    }
    val onLaunchCamera: () -> Unit = {
        if (cameraPermission.allRequiredPermissionsGranted) {
            cameraOutputFile = context.cacheDir.resolve("camera_${Uuid.random()}.jpg")
            cameraOutputUri = FileProvider.getUriForFile(
                context, "${context.packageName}.fileprovider", cameraOutputFile!!
            )
            cameraLauncher.launch(cameraOutputUri!!)
        } else {
            cameraPermission.requestPermissions()
        }
    }

    var preCropTempFile by remember { mutableStateOf<File?>(null) }
    val (_, launchImageCrop) = useCropLauncher(
        onCroppedImageReady = { croppedUri ->
            inputState.addImages(filesManager.createChatFilesByContents(listOf(croppedUri)))
            dismissAll()
        },
        onCropError = { error ->
            toaster.show(error.message ?: context.getString(R.string.backup_page_unknown_error), type = ToastType.Error)
        },
        onCleanup = {
            preCropTempFile?.delete()
            preCropTempFile = null
        }
    )
    val imagePickerLauncher =
        rememberLauncherForActivityResult(ActivityResultContracts.GetMultipleContents()) { selectedUris ->
            if (selectedUris.isNotEmpty()) {
                Log.d("ImagePickButton", "Selected URIs: $selectedUris")
                if (setting.displaySetting.skipCropImage) {
                    inputState.addImages(filesManager.createChatFilesByContents(selectedUris))
                    dismissAll()
                } else if (selectedUris.size == 1) {
                    val tempFile = File(context.appTempFolder, "pick_temp_${System.currentTimeMillis()}.jpg")
                    runCatching {
                        context.contentResolver.openInputStream(selectedUris.first())?.use { input ->
                            tempFile.outputStream().use { output -> input.copyTo(output) }
                        }
                        preCropTempFile = tempFile
                        launchImageCrop(tempFile.toUri())
                    }.onFailure {
                        Log.e("ImagePickButton", "Failed to copy image to temp, falling back", it)
                        launchImageCrop(selectedUris.first())
                    }
                } else {
                    inputState.addImages(filesManager.createChatFilesByContents(selectedUris))
                    dismissAll()
                }
            } else {
                Log.d("ImagePickButton", "No images selected")
            }
        }

    val videoPickerLauncher =
        rememberLauncherForActivityResult(ActivityResultContracts.GetMultipleContents()) { selectedUris ->
            if (selectedUris.isNotEmpty()) {
                inputState.addVideos(filesManager.createChatFilesByContents(selectedUris))
                dismissAll()
            }
        }

    val audioPickerLauncher =
        rememberLauncherForActivityResult(ActivityResultContracts.GetMultipleContents()) { selectedUris ->
            if (selectedUris.isNotEmpty()) {
                inputState.addAudios(filesManager.createChatFilesByContents(selectedUris))
                dismissAll()
            }
        }

    val filePickerLauncher =
        rememberLauncherForActivityResult(ActivityResultContracts.OpenMultipleDocuments()) { uris ->
            if (uris.isNotEmpty()) {
                val documents = uris.mapNotNull { uri ->
                    val fileName = filesManager.getFileNameFromUri(uri) ?: "file"
                    val mime = filesManager.getFileMimeType(uri) ?: "text/plain"
                    if (isAllowedFileType(fileName, mime)) {
                        val localUri = filesManager.createChatFilesByContents(listOf(uri)).firstOrNull()
                            ?: run {
                                toaster.show(
                                    context.getString(R.string.chat_input_file_read_failed, fileName),
                                    type = ToastType.Error
                                )
                                return@mapNotNull null
                            }
                        UIMessagePart.Document(url = localUri.toString(), fileName = fileName, mime = mime)
                    } else {
                        toaster.show(
                            context.getString(R.string.chat_input_unsupported_file_type, fileName),
                            type = ToastType.Error
                        )
                        null
                    }
                }
                if (documents.isNotEmpty()) {
                    inputState.addFiles(documents)
                    dismissAll()
                }
            }
        }

    val filesSheetState = rememberBottomSheetState(
        initialValue = SheetValue.Hidden,
        enabledValues = setOf(SheetValue.Hidden, SheetValue.Expanded)
    )
    ModalBottomSheet(
        sheetState = filesSheetState,
        onDismissRequest = { dismissAll() },
    ) {
        FilesPicker(
            conversation = conversation,
            state = inputState,
            assistant = assistant,
            mcpManager = vm.mcpManager,
            onCompressContext = { additionalPrompt, targetTokens, keepRecentMessages ->
                vm.handleCompressContext(additionalPrompt, targetTokens, keepRecentMessages)
            },
            onUpdateAssistant = {
                vm.updateSettings(
                    setting.copy(
                        assistants = setting.assistants.map { assistant ->
                            if (assistant.id == it.id) {
                                it
                            } else {
                                assistant
                            }
                        }
                    )
                )
            },
            onUpdateConversation = {
                vm.mutateMetadata(it)
            },
            showInjectionSheet = showInjectionSheet,
            onShowInjectionSheetChange = { showInjectionSheet = it },
            showCompressDialog = showCompressDialog,
            onShowCompressDialogChange = { showCompressDialog = it },
            onDismiss = { dismissAll() },
            onTakePic = onLaunchCamera,
            onPickImage = { imagePickerLauncher.launch("image/*") },
            onPickVideo = { videoPickerLauncher.launch("video/*") },
            onPickAudio = { audioPickerLauncher.launch("audio/*") },
            onPickFile = { filePickerLauncher.launch(arrayOf("*/*")) },
        )
    }
}

@Composable
private fun TopBar(
    settings: Settings,
    conversation: Conversation,
    drawerState: DrawerState,
    bigScreen: Boolean,
    previewMode: Boolean,
    onClickMenu: () -> Unit,
    onOpenDiagnostics: () -> Unit,
    onNewChat: () -> Unit,
    onUpdateTitle: (String) -> Unit
) {
    val scope = rememberCoroutineScope()
    val toaster = LocalToaster.current
    val titleState = useEditState<String> {
        onUpdateTitle(it)
    }

    TopAppBar(
        colors = TopAppBarDefaults.topAppBarColors(containerColor = Color.Transparent),
        navigationIcon = {
            if (!bigScreen) {
                IconButton(
                    onClick = {
                        scope.launch { drawerState.open() }
                    }
                ) {
                    Icon(HugeIcons.Menu03, "Messages")
                }
            }
        },
        title = {
            val editTitleWarning = stringResource(R.string.chat_page_edit_title_warning)
            Surface(
                onClick = {
                    if (conversation.messageNodes.isNotEmpty()) {
                        titleState.open(conversation.title)
                    } else {
                        toaster.show(editTitleWarning, type = ToastType.Warning)
                    }
                },
                color = Color.Transparent,
            ) {
                Column {
                    val assistant = settings.getCurrentAssistant()
                    val model = settings.getCurrentChatModel()
                    val provider = model?.findProvider(providers = settings.providers, checkOverwrite = false)
                    Text(
                        text = conversation.title.ifBlank { stringResource(R.string.chat_page_new_chat) },
                        maxLines = 1,
                        style = MaterialTheme.typography.bodyMedium,
                        overflow = TextOverflow.Ellipsis,
                    )
                    if (model != null && provider != null) {
                        Text(
                            text = "${assistant.name.ifBlank { stringResource(R.string.assistant_page_default_assistant) }} / ${model.displayName} (${provider.name})",
                            overflow = TextOverflow.Ellipsis,
                            maxLines = 1,
                            color = LocalContentColor.current.copy(0.65f),
                            style = MaterialTheme.typography.labelSmall.copy(
                                fontSize = 8.sp,
                            )
                        )
                    }
                }
            }
        },
        actions = {
            IconButton(onClick = onOpenDiagnostics) {
                Icon(HugeIcons.Activity01, "Runtime Diagnostics")
            }

            IconButton(
                onClick = {
                    onClickMenu()
                }
            ) {
                Icon(if (previewMode) HugeIcons.Cancel01 else HugeIcons.LeftToRightListBullet, "Chat Options")
            }

            IconButton(
                onClick = {
                    onNewChat()
                }
            ) {
                Icon(HugeIcons.MessageAdd01, "New Message")
            }
        },
    )
    titleState.EditStateContent { title, onUpdate ->
        AlertDialog(
            onDismissRequest = {
                titleState.dismiss()
            },
            title = {
                Text(stringResource(R.string.chat_page_edit_title))
            },
            text = {
                OutlinedTextField(
                    value = title,
                    onValueChange = onUpdate,
                    modifier = Modifier.fillMaxWidth(),
                    singleLine = true,
                )
            },
            confirmButton = {
                TextButton(
                    onClick = {
                        titleState.confirm()
                    }
                ) {
                    Text(stringResource(R.string.chat_page_save))
                }
            },
            dismissButton = {
                TextButton(
                    onClick = {
                        titleState.dismiss()
                    }
                ) {
                    Text(stringResource(R.string.chat_page_cancel))
                }
            }
        )
    }
}
