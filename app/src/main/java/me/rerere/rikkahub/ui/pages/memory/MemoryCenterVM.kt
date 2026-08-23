package me.rerere.rikkahub.ui.pages.memory

import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import androidx.paging.Pager
import androidx.paging.PagingConfig
import androidx.paging.cachedIn
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import me.rerere.rikkahub.data.datastore.SettingsStore
import me.rerere.rikkahub.data.datastore.findProvider
import me.rerere.rikkahub.data.datastore.getAssistantById
import me.rerere.rikkahub.data.db.dao.MemoryDAO
import me.rerere.rikkahub.data.db.dao.MemoryCaptureStatusCounts
import me.rerere.rikkahub.data.db.dao.MemoryV2Dao
import me.rerere.rikkahub.data.model.Assistant
import me.rerere.rikkahub.data.repository.DEFAULT_MEMORY_PROMPT_MAX_CHARS
import me.rerere.rikkahub.data.repository.MemoryRepository
import me.rerere.rikkahub.data.repository.ConversationRepository
import me.rerere.rikkahub.memory.MemoryAutoSaveMode
import me.rerere.rikkahub.memory.MemoryCaptureOrigin
import me.rerere.rikkahub.memory.MemoryMutationCommand
import me.rerere.rikkahub.memory.MemoryMutationCoordinator
import me.rerere.rikkahub.memory.MemoryMutationResult
import me.rerere.rikkahub.memory.MemoryReviewCommand
import me.rerere.rikkahub.memory.MemoryReviewResult
import me.rerere.rikkahub.memory.MemoryRelationReviewCommand
import me.rerere.rikkahub.memory.MemoryRelationReviewResult
import me.rerere.rikkahub.memory.MemoryV2Coordinator
import me.rerere.rikkahub.memory.MemoryWorkRequest
import me.rerere.rikkahub.memory.MemoryWorkScheduler
import me.rerere.rikkahub.memory.MemoryApprovalSource
import me.rerere.rikkahub.memory.MemoryWriteInput
import me.rerere.rikkahub.data.db.entity.MemoryCandidateEntity
import me.rerere.rikkahub.data.db.entity.MemoryEntity
import me.rerere.rikkahub.data.db.entity.MemoryRelationCandidateEntity
import me.rerere.rikkahub.memory.MemoryCandidateAction
import me.rerere.rikkahub.memory.MemoryCandidatePolicy
import me.rerere.rikkahub.memory.MemoryKind
import me.rerere.rikkahub.memory.MemoryProposal
import me.rerere.rikkahub.memory.resolveMemoryExtractionModel
import me.rerere.rikkahub.memory.dreaming.diagnostics.DreamObserverDiagnostics
import me.rerere.rikkahub.memory.dreaming.diagnostics.DreamObserverScopeDiagnostic
import me.rerere.rikkahub.memory.dreaming.model.DreamScopeId
import me.rerere.rikkahub.memory.dreaming.review.DreamClaimMutationTarget
import me.rerere.rikkahub.memory.dreaming.review.DreamCorrectionDraft
import me.rerere.rikkahub.memory.dreaming.review.DreamCorrectionResult
import me.rerere.rikkahub.memory.dreaming.review.DreamEvidenceRevealResult
import me.rerere.rikkahub.memory.dreaming.review.DreamEvidenceSummary
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewMutationResult
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewProjection
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewReadResult
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewRepository
import me.rerere.rikkahub.memory.dreaming.review.DreamReviewFence
import me.rerere.rikkahub.memory.dreaming.runtime.DreamSynthesisCoordinator
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingCostPolicy
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingScopePreferenceMutation
import me.rerere.rikkahub.memory.dreaming.runtime.DreamingScopePreferences
import me.rerere.rikkahub.utils.JsonInstant
import kotlin.uuid.Uuid

@OptIn(ExperimentalCoroutinesApi::class)
class MemoryCenterVM(
    id: String,
    private val settingsStore: SettingsStore,
    private val memoryDao: MemoryDAO,
    private val memoryV2Dao: MemoryV2Dao,
    private val memoryRepository: MemoryRepository,
    private val mutationCoordinator: MemoryMutationCoordinator,
    private val coordinator: MemoryV2Coordinator,
    private val scheduler: MemoryWorkScheduler,
    private val conversationRepository: ConversationRepository,
    private val dreamObserverDiagnostics: DreamObserverDiagnostics,
    private val dreamReviewRepository: DreamReviewRepository,
    private val dreamSynthesisCoordinator: DreamSynthesisCoordinator,
) : ViewModel() {
    private val candidatePolicy = MemoryCandidatePolicy()
    private val assistantId = Uuid.parse(id)
    val assistant = settingsStore.settingsFlow.map { settings ->
        settings.getAssistantById(assistantId) ?: Assistant(id = assistantId)
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), Assistant(id = assistantId))
    val assistants = settingsStore.settingsFlow.map { it.assistants }
        .stateIn(
            viewModelScope,
            SharingStarted.WhileSubscribed(5_000),
            listOf(Assistant(id = assistantId)),
        )

    val viewGlobal = MutableStateFlow(
        settingsStore.settingsFlow.value.getAssistantById(assistantId)?.useGlobalMemory == true,
    )
    val libraryFilter = MutableStateFlow(MemoryLibraryFilter())
    private val scopeId = combine(assistant, viewGlobal) { assistant, global ->
        if (global) MemoryRepository.GLOBAL_MEMORY_ID else assistant.id.toString()
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), assistantId.toString())

    val developerMode = settingsStore.settingsFlow
        .map { settings -> settings.developerMode }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), false)

    private val observerDiagnosticsRefresh = MutableStateFlow(0L)
    val observerDiagnostic = combine(
        scopeId,
        developerMode,
        observerDiagnosticsRefresh,
    ) { scope, enabled, _ -> scope to enabled }
        .flatMapLatest { (scope, enabled) ->
            flow<DreamObserverScopeDiagnostic?> {
                if (!enabled) {
                    emit(null)
                    return@flow
                }
                val typedScope = DreamScopeId.parseOrNull(scope)
                emit(typedScope?.let { dreamObserverDiagnostics.readScope(it) })
            }.catch { error ->
                if (error is CancellationException) throw error
                emit(null)
            }
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), null)

    private val pairDreamScopeId = DreamScopeId.pairScope(assistantId)

    val dreamProjection = dreamReviewRepository.observeScope(pairDreamScopeId)
        .map<DreamReviewProjection, DreamReviewProjection?> { it }
        .catch { error ->
            if (error is CancellationException) throw error
            emit(null)
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), null)

    val dreamingScopePreferences = settingsStore.settingsFlow
        .map { settings -> settings.dreamingPreferences.forScope(pairDreamScopeId) }
        .stateIn(
        viewModelScope,
        SharingStarted.WhileSubscribed(5_000),
        DreamingScopePreferences(),
    )

    val dreamingCostPolicy = settingsStore.settingsFlow
        .map { settings -> settings.dreamingPreferences.failClosed().costPolicy }
        .stateIn(
            viewModelScope,
            SharingStarted.WhileSubscribed(5_000),
            DreamingCostPolicy(),
        )

    val dreamDetailState = MutableStateFlow<MemoryDreamDetailState>(MemoryDreamDetailState.Closed)

    /** Command authorization must not wait for the asynchronous combine/stateIn propagation. */
    private fun currentCommandScopeId(): String = if (viewGlobal.value) {
        MemoryRepository.GLOBAL_MEMORY_ID
    } else {
        assistantId.toString()
    }

    val library = combine(scopeId, libraryFilter) { scope, filter -> scope to filter }
        .flatMapLatest { (scope, filter) ->
            Pager(PagingConfig(pageSize = 30, prefetchDistance = 10)) {
                memoryDao.pagingLibrary(
                    scopeId = scope,
                    includeArchived = filter.includeArchived,
                    nowMs = System.currentTimeMillis(),
                    query = filter.query.trim(),
                    kind = filter.kind?.name,
                    sourceType = filter.sourceType,
                    tag = filter.tag.trim(),
                    sort = filter.sort.name,
                )
            }.flow
        }.cachedIn(viewModelScope)

    val candidates = scopeId.flatMapLatest { scope ->
        Pager(PagingConfig(pageSize = 20, prefetchDistance = 5)) {
            memoryV2Dao.pagingPendingCandidates(scope)
        }.flow
    }.cachedIn(viewModelScope)

    val relationCandidates = scopeId
        .flatMapLatest { scope -> memoryV2Dao.observePendingRelationCandidates(scope) }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    val stats = scopeId.flatMapLatest { scope ->
        val memoryCounts = combine(
            memoryDao.observeActiveCount(scope, System.currentTimeMillis()),
            memoryDao.observeArchivedCount(scope),
        ) { active, archived -> active to archived }
        val queueCounts = combine(
            memoryV2Dao.observePendingCandidateCount(scope),
            memoryV2Dao.observePendingRelationCandidates(scope).map { it.size },
            memoryV2Dao.observeCaptureStatusCounts(scope),
        ) { memoryReview, relationReview, captures ->
            Triple(memoryReview, relationReview, captures)
        }
        combine(
            memoryCounts,
            queueCounts,
            memoryV2Dao.observeLastProcessedAt(scope),
        ) { memories, queues, last ->
            memoryCenterStats(
                active = memories.first,
                archived = memories.second,
                pendingReview = queues.first,
                captures = queues.third,
                lastProcessedAtMs = last,
                pendingRelationReview = queues.second,
            )
        }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), MemoryCenterStats())

    val latestFailure = scopeId
        .flatMapLatest { scope -> memoryV2Dao.observeLatestFailure(scope) }
        .map { failure ->
            failure?.let { formatMemoryFailureDetail(it.errorCode, it.errorMessage) }
        }
        .stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), null)

    val extractionModel = settingsStore.settingsFlow.map { settings ->
        val model = settings.resolveMemoryExtractionModel()
        val provider = model?.findProvider(settings.providers)
        MemoryExtractionModelUiState(
            modelName = model?.displayName?.ifBlank { model.modelId }.orEmpty(),
            providerName = provider?.name.orEmpty(),
            usingFastModel = settings.memoryExtractionModelId == null,
            available = model != null && provider != null && provider.enabled,
        )
    }.stateIn(
        viewModelScope,
        SharingStarted.WhileSubscribed(5_000),
        MemoryExtractionModelUiState("", "", true),
    )

    val modelOptions = settingsStore.settingsFlow.map { settings ->
        settings.providers.filter { it.enabled }.flatMap { provider ->
            provider.models.map { model ->
                MemoryModelOption(
                    id = model.id,
                    name = model.displayName.ifBlank { model.modelId },
                    providerName = provider.name,
                )
            }
        }
    }.stateIn(viewModelScope, SharingStarted.WhileSubscribed(5_000), emptyList())

    val recallTestState = MutableStateFlow<MemoryRecallTestState>(MemoryRecallTestState.Idle)

    fun refreshObserverDiagnostics() {
        observerDiagnosticsRefresh.update { current ->
            if (current == Long.MAX_VALUE) 0L else current + 1L
        }
    }
    val lastActionMessage = MutableStateFlow<String?>(null)

    init {
        viewModelScope.launch {
            val configured = settingsStore.settingsFlow.first { settings -> !settings.init }
                .getAssistantById(assistantId)
            if (configured?.useGlobalMemory == true) viewGlobal.value = true
        }
    }

    fun setViewGlobal(global: Boolean) {
        closeDreamClaim()
        viewGlobal.value = global
    }

    fun updateFilter(transform: (MemoryLibraryFilter) -> MemoryLibraryFilter) {
        libraryFilter.value = transform(libraryFilter.value)
    }

    fun updateAssistant(transform: (Assistant) -> Assistant) {
        viewModelScope.launch {
            settingsStore.update { settings ->
                settings.copy(
                    assistants = settings.assistants.map { current ->
                        if (current.id == assistantId) transform(current) else current
                    },
                )
            }
        }
    }

    fun setAutoSaveMode(mode: MemoryAutoSaveMode) {
        updateAssistant { it.copy(memoryAutoSaveMode = mode) }
        if (mode == MemoryAutoSaveMode.OFF) {
            val current = assistant.value
            val activeScope = if (current.useGlobalMemory) {
                MemoryRepository.GLOBAL_MEMORY_ID
            } else {
                current.id.toString()
            }
            viewModelScope.launch { scheduler.cancel(activeScope) }
        }
    }

    /**
     * Persists the user-facing batching controls together and immediately reapplies the schedule to
     * any already queued captures in the Assistant's active memory scope.
     */
    fun setScheduleTuning(
        idleMinutes: Int,
        immediateThreshold: Int,
        conversationContextTurns: Int,
    ) {
        val normalizedMinutes = idleMinutes.coerceIn(MIN_IDLE_MINUTES, MAX_IDLE_MINUTES)
        val normalizedThreshold = immediateThreshold.coerceIn(MIN_IMMEDIATE_THRESHOLD, MAX_IMMEDIATE_THRESHOLD)
        val normalizedContextTurns = conversationContextTurns.coerceIn(
            MIN_CONVERSATION_CONTEXT_TURNS,
            MAX_CONVERSATION_CONTEXT_TURNS,
        )
        updateAssistant {
            it.copy(
                memoryIdleDelayMinutes = normalizedMinutes,
                memoryImmediateCaptureThreshold = normalizedThreshold,
                memoryConversationContextTurns = normalizedContextTurns,
            )
        }
        viewModelScope.launch(Dispatchers.IO) {
            val currentAssistant = assistant.value
            val scope = if (currentAssistant.useGlobalMemory) {
                MemoryRepository.GLOBAL_MEMORY_ID
            } else {
                currentAssistant.id.toString()
            }
            val pending = memoryV2Dao.countPendingCaptures(scope)
            if (pending > 0) {
                val delayMs = if (pending >= normalizedThreshold) {
                    0L
                } else {
                    normalizedMinutes * 60_000L
                }
                scheduler.schedule(MemoryWorkRequest(scope, delayMs))
            }
        }
    }

    fun setNarrativeNames(userName: String, companionName: String) {
        updateAssistant {
            it.copy(
                memoryNarrativeUserName = userName.trim().take(MAX_NARRATIVE_NAME_CHARS),
                memoryNarrativeCompanionName = companionName.trim().take(MAX_NARRATIVE_NAME_CHARS),
            )
        }
    }

    fun setOrigin(origin: MemoryCaptureOrigin, enabled: Boolean) {
        if (origin !in USER_CONFIGURABLE_ORIGINS) return
        updateAssistant { assistant ->
            assistant.copy(
                memoryCaptureOrigins = if (enabled) {
                    assistant.memoryCaptureOrigins + origin
                } else {
                    assistant.memoryCaptureOrigins - origin
                },
            )
        }
    }

    fun setExtractionModel(modelId: Uuid?) {
        viewModelScope.launch {
            settingsStore.update { it.copy(memoryExtractionModelId = modelId) }
        }
    }

    fun processNow(retryFailed: Boolean = false) {
        viewModelScope.launch(Dispatchers.IO) {
            val scope = currentCommandScopeId()
            if (retryFailed) memoryV2Dao.retryScope(scope, System.currentTimeMillis())
            scheduler.schedule(MemoryWorkRequest(scope, 0L))
            lastActionMessage.value = "scheduled"
        }
    }

    fun review(command: MemoryReviewCommand) {
        viewModelScope.launch(Dispatchers.IO) {
            lastActionMessage.value = when (coordinator.review(command)) {
                is MemoryReviewResult.Applied -> "accepted"
                MemoryReviewResult.Rejected -> "rejected"
                MemoryReviewResult.Conflict -> "conflict"
                MemoryReviewResult.AlreadyResolved -> "already_resolved"
                MemoryReviewResult.NotFound -> "not_found"
                is MemoryReviewResult.Failed -> "failed"
            }
        }
    }

    fun reviewRelation(command: MemoryRelationReviewCommand) {
        viewModelScope.launch(Dispatchers.IO) {
            lastActionMessage.value = when (coordinator.reviewRelation(command)) {
                is MemoryRelationReviewResult.Applied -> "accepted"
                MemoryRelationReviewResult.Rejected -> "rejected"
                MemoryRelationReviewResult.Conflict -> "conflict"
                MemoryRelationReviewResult.AlreadyResolved -> "already_resolved"
                MemoryRelationReviewResult.NotFound -> "not_found"
                is MemoryRelationReviewResult.Failed -> "failed"
            }
        }
    }

    fun acceptRelationCandidate(candidate: MemoryRelationCandidateEntity) {
        reviewRelation(candidate.reviewCommand(accept = true))
    }

    fun rejectRelationCandidate(candidate: MemoryRelationCandidateEntity) {
        reviewRelation(candidate.reviewCommand(accept = false))
    }

    fun acceptCandidate(
        candidate: MemoryCandidateEntity,
        editedTitle: String? = null,
        editedContent: String? = null,
    ) {
        val proposal = candidate.toProposal().let { original ->
            if (editedTitle == null && editedContent == null) original
            else original.copy(
                title = editedTitle ?: original.title,
                content = editedContent ?: original.content,
            )
        }
        review(
            MemoryReviewCommand.Accept(
                candidateId = candidate.id,
                expectedScopeId = candidate.scopeId,
                editedProposal = proposal,
            ),
        )
    }

    fun rejectCandidate(candidate: MemoryCandidateEntity) {
        review(
            MemoryReviewCommand.Reject(
                candidateId = candidate.id,
                expectedScopeId = candidate.scopeId,
            ),
        )
    }

    /**
     * Applies only candidates that still meet the same clean CREATE policy used by automatic
     * write-back. The database review path rechecks revisions and duplicate conflicts per row.
     */
    fun acceptSafeNewCandidates() {
        reviewPendingCandidates { candidate ->
            candidate.takeIf { it.status == "PENDING_REVIEW" }
                ?.takeIf { candidate.hasNoReviewFlags() }
                ?.takeIf { candidatePolicy.isSafeNewCreate(candidate.toProposal()) }
                ?.let {
                    MemoryReviewCommand.Accept(
                        candidateId = it.id,
                        expectedScopeId = it.scopeId,
                    )
                }
        }
    }

    /** Rejects pending candidates only; conflicts remain visible for an explicit user decision. */
    fun rejectAllPendingCandidates() {
        reviewPendingCandidates { candidate ->
            candidate.takeIf { it.status == "PENDING_REVIEW" }
                ?.let {
                    MemoryReviewCommand.Reject(
                        candidateId = it.id,
                        expectedScopeId = it.scopeId,
                    )
                }
        }
    }

    private fun reviewPendingCandidates(
        commandFor: (MemoryCandidateEntity) -> MemoryReviewCommand?,
    ) {
        viewModelScope.launch(Dispatchers.IO) {
            var changed = 0
            var conflict = false
            val pending = memoryV2Dao.observePendingCandidates(currentCommandScopeId()).first()
            pending.forEach { candidate ->
                val command = commandFor(candidate) ?: return@forEach
                when (coordinator.review(command)) {
                    is MemoryReviewResult.Applied,
                    MemoryReviewResult.Rejected,
                    -> changed++

                    MemoryReviewResult.Conflict -> conflict = true
                    MemoryReviewResult.AlreadyResolved,
                    MemoryReviewResult.NotFound,
                    is MemoryReviewResult.Failed,
                    -> Unit
                }
            }
            lastActionMessage.value = when {
                changed > 0 -> "accepted"
                conflict -> "conflict"
                else -> "already_resolved"
            }
        }
    }

    suspend fun loadMemories(
        candidate: MemoryCandidateEntity,
        ids: List<Int>,
    ): List<MemoryEntity> =
        if (ids.isEmpty()) emptyList() else memoryDao.getMemoriesByIds(ids, candidate.scopeId)

    suspend fun resolveSource(candidate: MemoryCandidateEntity): MemorySourceLocation? {
        val conversationId = runCatching { Uuid.parse(candidate.sourceConversationId) }.getOrNull()
            ?: return null
        val conversation = conversationRepository.getConversationById(conversationId)
            ?: return null
        val evidence = runCatching {
            JsonInstant.decodeFromString<List<String>>(candidate.evidenceMessageIdsJson)
        }.getOrDefault(emptyList())
        val nodeId = evidence.asSequence()
            .mapNotNull { raw -> runCatching { Uuid.parse(raw) }.getOrNull() }
            .mapNotNull { messageId -> conversation.getMessageNodeByMessageId(messageId)?.id }
            .firstOrNull()
        return MemorySourceLocation(conversationId, nodeId)
    }

    fun archive(memory: MemoryEntity) {
        mutate(
            MemoryMutationCommand.Archive(
                memoryId = memory.id,
                expectedScopeId = memory.assistantId,
                expectedRevision = memory.revision,
                approvalSource = MemoryApprovalSource.MANUAL_UI,
            ),
        )
    }

    fun restore(memory: MemoryEntity) {
        mutate(
            MemoryMutationCommand.Restore(
                memoryId = memory.id,
                expectedScopeId = memory.assistantId,
                expectedRevision = memory.revision,
                approvalSource = MemoryApprovalSource.MANUAL_UI,
            ),
        )
    }

    fun updateMemory(memory: MemoryEntity, input: MemoryWriteInput) {
        mutate(
            MemoryMutationCommand.Update(
                memoryId = memory.id,
                expectedScopeId = memory.assistantId,
                expectedRevision = memory.revision,
                title = input.title,
                content = input.content,
                kind = input.kind,
                tags = input.tags,
                importance = input.importance,
                expiryUpdate = input.expiresAtMs?.let(
                    me.rerere.rikkahub.memory.MemoryExpiryUpdate::Set,
                ) ?: input.expiryUpdate,
                approvalSource = MemoryApprovalSource.MANUAL_UI,
            ),
        )
    }

    fun restoreRevision(memory: MemoryEntity, revision: Int) {
        mutate(
            MemoryMutationCommand.RestoreRevision(
                memoryId = memory.id,
                expectedScopeId = memory.assistantId,
                expectedCurrentRevision = memory.revision,
                revision = revision,
                approvalSource = MemoryApprovalSource.MANUAL_UI,
            ),
        )
    }

    fun revisions(memory: MemoryEntity) =
        memoryV2Dao.observeRevisions(memory.id, memory.assistantId)

    fun createMemory(input: MemoryWriteInput) {
        viewModelScope.launch(Dispatchers.IO) {
            val command = MemoryMutationCommand.Create(
                scopeId = currentCommandScopeId(),
                title = input.title,
                content = input.content,
                kind = input.kind ?: me.rerere.rikkahub.memory.MemoryKind.OTHER,
                tags = input.tags.orEmpty(),
                importance = input.importance ?: 0.5f,
                confidence = input.confidence ?: 1f,
                expiresAtMs = input.expiresAtMs,
                approvalSource = MemoryApprovalSource.MANUAL_UI,
                sourceType = "MANUAL_UI",
                originAssistantId = assistantId.toString(),
            )
            lastActionMessage.value = mutationCoordinator.mutate(command)
                .toMemoryMutationUiFeedback()
                .actionMessageCode
        }
    }

    fun mutate(command: MemoryMutationCommand) {
        viewModelScope.launch(Dispatchers.IO) {
            lastActionMessage.value = mutationCoordinator.mutate(command)
                .toMemoryMutationUiFeedback()
                .actionMessageCode
        }
    }

    fun runRecallTest(query: String) {
        val normalized = query.trim()
        if (normalized.isEmpty()) return
        recallTestState.value = MemoryRecallTestState.Loading
        viewModelScope.launch(Dispatchers.IO) {
            runCatching {
                memoryRepository.queryDetailed(
                    assistantId = assistantId,
                    query = normalized,
                    includeGlobal = viewGlobal.value,
                    limit = 8,
                )
            }.onSuccess { results ->
                recallTestState.value = MemoryRecallTestState.Ready(
                    query = normalized,
                    results = results,
                    usedCharacters = results.sumOf { it.content.length },
                    characterBudget = DEFAULT_MEMORY_PROMPT_MAX_CHARS,
                )
            }.onFailure { error ->
                recallTestState.value = MemoryRecallTestState.Failed(error.message.orEmpty())
            }
        }
    }

    fun openDreamClaim(target: DreamClaimMutationTarget) {
        if (target.fence.scopeId != currentDreamScopeId()) {
            dreamDetailState.value = MemoryDreamDetailState.Failed(target, "scope_changed")
            return
        }
        dreamDetailState.value = MemoryDreamDetailState.Loading(target)
        viewModelScope.launch(Dispatchers.IO) {
            val next = try {
                when (val result = dreamReviewRepository.readClaim(target)) {
                    is DreamReviewReadResult.Found -> MemoryDreamDetailState.Ready(result.value)
                    is DreamReviewReadResult.Conflict -> {
                        MemoryDreamDetailState.Failed(target, "conflict_${result.conflict.name.lowercase()}")
                    }
                    DreamReviewReadResult.NotFound -> MemoryDreamDetailState.Failed(target, "not_found")
                    DreamReviewReadResult.InvalidState -> MemoryDreamDetailState.Failed(target, "invalid_state")
                    DreamReviewReadResult.Corrupt -> MemoryDreamDetailState.Failed(target, "corrupt")
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                MemoryDreamDetailState.Failed(target, "failed")
            }
            val current = dreamDetailState.value
            if (current is MemoryDreamDetailState.Loading &&
                current.target == target &&
                target.fence.scopeId == currentDreamScopeId()
            ) {
                dreamDetailState.value = next
            }
        }
    }

    fun closeDreamClaim() {
        dreamDetailState.value = MemoryDreamDetailState.Closed
    }

    /** Sensitive excerpts are deliberately ephemeral and must not survive backgrounding. */
    fun onDreamUiHidden() = closeDreamClaim()

    fun revealDreamEvidence(summary: DreamEvidenceSummary) {
        val ready = dreamDetailState.value as? MemoryDreamDetailState.Ready ?: return
        if (summary !in ready.detail.evidence ||
            summary.reference.scopeId != currentDreamScopeId() ||
            summary.reference in ready.revealingEvidence
        ) {
            return
        }
        dreamDetailState.value = ready.copy(
            revealingEvidence = ready.revealingEvidence + summary.reference,
        )
        viewModelScope.launch(Dispatchers.IO) {
            val result = try {
                dreamReviewRepository.revealEvidence(summary.reference)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                DreamEvidenceRevealResult.Corrupt
            }
            val latest = dreamDetailState.value as? MemoryDreamDetailState.Ready ?: return@launch
            if (latest.detail.target != ready.detail.target) return@launch
            dreamDetailState.value = when (result) {
                is DreamEvidenceRevealResult.Revealed -> latest.copy(
                    revealedEvidence = latest.revealedEvidence + (summary.reference to result.excerpt),
                    revealingEvidence = latest.revealingEvidence - summary.reference,
                )
                is DreamEvidenceRevealResult.Invalid,
                DreamEvidenceRevealResult.NotFound,
                DreamEvidenceRevealResult.Corrupt,
                -> latest.copy(revealingEvidence = latest.revealingEvidence - summary.reference)
            }
        }
    }

    fun rejectDreamClaim(target: DreamClaimMutationTarget) {
        if (target.fence.scopeId != currentDreamScopeId()) {
            lastActionMessage.value = "conflict"
            return
        }
        viewModelScope.launch(Dispatchers.IO) {
            val result = try {
                dreamReviewRepository.reject(target)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                DreamReviewMutationResult.Corrupt
            }
            lastActionMessage.value = result.toDreamActionMessage()
            if (result is DreamReviewMutationResult.Applied) closeDreamClaim()
        }
    }

    fun correctDreamClaim(draft: DreamCorrectionDraft) {
        if (draft.target.fence.scopeId != currentDreamScopeId()) {
            lastActionMessage.value = "conflict"
            return
        }
        viewModelScope.launch(Dispatchers.IO) {
            val result = try {
                dreamReviewRepository.correct(draft)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                DreamCorrectionResult.Corrupt
            }
            lastActionMessage.value = result.toDreamActionMessage()
            if (result is DreamCorrectionResult.Applied ||
                result is DreamCorrectionResult.PairApplied ||
                result is DreamCorrectionResult.AuthorityAppliedRebuildPending
            ) {
                closeDreamClaim()
            }
        }
    }

    fun clearDreamDerived(fence: DreamReviewFence) {
        if (fence.scopeId != currentDreamScopeId()) {
            lastActionMessage.value = "conflict"
            return
        }
        viewModelScope.launch(Dispatchers.IO) {
            val result = try {
                dreamReviewRepository.clearDerived(fence)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                DreamReviewMutationResult.Corrupt
            }
            lastActionMessage.value = result.toDreamActionMessage()
            if (result is DreamReviewMutationResult.Applied || result is DreamReviewMutationResult.AlreadyClear) {
                closeDreamClaim()
            }
        }
    }

    fun updateDreamingScopePreference(mutation: DreamingScopePreferenceMutation) {
        val renderedScope = currentDreamScopeId()
        viewModelScope.launch(Dispatchers.IO) {
            try {
                val change = settingsStore.updateDreamingScopePreferences(renderedScope, mutation)
                dreamSynthesisCoordinator.onSettingsChanged(
                    renderedScope,
                    change.previous,
                    change.current,
                )
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                lastActionMessage.value = "failed"
            }
        }
    }

    fun updateDreamingCostPolicy(policy: DreamingCostPolicy) {
        viewModelScope.launch(Dispatchers.IO) {
            try {
                val change = settingsStore.updateDreamingCostPolicy(policy)
                dreamSynthesisCoordinator.onCostPolicyChanged(change.previous, change.current)
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                lastActionMessage.value = "failed"
            }
        }
    }

    private fun currentDreamScopeId(): DreamScopeId = pairDreamScopeId

    companion object {
        const val MIN_IDLE_MINUTES = 1
        const val MAX_IDLE_MINUTES = 1_440
        const val MIN_IMMEDIATE_THRESHOLD = 1
        const val MAX_IMMEDIATE_THRESHOLD = 50
        const val MIN_CONVERSATION_CONTEXT_TURNS = 3
        const val MAX_CONVERSATION_CONTEXT_TURNS = 30
        const val MAX_NARRATIVE_NAME_CHARS = 80

        val USER_CONFIGURABLE_ORIGINS = setOf(
            MemoryCaptureOrigin.APP_UI,
            MemoryCaptureOrigin.SYSTEM_ASSISTANT,
            MemoryCaptureOrigin.QUICK_CAPTURE,
            MemoryCaptureOrigin.TELEGRAM,
            MemoryCaptureOrigin.WEB_API,
        )
    }
}

private fun DreamReviewMutationResult.toDreamActionMessage(): String = when (this) {
    is DreamReviewMutationResult.Applied -> "accepted"
    is DreamReviewMutationResult.Conflict -> "conflict"
    DreamReviewMutationResult.AlreadyClear -> "already_resolved"
    DreamReviewMutationResult.NotFound -> "not_found"
    DreamReviewMutationResult.InvalidState,
    DreamReviewMutationResult.Corrupt,
    -> "failed"
}

private fun DreamCorrectionResult.toDreamActionMessage(): String = when (this) {
    is DreamCorrectionResult.Applied -> "accepted"
    is DreamCorrectionResult.PairApplied -> "accepted"
    is DreamCorrectionResult.AuthorityAppliedRebuildPending -> "dream_authority_applied_rebuild_pending"
    is DreamCorrectionResult.Conflict -> "conflict"
    DreamCorrectionResult.NotFound -> "not_found"
    DreamCorrectionResult.InvalidState,
    DreamCorrectionResult.Corrupt,
    is DreamCorrectionResult.AuthorityRejected,
    -> "failed"
}

internal fun memoryCenterStats(
    active: Int,
    archived: Int,
    pendingReview: Int,
    captures: MemoryCaptureStatusCounts,
    lastProcessedAtMs: Long?,
    pendingRelationReview: Int = 0,
) = MemoryCenterStats(
    active = active,
    archived = archived,
    pendingReview = pendingReview + pendingRelationReview,
    pendingRelationReview = pendingRelationReview,
    pendingCaptures = captures.pendingCaptures,
    processingCaptures = captures.processingCaptures,
    processedCaptures = captures.processedCaptures,
    noLongTermSignalCaptures = captures.noLongTermSignalCaptures,
    failedCaptures = captures.failedCaptures,
    pausedCaptures = captures.pausedCaptures,
    discardedCaptures = captures.discardedCaptures,
    lastProcessedAtMs = lastProcessedAtMs,
)

private fun MemoryCandidateEntity.toProposal() = MemoryProposal(
    action = runCatching { MemoryCandidateAction.valueOf(action) }
        .getOrDefault(MemoryCandidateAction.IGNORE),
    targetIds = runCatching { JsonInstant.decodeFromString<List<Int>>(targetMemoryIdsJson) }
        .getOrDefault(emptyList()),
    expectedRevisions = runCatching {
        JsonInstant.decodeFromString<List<Int>>(expectedRevisionsJson)
    }.getOrDefault(emptyList()),
    title = title,
    content = content,
    kind = runCatching { MemoryKind.valueOf(memoryKind) }.getOrDefault(MemoryKind.OTHER),
    tags = runCatching { JsonInstant.decodeFromString<List<String>>(tagsJson) }
        .getOrDefault(emptyList()),
    importance = importance,
    confidence = confidence,
    expiresAtMs = expiresAtMs,
    evidenceMessageIds = runCatching {
        JsonInstant.decodeFromString<List<String>>(evidenceMessageIdsJson)
    }.getOrDefault(emptyList()),
    reason = reason,
)

/** Unknown or malformed persisted flags fail closed so bulk acceptance cannot bypass review. */
private fun MemoryCandidateEntity.hasNoReviewFlags(): Boolean = runCatching {
    JsonInstant.decodeFromString<List<String>>(riskFlagsJson).isEmpty()
}.getOrDefault(false)
