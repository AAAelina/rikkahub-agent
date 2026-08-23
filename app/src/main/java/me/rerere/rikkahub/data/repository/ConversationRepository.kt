package me.rerere.rikkahub.data.repository

import android.database.sqlite.SQLiteBlobTooBigException
import android.util.Log
import androidx.paging.Pager
import androidx.paging.PagingConfig
import androidx.paging.PagingData
import androidx.paging.PagingSource
import androidx.paging.map
import androidx.room.withTransaction
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.withContext
import me.rerere.ai.ui.UIMessage
import me.rerere.rikkahub.data.db.AppDatabase
import me.rerere.rikkahub.data.db.fts.MessageFtsManager
import me.rerere.rikkahub.data.db.fts.MessageSearchSort
import me.rerere.rikkahub.data.db.dao.ConversationDAO
import me.rerere.rikkahub.data.db.dao.FavoriteDAO
import me.rerere.rikkahub.data.db.dao.MessageNodeDAO
import me.rerere.rikkahub.data.db.entity.ConversationEntity
import me.rerere.rikkahub.data.db.entity.MessageNodeEntity
import me.rerere.rikkahub.data.files.FilesManager
import me.rerere.rikkahub.data.model.Conversation
import me.rerere.rikkahub.data.model.MessageNode
import me.rerere.rikkahub.data.authority.source.ConversationSourceAuthorityCommit
import me.rerere.rikkahub.data.authority.source.ConversationSourceAuthorityWriter
import me.rerere.rikkahub.data.authority.source.ConversationSourceScope
import me.rerere.rikkahub.data.authority.source.ConversationSourceScopeKind
import me.rerere.rikkahub.data.authority.source.ConversationSourceSnapshot
import me.rerere.rikkahub.data.authority.source.ConversationSourceSnapshotFactory
import me.rerere.rikkahub.data.authority.transaction.ConversationGraphAuthorityMutation
import me.rerere.rikkahub.data.authority.transaction.TransientConversationFinalizationAuthorityCoordinator
import me.rerere.rikkahub.memory.MemorySourceVersion
import me.rerere.rikkahub.memory.MemoryScopeSourceInvalidation
import me.rerere.rikkahub.memory.MemorySourceInvalidationBatch
import me.rerere.rikkahub.memory.memoryCaptureSourcesForMessage
import me.rerere.rikkahub.memory.memorySourceTextDigest
import me.rerere.rikkahub.utils.JsonInstant
import java.time.Instant
import kotlin.uuid.Uuid

class ConversationRepository(
    private val conversationDAO: ConversationDAO,
    private val messageNodeDAO: MessageNodeDAO,
    private val favoriteDAO: FavoriteDAO,
    private val database: AppDatabase,
    private val filesManager: FilesManager,
    private val messageFtsManager: MessageFtsManager,
    private val deletionPolicy: ConversationDeletionPolicy,
    private val memoryRepository: MemoryRepository,
    private val sourceAuthorityWriter: ConversationSourceAuthorityWriter,
    private val transientFinalizationAuthority: TransientConversationFinalizationAuthorityCoordinator,
) {
    companion object {
        private const val TAG = "ConversationRepository"
        private const val PAGE_SIZE = 20
        private const val INITIAL_LOAD_SIZE = 40
    }

    suspend fun listRecentConversationSummaries(
        excludeConversationId: Uuid,
        limit: Int = 20,
    ): List<LightConversationEntity> = conversationDAO.getRecentConversationSummaries(
        excludeConversationId = excludeConversationId.toString(),
        limit = limit.coerceIn(1, 50),
    )

    suspend fun getConversationSummaryById(conversationId: Uuid): LightConversationEntity? =
        conversationDAO.getConversationSummaryById(conversationId.toString())

    suspend fun getRecentNodeEntitiesBefore(
        conversationId: Uuid,
        beforeNodeIndex: Int?,
        limit: Int,
    ): List<MessageNodeEntity> = messageNodeDAO.getRecentNodesBefore(
        conversationId = conversationId.toString(),
        beforeNodeIndex = beforeNodeIndex,
        limit = limit.coerceIn(1, 64),
    )

    suspend fun getNodeEntitiesByIds(
        conversationId: Uuid,
        nodeIds: List<String>,
    ): List<MessageNodeEntity> = if (nodeIds.isEmpty()) emptyList() else
        messageNodeDAO.getNodesByIds(conversationId.toString(), nodeIds.distinct().take(50))

    suspend fun hasNodeBefore(conversationId: Uuid, beforeNodeIndex: Int): Boolean =
        messageNodeDAO.hasNodeBefore(conversationId.toString(), beforeNodeIndex)

    suspend fun searchMessagesInConversation(
        keyword: String,
        conversationId: Uuid,
        sort: MessageSearchSort = MessageSearchSort.RELEVANCE,
        limit: Int = 20,
        offset: Int = 0,
    ) = messageFtsManager.searchInConversation(
        keyword = keyword,
        conversationId = conversationId.toString(),
        sort = sort,
        limit = limit,
        offset = offset,
    )

    suspend fun getRecentConversations(assistantId: Uuid, limit: Int = 10): List<Conversation> {
        return conversationDAO.getRecentConversationsOfAssistant(
            assistantId = assistantId.toString(),
            limit = limit
        ).map { entity ->
            val nodes = loadMessageNodes(entity.id)
            conversationEntityToConversation(entity, nodes)
        }
    }

    fun getConversationsOfAssistant(assistantId: Uuid): Flow<List<Conversation>> {
        return conversationDAO
            .getConversationsOfAssistant(assistantId.toString())
            .map { flow ->
                flow.map { entity ->
                    // 列表视图不需要完整的 nodes，使用空列表
                    conversationEntityToConversation(entity, emptyList())
                }
            }
    }

    fun getConversationsOfAssistantPaging(assistantId: Uuid): Flow<PagingData<Conversation>> = Pager(
        config = PagingConfig(
            pageSize = PAGE_SIZE,
            initialLoadSize = INITIAL_LOAD_SIZE,
            enablePlaceholders = false
        ),
        pagingSourceFactory = { conversationDAO.getConversationsOfAssistantPaging(assistantId.toString()) }
    ).flow.map { pagingData ->
        pagingData.map { entity ->
            conversationSummaryToConversation(entity)
        }
    }

    suspend fun getConversationsOfAssistantPage(
        assistantId: Uuid,
        offset: Int,
        limit: Int,
    ): ConversationPageResult {
        val pagingSource = conversationDAO.getConversationsOfAssistantPaging(assistantId.toString())
        return try {
            when (
                val result = pagingSource.load(
                    PagingSource.LoadParams.Refresh(
                        key = if (offset == 0) null else offset,
                        loadSize = limit,
                        placeholdersEnabled = false
                    )
                )
            ) {
                is PagingSource.LoadResult.Page -> ConversationPageResult(
                    items = result.data.map { entity ->
                        conversationSummaryToConversation(entity)
                    },
                    nextOffset = result.nextKey
                )

                is PagingSource.LoadResult.Error -> throw result.throwable
                is PagingSource.LoadResult.Invalid -> ConversationPageResult(emptyList(), null)
            }
        } finally {
            pagingSource.invalidate()
        }
    }

    suspend fun searchConversationsOfAssistantPage(
        assistantId: Uuid,
        titleKeyword: String,
        offset: Int,
        limit: Int,
    ): ConversationPageResult {
        val pagingSource = conversationDAO.searchConversationsOfAssistantPaging(
            assistantId = assistantId.toString(),
            searchText = titleKeyword
        )
        return try {
            when (
                val result = pagingSource.load(
                    PagingSource.LoadParams.Refresh(
                        key = if (offset == 0) null else offset,
                        loadSize = limit,
                        placeholdersEnabled = false
                    )
                )
            ) {
                is PagingSource.LoadResult.Page -> ConversationPageResult(
                    items = result.data.map { entity ->
                        conversationSummaryToConversation(entity)
                    },
                    nextOffset = result.nextKey
                )

                is PagingSource.LoadResult.Error -> throw result.throwable
                is PagingSource.LoadResult.Invalid -> ConversationPageResult(emptyList(), null)
            }
        } finally {
            pagingSource.invalidate()
        }
    }

    fun searchConversations(titleKeyword: String): Flow<List<Conversation>> {
        return conversationDAO
            .searchConversations(titleKeyword)
            .map { flow ->
                flow.map { entity ->
                    conversationEntityToConversation(entity, emptyList())
                }
            }
    }

    fun searchConversationsPaging(titleKeyword: String): Flow<PagingData<Conversation>> = Pager(
        config = PagingConfig(
            pageSize = PAGE_SIZE,
            initialLoadSize = INITIAL_LOAD_SIZE,
            enablePlaceholders = false
        ),
        pagingSourceFactory = { conversationDAO.searchConversationsPaging(titleKeyword) }
    ).flow.map { pagingData ->
        pagingData.map { entity ->
            conversationSummaryToConversation(entity)
        }
    }

    fun searchConversationsOfAssistant(assistantId: Uuid, titleKeyword: String): Flow<List<Conversation>> {
        return conversationDAO
            .searchConversationsOfAssistant(assistantId.toString(), titleKeyword)
            .map { flow ->
                flow.map { entity ->
                    conversationEntityToConversation(entity, emptyList())
                }
            }
    }

    fun searchConversationsOfAssistantPaging(assistantId: Uuid, titleKeyword: String): Flow<PagingData<Conversation>> =
        Pager(
            config = PagingConfig(
                pageSize = PAGE_SIZE,
                initialLoadSize = INITIAL_LOAD_SIZE,
                enablePlaceholders = false
            ),
            pagingSourceFactory = {
                conversationDAO.searchConversationsOfAssistantPaging(
                    assistantId.toString(),
                    titleKeyword
                )
            }
        ).flow.map { pagingData ->
            pagingData.map { entity ->
                conversationSummaryToConversation(entity)
            }
        }

    suspend fun getConversationById(uuid: Uuid): Conversation? {
        val entity = conversationDAO.getConversationById(uuid.toString())
        return if (entity != null) {
            val nodes = loadMessageNodes(entity.id)
            conversationEntityToConversation(entity, nodes)
        } else null
    }

    suspend fun getConversationIdsOfAssistant(assistantId: Uuid): List<Uuid> =
        conversationDAO.getAllIdsOfAssistant(assistantId.toString())
            .mapNotNull { raw -> runCatching { Uuid.parse(raw) }.getOrNull() }

    suspend fun existsConversationById(uuid: Uuid): Boolean {
        return conversationDAO.existsById(uuid.toString())
    }

    /** Lightweight ownership lookup; never loads or deserializes message nodes. */
    suspend fun getAssistantIdOfConversation(uuid: Uuid): Uuid? =
        conversationDAO.getAssistantIdByConversationId(uuid.toString())
            ?.let { raw -> runCatching { Uuid.parse(raw) }.getOrNull() }

    suspend fun countConversations(): Int {
        return conversationDAO.countAll()
    }

    suspend fun insertConversation(conversation: Conversation) {
        val authorityCommits = database.withTransaction {
            persistConversationInCurrentTransaction(conversation, insert = true)
            reconcileOrdinarySourceInCurrentTransaction(conversation)
        }
        authorityCommits.forEach(sourceAuthorityWriter::dispatchPostCommit)
        messageFtsManager.indexConversation(conversation)
    }

    suspend fun updateConversation(
        conversation: Conversation,
        sourceInvalidationMode: ConversationSourceInvalidationMode =
            ConversationSourceInvalidationMode.APPLY,
        sourceInvalidationNowMs: Long = System.currentTimeMillis(),
    ): ConversationUpdateResult {
        // Read the stored owner before writing. A caller can update title, prompt, pinned state,
        // and message graph on the protected second-user session, but no in-app route may move
        // that session to another assistant and thereby evade its deletion/authority guard.
        val stored = getConversationById(conversation.id)
            ?: return ConversationUpdateResult.Missing(conversation.id)
        if (stored.assistantId != conversation.assistantId && !deletionPolicy.canReassignAssistant(stored)) {
            return ConversationUpdateResult.RetainedSecondUser(stored.id)
        }
        val authorityCommits = database.withTransaction {
            persistConversationInCurrentTransaction(
                conversation = conversation,
                insert = false,
                sourceInvalidationMode = sourceInvalidationMode,
                sourceInvalidationNowMs = sourceInvalidationNowMs,
            )
            if (sourceInvalidationMode == ConversationSourceInvalidationMode.SKIP_TRANSIENT_WRITE) {
                emptyList()
            } else {
                reconcileOrdinarySourceInCurrentTransaction(
                    conversation = conversation,
                    occurredAtMs = sourceInvalidationNowMs,
                )
            }
        }
        authorityCommits.forEach(sourceAuthorityWriter::dispatchPostCommit)
        messageFtsManager.indexConversation(conversation)
        return ConversationUpdateResult.Updated(conversation.id)
    }

    /**
     * Persists the executable conversation graph inside an already-open Room transaction.
     *
     * Approval lifecycle code uses this narrow entry point so the message payload, redacted
     * approval projection, execution snapshot, and execution event either commit together or
     * all roll back. Search indexing is intentionally performed after that critical commit.
     */
    suspend fun persistConversationInCurrentTransaction(
        conversation: Conversation,
        insert: Boolean? = null,
        sourceInvalidationMode: ConversationSourceInvalidationMode =
            ConversationSourceInvalidationMode.APPLY,
        sourceInvalidationNowMs: Long = System.currentTimeMillis(),
    ) {
        check(database.inTransaction()) { "conversation_transaction_required" }
        val entity = conversationToConversationEntity(conversation)
        val shouldInsert = insert ?: !conversationDAO.existsById(conversation.id.toString())
        if (shouldInsert) {
            conversationDAO.insert(entity)
        } else {
            if (sourceInvalidationMode == ConversationSourceInvalidationMode.APPLY) {
                val storedEntity = conversationDAO.getConversationById(conversation.id.toString())
                if (storedEntity != null) {
                    val previousSources = loadPersistedSelectedSourceVersionsInCurrentTransaction(
                        conversation.id.toString(),
                    )
                    val nextSources = conversation.selectedMemorySourceVersions()
                    val plan = planConversationSourceInvalidation(
                        previousAssistantScopeId = storedEntity.assistantId,
                        nextAssistantScopeId = conversation.assistantId.toString(),
                        previousSelectedMessageIds = previousSources.messageIds(),
                        nextSelectedMessageIds = nextSources.messageIds(),
                        previousSelectedSourceVersions = previousSources,
                        nextSelectedSourceVersions = nextSources,
                    )
                    applySourceInvalidationPlan(
                        conversationId = conversation.id.toString(),
                        plan = plan,
                        nowMs = sourceInvalidationNowMs,
                    )
                }
            }
            conversationDAO.update(entity)
            messageNodeDAO.deleteByConversation(conversation.id.toString())
        }
        saveMessageNodes(conversation.id.toString(), conversation.messageNodes)
    }

    /**
     * Exact graph mutation used by the combined command coordinators. It deliberately does not
     * reconcile or dispatch on its own: the outer coordinator owns both operations.
     */
    suspend fun persistAuthorityGraphInCurrentTransaction(
        conversation: Conversation,
        scope: ConversationSourceScope,
        insert: Boolean? = null,
        sourceInvalidationMode: ConversationSourceInvalidationMode =
            ConversationSourceInvalidationMode.APPLY,
        sourceInvalidationNowMs: Long = System.currentTimeMillis(),
    ): ConversationSourceSnapshot {
        persistConversationInCurrentTransaction(
            conversation = conversation,
            insert = insert,
            sourceInvalidationMode = sourceInvalidationMode,
            sourceInvalidationNowMs = sourceInvalidationNowMs,
        )
        return ConversationSourceSnapshotFactory.fromConversation(
            scope = scope,
            conversation = conversation,
            occurredAtMs = sourceInvalidationNowMs,
        )
    }

    private suspend fun reconcileOrdinarySourceInCurrentTransaction(
        conversation: Conversation,
        occurredAtMs: Long = System.currentTimeMillis(),
    ): List<ConversationSourceAuthorityCommit> {
        check(database.inTransaction()) { "conversation_source_authority_transaction_required" }
        val snapshot = ConversationSourceSnapshotFactory.fromConversation(
            scope = ConversationSourceScope(
                ConversationSourceScopeKind.ASSISTANT,
                conversation.assistantId.toString(),
            ),
            conversation = conversation,
            occurredAtMs = occurredAtMs,
        )
        return sourceAuthorityWriter.reconcileAllKnownScopesInCurrentTransaction(snapshot)
    }

    /**
     * Atomically commits a regeneration's final graph and the source invalidation calculated from
     * its durable pre-regeneration selected branch. Intermediate regeneration snapshots must use
     * [ConversationSourceInvalidationMode.SKIP_TRANSIENT_WRITE] and finish through this method.
     */
    suspend fun finalizeTransientConversationUpdate(
        conversation: Conversation,
        baselineAssistantScopeId: String,
        baselineSelectedMessageIds: Collection<String>,
        baselineSelectedSourceVersions: Collection<MemorySourceVersion> = emptyList(),
        sourceInvalidationNowMs: Long,
    ): ConversationUpdateResult {
        val normalizedBaselineScopeId = baselineAssistantScopeId.trim()
        if (normalizedBaselineScopeId.isEmpty()) {
            return ConversationUpdateResult.RetainedSecondUser(conversation.id)
        }
        try {
            transientFinalizationAuthority.finish(
                graphMutation = ConversationGraphAuthorityMutation {
                    val conversationId = conversation.id.toString()
                    val authoritativeEntity = conversationDAO.getConversationById(conversationId)
                        ?: throw TransientConversationFinalizationRejected(
                            TransientConversationFinalizationRejection.CONVERSATION_MISSING,
                        )
                    // Regeneration is never an ownership mutation. Check both the final graph and
                    // its durable pre-regeneration owner under the same write transaction.
                    if (authoritativeEntity.assistantId != conversation.assistantId.toString() ||
                        authoritativeEntity.assistantId != normalizedBaselineScopeId
                    ) {
                        throw TransientConversationFinalizationRejected(
                            TransientConversationFinalizationRejection.OWNER_CHANGED,
                        )
                    }
                    val nextSources = conversation.selectedMemorySourceVersions()
                    val plan = planConversationSourceInvalidation(
                        previousAssistantScopeId = normalizedBaselineScopeId,
                        nextAssistantScopeId = conversation.assistantId.toString(),
                        previousSelectedMessageIds = baselineSelectedMessageIds.toSet(),
                        nextSelectedMessageIds = nextSources.messageIds(),
                        previousSelectedSourceVersions = baselineSelectedSourceVersions.toSet(),
                        nextSelectedSourceVersions = nextSources,
                    )
                    applySourceInvalidationPlan(
                        conversationId = conversationId,
                        plan = plan,
                        nowMs = sourceInvalidationNowMs,
                    )
                    persistAuthorityGraphInCurrentTransaction(
                        conversation = conversation,
                        scope = ConversationSourceScope(
                            kind = ConversationSourceScopeKind.ASSISTANT,
                            id = normalizedBaselineScopeId,
                        ),
                        insert = false,
                        sourceInvalidationMode =
                            ConversationSourceInvalidationMode.SKIP_TRANSIENT_WRITE,
                        sourceInvalidationNowMs = sourceInvalidationNowMs,
                    )
                },
            )
        } catch (rejected: TransientConversationFinalizationRejected) {
            return when (rejected.reason) {
                TransientConversationFinalizationRejection.CONVERSATION_MISSING ->
                    ConversationUpdateResult.Missing(conversation.id)
                TransientConversationFinalizationRejection.OWNER_CHANGED ->
                    ConversationUpdateResult.RetainedSecondUser(conversation.id)
            }
        }
        // The authority transaction is already committed. A derived FTS failure or cancellation
        // must not make the caller restore the old graph after source tombstones and the exact
        // ALR source-authority/outbox transitions became durable together.
        try {
            messageFtsManager.indexConversation(conversation)
        } catch (error: Exception) {
            Log.w(TAG, "Final regeneration search projection refresh failed", error)
        }
        return ConversationUpdateResult.Updated(conversation.id)
    }

    /** Refreshes the non-authoritative FTS projection after a critical transaction commits. */
    suspend fun refreshSearchProjection(conversation: Conversation) {
        messageFtsManager.indexConversation(conversation)
    }

    suspend fun updateConversationTitle(conversationId: Uuid, title: String) {
        conversationDAO.updateTitle(conversationId.toString(), title)
    }

    suspend fun updateConversationSuggestions(conversationId: Uuid, suggestions: List<String>) {
        conversationDAO.updateSuggestions(
            conversationId.toString(),
            JsonInstant.encodeToString(suggestions),
        )
    }

    suspend fun deleteConversation(conversation: Conversation): ConversationDeletionResult {
        // 获取完整的 Conversation（包含 messageNodes）以正确清理文件
        val fullConversation = getConversationById(conversation.id)
            ?: return ConversationDeletionResult.Missing(conversation.id)
        if (!deletionPolicy.canDelete(fullConversation)) {
            return ConversationDeletionResult.RetainedSecondUser(fullConversation.id)
        }
        var authorityCommits: List<ConversationSourceAuthorityCommit> = emptyList()
        val deleted = database.withTransaction {
            val conversationId = fullConversation.id.toString()
            val authoritativeEntity = conversationDAO.getConversationById(conversationId)
                ?: return@withTransaction false
            val sourceInvalidationNowMs = System.currentTimeMillis()
            // Captures may belong to the assistant scope or to the shared global scope. Resolve
            // exact provenance in both domains before the authoritative source messages vanish.
            // Re-read the owner under the write transaction: a concurrent assistant move after
            // the policy preflight must not leave the new assistant scope uninvaldated.
            memoryRepository.invalidateSourceConversation(
                scopeIds = setOf(
                    authoritativeEntity.assistantId,
                    MemoryRepository.GLOBAL_MEMORY_ID,
                ),
                conversationId = conversationId,
                nowMs = sourceInvalidationNowMs,
            )
            authorityCommits = sourceAuthorityWriter.tombstoneAllScopesInCurrentTransaction(
                conversationId = conversationId,
                occurredAtMs = sourceInvalidationNowMs,
            )
            // message_node 会通过 CASCADE 自动删除
            conversationDAO.delete(authoritativeEntity)
            true
        }
        if (!deleted) return ConversationDeletionResult.Missing(fullConversation.id)
        authorityCommits.forEach(sourceAuthorityWriter::dispatchPostCommit)
        // FTS is a derived projection, so mutate it only after the authoritative transaction.
        messageFtsManager.deleteConversation(fullConversation.id.toString())
        filesManager.deleteChatFiles(fullConversation.files)
        return ConversationDeletionResult.Deleted(fullConversation.id)
    }

    suspend fun searchMessages(
        keyword: String,
        sort: MessageSearchSort = MessageSearchSort.RELEVANCE,
    ) = messageFtsManager.search(keyword, sort)

    suspend fun rebuildAllIndexes(onProgress: (current: Int, total: Int) -> Unit = { _, _ -> }) {
        messageFtsManager.deleteAll()
        val allIds = conversationDAO.getAllIds()
        val total = allIds.size
        allIds.forEachIndexed { index, id ->
            val entity = conversationDAO.getConversationById(id) ?: return@forEachIndexed
            val nodes = loadMessageNodes(entity.id)
            val conversation = conversationEntityToConversation(entity, nodes)
            messageFtsManager.indexConversation(conversation)
            onProgress(index + 1, total)
        }
    }

    /**
     * Repair the FTS5 search index when SQLite reports a malformed inverted index. Drops
     * the message_fts virtual table (frees the corrupted index pages — DELETE alone won't),
     * recreates it via the shared schema, then re-indexes every conversation. Returns the
     * number of conversations re-indexed so the Doctor can report progress.
     */
    suspend fun repairAndRebuildIndexes(onProgress: (current: Int, total: Int) -> Unit = { _, _ -> }): Int {
        messageFtsManager.dropAndRecreate()
        val allIds = conversationDAO.getAllIds()
        val total = allIds.size
        allIds.forEachIndexed { index, id ->
            val entity = conversationDAO.getConversationById(id) ?: return@forEachIndexed
            val nodes = loadMessageNodes(entity.id)
            val conversation = conversationEntityToConversation(entity, nodes)
            messageFtsManager.indexConversation(conversation)
            onProgress(index + 1, total)
        }
        return total
    }

    suspend fun deleteConversationOfAssistant(assistantId: Uuid): ConversationBatchDeletionResult {
        var deleted = 0
        val retained = mutableListOf<Uuid>()
        getConversationsOfAssistant(assistantId).first().forEach { conversation ->
            when (deleteConversation(conversation)) {
                is ConversationDeletionResult.Deleted -> deleted++
                is ConversationDeletionResult.RetainedSecondUser -> retained += conversation.id
                is ConversationDeletionResult.Missing -> Unit
            }
        }
        return ConversationBatchDeletionResult(deleted, retained)
    }

    fun conversationToConversationEntity(conversation: Conversation): ConversationEntity {
        require(conversation.messageNodes.none { it.messages.any { message -> message.hasBase64Part() } })
        return ConversationEntity(
            id = conversation.id.toString(),
            title = conversation.title,
            nodes = "[]",  // nodes 现在存储在单独的表中
            createAt = conversation.createAt.toEpochMilli(),
            updateAt = conversation.updateAt.toEpochMilli(),
            assistantId = conversation.assistantId.toString(),
            chatSuggestions = JsonInstant.encodeToString(conversation.chatSuggestions),
            isPinned = conversation.isPinned,
            customSystemPrompt = conversation.customSystemPrompt ?: "",
            modeInjectionIds = JsonInstant.encodeToString(conversation.modeInjectionIds),
            lorebookIds = JsonInstant.encodeToString(conversation.lorebookIds),
            workspaceCwd = conversation.workspaceCwd ?: "",
            folderId = conversation.folderId,
        )
    }

    fun conversationEntityToConversation(
        conversationEntity: ConversationEntity,
        messageNodes: List<MessageNode>
    ): Conversation {
        return Conversation(
            id = Uuid.parse(conversationEntity.id),
            title = conversationEntity.title,
            messageNodes = messageNodes.filter { it.messages.isNotEmpty() },
            createAt = Instant.ofEpochMilli(conversationEntity.createAt),
            updateAt = Instant.ofEpochMilli(conversationEntity.updateAt),
            assistantId = Uuid.parse(conversationEntity.assistantId),
            chatSuggestions = JsonInstant.decodeFromString(conversationEntity.chatSuggestions),
            isPinned = conversationEntity.isPinned,
            customSystemPrompt = conversationEntity.customSystemPrompt.ifEmpty { null },
            modeInjectionIds = JsonInstant.decodeFromString(conversationEntity.modeInjectionIds),
            lorebookIds = JsonInstant.decodeFromString(conversationEntity.lorebookIds),
            workspaceCwd = conversationEntity.workspaceCwd.ifEmpty { null },
            folderId = conversationEntity.folderId,
        )
    }

    fun getPinnedConversations(): Flow<List<Conversation>> {
        return conversationDAO
            .getPinnedConversations()
            .map { flow ->
                flow.map { entity ->
                    conversationEntityToConversation(entity, emptyList())
                }
            }
    }

    suspend fun togglePinStatus(conversationId: Uuid) {
        // Single atomic UPDATE — avoids the read→write TOCTOU that existed when
        // we read isPinned with getConversationById() and then flipped it.
        conversationDAO.togglePinStatus(conversationId.toString())
    }

    private fun conversationSummaryToConversation(entity: LightConversationEntity): Conversation {
        return Conversation(
            id = Uuid.parse(entity.id),
            assistantId = Uuid.parse(entity.assistantId),
            title = entity.title,
            isPinned = entity.isPinned,
            createAt = Instant.ofEpochMilli(entity.createAt),
            updateAt = Instant.ofEpochMilli(entity.updateAt),
            messageNodes = emptyList(),
        )
    }

    private suspend fun loadMessageNodes(conversationId: String): List<MessageNode> {
        val favoriteNodeIds = favoriteDAO
            .getFavoriteNodeIdsOfConversation(conversationId)
            .mapNotNull { runCatching { Uuid.parse(it) }.getOrNull() }
            .toSet()

        val entities = database.withTransaction {
            val rows = mutableListOf<MessageNodeEntity>()
            var offset = 0
            val pageSize = 64
            while (true) {
                val page = try {
                    messageNodeDAO.getNodesOfConversationPaged(conversationId, pageSize, offset)
                } catch (e: SQLiteBlobTooBigException) {
                    e.printStackTrace()
                    offset += pageSize
                    continue
                } catch (e: IllegalStateException) {
                    e.printStackTrace()
                    offset += pageSize
                    continue
                }
                if (page.isEmpty()) break
                rows += page
                offset += page.size
            }
            rows
        }
        // JSON decoding is CPU work and must not hold Room's transaction executor. Long agent
        // conversations can contain hundreds of nodes; keeping the transaction open here made
        // unrelated Paging loads appear empty while the system-assistant overlay was opening.
        return withContext(Dispatchers.Default) {
            entities.map { entity ->
                // Long agent histories can contain hundreds of JSON blobs. Keep this CPU work
                // off AppScope's main dispatcher and honour an overlay close between nodes.
                ensureActive()
                val nodeId = Uuid.parse(entity.id)
                MessageNode(
                    id = nodeId,
                    messages = JsonInstant.decodeFromString<List<UIMessage>>(entity.messages),
                    selectIndex = entity.selectIndex,
                    isFavorite = favoriteNodeIds.contains(nodeId),
                )
            }
        }
    }

    private suspend fun saveMessageNodes(conversationId: String, nodes: List<MessageNode>) {
        val entities = nodes.mapIndexed { index, node ->
            MessageNodeEntity(
                id = node.id.toString(),
                conversationId = conversationId,
                nodeIndex = index,
                messages = JsonInstant.encodeToString(node.messages),
                selectIndex = node.selectIndex
            )
        }
        messageNodeDAO.insertAll(entities)
    }

    private suspend fun loadPersistedSelectedSourceVersionsInCurrentTransaction(
        conversationId: String,
    ): Set<MemorySourceVersion> {
        check(database.inTransaction()) { "conversation_transaction_required" }
        return messageNodeDAO.getNodesOfConversation(conversationId)
            .asSequence()
            .mapNotNull { entity ->
                JsonInstant.decodeFromString<List<UIMessage>>(entity.messages)
                    .getOrNull(entity.selectIndex)
            }
            .flatMap { message -> message.memorySourceVersions().asSequence() }
            .toSet()
    }

    private suspend fun applySourceInvalidationPlan(
        conversationId: String,
        plan: ConversationSourceInvalidationPlan,
        nowMs: Long,
    ): Int {
        check(database.inTransaction()) { "conversation_transaction_required" }
        val allScopes = plan.invalidateWholeScopeIds + plan.invalidateMessageScopeIds
        if (allScopes.isEmpty()) return 0
        return memoryRepository.invalidateSources(
            batch = MemorySourceInvalidationBatch(
                conversationId = conversationId,
                scopes = allScopes.map { scopeId ->
                    MemoryScopeSourceInvalidation(
                        scopeId = scopeId,
                        invalidateWholeConversation = scopeId in plan.invalidateWholeScopeIds,
                        removedMessageIds = if (scopeId in plan.invalidateMessageScopeIds) {
                            plan.removedMessageIds
                        } else {
                            emptySet()
                        },
                        removedSourceVersions = if (scopeId in plan.invalidateMessageScopeIds) {
                            plan.changedSourceVersions
                        } else {
                            emptySet()
                        },
                    )
                },
            ),
            nowMs = nowMs,
        )
    }
}

/**
 * APPLY is the safe default for every authoritative write. SKIP_TRANSIENT_WRITE is only for a
 * persisted snapshot that is guaranteed to be restored on failure or finalized through
 * [ConversationRepository.finalizeTransientConversationUpdate].
 */
enum class ConversationSourceInvalidationMode {
    APPLY,
    SKIP_TRANSIENT_WRITE,
}

private enum class TransientConversationFinalizationRejection {
    CONVERSATION_MISSING,
    OWNER_CHANGED,
}

private class TransientConversationFinalizationRejected(
    val reason: TransientConversationFinalizationRejection,
) : IllegalStateException(reason.name)

internal data class ConversationSourceInvalidationPlan(
    val invalidateWholeScopeIds: Set<String>,
    val invalidateMessageScopeIds: Set<String>,
    val removedMessageIds: Set<String>,
    val changedSourceVersions: Set<MemorySourceVersion> = emptySet(),
)

internal fun planConversationSourceInvalidation(
    previousAssistantScopeId: String,
    nextAssistantScopeId: String,
    previousSelectedMessageIds: Set<String>,
    nextSelectedMessageIds: Set<String>,
    previousSelectedSourceVersions: Set<MemorySourceVersion> = emptySet(),
    nextSelectedSourceVersions: Set<MemorySourceVersion> = emptySet(),
): ConversationSourceInvalidationPlan {
    val previousScope = previousAssistantScopeId.trim()
    val nextScope = nextAssistantScopeId.trim()
    require(previousScope.isNotEmpty()) { "previous_assistant_scope_required" }
    require(nextScope.isNotEmpty()) { "next_assistant_scope_required" }

    val normalizedPreviousVersions = normalizeMemorySourceVersions(
        previousSelectedSourceVersions,
    )
    val normalizedNextVersions = normalizeMemorySourceVersions(nextSelectedSourceVersions)
    val normalizedPreviousIds = previousSelectedMessageIds.asSequence()
        .map(String::trim)
        .filter(String::isNotEmpty)
        .toSet() + normalizedPreviousVersions.messageIds()
    val normalizedNextIds = nextSelectedMessageIds.asSequence()
        .map(String::trim)
        .filter(String::isNotEmpty)
        .toSet() + normalizedNextVersions.messageIds()
    val removedIds = normalizedPreviousIds - normalizedNextIds
    val changedVersions = normalizedPreviousVersions.filterTo(mutableSetOf()) { oldVersion ->
        oldVersion.messageId in normalizedNextIds && oldVersion !in normalizedNextVersions
    }
    val assistantMoved = previousScope != nextScope
    val wholeScopes = if (assistantMoved) setOf(previousScope) else emptySet()
    val messageScopes = when {
        removedIds.isEmpty() && changedVersions.isEmpty() -> emptySet()
        assistantMoved -> setOf(MemoryRepository.GLOBAL_MEMORY_ID)
        else -> setOf(previousScope, MemoryRepository.GLOBAL_MEMORY_ID)
    }
    return ConversationSourceInvalidationPlan(
        invalidateWholeScopeIds = wholeScopes,
        invalidateMessageScopeIds = messageScopes - wholeScopes,
        removedMessageIds = removedIds,
        changedSourceVersions = changedVersions,
    )
}

internal fun Conversation.selectedMemorySourceVersions(): Set<MemorySourceVersion> =
    messageNodes.asSequence()
    .mapNotNull { node -> node.messages.getOrNull(node.selectIndex) }
    .flatMap { message -> message.memorySourceVersions().asSequence() }
    .toSet()

internal fun Conversation.selectedMessageIds(): Set<String> =
    selectedMemorySourceVersions().messageIds()

private fun UIMessage.memorySourceVersions(): List<MemorySourceVersion> =
    memoryCaptureSourcesForMessage(this).map { source ->
        MemorySourceVersion(
            messageId = source.messageId,
            consumedTextDigest = memorySourceTextDigest(source.text),
        )
    }

private fun Iterable<MemorySourceVersion>.messageIds(): Set<String> =
    mapTo(mutableSetOf(), MemorySourceVersion::messageId)

private fun normalizeMemorySourceVersions(
    versions: Set<MemorySourceVersion>,
): Set<MemorySourceVersion> = versions.asSequence()
    .map { version ->
        MemorySourceVersion(
            messageId = version.messageId.trim(),
            consumedTextDigest = version.consumedTextDigest.trim().lowercase(),
        )
    }
    .filter { version ->
        version.messageId.isNotEmpty() &&
            version.consumedTextDigest.length == 64 &&
            version.consumedTextDigest.all { char -> char in '0'..'9' || char in 'a'..'f' }
    }
    .toSet()

/**
 * 轻量级的会话查询结果，不包含 nodes 和 suggestions 字段
 */
data class LightConversationEntity(
    val id: String,
    val assistantId: String,
    val title: String,
    val isPinned: Boolean,
    val createAt: Long,
    val updateAt: Long,
)

data class ConversationPageResult(
    val items: List<Conversation>,
    val nextOffset: Int?,
)
