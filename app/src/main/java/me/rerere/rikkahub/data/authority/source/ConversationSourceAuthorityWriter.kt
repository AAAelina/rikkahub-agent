package me.rerere.rikkahub.data.authority.source

data class MessageSourceRevisionTransition(
    val previous: MessageSourceAuthorityHead,
    val current: MessageSourceAuthorityHead,
) {
    init {
        require(previous.scope == current.scope)
        require(previous.conversationId == current.conversationId)
        require(previous.messageId == current.messageId)
        require(current.previousSourceRevision == previous.sourceRevision)
        require(current.sourceRevision == previous.sourceRevision + 1L)
    }

    override fun toString(): String =
        "MessageSourceRevisionTransition(previous=${previous.sourceRevision}, " +
            "current=${current.sourceRevision}, state=${current.sourceState}, ids=<redacted>)"
}

/**
 * Narrow same-transaction seam for authorities whose evidence is pinned to a message revision.
 * Implementations must not open a nested transaction or dispatch work from this method. Returning
 * true means that at least one durable outbox row was inserted and the owner must wake after commit.
 */
fun interface MessageSourceTransitionInvalidationPort {
    suspend fun invalidateInCurrentTransaction(
        transition: MessageSourceRevisionTransition,
    ): Boolean
}

object NoOpMessageSourceTransitionInvalidationPort : MessageSourceTransitionInvalidationPort {
    override suspend fun invalidateInCurrentTransaction(
        transition: MessageSourceRevisionTransition,
    ): Boolean = false
}

/** Initial capture gate; existing authority heads always advance so stale evidence cannot revive. */
fun interface ConversationSourceInitialCaptureGate {
    fun allowInitialCapture(scope: ConversationSourceScope): Boolean
}

object AllowConversationSourceInitialCapture : ConversationSourceInitialCaptureGate {
    override fun allowInitialCapture(scope: ConversationSourceScope): Boolean = true
}

object DenyConversationSourceInitialCapture : ConversationSourceInitialCaptureGate {
    override fun allowInitialCapture(scope: ConversationSourceScope): Boolean = false
}

data class ConversationSourceAuthorityCommit(
    val conversation: ConversationSourceAuthorityHead,
    val previousConversation: ConversationSourceAuthorityHead?,
    val messagesById: Map<String, MessageSourceAuthorityHead>,
    val messageTransitions: List<MessageSourceRevisionTransition>,
    val insertedInitialMessageCount: Int,
    val conversationMutated: Boolean,
    val insertedOutbox: Boolean,
) {
    val didMutate: Boolean
        get() = conversationMutated || insertedInitialMessageCount > 0 || messageTransitions.isNotEmpty()

    fun requireActiveMessage(
        messageId: String,
        expectedRole: String,
    ): MessageSourceAuthorityHead {
        val head = messagesById[messageId]
            ?: throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_MISSING")
        if (head.sourceState != ConversationSourceState.ACTIVE) {
            throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_NOT_ACTIVE")
        }
        if (head.messageRole != expectedRole) {
            throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_ROLE_MISMATCH")
        }
        return head
    }

    override fun toString(): String =
        "ConversationSourceAuthorityCommit(conversation=$conversation, messages=${messagesById.size}, " +
            "transitions=${messageTransitions.size}, initial=$insertedInitialMessageCount, " +
            "mutated=$didMutate, outbox=$insertedOutbox)"
}

/**
 * Monotonic source-head writer used by the owning Conversation Room transaction.
 *
 * Payload digests are compared only to detect a changed/restored payload. The next revision always
 * comes from the currently stored head, so restoring old bytes creates another revision instead of
 * reviving the old one. A TOMBSTONED head is never writable again.
 */
class ConversationSourceAuthorityWriter(
    private val store: ConversationSourceAuthorityStore,
    private val events: SourceInvalidationAuthorityEventPort =
        NoOpSourceInvalidationAuthorityEventPort,
    private val transitionInvalidations: MessageSourceTransitionInvalidationPort =
        NoOpMessageSourceTransitionInvalidationPort,
    private val initialCaptureGate: ConversationSourceInitialCaptureGate =
        AllowConversationSourceInitialCapture,
    private val maxMessagesPerConversation: Int = DEFAULT_MAX_MESSAGES_PER_CONVERSATION,
    private val maxScopesPerConversation: Int = DEFAULT_MAX_SCOPES_PER_CONVERSATION,
    private val pageSize: Int = DEFAULT_PAGE_SIZE,
) {
    init {
        require(maxMessagesPerConversation in 1..MAX_ALLOWED_MESSAGES_PER_CONVERSATION)
        require(maxScopesPerConversation in 1..MAX_ALLOWED_SCOPES_PER_CONVERSATION)
        require(pageSize in 1..MAX_PAGE_SIZE)
    }

    /**
     * Privacy deletion helper. It snapshots every known scope before the Conversation row is
     * removed and tombstones all of them in the caller's single authority transaction.
     */
    suspend fun tombstoneAllScopesInCurrentTransaction(
        conversationId: String,
        occurredAtMs: Long,
    ): List<ConversationSourceAuthorityCommit> {
        if (!conversationId.isSourceAuthorityIdentifier()) {
            throw ConversationSourceAuthorityConflictException("SOURCE_CONVERSATION_ID_INVALID")
        }
        if (occurredAtMs < 0L) {
            throw ConversationSourceAuthorityConflictException("SOURCE_AUTHORITY_TIME_INVALID")
        }
        val count = store.countConversationScopes(conversationId)
        if (count < 0) {
            throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_COUNT_INVALID")
        }
        if (count > maxScopesPerConversation) {
            throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_LIMIT_EXCEEDED")
        }
        if (count == 0) return emptyList()

        val heads = ArrayList<ConversationSourceAuthorityHead>(count)
        var afterKind = ""
        var afterId = ""
        while (heads.size < count) {
            val page = store.listConversationScopesAfter(
                conversationId = conversationId,
                afterScopeKind = afterKind,
                afterScopeId = afterId,
                limit = minOf(pageSize, count - heads.size),
            )
            if (page.isEmpty()) {
                throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_INCOMPLETE")
            }
            page.forEach { head ->
                if (head.conversationId != conversationId) {
                    throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_IDENTITY_MISMATCH")
                }
                val key = scopeOrderKey(head.scope)
                val afterKey = "$afterKind\u0000$afterId"
                if (key <= afterKey) {
                    throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_ORDER_INVALID")
                }
                afterKind = head.scope.kind.name
                afterId = head.scope.id
                heads += head
            }
            if (heads.size > count) {
                throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_COUNT_CHANGED")
            }
        }
        if (heads.size != count || heads.map { it.scope }.toSet().size != heads.size) {
            throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_COUNT_CHANGED")
        }
        return heads.map { head ->
            reconcileInCurrentTransaction(
                ConversationSourceSnapshot(
                    scope = head.scope,
                    conversationId = head.conversationId,
                    assistantIdSnapshot = head.assistantIdSnapshot,
                    messages = emptyList(),
                    selectedBranchMessageIds = emptyList(),
                    occurredAtMs = occurredAtMs,
                    conversationDeleted = true,
                ),
            )
        }
    }

    /**
     * Reconciles every already-known scope for an ordinary Conversation graph write. The
     * supplied default scope is also created when this is the first authority write. This keeps
     * assistant edits and protected authority-subject views on the same exact graph revision
     * without asking callers to guess which scopes were previously materialised.
     */
    suspend fun reconcileAllKnownScopesInCurrentTransaction(
        snapshot: ConversationSourceSnapshot,
    ): List<ConversationSourceAuthorityCommit> {
        val count = store.countConversationScopes(snapshot.conversationId)
        if (count < 0) {
            throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_COUNT_INVALID")
        }
        if (count > maxScopesPerConversation) {
            throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_LIMIT_EXCEEDED")
        }
        val scopes = ArrayList<ConversationSourceScope>(count + 1)
        var afterKind = ""
        var afterId = ""
        while (scopes.size < count) {
            val page = store.listConversationScopesAfter(
                conversationId = snapshot.conversationId,
                afterScopeKind = afterKind,
                afterScopeId = afterId,
                limit = minOf(pageSize, count - scopes.size),
            )
            if (page.isEmpty()) {
                throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_INCOMPLETE")
            }
            page.forEach { head ->
                if (head.conversationId != snapshot.conversationId) {
                    throw ConversationSourceAuthorityConflictException(
                        "SOURCE_SCOPE_SNAPSHOT_IDENTITY_MISMATCH",
                    )
                }
                val key = scopeOrderKey(head.scope)
                val afterKey = "$afterKind\u0000$afterId"
                if (key <= afterKey) {
                    throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_ORDER_INVALID")
                }
                afterKind = head.scope.kind.name
                afterId = head.scope.id
                scopes += head.scope
            }
            if (scopes.size > count) {
                throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_COUNT_CHANGED")
            }
        }
        if (scopes.size != count || scopes.toSet().size != scopes.size) {
            throw ConversationSourceAuthorityConflictException("SOURCE_SCOPE_SNAPSHOT_COUNT_CHANGED")
        }
        if (snapshot.scope !in scopes) {
            if (initialCaptureGate.allowInitialCapture(snapshot.scope)) {
                scopes += snapshot.scope
            } else if (scopes.isEmpty()) {
                // OFF/consent-denied conversations remain byte-for-byte baseline in the DB. The
                // ephemeral projection only supplies command-transaction correlation and is
                // neither persisted nor replayable as Learning evidence.
                return listOf(snapshot.toEphemeralAuthorityCommit())
            }
        }
        return scopes.map { scope ->
            reconcileInCurrentTransaction(snapshot.copy(scope = scope))
        }
    }

    suspend fun reconcileInCurrentTransaction(
        snapshot: ConversationSourceSnapshot,
    ): ConversationSourceAuthorityCommit {
        if (snapshot.messages.size > maxMessagesPerConversation) {
            throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_LIMIT_EXCEEDED")
        }
        val existingConversation = store.findConversation(snapshot.scope, snapshot.conversationId)
        if (
            existingConversation == null &&
            !initialCaptureGate.allowInitialCapture(snapshot.scope)
        ) {
            return snapshot.toEphemeralAuthorityCommit()
        }
        if (existingConversation?.sourceState == ConversationSourceState.TOMBSTONED &&
            !snapshot.conversationDeleted
        ) {
            throw ConversationSourceAuthorityConflictException(
                "SOURCE_CONVERSATION_TOMBSTONE_REVIVAL",
            )
        }
        val existingMessages = loadCompleteMessageSnapshot(snapshot)
        val selectedIds = snapshot.selectedBranchMessageIds.toHashSet()
        val incomingById = snapshot.messages.associateBy(MessageSourceSnapshot::messageId)
        val finalMessages = LinkedHashMap<String, MessageSourceAuthorityHead>(
            maxOf(existingMessages.size, incomingById.size),
        )
        val transitions = ArrayList<MessageSourceRevisionTransition>()
        var insertedInitialMessages = 0

        incomingById.toSortedMap().forEach { (messageId, incoming) ->
            val desiredState = if (messageId in selectedIds) {
                ConversationSourceState.ACTIVE
            } else {
                ConversationSourceState.SUPERSEDED
            }
            val existing = store.findMessage(snapshot.scope, messageId)
            if (existing == null) {
                val initial = MessageSourceAuthorityHead(
                    scope = snapshot.scope,
                    conversationId = snapshot.conversationId,
                    messageId = messageId,
                    messageRole = incoming.messageRole,
                    sourceRevision = 1L,
                    previousSourceRevision = null,
                    sourceState = desiredState,
                    changeKind = ConversationSourceChangeKind.CREATED,
                    payloadIntegritySha256 = incoming.payloadIntegritySha256,
                    occurredAtMs = snapshot.occurredAtMs,
                    updatedAtMs = snapshot.occurredAtMs,
                )
                if (!store.insertMessageInitial(initial)) {
                    val raced = store.findMessage(snapshot.scope, messageId)
                    if (raced != initial) {
                        throw ConversationSourceAuthorityConflictException(
                            "SOURCE_MESSAGE_INITIAL_INSERT_CONFLICT",
                        )
                    }
                    finalMessages[messageId] = raced
                } else {
                    insertedInitialMessages++
                    finalMessages[messageId] = initial
                }
                return@forEach
            }
            requireSameMessageIdentity(existing, snapshot, messageId)
            if (existing.sourceState == ConversationSourceState.TOMBSTONED) {
                throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_TOMBSTONE_REVIVAL")
            }
            val contentChanged = existing.messageRole != incoming.messageRole ||
                existing.payloadIntegritySha256 != incoming.payloadIntegritySha256
            val stateChanged = existing.sourceState != desiredState
            if (!contentChanged && !stateChanged) {
                finalMessages[messageId] = existing
                return@forEach
            }
            requireNonRegressingTime(existing.updatedAtMs, snapshot.occurredAtMs)
            val changeKind = when {
                contentChanged -> ConversationSourceChangeKind.UPDATED
                desiredState == ConversationSourceState.ACTIVE ->
                    ConversationSourceChangeKind.BRANCH_SELECTED
                else -> ConversationSourceChangeKind.BRANCH_SUPERSEDED
            }
            val next = existing.copy(
                messageRole = incoming.messageRole,
                sourceRevision = nextRevision(existing.sourceRevision),
                previousSourceRevision = existing.sourceRevision,
                sourceState = desiredState,
                changeKind = changeKind,
                payloadIntegritySha256 = incoming.payloadIntegritySha256,
                occurredAtMs = snapshot.occurredAtMs,
                updatedAtMs = snapshot.occurredAtMs,
            )
            if (!store.updateMessageFenced(existing.sourceRevision, next)) {
                throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_REVISION_CONFLICT")
            }
            finalMessages[messageId] = next
            transitions += MessageSourceRevisionTransition(existing, next)
        }

        existingMessages.values.sortedBy(MessageSourceAuthorityHead::messageId).forEach { existing ->
            if (existing.messageId in incomingById) return@forEach
            if (existing.sourceState == ConversationSourceState.TOMBSTONED) {
                finalMessages[existing.messageId] = existing
                return@forEach
            }
            requireNonRegressingTime(existing.updatedAtMs, snapshot.occurredAtMs)
            val next = existing.copy(
                sourceRevision = nextRevision(existing.sourceRevision),
                previousSourceRevision = existing.sourceRevision,
                sourceState = ConversationSourceState.TOMBSTONED,
                changeKind = if (snapshot.conversationDeleted) {
                    ConversationSourceChangeKind.CONVERSATION_DELETED
                } else {
                    ConversationSourceChangeKind.DELETED
                },
                payloadIntegritySha256 = null,
                occurredAtMs = snapshot.occurredAtMs,
                updatedAtMs = snapshot.occurredAtMs,
            )
            if (!store.updateMessageFenced(existing.sourceRevision, next)) {
                throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_DELETE_CONFLICT")
            }
            finalMessages[existing.messageId] = next
            transitions += MessageSourceRevisionTransition(existing, next)
        }

        val branchHead = if (snapshot.conversationDeleted) {
            null
        } else {
            snapshot.branchHeadMessageId?.let { id ->
                val head = finalMessages[id]
                    ?: throw ConversationSourceAuthorityConflictException("SOURCE_BRANCH_HEAD_MISSING")
                if (head.sourceState != ConversationSourceState.ACTIVE) {
                    throw ConversationSourceAuthorityConflictException("SOURCE_BRANCH_HEAD_NOT_ACTIVE")
                }
                head
            }
        }
        val conversationMutation = reconcileConversationHead(
            snapshot = snapshot,
            existing = existingConversation,
            branchHead = branchHead,
            messageAuthorityChanged = insertedInitialMessages > 0 || transitions.isNotEmpty(),
        )

        var insertedOutbox = false
        transitions.forEach { transition ->
            val inserted = events.appendInCurrentTransaction(
                SourceInvalidationAuthorityEvent(
                    scope = transition.current.scope,
                    conversationId = transition.current.conversationId,
                    objectKind = SourceAuthorityObjectKind.MESSAGE,
                    sourceId = transition.current.messageId,
                    sourceRevision = transition.current.sourceRevision,
                    previousSourceRevision = transition.previous.sourceRevision,
                    conversationSourceRevision = conversationMutation.head.sourceRevision,
                    sourceState = transition.current.sourceState,
                    changeKind = transition.current.changeKind,
                    occurredAtMs = transition.current.occurredAtMs,
                ),
            )
            if (inserted) insertedOutbox = true
            if (transitionInvalidations.invalidateInCurrentTransaction(transition)) {
                insertedOutbox = true
            }
        }
        val previousConversation = conversationMutation.previous
        if (previousConversation != null) {
            val current = conversationMutation.head
            val inserted = events.appendInCurrentTransaction(
                SourceInvalidationAuthorityEvent(
                    scope = current.scope,
                    conversationId = current.conversationId,
                    objectKind = SourceAuthorityObjectKind.CONVERSATION,
                    sourceId = current.conversationId,
                    sourceRevision = current.sourceRevision,
                    previousSourceRevision = previousConversation.sourceRevision,
                    conversationSourceRevision = current.sourceRevision,
                    sourceState = current.sourceState,
                    changeKind = current.changeKind,
                    occurredAtMs = current.occurredAtMs,
                ),
            )
            if (inserted) insertedOutbox = true
        }

        return ConversationSourceAuthorityCommit(
            conversation = conversationMutation.head,
            previousConversation = previousConversation,
            messagesById = finalMessages.toMap(),
            messageTransitions = transitions.toList(),
            insertedInitialMessageCount = insertedInitialMessages,
            conversationMutated = conversationMutation.mutated,
            insertedOutbox = insertedOutbox,
        )
    }

    suspend fun findActiveMessageInCurrentTransaction(
        scope: ConversationSourceScope,
        messageId: String,
    ): MessageSourceAuthorityHead? = store.findMessage(scope, messageId)
        ?.takeIf { it.sourceState == ConversationSourceState.ACTIVE }

    /** Must be called only after the outer authority transaction returned successfully. */
    fun dispatchPostCommit(commit: ConversationSourceAuthorityCommit) {
        events.dispatchPostCommit(commit.insertedOutbox)
    }

    /**
     * Coalesces a bounded multi-scope reconciliation into one wake. A Conversation write can
     * project the same graph into the assistant scope plus one or more authority-subject scopes;
     * waking once after the owning transaction commits avoids a scheduler hot spot without
     * weakening the durable outbox contract.
     */
    fun dispatchPostCommit(commits: Collection<ConversationSourceAuthorityCommit>) {
        events.dispatchPostCommit(commits.any(ConversationSourceAuthorityCommit::insertedOutbox))
    }

    private suspend fun loadCompleteMessageSnapshot(
        snapshot: ConversationSourceSnapshot,
    ): Map<String, MessageSourceAuthorityHead> {
        val count = store.countMessagesForConversation(snapshot.scope, snapshot.conversationId)
        if (count < 0) {
            throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_COUNT_INVALID")
        }
        if (count > maxMessagesPerConversation) {
            throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_LIMIT_EXCEEDED")
        }
        if (count == 0) return emptyMap()

        val rows = ArrayList<MessageSourceAuthorityHead>(count)
        var after = ""
        while (rows.size < count) {
            val remaining = count - rows.size
            val page = store.listMessagesForConversationAfter(
                scope = snapshot.scope,
                conversationId = snapshot.conversationId,
                afterMessageId = after,
                limit = minOf(pageSize, remaining),
            )
            if (page.isEmpty()) {
                throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_SNAPSHOT_INCOMPLETE")
            }
            page.forEach { row ->
                if (row.scope != snapshot.scope || row.conversationId != snapshot.conversationId) {
                    throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_SNAPSHOT_SCOPE_MISMATCH")
                }
                if (row.messageId <= after) {
                    throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_SNAPSHOT_ORDER_INVALID")
                }
                after = row.messageId
                rows += row
            }
            if (rows.size > count) {
                throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_SNAPSHOT_COUNT_CHANGED")
            }
        }
        if (rows.size != count || rows.map(MessageSourceAuthorityHead::messageId).toSet().size != rows.size) {
            throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_SNAPSHOT_COUNT_CHANGED")
        }
        return rows.associateBy(MessageSourceAuthorityHead::messageId)
    }

    private suspend fun reconcileConversationHead(
        snapshot: ConversationSourceSnapshot,
        existing: ConversationSourceAuthorityHead?,
        branchHead: MessageSourceAuthorityHead?,
        messageAuthorityChanged: Boolean,
    ): ConversationMutation {
        val desiredState = if (snapshot.conversationDeleted) {
            ConversationSourceState.TOMBSTONED
        } else {
            ConversationSourceState.ACTIVE
        }
        if (existing == null) {
            val initial = ConversationSourceAuthorityHead(
                scope = snapshot.scope,
                conversationId = snapshot.conversationId,
                assistantIdSnapshot = snapshot.assistantIdSnapshot,
                sourceRevision = 1L,
                previousSourceRevision = null,
                sourceState = desiredState,
                changeKind = if (snapshot.conversationDeleted) {
                    ConversationSourceChangeKind.CONVERSATION_DELETED
                } else {
                    ConversationSourceChangeKind.CREATED
                },
                branchHeadMessageId = branchHead?.messageId,
                branchHeadMessageRevision = branchHead?.sourceRevision,
                occurredAtMs = snapshot.occurredAtMs,
                updatedAtMs = snapshot.occurredAtMs,
            )
            if (!store.insertConversationInitial(initial)) {
                val raced = store.findConversation(snapshot.scope, snapshot.conversationId)
                if (raced != initial) {
                    throw ConversationSourceAuthorityConflictException(
                        "SOURCE_CONVERSATION_INITIAL_INSERT_CONFLICT",
                    )
                }
                return ConversationMutation(raced, previous = null, mutated = false)
            }
            return ConversationMutation(initial, previous = null, mutated = true)
        }
        if (existing.scope != snapshot.scope || existing.conversationId != snapshot.conversationId) {
            throw ConversationSourceAuthorityConflictException("SOURCE_CONVERSATION_IDENTITY_MISMATCH")
        }
        if (existing.sourceState == ConversationSourceState.TOMBSTONED) {
            if (!snapshot.conversationDeleted) {
                throw ConversationSourceAuthorityConflictException("SOURCE_CONVERSATION_TOMBSTONE_REVIVAL")
            }
            return ConversationMutation(existing, previous = null, mutated = false)
        }
        val branchChanged = existing.branchHeadMessageId != branchHead?.messageId ||
            existing.branchHeadMessageRevision != branchHead?.sourceRevision
        val changed = messageAuthorityChanged || branchChanged ||
            existing.assistantIdSnapshot != snapshot.assistantIdSnapshot ||
            existing.sourceState != desiredState
        if (!changed) return ConversationMutation(existing, previous = null, mutated = false)

        requireNonRegressingTime(existing.updatedAtMs, snapshot.occurredAtMs)
        val next = existing.copy(
            assistantIdSnapshot = snapshot.assistantIdSnapshot,
            sourceRevision = nextRevision(existing.sourceRevision),
            previousSourceRevision = existing.sourceRevision,
            sourceState = desiredState,
            changeKind = when {
                snapshot.conversationDeleted -> ConversationSourceChangeKind.CONVERSATION_DELETED
                branchChanged -> ConversationSourceChangeKind.BRANCH_SELECTED
                else -> ConversationSourceChangeKind.UPDATED
            },
            branchHeadMessageId = branchHead?.messageId,
            branchHeadMessageRevision = branchHead?.sourceRevision,
            occurredAtMs = snapshot.occurredAtMs,
            updatedAtMs = snapshot.occurredAtMs,
        )
        if (!store.updateConversationFenced(existing.sourceRevision, next)) {
            throw ConversationSourceAuthorityConflictException("SOURCE_CONVERSATION_REVISION_CONFLICT")
        }
        return ConversationMutation(next, previous = existing, mutated = true)
    }

    private fun requireSameMessageIdentity(
        existing: MessageSourceAuthorityHead,
        snapshot: ConversationSourceSnapshot,
        messageId: String,
    ) {
        if (existing.scope != snapshot.scope ||
            existing.conversationId != snapshot.conversationId ||
            existing.messageId != messageId
        ) {
            throw ConversationSourceAuthorityConflictException("SOURCE_MESSAGE_IDENTITY_MISMATCH")
        }
    }

    private fun requireNonRegressingTime(existingUpdatedAtMs: Long, occurredAtMs: Long) {
        if (occurredAtMs < existingUpdatedAtMs) {
            throw ConversationSourceAuthorityConflictException("SOURCE_AUTHORITY_TIME_REGRESSION")
        }
    }

    private fun nextRevision(current: Long): Long = runCatching { Math.addExact(current, 1L) }
        .getOrElse {
            throw ConversationSourceAuthorityConflictException("SOURCE_REVISION_EXHAUSTED")
        }

    private data class ConversationMutation(
        val head: ConversationSourceAuthorityHead,
        val previous: ConversationSourceAuthorityHead?,
        val mutated: Boolean,
    )

    private companion object {
        const val DEFAULT_MAX_MESSAGES_PER_CONVERSATION = 4_096
        const val MAX_ALLOWED_MESSAGES_PER_CONVERSATION = 16_384
        const val DEFAULT_MAX_SCOPES_PER_CONVERSATION = 64
        const val MAX_ALLOWED_SCOPES_PER_CONVERSATION = 256
        const val DEFAULT_PAGE_SIZE = 128
        const val MAX_PAGE_SIZE = 512
    }
}

private fun scopeOrderKey(scope: ConversationSourceScope): String =
    "${scope.kind.name}\u0000${scope.id}"

private fun ConversationSourceSnapshot.toEphemeralAuthorityCommit(): ConversationSourceAuthorityCommit {
    val selected = selectedBranchMessageIds.toHashSet()
    val messageHeads = messages.associate { message ->
        message.messageId to MessageSourceAuthorityHead(
            scope = scope,
            conversationId = conversationId,
            messageId = message.messageId,
            messageRole = message.messageRole,
            sourceRevision = 1L,
            previousSourceRevision = null,
            sourceState = if (message.messageId in selected) {
                ConversationSourceState.ACTIVE
            } else {
                ConversationSourceState.SUPERSEDED
            },
            changeKind = ConversationSourceChangeKind.CREATED,
            payloadIntegritySha256 = message.payloadIntegritySha256,
            occurredAtMs = occurredAtMs,
            updatedAtMs = occurredAtMs,
        )
    }
    val branchHead = branchHeadMessageId?.let(messageHeads::get)
    return ConversationSourceAuthorityCommit(
        conversation = ConversationSourceAuthorityHead(
            scope = scope,
            conversationId = conversationId,
            assistantIdSnapshot = assistantIdSnapshot,
            sourceRevision = 1L,
            previousSourceRevision = null,
            sourceState = if (conversationDeleted) {
                ConversationSourceState.TOMBSTONED
            } else {
                ConversationSourceState.ACTIVE
            },
            changeKind = if (conversationDeleted) {
                ConversationSourceChangeKind.CONVERSATION_DELETED
            } else {
                ConversationSourceChangeKind.CREATED
            },
            branchHeadMessageId = branchHead?.messageId,
            branchHeadMessageRevision = branchHead?.sourceRevision,
            occurredAtMs = occurredAtMs,
            updatedAtMs = occurredAtMs,
        ),
        previousConversation = null,
        messagesById = messageHeads,
        messageTransitions = emptyList(),
        insertedInitialMessageCount = 0,
        conversationMutated = false,
        insertedOutbox = false,
    )
}
