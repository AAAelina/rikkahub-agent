package me.rerere.rikkahub.service

import android.util.Log
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.getAndUpdate
import kotlinx.coroutines.launch
import me.rerere.rikkahub.data.model.Conversation
import java.util.concurrent.atomic.AtomicInteger
import kotlin.uuid.Uuid

private const val TAG = "ConversationSession"
private const val IDLE_TIMEOUT_MS = 5_000L

class ConversationSession(
    val id: Uuid,
    initial: Conversation,
    private val scope: CoroutineScope,
    private val onIdle: (Uuid) -> Unit,
    private val canEvict: () -> Boolean = { true },
    private val idleTimeoutMs: Long = IDLE_TIMEOUT_MS,
) {
    // 会话状态
    val state = MutableStateFlow(initial)
    internal val metadataMutationMutex = kotlinx.coroutines.sync.Mutex()
    private val stateLock = Any()
    @Volatile
    private var hydrated = false

    val isHydrated: Boolean get() = hydrated

    fun hydrateIfNeeded(snapshot: Conversation): Boolean = synchronized(stateLock) {
        if (hydrated) return@synchronized false
        state.value = snapshot
        hydrated = true
        true
    }

    fun replaceState(snapshot: Conversation) {
        synchronized(stateLock) {
            state.value = snapshot
            hydrated = true
        }
    }

    fun updateState(transform: (Conversation) -> Conversation): Conversation = synchronized(stateLock) {
        state.value = transform(state.value)
        state.value
    }

    // 原子引用计数
    private val refCount = AtomicInteger(0)

    // 处理状态（如 OCR 识别中）
    val processingStatus = MutableStateFlow<String?>(null)

    // 生成任务（内聚在 session 中）
    private val _generationJob = MutableStateFlow<Job?>(null)
    val generationJob: StateFlow<Job?> = _generationJob.asStateFlow()
    val isGenerating: Boolean get() = _generationJob.value?.isActive == true
    val isInUse: Boolean get() = refCount.get() > 0 || isGenerating || !canEvict()

    // 空闲检查任务
    private var idleCheckJob: Job? = null

    fun acquire(): Int = refCount.incrementAndGet().also {
        cancelIdleCheck()
        Log.d(TAG, "acquire $id (refs=$it)")
    }

    fun release(): Int = refCount.decrementAndGet().also {
        Log.d(TAG, "release $id (refs=$it)")
        if (it <= 0) scheduleIdleCheck()
    }

    // 作用域 API - 短请求（REST）
    inline fun <T> withRef(block: () -> T): T {
        acquire()
        try {
            return block()
        } finally {
            release()
        }
    }

    // 作用域 API - 长连接（SSE、挂起函数）
    suspend inline fun <T> withRefSuspend(block: () -> T): T {
        acquire()
        try {
            return block()
        } finally {
            release()
        }
    }

    /** Attach the current run without making the setter itself a cancellation authority. */
    fun attachRunJob(job: Job?) {
        _generationJob.getAndUpdate { job }
        // Identity-checked completion handler: only null the StateFlow if the value is
        // STILL the same job we just set. Without this an out-of-order setJob(B) →
        // A.invokeOnCompletion → clobber-B race could null out the live job.
        job?.invokeOnCompletion {
            _generationJob.compareAndSet(job, null)
            if (refCount.get() <= 0) {
                scheduleIdleCheck()
            }
        }
    }

    fun getJob(): Job? = _generationJob.value

    fun requestIdleCheck() {
        if (refCount.get() <= 0) scheduleIdleCheck()
    }

    private fun scheduleIdleCheck() {
        idleCheckJob?.cancel()
        idleCheckJob = scope.launch {
            delay(idleTimeoutMs)
            if (refCount.get() <= 0 && !isGenerating && canEvict()) {
                onIdle(id)
            }
        }
    }

    private fun cancelIdleCheck() {
        idleCheckJob?.cancel()
        idleCheckJob = null
    }

    fun cleanup() {
        // Use getAndUpdate (same as setJob) so cleanup() is consistent with the atomic
        // swap used elsewhere. Direct .value = null would bypass the CAS and could
        // theoretically race with a concurrent setJob that's running post-removal
        // (e.g., a coroutine that had already acquired a session reference before
        // dropSession removed it from the map). In practice the risk is tiny because
        // cleanup() is only called after removal, but correctness still matters.
        val job = _generationJob.getAndUpdate { null }
        job?.cancel()
        idleCheckJob?.cancel()
        idleCheckJob = null
    }
}
