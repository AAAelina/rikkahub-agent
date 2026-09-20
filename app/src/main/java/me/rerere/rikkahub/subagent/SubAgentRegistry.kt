package me.rerere.rikkahub.subagent

import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.update
import java.util.concurrent.ConcurrentHashMap

/**
 * Phase 11 — in-memory store of sub-agent runs + their associated coroutine [Job]s.
 *
 * The map of runs is a [StateFlow] so the chat UI's chip row can collect it; the map of
 * jobs is private since callers shouldn't be cancelling Jobs through random handles.
 * Capped at [SubAgentDefaults.REGISTRY_LRU_CAP] entries — when the cap is reached, the
 * oldest TERMINAL run gets evicted (running runs are never evicted).
 *
 * Admission atomically reserves capacity using the caps supplied by the engine.
 */
class SubAgentRegistry {

    private val _runs = MutableStateFlow<Map<String, SubAgentRun>>(emptyMap())
    val runs: StateFlow<Map<String, SubAgentRun>> = _runs

    /**
     * Side-table of cancellable Jobs for currently RUNNING runs. Removed once the run
     * reaches a terminal status. Kept separate from the StateFlow because [Job] is not
     * serialisable and we don't want UI consumers re-collecting on Job-pointer churn.
     */
    private val activeJobs: ConcurrentHashMap<String, Job> = ConcurrentHashMap()

    enum class Reservation { RESERVED, GLOBAL_CAP, ASSISTANT_CAP, DUPLICATE }

    fun reservePending(run: SubAgentRun, globalCap: Int, assistantCap: Int): Reservation {
        require(run.status == SubAgentStatus.PENDING)
        require(globalCap > 0 && assistantCap > 0)
        while (true) {
            val current = _runs.value
            if (run.id in current) return Reservation.DUPLICATE
            val active = current.values.filter { it.status == SubAgentStatus.PENDING || it.status == SubAgentStatus.RUNNING }
            if (active.size >= globalCap) return Reservation.GLOBAL_CAP
            if (active.count { it.parentAssistantId == run.parentAssistantId } >= assistantCap) return Reservation.ASSISTANT_CAP
            if (_runs.compareAndSet(current, pruneIfNeeded(current) + (run.id to run))) return Reservation.RESERVED
        }
    }

    /** Covers failure before launch and cancellation before a lazy coroutine enters its body. */
    fun terminalizeIfActive(id: String, status: SubAgentStatus, error: String?) {
        update(id) {
            if (it.status == SubAgentStatus.PENDING || it.status == SubAgentStatus.RUNNING)
                it.copy(status = status, error = error, finishedAtMs = System.currentTimeMillis())
            else it
        }
        activeJobs.remove(id)?.cancel()
    }

    fun addPending(run: SubAgentRun, job: Job? = null) {
        _runs.update { current ->
            val pruned = pruneIfNeeded(current)
            pruned + (run.id to run)
        }
        if (job != null) activeJobs[run.id] = job
    }

    fun update(id: String, transform: (SubAgentRun) -> SubAgentRun) {
        _runs.update { current ->
            val existing = current[id] ?: return@update current
            val next = transform(existing)
            if (existing.status !in setOf(SubAgentStatus.PENDING, SubAgentStatus.RUNNING) &&
                next.status in setOf(SubAgentStatus.PENDING, SubAgentStatus.RUNNING)) current
            else current + (id to next)
        }
    }

    fun setJob(id: String, job: Job) {
        activeJobs[id] = job
        val status = get(id)?.status
        if (status != SubAgentStatus.PENDING && status != SubAgentStatus.RUNNING) {
            activeJobs.remove(id, job)
            job.cancel()
        }
    }

    fun get(id: String): SubAgentRun? = _runs.value[id]

    fun getForAssistant(id: String, parentAssistantId: String): SubAgentRun? =
        _runs.value[id]?.takeIf { it.parentAssistantId == parentAssistantId }

    fun list(activeOnly: Boolean): List<SubAgentRun> {
        val all = _runs.value.values
        return if (activeOnly) all.filter { it.status == SubAgentStatus.RUNNING || it.status == SubAgentStatus.PENDING }
        else all.toList()
    }

    fun listForAssistant(
        parentAssistantId: String,
        activeOnly: Boolean,
    ): List<SubAgentRun> = list(activeOnly).filter {
        it.parentAssistantId == parentAssistantId
    }

    fun activeCountForAssistant(parentAssistantId: String): Int =
        _runs.value.values.count {
            it.parentAssistantId == parentAssistantId &&
                (it.status == SubAgentStatus.RUNNING || it.status == SubAgentStatus.PENDING)
        }

    fun globalActiveCount(): Int =
        _runs.value.values.count {
            it.status == SubAgentStatus.RUNNING || it.status == SubAgentStatus.PENDING
        }

    /**
     * Cancel a single run by id. Returns true if a cancellable job existed; false if the
     * run was already in a terminal state or if the id is unknown. Marking the status to
     * CANCELLED is the caller's job (typically the engine after the Job's onCompletion
     * fires) so we don't double-write.
     */
    fun requestCancel(id: String): Boolean {
        val run = get(id) ?: return false
        if (run.status != SubAgentStatus.PENDING && run.status != SubAgentStatus.RUNNING) return false
        val job = activeJobs.remove(id)
        if (job != null) job.cancel()
        else terminalizeIfActive(id, SubAgentStatus.CANCELLED, "cancelled_before_start")
        return true
    }

    fun requestCancelForAssistant(id: String, parentAssistantId: String): Boolean {
        val run = getForAssistant(id, parentAssistantId) ?: return false
        if (run.status != SubAgentStatus.RUNNING && run.status != SubAgentStatus.PENDING) {
            return false
        }
        return requestCancel(id)
    }

    /**
     * Cancel every currently-active run dispatched from [parentChatId]. Hooked into the
     * Telegram /stop handler and the in-app stop button so a single tick takes down the
     * parent generation AND all of its sub-agents. Returns the count cancelled.
     */
    fun cancelAllForParent(parentChatId: String): Int {
        var count = 0
        val toCancel = _runs.value.values
            .filter { it.parentChatId == parentChatId && (it.status == SubAgentStatus.RUNNING || it.status == SubAgentStatus.PENDING) }
            .map { it.id }
        for (runId in toCancel) {
            if (requestCancel(runId)) count++
        }
        return count
    }

    /** Cancel every active sub-agent regardless of its parent surface. */
    fun cancelAllActive(): Int {
        var count = 0
        val toCancel = _runs.value.values
            .filter { it.status == SubAgentStatus.RUNNING || it.status == SubAgentStatus.PENDING }
            .map { it.id }
        for (runId in toCancel) {
            if (requestCancel(runId)) count++
        }
        return count
    }

    fun clearJob(id: String) {
        activeJobs.remove(id)
    }

    private fun pruneIfNeeded(current: Map<String, SubAgentRun>): Map<String, SubAgentRun> {
        if (current.size < SubAgentDefaults.REGISTRY_LRU_CAP) return current
        // Evict the oldest TERMINAL run; never evict a running one. If every run is
        // running, the cap would be exceeded — we accept this since it should be rare
        // (50 concurrent sub-agents would already have been blocked by the global cap of 16).
        val terminalSorted = current.values
            .filter { it.status != SubAgentStatus.RUNNING && it.status != SubAgentStatus.PENDING }
            .sortedBy { it.finishedAtMs ?: it.startedAtMs }
        val toEvictId = terminalSorted.firstOrNull()?.id
        return if (toEvictId != null) current - toEvictId else current
    }
}
