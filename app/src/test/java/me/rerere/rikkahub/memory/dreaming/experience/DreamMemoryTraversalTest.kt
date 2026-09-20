package me.rerere.rikkahub.memory.dreaming.experience

import kotlinx.coroutines.runBlocking
import org.junit.Assert.*
import org.junit.Test

class DreamMemoryTraversalTest {
    @Test fun `127 128 129 and 300 are fully traversed in one sync`() = runBlocking {
        for (size in listOf(127,128,129,300)) {
            val source = (1..size).toList()
            val visited = mutableListOf<Int>()
            traverseDreamMemoryPages(128, { after, limit -> source.filter { it > after }.take(limit) }, { it }) { visited += it }
            assertEquals(source, visited)
        }
    }
    @Test fun `crash leaves backfill unmarked and retry deduplicates preceding pages`() = runBlocking {
        val source = (1..300).toList()
        val durable = mutableSetOf<Int>()
        var marked = false
        try {
            traverseDreamMemoryPages(128, { after, limit -> source.filter { it > after }.take(limit) }, { it }) {
                if (it == 150) error("crash")
                durable += it
            }
            marked = true
        } catch (_: IllegalStateException) { }
        assertFalse(marked)
        assertEquals(149, durable.size)
        traverseDreamMemoryPages(128, { after, limit -> source.filter { it > after }.take(limit) }, { it }) { durable += it }
        marked = true
        assertTrue(marked)
        assertEquals(source.toSet(), durable)
    }
    @Test fun `duplicate or backwards page fails instead of silently marking complete`() = runBlocking {
        var failed = false
        try { traverseDreamMemoryPages(2, { _, _ -> listOf(1,1) }, { it }) { } }
        catch (_: IllegalStateException) { failed = true }
        assertTrue(failed)
    }
}
