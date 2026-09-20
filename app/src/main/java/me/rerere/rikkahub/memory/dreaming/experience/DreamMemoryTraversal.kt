package me.rerere.rikkahub.memory.dreaming.experience

/** One complete keyset pass. The caller owns a fixed clock and only marks backfilled on return.
 * Retry starts from the beginning; stable experience IDs make already ingested pages idempotent.
 */
internal suspend fun <T> traverseDreamMemoryPages(
    pageSize: Int,
    readPage: suspend (afterId: Int, pageSize: Int) -> List<T>,
    id: (T) -> Int,
    ingest: suspend (T) -> Unit,
) {
    require(pageSize > 0)
    var afterId = Int.MIN_VALUE
    while (true) {
        val page = readPage(afterId, pageSize)
        if (page.isEmpty()) return
        for (item in page) {
            val nextId = id(item)
            check(nextId > afterId) { "Dream memory page must advance in strict ID order" }
            ingest(item)
            afterId = nextId
        }
        if (page.size < pageSize) return
    }
}
