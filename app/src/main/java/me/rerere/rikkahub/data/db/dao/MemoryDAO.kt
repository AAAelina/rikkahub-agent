package me.rerere.rikkahub.data.db.dao

import androidx.room.Dao
import androidx.room.Insert
import androidx.room.Query
import androidx.room.Update
import androidx.paging.PagingSource
import kotlinx.coroutines.flow.Flow
import me.rerere.rikkahub.data.db.entity.MemoryEntity

@Dao
interface MemoryDAO {
    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :assistantId " +
            "AND lifecycle_status = 'ACTIVE' AND truth_status = 'CONFIRMED' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs)",
    )
    fun getMemoriesOfAssistantFlow(assistantId: String, nowMs: Long): Flow<List<MemoryEntity>>

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :assistantId " +
            "AND lifecycle_status = 'ACTIVE' AND truth_status = 'CONFIRMED' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs)",
    )
    suspend fun getMemoriesOfAssistant(assistantId: String, nowMs: Long): List<MemoryEntity>

    /** Bounded deterministic authority projection used by Dream FULL bootstrap. */
    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId " +
            "AND lifecycle_status = 'ACTIVE' AND truth_status = 'CONFIRMED' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs) " +
            "ORDER BY id ASC LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun getActiveConfirmedMemoriesForDream(
        scopeId: String,
        nowMs: Long,
        limit: Int,
    ): List<MemoryEntity>

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId " +
            "AND lifecycle_status = 'ACTIVE' AND truth_status = 'CONFIRMED' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs) " +
            "AND id > :afterId ORDER BY id ASC LIMIT :pageSize",
    )
    suspend fun getActiveConfirmedMemoriesForDreamPage(
        scopeId: String, nowMs: Long, afterId: Int, pageSize: Int,
    ): List<MemoryEntity>

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId " +
            "AND lifecycle_status = 'ACTIVE' AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs) " +
            "AND truth_status = 'CONFIRMED' " +
            "AND memory_kind IN ('USER_PROFILE', 'PREFERENCE', 'WORKING_CONSTRAINT') " +
            "AND approval_source IN ('MANUAL_UI', 'USER_REVIEWED') " +
            "ORDER BY importance DESC, updated_at_ms DESC, id ASC LIMIT :limit",
    )
    suspend fun getUserApprovedStandingMemories(
        scopeId: String,
        nowMs: Long,
        limit: Int,
    ): List<MemoryEntity>

    @Query("SELECT * FROM memoryentity")
    fun getAllMemoriesFlow(): Flow<List<MemoryEntity>>

    @Query("SELECT * FROM memoryentity")
    suspend fun getAllMemories(): List<MemoryEntity>

    /** Bounded authority read for privacy erasure; includes archived/stale/expired rows. */
    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId " +
            "ORDER BY id ASC LIMIT CASE WHEN :limit < 0 THEN 0 ELSE :limit END",
    )
    suspend fun getMemoriesOfScopeIncludingInactive(
        scopeId: String,
        limit: Int,
    ): List<MemoryEntity>

    @Query("SELECT * FROM memoryentity WHERE id = :id AND assistant_id = :scopeId LIMIT 1")
    suspend fun getMemoryById(id: Int, scopeId: String): MemoryEntity?

    @Query(
        "SELECT * FROM memoryentity WHERE id = :id AND assistant_id = :scopeId " +
            "AND lifecycle_status = 'ACTIVE' AND truth_status = 'CONFIRMED' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs) LIMIT 1",
    )
    suspend fun getActiveConfirmedMemoryById(
        id: Int,
        scopeId: String,
        nowMs: Long,
    ): MemoryEntity?

    @Query("SELECT * FROM memoryentity WHERE id IN (:ids) AND assistant_id = :scopeId")
    suspend fun getMemoriesByIds(ids: List<Int>, scopeId: String): List<MemoryEntity>

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId AND content_hash = :contentHash " +
            "AND lifecycle_status = 'ACTIVE' AND truth_status = 'CONFIRMED' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs) " +
            "LIMIT 1",
    )
    suspend fun findActiveByContentHash(
        scopeId: String,
        contentHash: String,
        nowMs: Long,
    ): MemoryEntity?

    @Query(
        "SELECT * FROM memoryentity WHERE content_hash = '' OR created_at_ms = 0 " +
            "ORDER BY id ASC LIMIT :limit",
    )
    suspend fun getMemoriesMissingV2Metadata(limit: Int): List<MemoryEntity>

    @Query(
        "UPDATE memoryentity SET content_hash = :contentHash, tags_search = :tagsSearch, " +
            "created_at_ms = :createdAtMs WHERE id = :id",
    )
    suspend fun updateV2Metadata(
        id: Int,
        contentHash: String,
        tagsSearch: String,
        createdAtMs: Long,
    )

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId " +
            "AND (title LIKE '%' || :query || '%' OR content LIKE '%' || :query || '%' " +
            "OR tags_search LIKE '%' || :query || '%') " +
            "ORDER BY updated_at_ms DESC, id ASC LIMIT :limit",
    )
    suspend fun searchIncludingArchived(
        scopeId: String,
        query: String,
        limit: Int,
    ): List<MemoryEntity>

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId " +
            "AND (:includeArchived = 1 OR (lifecycle_status = 'ACTIVE' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs))) " +
            "AND (:query = '' OR title LIKE '%' || :query || '%' " +
            "OR content LIKE '%' || :query || '%' OR tags_search LIKE '%' || :query || '%') " +
            "AND (:kind IS NULL OR memory_kind = :kind) " +
            "AND (:sourceType IS NULL OR source_type = :sourceType) " +
            "AND (:tag = '' OR tags_search LIKE '%' || :tag || '%') " +
            "ORDER BY " +
            "CASE WHEN :sort = 'IMPORTANCE' THEN importance END DESC, " +
            "CASE WHEN :sort = 'RECENT_ACCESS' THEN COALESCE(last_accessed_at_ms, 0) END DESC, " +
            "updated_at_ms DESC, id DESC",
    )
    fun pagingLibrary(
        scopeId: String,
        includeArchived: Boolean,
        nowMs: Long,
        query: String,
        kind: String?,
        sourceType: String?,
        tag: String,
        sort: String,
    ): PagingSource<Int, MemoryEntity>

    @Query(
        "SELECT COUNT(*) FROM memoryentity WHERE assistant_id = :scopeId " +
            "AND lifecycle_status = 'ACTIVE' AND truth_status = 'CONFIRMED' " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :nowMs)",
    )
    fun observeActiveCount(scopeId: String, nowMs: Long): Flow<Int>

    @Query("SELECT COUNT(*) FROM memoryentity WHERE assistant_id = :scopeId AND lifecycle_status = 'ARCHIVED'")
    fun observeArchivedCount(scopeId: String): Flow<Int>

    @Insert
    suspend fun insertMemory(memory: MemoryEntity): Long

    @Update
    suspend fun updateMemory(memory: MemoryEntity): Int

    @Query("DELETE FROM memoryentity WHERE id = :id AND assistant_id = :scopeId")
    suspend fun deleteMemory(id: Int, scopeId: String): Int

    @Query(
        "UPDATE memoryentity SET last_accessed_at_ms = " +
            "CASE WHEN last_accessed_at_ms IS NULL OR last_accessed_at_ms < :accessedAtMs " +
            "THEN :accessedAtMs ELSE last_accessed_at_ms END " +
            "WHERE id IN (:ids) AND assistant_id = :scopeId AND lifecycle_status = 'ACTIVE' " +
            "AND truth_status = 'CONFIRMED' " +
            "AND (last_accessed_at_ms IS NULL OR last_accessed_at_ms < :accessedAtMs) " +
            "AND (expires_at_ms IS NULL OR expires_at_ms > :frozenNowMs)",
    )
    suspend fun markLastAccessed(
        ids: List<Int>,
        scopeId: String,
        accessedAtMs: Long,
        frozenNowMs: Long,
    ): Int

    @Query(
        "SELECT DISTINCT m.* FROM memoryentity m LEFT JOIN memory_evidence e " +
            "ON e.memory_id = m.id WHERE m.assistant_id = :scopeId AND (" +
            "m.source_conversation_id = :conversationId OR e.conversation_id = :conversationId) " +
            "ORDER BY m.id ASC",
    )
    suspend fun getMemoriesBySourceConversation(
        scopeId: String,
        conversationId: String,
    ): List<MemoryEntity>

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :scopeId " +
            "AND lifecycle_status = 'ACTIVE' AND expires_at_ms IS NOT NULL " +
            "AND expires_at_ms <= :nowMs ORDER BY expires_at_ms ASC, id ASC LIMIT :limit",
    )
    suspend fun getDueForExpiryMaterialization(
        scopeId: String,
        nowMs: Long,
        limit: Int,
    ): List<MemoryEntity>

    @Query(
        "SELECT * FROM memoryentity WHERE lifecycle_status = 'ACTIVE' " +
            "AND expires_at_ms IS NOT NULL AND expires_at_ms <= :nowMs " +
            "ORDER BY expires_at_ms ASC, id ASC LIMIT :limit",
    )
    suspend fun getAllDueForExpiryMaterialization(
        nowMs: Long,
        limit: Int,
    ): List<MemoryEntity>

    @Query(
        "SELECT * FROM memoryentity WHERE assistant_id = :globalScopeId " +
            "AND origin_assistant_id = :originAssistantId ORDER BY id ASC",
    )
    suspend fun getGlobalMemoriesByOriginAssistant(
        globalScopeId: String,
        originAssistantId: String,
    ): List<MemoryEntity>

    @Query("DELETE FROM memoryentity WHERE assistant_id = :assistantId")
    suspend fun deleteMemoriesOfAssistant(assistantId: String): Int
}
