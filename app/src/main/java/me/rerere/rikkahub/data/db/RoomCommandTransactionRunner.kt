package me.rerere.rikkahub.data.db

import androidx.room.withTransaction
import me.rerere.rikkahub.service.chat.CommandTransactionRunner

/** Main-database transaction adapter for durable command authority state. */
class RoomCommandTransactionRunner(
    private val database: AppDatabase,
) : CommandTransactionRunner {
    override suspend fun <T> inTransaction(block: suspend () -> T): T =
        database.withTransaction { block() }
}
