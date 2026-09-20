package me.rerere.rikkahub.data.repository

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

/** Source-level production composition checks for the command-authority-free regeneration path. */
class TransientConversationFinalizationProductionContractTest {
    @Test
    fun `fallback final graph legacy invalidation and exact ALR authority share one transaction`() {
        val root = locateProjectRoot()
        val repository = read(
            root,
            "app/src/main/java/me/rerere/rikkahub/data/repository/ConversationRepository.kt",
        )
        val coordinator = read(
            root,
            "app/src/main/java/me/rerere/rikkahub/data/authority/transaction/" +
                "ConversationCommandAuthorityTransactions.kt",
        )
        val method = repository.substring(
            repository.indexOf("suspend fun finalizeTransientConversationUpdate("),
            repository.indexOf("/** Refreshes the non-authoritative FTS projection", repository.indexOf(
                "suspend fun finalizeTransientConversationUpdate(",
            )),
        )

        val legacyInvalidation = method.indexOf("applySourceInvalidationPlan(")
        val graphPersistence = method.indexOf("persistAuthorityGraphInCurrentTransaction(")
        assertTrue("legacy invalidation must remain in the final graph mutation", legacyInvalidation >= 0)
        assertTrue("final graph must persist after its baseline invalidation", graphPersistence > legacyInvalidation)
        assertTrue("fallback must use the typed authority coordinator", method.contains(
            "transientFinalizationAuthority.finish(",
        ))
        assertTrue(method.contains("ConversationSourceInvalidationMode.SKIP_TRANSIENT_WRITE"))
        assertFalse("owner preflight must not deserialize a graph outside the transaction", method.contains(
            "val stored = getConversationById",
        ))

        val coordinatorStart = coordinator.indexOf(
            "class TransientConversationFinalizationAuthorityCoordinator(",
        )
        val coordinatorEnd = coordinator.indexOf(
            "/** Captures the full durable command row",
            coordinatorStart,
        )
        val body = coordinator.substring(coordinatorStart, coordinatorEnd)
        val transaction = body.indexOf("transactions.inTransaction {")
        val graph = body.indexOf("graphMutation.persistInCurrentTransaction()")
        val source = body.indexOf("sources.reconcileAllKnownScopesInCurrentTransaction(conversation)")
        val dispatch = body.indexOf("sources.dispatchPostCommit(commit.sources)")
        assertTrue(transaction >= 0)
        assertTrue("graph must precede ALR source reconciliation in the owning transaction", graph > transaction)
        assertTrue("ALR authority and outbox must follow the exact persisted graph", source > graph)
        assertTrue("dispatch must follow the completed transaction", dispatch > source)
    }

    @Test
    fun `production DI injects the fallback coordinator into ConversationRepository`() {
        val root = locateProjectRoot()
        val dataSource = read(root, "app/src/main/java/me/rerere/rikkahub/di/DataSourceModule.kt")
        val repositoryModule = read(root, "app/src/main/java/me/rerere/rikkahub/di/RepositoryModule.kt")

        assertTrue(dataSource.contains("TransientConversationFinalizationAuthorityCoordinator("))
        assertTrue(dataSource.contains("transactions = get()"))
        assertTrue(dataSource.contains("sources = get()"))
        assertTrue(repositoryModule.contains(
            "ConversationRepository(get(), get(), get(), get(), get(), get(), get(), get(), get(), get(), get())",
        ))
    }

    private fun read(root: Path, relative: String): String =
        Files.readString(root.resolve(relative), StandardCharsets.UTF_8)

    private fun locateProjectRoot(): Path {
        var cursor = Paths.get(System.getProperty("user.dir")).toAbsolutePath().normalize()
        repeat(6) {
            if (Files.isDirectory(cursor.resolve("app/src/main/java"))) return cursor
            cursor = cursor.parent ?: return@repeat
        }
        error("Unable to locate project root")
    }
}
