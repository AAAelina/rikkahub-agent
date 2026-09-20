package me.rerere.rikkahub.automation

import java.nio.file.Files
import java.nio.file.Paths
import org.junit.Assert.assertTrue
import org.junit.Test

/** Source-level red gate: accepted callbacks must not precede unprotected setup failures. */
class ExternalHeadlessSetupContractTest {
    @Test
    fun `setup failures are enclosed in a compensating terminal boundary`() {
        var root = Paths.get(System.getProperty("user.dir")).toAbsolutePath()
        while (!Files.isDirectory(root.resolve("app/src/main/java"))) root = requireNotNull(root.parent)
        val src = Files.readString(root.resolve(
            "app/src/main/java/me/rerere/rikkahub/automation/ExternalAutomationDispatcher.kt"))
        val start = src.indexOf("private suspend fun runHeadless(")
        val firstSetup = src.indexOf("val assistant = ", start)
        val guarded = src.indexOf("setup.prepare(", start)
        val tryBoundary = src.indexOf("try {", start)
        assertTrue("headless setup must be inside compensating catch/finally with terminal callback",
            start >= 0 && firstSetup > start && tryBoundary in (start + 1)..guarded &&
                guarded in (tryBoundary + 1)..firstSetup)
    }
}
