package me.rerere.rikkahub.data.db

import java.io.File
import org.junit.Assert.assertEquals
import org.junit.Assert.assertThrows
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class ImportedDatabaseReconcilerContractTest {
    @Test
    fun `frozen v49 migrates while exact v50 is current`() {
        assertEquals("MIGRATE_FINAL_V49", ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
            49, "967f2a908998f5bac733c1ae71bee5bb",
        ).name)
        assertEquals(ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(49, "967f2a908998f5bac733c1ae71bee5bb"))
        assertEquals("ALREADY_CURRENT", ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
            50, "73b32c82384f8b24fe576d123524249a",
        ).name)
    }

    @Test
    fun `staged v49 v50 reject unknown cross version and future identities`() {
        listOf(49 to "unknown", 50 to "unknown",
            49 to "73b32c82384f8b24fe576d123524249a",
            50 to "967f2a908998f5bac733c1ae71bee5bb",
            51 to "73b32c82384f8b24fe576d123524249a").forEach { (version, identity) ->
            assertThrows(IllegalStateException::class.java) {
                ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(version, identity)
            }
        }
    }

    @get:Rule
    val temporaryFolder = TemporaryFolder()

    @Test
    fun `reconciler pins the exported v50 identity and exact v48 predecessor`() {
        assertEquals(50, ImportedDatabaseReconciler.EXPECTED_VERSION)
        assertEquals(
            "73b32c82384f8b24fe576d123524249a",
            ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH,
        )
        assertEquals(
            "74be67f9e9e32264c091b1d6c4a32b17",
            ImportedDatabaseReconciler.FINAL_V48_IDENTITY_HASH,
        )
        assertEquals(
            "3208afdfb6ec01eb325a598464e56940",
            ImportedDatabaseReconciler.FINAL_V47_IDENTITY_HASH,
        )
        assertEquals(
            "102b6a6fc51154abdac792d133d461a3",
            ImportedDatabaseReconciler.PRE_P1_V46_IDENTITY_HASH,
        )
        assertEquals(
            "8ef3ddc71d855013202bb11b0493d6e6",
            ImportedDatabaseReconciler.PRE_LEARNING_V46_IDENTITY_HASH,
        )
    }

    @Test
    fun `installed pre-storage v35 now follows normal migrations`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 35,
                identityHash = "2a74d694211f0df9f9094c7571ec71dd",
            ),
        )
    }

    @Test
    fun `exact P0 v46 follows normal 46 to 47 migration`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 46,
                identityHash = "102b6a6fc51154abdac792d133d461a3",
            ),
        )
    }

    @Test
    fun `exact pre-learning v46 receives compatibility floor before 46 to 47`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 46,
                identityHash = "8ef3ddc71d855013202bb11b0493d6e6",
            ),
        )
    }

    @Test
    fun `unknown current schema is refused and never compatibility stamped`() {
        listOf(46, 47, 48, 49, 50).forEach { version ->
            listOf(null, "", "unknown", ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH)
                .forEach { identity ->
                    val isExactCurrent = version == ImportedDatabaseReconciler.EXPECTED_VERSION &&
                        identity == ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH
                    if (!isExactCurrent) {
                        assertEquals(
                            ImportedDatabaseReconciler.ReconcilePlan.REFUSE_UNKNOWN_CURRENT,
                            ImportedDatabaseReconciler.reconcilePlan(version, identity),
                        )
                    }
                }
        }
    }

    @Test
    fun `authority UUID contract is canonical lowercase and non nil`() {
        assertEquals(
            true,
            ImportedDatabaseReconciler.isCanonicalNonNilDatabaseUuid(
                "80000000-0000-0000-0000-000000000008",
            ),
        )
        listOf(
            "00000000-0000-0000-0000-000000000000",
            "A0000000-0000-0000-0000-000000000008",
            "80000000000000000000000000000008",
            "80000000-0000-0000-0000-00000000000z",
            "",
        ).forEach { malformed ->
            assertEquals(
                false,
                ImportedDatabaseReconciler.isCanonicalNonNilDatabaseUuid(malformed),
            )
        }
    }

    @Test
    fun `restore compares every policy grant head field with its current revision`() {
        assertEquals(
            setOf(
                "grant_id",
                "source_stream_id",
                "policy_id",
                "policy_revision",
                "artifact_sha256",
                "scope_kind",
                "scope_id",
                "consuming_assistant_id",
                "actor",
                "state",
                "state_version",
                "granted_at_ms",
                "revoked_at_ms",
                "reason_code",
                "created_at_ms",
                "updated_at_ms",
            ),
            ImportedDatabaseReconciler.V48_POLICY_GRANT_HEAD_COLUMNS.toSet(),
        )
        assertEquals(
            ImportedDatabaseReconciler.V48_POLICY_GRANT_HEAD_COLUMNS.size,
            ImportedDatabaseReconciler.V48_POLICY_GRANT_HEAD_COLUMNS.distinct().size,
        )
    }

    @Test
    fun `staged restore gate throws for unknown current and newer databases`() {
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = ImportedDatabaseReconciler.EXPECTED_VERSION,
                identityHash = "unknown",
            )
        }
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = ImportedDatabaseReconciler.EXPECTED_VERSION + 1,
                identityHash = null,
            )
        }
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = ImportedDatabaseReconciler.EXPECTED_VERSION - 1,
                identityHash = null,
            )
        }
        assertEquals(
            ImportedDatabaseReconciler.StagedReconcilePlan.ALREADY_CURRENT,
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = ImportedDatabaseReconciler.EXPECTED_VERSION,
                identityHash = ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH,
            ),
        )
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = ImportedDatabaseReconciler.EXPECTED_VERSION,
                identityHash = ImportedDatabaseReconciler.PRE_LEARNING_V46_IDENTITY_HASH,
            )
        }
    }

    @Test
    fun `cold staged restore accepts only exact frozen v46 and v47 identities`() {
        assertEquals(
            listOf(46 to 47, 47 to 48, 48 to 49, 49 to 50),
            ImportedDatabaseReconciler.STAGED_COLD_RESTORE_MIGRATIONS.map {
                it.startVersion to it.endVersion
            },
        )
        assertEquals(
            ImportedDatabaseReconciler.StagedReconcilePlan.MIGRATE_FINAL_V48,
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = 48,
                identityHash = ImportedDatabaseReconciler.FINAL_V48_IDENTITY_HASH,
            ),
        )
        assertEquals(
            ImportedDatabaseReconciler.StagedReconcilePlan.MIGRATE_FINAL_V47,
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = 47,
                identityHash = ImportedDatabaseReconciler.FINAL_V47_IDENTITY_HASH,
            ),
        )
        assertEquals(
            ImportedDatabaseReconciler.StagedReconcilePlan.MIGRATE_FINAL_V46,
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = 46,
                identityHash = ImportedDatabaseReconciler.FINAL_V46_IDENTITY_HASH,
            ),
        )
        assertEquals(
            ImportedDatabaseReconciler.StagedReconcilePlan.MIGRATE_PRE_P1_V46,
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = 46,
                identityHash = ImportedDatabaseReconciler.PRE_P1_V46_IDENTITY_HASH,
            ),
        )
        assertEquals(
            ImportedDatabaseReconciler.StagedReconcilePlan.MIGRATE_PRE_LEARNING_V46,
            ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(
                version = 46,
                identityHash = ImportedDatabaseReconciler.PRE_LEARNING_V46_IDENTITY_HASH,
            ),
        )
        listOf(null, "unknown-v46", ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH).forEach { identity ->
            assertThrows(IllegalStateException::class.java) {
                ImportedDatabaseReconciler.stagedReconcilePlanOrThrow(46, identity)
            }
        }
    }

    @Test
    fun `legacy v46 descriptor cannot classify unknown or v47 identity`() {
        assertEquals(
            ImportedDatabaseReconciler.LegacyV46AuthorityPlan.READ_EXISTING_STREAM,
            ImportedDatabaseReconciler.legacyV46AuthorityPlanOrThrow(
                46,
                ImportedDatabaseReconciler.FINAL_V46_IDENTITY_HASH,
            ),
        )
        assertEquals(
            ImportedDatabaseReconciler.LegacyV46AuthorityPlan.CREATE_STREAM,
            ImportedDatabaseReconciler.legacyV46AuthorityPlanOrThrow(
                46,
                ImportedDatabaseReconciler.PRE_LEARNING_V46_IDENTITY_HASH,
            ),
        )
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.legacyV46AuthorityPlanOrThrow(46, "unknown-v46")
        }
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.legacyV46AuthorityPlanOrThrow(
                47,
                ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH,
            )
        }
    }

    @Test
    fun `staged file API refuses a non-private candidate before opening SQLite`() {
        assertThrows(IllegalArgumentException::class.java) {
            ImportedDatabaseReconciler.reconcileStagedFileOrThrow(
                databaseFile = File("rikka_hub"),
                expectedStreamId = "00000000-0000-0000-0000-000000000001",
                expectedHeadSeq = 1L,
            )
        }
    }

    @Test
    fun `installed and staged validators have disjoint exact filename contracts`() {
        val databases = File(temporaryFolder.newFolder("restore-target"), "databases")
            .apply { check(mkdir()) }
        val installed = File(databases, "rikka_hub").apply { writeText("not sqlite") }
        val staged = File(
            databases,
            ".rikka_hub.restore_0123456789abcdef0123456789abcdef.ready",
        ).apply { writeText("not sqlite") }
        val streamId = "00000000-0000-0000-0000-000000000001"

        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.validateInstalledFileOrThrow(
                databaseFile = staged,
                expectedStreamId = streamId,
                expectedHeadSeq = 1L,
            )
        }
        assertThrows(IllegalStateException::class.java) {
            ImportedDatabaseReconciler.validateStagedFileOrThrow(
                databaseFile = installed,
                expectedStreamId = streamId,
                expectedHeadSeq = 1L,
            )
        }
    }

    @Test
    fun `final v50 identity skips raw framework reconciliation`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.SKIP,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 50,
                identityHash = ImportedDatabaseReconciler.EXPECTED_IDENTITY_HASH,
            ),
        )
    }

    @Test
    fun `exact final v48 identity follows the raw 48 to 49 migration`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 48,
                identityHash = ImportedDatabaseReconciler.FINAL_V48_IDENTITY_HASH,
            ),
        )
    }

    @Test
    fun `exact final v47 identity follows the raw 47 to 49 migration`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 47,
                identityHash = ImportedDatabaseReconciler.FINAL_V47_IDENTITY_HASH,
            ),
        )
    }

    @Test
    fun `final v46 identity remains an older migration input`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 46,
                identityHash = ImportedDatabaseReconciler.FINAL_V46_IDENTITY_HASH,
            ),
        )
    }

    @Test
    fun `previous v45 is reconciled before Room installs synthesis tables`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 45,
                identityHash = "1d710a3e2caa9b79a2f7eb0c94bb3d6a",
            ),
        )
    }

    @Test
    fun `previous v44 is reconciled before Room installs observer tables`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 44,
                identityHash = "1c505f7d10b206eaa420365150f2eb7d",
            ),
        )
    }

    @Test
    fun `previous v43 is reconciled before Room applies source identity migration`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 43,
                identityHash = "993e84f6266c165ffd196d30acb1969d",
            ),
        )
    }

    @Test
    fun `previous v39 is reconciled before Room creates tool experience tables`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 39,
                identityHash = "98db479b6258269f2aef18ce15e0f2f9",
            ),
        )
    }

    @Test
    fun `previous v40 is reconciled before Room creates fast lane shortcuts`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 40,
                identityHash = "bf0cc9fcad994a73ac34982cf526e2ce",
            ),
        )
    }

    @Test
    fun `previous P0 v37 receives requested terminal outcome compatibility column`() {
        assertEquals(
            ImportedDatabaseReconciler.ReconcilePlan.FULL_COMPATIBILITY,
            ImportedDatabaseReconciler.reconcilePlan(
                version = 37,
                identityHash = "8cb20e594bfefae355191428fcd7ca9a",
            ),
        )
    }
}
