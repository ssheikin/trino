/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition.Column;
import io.trino.spi.connector.MaterializedViewFreshness;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Duration;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD) // uses shared TestingAccessControlManager
public class TestMaterializedViewsOperation
        extends AbstractTestQueryFramework
{
    private static final String EXTRA_CREDENTIAL_KEY = "alternate_table_name";

    private TestingAccessControlManager accessControlManager;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog("mock")
                        .setSchema("mock")
                        .build())
                .build();

        queryRunner.installPlugin(
                new MockConnectorPlugin(
                        MockConnectorFactory.builder()
                                .withListSchemaNames(_ -> ImmutableList.of("analysis", "default"))
                                .withRowFilter(_ -> null)
                                .withGetColumns(schemaTableName -> {
                                    if (schemaTableName.getSchemaName().equals("analysis")) {
                                        return ImmutableList.of(
                                                new ColumnMetadata("a", BIGINT),
                                                new ColumnMetadata("b", BIGINT));
                                    }
                                    return ImmutableList.of(new ColumnMetadata("table_name", VARCHAR));
                                })
                                .withRedirectTable((session, schemaTableName) -> {
                                    if (schemaTableName.getSchemaName().equals("analysis")) {
                                        return Optional.empty();
                                    }
                                    String virtualTableName = session.getUser() + "_" + session.getIdentity().getExtraCredentials().getOrDefault(EXTRA_CREDENTIAL_KEY, "");
                                    // Redirect to a virtual table named as current user
                                    if (!schemaTableName.getTableName().equals(virtualTableName)) {
                                        return Optional.of(new CatalogSchemaTableName(
                                                "mock",
                                                schemaTableName.getSchemaName(),
                                                virtualTableName));
                                    }
                                    return Optional.empty();
                                })
                                .withGetMaterializedViewsFreshness((_, schemaTableName) -> {
                                    if (schemaTableName.getTableName().startsWith("fresh_materialized_view")) {
                                        return new MaterializedViewFreshness(MaterializedViewFreshness.Freshness.FRESH, Optional.empty());
                                    }
                                    return new MaterializedViewFreshness(MaterializedViewFreshness.Freshness.STALE, Optional.empty());
                                })
                                .withGetTableHandle((_, schemaTableName) -> {
                                    if (schemaTableName.getTableName().equals("non_existent_table")) {
                                        return null;
                                    }
                                    return new MockConnectorTableHandle(schemaTableName);
                                })
                                .withGetMaterializedViews((_, _) -> ImmutableMap.of(
                                        new SchemaTableName("default", "materialized_view_with_no_owner_and_should_use_invoker"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                true,
                                                true),
                                        new SchemaTableName("default", "materialized_view_with_no_owner"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                false,
                                                true),
                                        new SchemaTableName("default", "materialized_view_with_owner_and_should_use_invoker"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(),
                                                true,
                                                true),
                                        new SchemaTableName("default", "materialized_view_with_owner"), new ConnectorMaterializedViewDefinition(
                                                "SELECT * FROM mock.default.default_table",
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.empty(),
                                                ImmutableList.of(new Column("table_name", VARCHAR.getTypeId(), Optional.empty())),
                                                Optional.of(Duration.ZERO),
                                                Optional.empty(),
                                                Optional.empty(),
                                                Optional.of("mv_owner"),
                                                ImmutableList.of(),
                                                false,
                                                true),
                                        new SchemaTableName("analysis", "fresh_materialized_view"), createMaterializedViewRequiringAnalysis("SELECT * FROM mock.analysis.t1"),
                                        new SchemaTableName("analysis", "fresh_materialized_view_non_existent_table"), createMaterializedViewRequiringAnalysis("SELECT * FROM mock.analysis.non_existent_table"),
                                        new SchemaTableName("analysis", "stale_materialized_view"), createMaterializedViewRequiringAnalysis("SELECT * FROM mock.analysis.t1")))
                                .withData(schemaTableName -> {
                                    if (schemaTableName.getSchemaName().equals("analysis")) {
                                        return ImmutableList.of();
                                    }
                                    return ImmutableList.of(ImmutableList.of(schemaTableName.getTableName()));
                                })
                                .build()));
        queryRunner.createCatalog("mock", "mock");

        accessControlManager = queryRunner.getAccessControl();

        return queryRunner;
    }

    @BeforeEach
    public void resetAccessControlManager()
    {
        accessControlManager.reset();
    }

    @Test
    public void testSelectFromMaterializedViewWithNoOwnerSpecifiedAndShouldUseInvoker()
    {
        assertQuery(sessionForUser("alice", "credential"), "SELECT * FROM mock.default.materialized_view_with_no_owner_and_should_use_invoker", "VALUES 'alice_credential'");
        assertQuery(sessionForUser("bob", "new_credential"), "SELECT * FROM mock.default.materialized_view_with_no_owner_and_should_use_invoker", "VALUES 'bob_new_credential'");
        assertQuery(sessionForUser("charlie", "credential"), "SELECT * FROM mock.default.materialized_view_with_no_owner_and_should_use_invoker", "VALUES 'charlie_credential'");
    }

    @Test
    public void testSelectFromMaterializedViewWithNoOwnerSpecified()
    {
        assertQueryFails(
                sessionForUser("alice", "credential"),
                "SELECT * FROM mock.default.materialized_view_with_no_owner",
                "Owner not set for a run-as invoker view: mock.default.materialized_view_with_no_owner");
        assertQueryFails(
                sessionForUser("bob", "new_credential"),
                "SELECT * FROM mock.default.materialized_view_with_no_owner",
                "Owner not set for a run-as invoker view: mock.default.materialized_view_with_no_owner");
        assertQueryFails(
                sessionForUser("charlie", "credential"),
                "SELECT * FROM mock.default.materialized_view_with_no_owner",
                "Owner not set for a run-as invoker view: mock.default.materialized_view_with_no_owner");
    }

    @Test
    public void testSelectFromMaterializedViewWithSpecificOwnerAndShouldUseInvoker()
    {
        assertQuery(sessionForUser("alice", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner_and_should_use_invoker", "VALUES 'alice_credential'");
        assertQuery(sessionForUser("bob", "new_credential"), "SELECT * FROM mock.default.materialized_view_with_owner_and_should_use_invoker", "VALUES 'bob_new_credential'");
        assertQuery(sessionForUser("charlie", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner_and_should_use_invoker", "VALUES 'charlie_credential'");
    }

    @Test
    public void testSelectFromMaterializedViewWithSpecificOwner()
    {
        assertQuery(sessionForUser("alice", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner", "VALUES 'mv_owner_'");
        assertQuery(sessionForUser("bob", "new_credential"), "SELECT * FROM mock.default.materialized_view_with_owner", "VALUES 'mv_owner_'");
        assertQuery(sessionForUser("charlie", "credential"), "SELECT * FROM mock.default.materialized_view_with_owner", "VALUES 'mv_owner_'");
    }

    @Test
    public void testAnalyzeFreshMaterializedView()
    {
        assertQuerySucceeds("SELECT * FROM mock.analysis.fresh_materialized_view");
        assertQueryFails(
                "SELECT * FROM mock.analysis.fresh_materialized_view_non_existent_table",
                "line 1:15: Failed analyzing stored view 'mock.analysis.fresh_materialized_view_non_existent_table': line 1:15: Table 'mock.analysis.non_existent_table' does not exist");
        assertQueryFails(
                "REFRESH MATERIALIZED VIEW mock.analysis.fresh_materialized_view_non_existent_table",
                "line 1:15: Table 'mock.analysis.non_existent_table' does not exist");
    }

    @Test
    public void testAnalyzeMaterializedViewWithAccessControl()
    {
        assertQuerySucceeds("SELECT * FROM mock.analysis.fresh_materialized_view");

        // materialized view analysis should succeed even if access to storage table is denied when querying the table directly
        accessControlManager.deny(privilege("t2.a", SELECT_COLUMN));
        assertQuerySucceeds("SELECT * FROM mock.analysis.fresh_materialized_view");

        accessControlManager.deny(privilege("fresh_materialized_view.a", SELECT_COLUMN));
        assertQueryFails(
                "SELECT * FROM mock.analysis.fresh_materialized_view",
                "Access Denied: Cannot select from columns \\[a, b] in table or view mock\\.analysis\\.fresh_materialized_view");
        accessControlManager.reset();

        // Deny access to the table referenced by the underlying query
        accessControlManager.denyIdentityTable((_, table) -> !"t1".equals(table));
        assertQueryFails(
                "SELECT * FROM mock.analysis.fresh_materialized_view",
                "Access Denied: View owner does not have sufficient privileges: View owner 'some user' cannot create view that selects from mock\\.analysis\\.t1");
        assertQueryFails(
                "REFRESH MATERIALIZED VIEW mock.analysis.fresh_materialized_view",
                "Access Denied: Cannot select from columns \\[a, b] in table or view mock\\.analysis\\.t1");
        assertQueryFails(
                "SELECT * FROM mock.analysis.stale_materialized_view",
                "Access Denied: View owner does not have sufficient privileges: View owner 'some user' cannot create view that selects from mock\\.analysis\\.t1");
        assertQueryFails(
                "REFRESH MATERIALIZED VIEW mock.analysis.stale_materialized_view",
                "Access Denied: Cannot select from columns \\[a, b] in table or view mock\\.analysis\\.t1");
    }

    private Session sessionForUser(String user, String extraCredential)
    {
        return testSessionBuilder()
                .setCatalog("mock")
                .setSchema("mock")
                .setIdentity(
                        Identity.forUser(user)
                                .withExtraCredentials(ImmutableMap.of(EXTRA_CREDENTIAL_KEY, extraCredential))
                                .build())
                .build();
    }

    private static ConnectorMaterializedViewDefinition createMaterializedViewRequiringAnalysis(String query)
    {
        boolean canSkipQueryAnalysis = false;
        return new ConnectorMaterializedViewDefinition(
                query,
                Optional.of(new CatalogSchemaTableName("mock", "analysis", "t3")),
                Optional.of("mock"),
                Optional.of("analysis"),
                ImmutableList.of(new Column("a", BIGINT.getTypeId(), Optional.empty()), new Column("b", BIGINT.getTypeId(), Optional.empty())),
                Optional.of(Duration.ZERO),
                Optional.empty(),
                Optional.empty(),
                Optional.of("some user"),
                ImmutableList.of(),
                false,
                canSkipQueryAnalysis);
    }
}
