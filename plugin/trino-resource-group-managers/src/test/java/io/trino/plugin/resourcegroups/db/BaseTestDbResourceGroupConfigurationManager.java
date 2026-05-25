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
package io.trino.plugin.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonCodec;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.resourcegroups.ResourceGroupIdTemplate;
import io.trino.spi.memory.ClusterMemoryPoolManager;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.spi.resourcegroups.SelectionContext;
import io.trino.spi.resourcegroups.SelectionCriteria;
import io.trino.spi.session.ResourceEstimates;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.Isolated;
import org.testcontainers.containers.JdbcDatabaseContainer;

import java.util.Optional;

import static io.trino.spi.resourcegroups.QueryType.INSERT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
@Isolated
public abstract class BaseTestDbResourceGroupConfigurationManager
{
    private static final String ENVIRONMENT = "test";
    private static final String PROD_ENVIRONMENT = "prod";
    private static final ResourceEstimates EMPTY_RESOURCE_ESTIMATES = new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty());
    private static final String QUERY = "SELECT * FROM tableX";

    @AutoClose
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private Handle handle;
    private ResourceGroupsDao resourceGroupsDao;

    protected abstract JdbcDatabaseContainer<?> createDatabaseContainer();

    @BeforeAll
    public void setup()
    {
        JdbcDatabaseContainer<?> database = closer.register(createDatabaseContainer());
        database.start();
        Bootstrap app = new Bootstrap(
                new DbResourceGroupsModule(),
                binder -> binder.bind(String.class).annotatedWith(ForEnvironment.class).toInstance(ENVIRONMENT),
                binder -> binder.bind(ClusterMemoryPoolManager.class).toInstance(_ -> {}));

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(
                        ImmutableMap.<String, String>builder()
                                .put("resource-groups.config-db-url", database.getJdbcUrl())
                                .put("resource-groups.config-db-user", database.getUsername())
                                .put("resource-groups.config-db-password", database.getPassword())
                                .buildOrThrow())
                .initialize();

        injector.getInstance(FlywayMigration.class).migrate();
        Jdbi jdbi = injector.getInstance(Jdbi.class);
        resourceGroupsDao = injector.getInstance(ResourceGroupsDao.class);
        handle = jdbi.open();
    }

    @AfterEach
    public void cleanup()
    {
        handle.execute("DELETE FROM selectors");
        handle.execute("DELETE FROM exact_match_source_selectors");
        handle.execute("DELETE FROM resource_groups");
        handle.execute("DELETE FROM resource_groups_global_properties");
    }

    @Test
    public void testConfiguration()
    {
        insertResourceGroup(1, "global", "1MB", 100, 10, 10, null, ENVIRONMENT);
        insertResourceGroup(2, "sub", "1MB", 10, 5, 5, 1L, ENVIRONMENT);
        insertSelector(2, 1, "user");

        DbResourceGroupConfigurationManager manager = createManager(ENVIRONMENT);
        assertThat(manager.getRootGroups()).hasSize(1);
        assertThat(manager.match(selectionCriteria("user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("global.sub")));
    }

    @Test
    public void testSelectorPriority()
    {
        insertResourceGroup(1, "low", "1MB", 100, 10, 10, null, ENVIRONMENT);
        insertResourceGroup(2, "high", "1MB", 100, 10, 10, null, ENVIRONMENT);
        insertSelector(1, 1, "user");
        insertSelector(2, 100, "user");

        DbResourceGroupConfigurationManager manager = createManager(ENVIRONMENT);
        assertThat(manager.match(selectionCriteria("user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("high")));
    }

    @Test
    public void testEnvironmentSeparation()
    {
        insertResourceGroup(1, "test_root", "1MB", 100, 10, 10, null, ENVIRONMENT);
        insertResourceGroup(2, "prod_root", "1MB", 100, 10, 10, null, PROD_ENVIRONMENT);
        insertResourceGroup(3, "test_sub", "1MB", 10, 5, 5, 1L, ENVIRONMENT);
        insertSelector(1, 1, "test_root_user");
        insertSelector(2, 2, "prod_root_user");
        insertSelector(3, 3, "test_sub_user");

        DbResourceGroupConfigurationManager testManager = createManager(ENVIRONMENT);
        DbResourceGroupConfigurationManager prodManager = createManager(PROD_ENVIRONMENT);

        assertThat(testManager.getRootGroups()).hasSize(1);
        assertThat(prodManager.getRootGroups()).hasSize(1);

        assertThat(testManager.match(selectionCriteria("test_root_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("test_root")));
        assertThat(testManager.match(selectionCriteria("test_sub_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("test_root.test_sub")));
        assertThat(testManager.match(selectionCriteria("prod_root_user"))).isEmpty();

        assertThat(prodManager.match(selectionCriteria("prod_root_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("prod_root")));
        assertThat(prodManager.match(selectionCriteria("test_root_user"))).isEmpty();
        assertThat(prodManager.match(selectionCriteria("test_sub_user"))).isEmpty();
    }

    @Test
    public void testNullEnvironmentWildcard()
    {
        insertResourceGroup(1, "test_global", "1MB", 100, 10, 10, null, ENVIRONMENT);
        insertResourceGroup(2, "shared_global", "1MB", 100, 10, 10, null, null);
        insertSelector(1, 1, "test_user");
        insertSelector(2, 2, "shared_user");

        DbResourceGroupConfigurationManager testManager = createManager(ENVIRONMENT);
        DbResourceGroupConfigurationManager prodManager = createManager(PROD_ENVIRONMENT);

        assertThat(testManager.match(selectionCriteria("test_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("test_global")));
        assertThat(testManager.match(selectionCriteria("shared_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("shared_global")));

        assertThat(prodManager.match(selectionCriteria("test_user"))).isEmpty();
        assertThat(prodManager.match(selectionCriteria("shared_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("shared_global")));
    }

    @Test
    public void testNullEnvironmentFullPath()
    {
        insertResourceGroup(1, "root", "1MB", 100, 10, 10, null, null);
        insertResourceGroup(2, "child", "1MB", 100, 10, 10, 1L, null);
        insertResourceGroup(3, "grandchild", "1MB", 100, 10, 10, 2L, null);
        insertSelector(3, 1, "user");

        DbResourceGroupConfigurationManager testManager = createManager(ENVIRONMENT);
        DbResourceGroupConfigurationManager prodManager = createManager(PROD_ENVIRONMENT);

        assertThat(testManager.match(selectionCriteria("user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("root.child.grandchild")));
        assertThat(prodManager.match(selectionCriteria("user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("root.child.grandchild")));
    }

    @Test
    public void testNullEnvironmentChildrenWithSpecificParent()
    {
        insertResourceGroup(1, "root", "1MB", 100, 10, 10, null, ENVIRONMENT);
        insertResourceGroup(2, "child", "1MB", 100, 10, 10, 1L, null);
        insertResourceGroup(3, "grandchild", "1MB", 100, 10, 10, 2L, null);
        insertSelector(3, 1, "user");

        DbResourceGroupConfigurationManager testManager = createManager(ENVIRONMENT);
        DbResourceGroupConfigurationManager prodManager = createManager(PROD_ENVIRONMENT);

        assertThat(testManager.match(selectionCriteria("user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("root.child.grandchild")));
        // root is test-only, so the entire subtree is unreachable for prod — no selectors loaded
        assertThatThrownBy(() -> prodManager.match(selectionCriteria("user")))
                .hasMessage("No selectors are configured");
    }

    @Test
    public void testNullEnvironmentMixedHierarchy()
    {
        insertResourceGroup(1, "root", "1MB", 100, 10, 10, null, null);
        insertResourceGroup(2, "test_child", "1MB", 100, 10, 10, 1L, ENVIRONMENT);
        insertResourceGroup(3, "prod_child", "1MB", 100, 10, 10, 1L, PROD_ENVIRONMENT);
        // shared_grandchild has null env, but its parent (test_child) is test-only
        insertResourceGroup(4, "shared_grandchild", "1MB", 100, 10, 10, 2L, null);

        insertSelector(1, 1, "root_user");
        insertSelector(2, 2, "test_user");
        insertSelector(3, 3, "prod_user");
        insertSelector(4, 4, "shared_user");

        DbResourceGroupConfigurationManager testManager = createManager(ENVIRONMENT);
        DbResourceGroupConfigurationManager prodManager = createManager(PROD_ENVIRONMENT);

        // Test env sees root, test_child, shared_grandchild
        assertThat(testManager.getSelectors()).hasSize(3);
        assertThat(testManager.match(selectionCriteria("shared_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("root.test_child.shared_grandchild")));
        assertThat(testManager.match(selectionCriteria("prod_user"))).isEmpty();

        // Prod env sees root, prod_child only — shared_grandchild excluded (unreachable parent)
        assertThat(prodManager.getSelectors()).hasSize(2);
        assertThat(prodManager.match(selectionCriteria("shared_user"))).isEmpty();
        assertThat(prodManager.match(selectionCriteria("prod_user")))
                .map(SelectionContext::getContext)
                .isEqualTo(Optional.of(new ResourceGroupIdTemplate("root.prod_child")));
    }

    @Test
    public void testNullEnvironmentExactMatchSelector()
    {
        insertResourceGroup(1, "global", "1MB", 100, 10, 10, null, null);
        insertSelector(1, 1, "dummy_user_never_matched");

        JsonCodec<ResourceGroupId> codec = JsonCodec.jsonCodec(ResourceGroupId.class);
        ResourceGroupId wildcardGroup = new ResourceGroupId(ImmutableList.of("global", "wildcard"));
        ResourceGroupId testSpecificGroup = new ResourceGroupId(ImmutableList.of("global", "test_specific"));

        insertExactMatchSelector(null, "@shared@pipeline", INSERT.name(), codec.toJson(wildcardGroup));
        insertExactMatchSelector(ENVIRONMENT, "@shared@pipeline", INSERT.name(), codec.toJson(testSpecificGroup));

        DbResourceGroupConfig config = new DbResourceGroupConfig();
        config.setExactMatchSelectorEnabled(true);

        DbResourceGroupConfigurationManager testManager = createManager(config, ENVIRONMENT);
        DbResourceGroupConfigurationManager prodManager = createManager(config, PROD_ENVIRONMENT);

        // Specific env wins
        assertThat(testManager.match(exactMatchCriteria("@shared@pipeline", INSERT.name())))
                .map(ctx -> ctx.getResourceGroupId().toString())
                .isEqualTo(Optional.of("global.test_specific"));

        // Falls back to wildcard
        assertThat(prodManager.match(exactMatchCriteria("@shared@pipeline", INSERT.name())))
                .map(ctx -> ctx.getResourceGroupId().toString())
                .isEqualTo(Optional.of("global.wildcard"));

        // Non-matching source
        assertThat(testManager.match(exactMatchCriteria("@unknown@pipeline", INSERT.name()))).isEmpty();
    }

    private DbResourceGroupConfigurationManager createManager(String environment)
    {
        return createManager(new DbResourceGroupConfig(), environment);
    }

    private DbResourceGroupConfigurationManager createManager(DbResourceGroupConfig config, String environment)
    {
        return new DbResourceGroupConfigurationManager(_ -> {}, config, resourceGroupsDao, environment);
    }

    private void insertResourceGroup(
            long id,
            String name,
            String softMemoryLimit,
            int maxQueued,
            int softConcurrencyLimit,
            int hardConcurrencyLimit,
            Long parent,
            String environment)
    {
        handle.execute(
                "INSERT INTO resource_groups " +
                        "(resource_group_id, name, soft_memory_limit, max_queued, " +
                        "soft_concurrency_limit, hard_concurrency_limit, parent, environment) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                id,
                name,
                softMemoryLimit,
                maxQueued,
                softConcurrencyLimit,
                hardConcurrencyLimit,
                parent,
                environment);
    }

    private void insertSelector(long resourceGroupId, long priority, String userRegex)
    {
        handle.execute(
                "INSERT INTO selectors (resource_group_id, priority, user_regex) VALUES (?, ?, ?)",
                resourceGroupId,
                priority,
                userRegex);
    }

    private void insertExactMatchSelector(String environment, String source, String queryType, String resourceGroupId)
    {
        handle.execute(
                "INSERT INTO exact_match_source_selectors " +
                        "(environment, source, query_type, update_time, resource_group_id) " +
                        "VALUES (?, ?, ?, CURRENT_TIMESTAMP, ?)",
                environment,
                source,
                queryType,
                resourceGroupId);
    }

    private static SelectionCriteria selectionCriteria(String user)
    {
        return new SelectionCriteria(true, user, ImmutableSet.of(), user, Optional.empty(), Optional.empty(), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, QUERY, Optional.empty());
    }

    private static SelectionCriteria exactMatchCriteria(String source, String queryType)
    {
        return new SelectionCriteria(true, "user", ImmutableSet.of(), "user", Optional.empty(), Optional.of(source), ImmutableSet.of(), EMPTY_RESOURCE_ESTIMATES, QUERY, Optional.of(queryType));
    }
}
