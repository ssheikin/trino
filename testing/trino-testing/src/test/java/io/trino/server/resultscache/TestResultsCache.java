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
package io.trino.server.resultscache;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.server.resultscache.ResultsCacheSessionProperties.CACHE_KEY;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestResultsCache
        extends AbstractTestQueryFramework
{
    private final TestingCacheClient cacheClient = new TestingCacheClient();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setCoordinatorProperties(ImmutableMap.of("results-cache.enabled", "true"))
                .setAdditionalModule(binder -> newOptionalBinder(binder, CacheClient.class).setBinding().toInstance(cacheClient))
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        return queryRunner;
    }

    @ParameterizedTest
    @MethodSource("cacheableQueries")
    public void testCacheableQueries(String cacheKey, String query)
    {
        Session session = testSessionBuilder()
                .setSystemProperty(CACHE_KEY, cacheKey)
                .build();

        getQueryRunner().execute(session, query);

        cacheClient.assertContains(cacheKey);
    }

    public Stream<Arguments> cacheableQueries()
    {
        return Stream.of(
                Arguments.of(
                        "testRegularTable",
                        "SELECT * FROM tpch.tiny.nation"),
                Arguments.of(
                        "testJoinedTables",
                        """
                                SELECT nation.name, customer.name
                                FROM tpch.tiny.customer
                                JOIN tpch.tiny.nation ON nation.nationkey = customer.nationkey
                                JOIN tpch.tiny.orders ON orders.custkey = customer.custkey
                                LIMIT 10
                                """));
    }

    @ParameterizedTest
    @MethodSource("nonCacheableQueries")
    public void testNonCacheableQueries(String cacheKey, String query)
            throws InterruptedException
    {
        Session session = testSessionBuilder()
                .setSystemProperty(CACHE_KEY, cacheKey)
                .build();

        getQueryRunner().execute(session, query);

        cacheClient.assertDoesNotContain(cacheKey);
    }

    public Stream<Arguments> nonCacheableQueries()
    {
        return Stream.of(
                Arguments.of("testSystemTable", "SELECT * FROM system.runtime.nodes"),
                Arguments.of("testMixedTablesWithSystem", "SELECT count(*) FROM tpch.tiny.nation CROSS JOIN system.runtime.nodes"),
                Arguments.of("testInformationSchema", "SELECT * FROM tpch.information_schema.tables"),
                Arguments.of("testSystemMetadataTable", "SELECT count(*) FROM system.metadata.catalogs"),
                Arguments.of("testMixedTablesWithoutSystem", "SELECT * FROM tpch.tiny.nation CROSS JOIN tpch.information_schema.tables CROSS JOIN system.metadata.catalogs"),
                Arguments.of("testShowCatalogs", "SHOW catalogs"),
                Arguments.of("testShowSchemas", "SHOW SCHEMAS IN tpch"),
                Arguments.of("testShowTables", "SHOW TABLES IN tpch.tiny"),
                Arguments.of("testShowColumns", "SHOW COLUMNS IN tpch.tiny.nation"));
    }

    private static class TestingCacheClient
            implements CacheClient
    {
        private final Set<String> entries = newSetFromMap(new ConcurrentHashMap<>());

        @Override
        public void insertCacheEntry(CacheEntry cacheEntry)
        {
            assertThat(entries.add(cacheEntry.key())).isTrue();
        }

        void assertContains(String cacheEntry)
        {
            // upload of results cache entry is asynchronous so there might be a slight delay before results are cached
            assertEventually(
                    new Duration(5, SECONDS),
                    () -> assertThat(entries).contains(cacheEntry));
        }

        void assertDoesNotContain(String cacheEntry)
                throws InterruptedException
        {
            // upload of results cache entry is asynchronous so we need to wait in order to confirm that the results were not cached
            Thread.sleep(1_000);
            assertThat(entries).doesNotContain(cacheEntry);
        }
    }
}
