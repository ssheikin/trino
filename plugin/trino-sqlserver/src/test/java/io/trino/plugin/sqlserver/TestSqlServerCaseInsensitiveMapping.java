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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.plugin.sqlserver.SqlServerQueryRunner.createSqlServerQueryRunner;
import static java.util.Objects.requireNonNull;

// With case-insensitive-name-matching enabled colliding schema/table names are considered as errors.
// Some tests here create colliding names which can cause any other concurrent test to fail.
@Test(singleThreaded = true)
public class TestSqlServerCaseInsensitiveMapping
        extends BaseSqlServerCaseInsensitiveMappingTest
{
    private TestingSqlServer sqlServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        sqlServer = closeAfterClass(new TestingSqlServer());
        sqlServer.start();
        return createSqlServerQueryRunner(
                sqlServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("case-insensitive-name-matching", "true")
                        .build(),
                ImmutableList.of());
    }

    @Override
    protected TestingSqlServer getSqlServer()
    {
        return requireNonNull(sqlServer, "sqlServer is null");
    }
}
