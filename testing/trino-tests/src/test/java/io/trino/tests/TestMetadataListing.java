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

import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.SystemSessionProperties.IGNORE_METADATA_LISTING_EXCEPTIONS;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestMetadataListing
        extends AbstractTestQueryFramework
{
    private static final String EXCEPTION_MESSAGE = "Remote database returned wrong metadata for a column, e.g. VARCHAR.length = -1, or requiredDecimalDigits is not present";
    private static final String CONNECTOR_WITH_FAILING_METADATA = "connector_with_failing_metadata";
    private static final String CATALOG_WITH_FAILING_METADATA = "catalog_with_failing_metadata";
    private static final String TABLE_WITH_FAILING_METADATA = "table_with_failing_metadata";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();
        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withName(CONNECTOR_WITH_FAILING_METADATA)
                .withStreamTableColumns((_, _) -> {
                    throw new IllegalArgumentException(EXCEPTION_MESSAGE);
                })
                .build()));
        queryRunner.createCatalog(CATALOG_WITH_FAILING_METADATA, CONNECTOR_WITH_FAILING_METADATA);
        return queryRunner;
    }

    @Test
    void testFailingBulkListingOfColumns()
    {
        assertQueryFails("select * from %s.information_schema.columns".formatted(CATALOG_WITH_FAILING_METADATA), ".*" + EXCEPTION_MESSAGE);
        assertThat(getQueryRunner().execute(ignoringExceptions(), "select * from %s.information_schema.columns".formatted(CATALOG_WITH_FAILING_METADATA)))
                .hasSize(34);
    }

    @Test
    void testFailingListTableColumnsWithTablePredicate()
    {
        // TODO https://starburstdata.atlassian.net/browse/TRINO-10
        // query with predicate should also throw by default, like query without predicate
        assertQueryReturnsEmptyResult("select * from %s.information_schema.columns where table_name = '%s'".formatted(CATALOG_WITH_FAILING_METADATA, TABLE_WITH_FAILING_METADATA));
        assertQueryReturnsEmptyResult(ignoringExceptions(), "select * from %s.information_schema.columns where table_name = '%s'".formatted(CATALOG_WITH_FAILING_METADATA, TABLE_WITH_FAILING_METADATA));
    }

    private Session ignoringExceptions()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(IGNORE_METADATA_LISTING_EXCEPTIONS, "true")
                .build();
    }
}
