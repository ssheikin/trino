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
package io.trino.plugin.kudu;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.FixedSplitSource;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.kudu.KuduSessionProperties.getDynamicFilteringWaitTimeout;
import static java.util.Objects.requireNonNull;

public class KuduSplitManager
        implements ConnectorSplitManager
{
    private final KuduClientSession clientSession;

    @Inject
    public KuduSplitManager(KuduClientSession clientSession)
    {
        this.clientSession = requireNonNull(clientSession, "clientSession is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            Set<ColumnHandle> dynamicFilterColumns,
            Constraint constraint)
    {
        return new KuduDynamicFilteringSplitSource(session, clientSession, (KuduTableHandle) table);
    }

    private static class KuduDynamicFilteringSplitSource
            implements ConnectorSplitSource
    {
        private final ConnectorSession connectorSession;
        private final KuduClientSession clientSession;
        private final KuduTableHandle tableHandle;
        private final long dynamicFilteringTimeoutMillis;

        private Optional<ConnectorSplitSource> delegateSplitSource = Optional.empty();

        private KuduDynamicFilteringSplitSource(
                ConnectorSession connectorSession,
                KuduClientSession clientSession,
                KuduTableHandle tableHandle)
        {
            this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
            this.clientSession = requireNonNull(clientSession, "clientSession is null");
            this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
            this.dynamicFilteringTimeoutMillis = getDynamicFilteringWaitTimeout(connectorSession).toMillis();
        }

        @Override
        public long getRequestedDynamicFilterWaitTimeoutMillis()
        {
            return dynamicFilteringTimeoutMillis;
        }

        @Override
        public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
        {
            if (delegateSplitSource.isEmpty()) {
                List<KuduSplit> splits = clientSession.buildKuduSplits(connectorSession, tableHandle, dynamicFilterSnapshot.currentPredicate());
                delegateSplitSource = Optional.of(new FixedSplitSource(splits));
            }

            return delegateSplitSource.get().getNextBatch(maxSize, dynamicFilterSnapshot);
        }

        @Override
        public void close()
        {
            delegateSplitSource.ifPresent(ConnectorSplitSource::close);
        }

        @Override
        public boolean isFinished()
        {
            return delegateSplitSource
                    .map(ConnectorSplitSource::isFinished)
                    .orElse(false);
        }
    }
}
