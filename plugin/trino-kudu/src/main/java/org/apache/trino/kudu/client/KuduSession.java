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

package org.apache.trino.kudu.client;

import com.stumbleupon.async.Deferred;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public class KuduSession
        implements SessionConfiguration
{
    private static final Logger LOG = LoggerFactory.getLogger(KuduSession.class);
    private final AsyncKuduSession session;

    KuduSession(AsyncKuduSession session)
    {
        this.session = session;
    }

    public OperationResponse apply(Operation operation)
            throws KuduException
    {
        while (true) {
            try {
                Deferred<OperationResponse> d = session.apply(operation);
                if (getFlushMode() == FlushMode.AUTO_FLUSH_SYNC) {
                    return d.join();
                }
                break;
            }
            catch (PleaseThrottleException ex) {
                try {
                    ex.getDeferred().join();
                }
                catch (Exception e) {
                    // This is the error response from the buffer that was flushing,
                    // we can't do much with it at this point.
                    LOG.error("Previous batch had this exception", e);
                }
            }
            catch (Exception e) {
                throw KuduException.transformException(e);
            }
        }
        return null;
    }

    /**
     * Blocking call that force flushes this session's buffers. Data is persisted when this call
     * returns, else it will throw an exception.
     *
     * @return a list of OperationResponse, one per operation that was flushed
     * @throws KuduException if anything went wrong
     */
    public List<OperationResponse> flush()
            throws KuduException
    {
        return KuduClient.joinAndHandleException(session.flush());
    }

    /**
     * Blocking call that flushes the buffers (see {@link #flush()}) and closes the sessions.
     *
     * @return List of OperationResponse, one per operation that was flushed
     * @throws KuduException if anything went wrong
     */
    public List<OperationResponse> close()
            throws KuduException
    {
        return KuduClient.joinAndHandleException(session.close());
    }

    @Override
    public FlushMode getFlushMode()
    {
        return session.getFlushMode();
    }

    @Override
    public void setFlushMode(FlushMode flushMode)
    {
        session.setFlushMode(flushMode);
    }

    @Override
    public void setMutationBufferSpace(int numOps)
    {
        session.setMutationBufferSpace(numOps);
    }

    @Override
    public void setErrorCollectorSpace(int size)
    {
        session.setErrorCollectorSpace(size);
    }

    @Override
    @Deprecated
    public void setMutationBufferLowWatermark(float mutationBufferLowWatermarkPercentage)
    {
        LOG.warn("setMutationBufferLowWatermark is deprecated");
    }

    @Override
    public void setFlushInterval(int intervalMillis)
    {
        session.setFlushInterval(intervalMillis);
    }

    @Override
    public long getTimeoutMillis()
    {
        return session.getTimeoutMillis();
    }

    @Override
    public void setTimeoutMillis(long timeout)
    {
        session.setTimeoutMillis(timeout);
    }

    @Override
    public boolean isClosed()
    {
        return session.isClosed();
    }

    @Override
    public boolean hasPendingOperations()
    {
        return session.hasPendingOperations();
    }

    @Override
    public void setExternalConsistencyMode(ExternalConsistencyMode consistencyMode)
    {
        session.setExternalConsistencyMode(consistencyMode);
    }

    @Override
    public boolean isIgnoreAllDuplicateRows()
    {
        return session.isIgnoreAllDuplicateRows();
    }

    @Override
    public void setIgnoreAllDuplicateRows(boolean ignoreAllDuplicateRows)
    {
        session.setIgnoreAllDuplicateRows(ignoreAllDuplicateRows);
    }

    @Override
    public boolean isIgnoreAllNotFoundRows()
    {
        return session.isIgnoreAllNotFoundRows();
    }

    @Override
    public void setIgnoreAllNotFoundRows(boolean ignoreAllNotFoundRows)
    {
        session.setIgnoreAllNotFoundRows(ignoreAllNotFoundRows);
    }

    @Override
    public int countPendingErrors()
    {
        return session.countPendingErrors();
    }

    @Override
    public RowErrorsAndOverflowStatus getPendingErrors()
    {
        return session.getPendingErrors();
    }

    @Override
    public ResourceMetrics getWriteOpMetrics()
    {
        return session.getWriteOpMetrics();
    }
}
