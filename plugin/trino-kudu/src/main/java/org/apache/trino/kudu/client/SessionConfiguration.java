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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Interface that defines the methods used to configure a session. It also exposes ways to
 * query its state.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface SessionConfiguration
{
    @InterfaceAudience.Public
    @InterfaceStability.Evolving
    enum FlushMode
    {
        AUTO_FLUSH_SYNC,
        AUTO_FLUSH_BACKGROUND,
        MANUAL_FLUSH
    }

    /**
     * Get the current flush mode.
     *
     * @return flush mode, {@link FlushMode#AUTO_FLUSH_SYNC AUTO_FLUSH_SYNC} by default
     */
    FlushMode getFlushMode();

    /**
     * Set the new flush mode for this session.
     *
     * @param flushMode new flush mode, can be the same as the previous one.
     * @throws IllegalArgumentException if the buffer isn't empty.
     */
    void setFlushMode(FlushMode flushMode);

    /**
     * Set the number of operations that can be buffered.
     *
     * @param size number of ops.
     * @throws IllegalArgumentException if the buffer isn't empty.
     */
    void setMutationBufferSpace(int size);

    /**
     * Set the low watermark for this session. The default is set to half the mutation buffer space.
     * For example, a buffer space of 1000 with a low watermark set to 50% (0.5) will start randomly
     * sending PleaseRetryExceptions once there's an outstanding flush and the buffer is over 500.
     * As the buffer gets fuller, it becomes likelier to hit the exception.
     *
     * @param mutationBufferLowWatermarkPercentage a new low watermark as a percentage,
     * has to be between 0  and 1 (inclusive). A value of 1 disables
     * the low watermark since it's the same as the high one
     * @throws IllegalArgumentException if the buffer isn't empty or if the watermark isn't between
     * 0 and 1
     * @deprecated The low watermark no longer has any effect.
     */
    @Deprecated
    void setMutationBufferLowWatermark(float mutationBufferLowWatermarkPercentage);

    /**
     * Set the flush interval, which will be used for the next scheduling decision.
     *
     * @param interval interval in milliseconds.
     */
    void setFlushInterval(int interval);

    /**
     * Get the current timeout.
     *
     * @return operation timeout in milliseconds, 0 if none was configured.
     */
    long getTimeoutMillis();

    /**
     * Sets the timeout for the next applied operations.
     * The default timeout is 0, which disables the timeout functionality.
     *
     * @param timeout Timeout in milliseconds.
     */
    void setTimeoutMillis(long timeout);

    /**
     * Returns true if this session has already been closed.
     */
    boolean isClosed();

    /**
     * Check if there are operations that haven't been completely applied.
     *
     * @return true if operations are pending, else false.
     */
    boolean hasPendingOperations();

    /**
     * Set the new external consistency mode for this session.
     *
     * @param consistencyMode new external consistency mode, can the same as the previous one.
     * @throws IllegalArgumentException if the buffer isn't empty.
     */
    void setExternalConsistencyMode(ExternalConsistencyMode consistencyMode);

    /**
     * Tells if the session is currently ignoring row errors when the whole list returned by a tablet
     * server is of the AlreadyPresent type.
     *
     * @return true if the session is enforcing this, else false
     */
    boolean isIgnoreAllDuplicateRows();

    /**
     * Configures the option to ignore all the row errors if they are all of the AlreadyPresent type.
     * This can be useful when it is possible for INSERT operations to be retried and fail.
     * The effect of enabling this is that operation responses that match this pattern will be
     * cleared of their row errors, meaning that we consider them successful.
     * p
     * TODO(KUDU-1563): Implement server side ignore capabilities to improve performance and
     *  reliability of INSERT ignore operations.
     *
     * @param ignoreAllDuplicateRows true if this session should enforce this, else false
     */
    void setIgnoreAllDuplicateRows(boolean ignoreAllDuplicateRows);

    /**
     * Tells if the session is currently ignoring row errors when the whole list returned by a tablet
     * server is of the NotFound type.
     *
     * @return true if the session is enforcing this, else false
     */
    boolean isIgnoreAllNotFoundRows();

    /**
     * Configures the option to ignore all the row errors if they are all of the NotFound type.
     * This can be useful when it is possible for DELETE operations to be retried and fail.
     * The effect of enabling this is that operation responses that match this pattern will be
     * cleared of their row errors, meaning that we consider them successful.
     * p
     * TODO(KUDU-1563): Implement server side ignore capabilities to improve performance and
     *  reliability of DELETE ignore operations.
     *
     * @param ignoreAllNotFoundRows true if this session should enforce this, else false
     */
    void setIgnoreAllNotFoundRows(boolean ignoreAllNotFoundRows);

    /**
     * Set the number of errors that can be collected.
     *
     * @param size number of errors.
     */
    void setErrorCollectorSpace(int size);

    /**
     * Return the number of errors which are pending. Errors may accumulate when
     * using {@link FlushMode#AUTO_FLUSH_BACKGROUND AUTO_FLUSH_BACKGROUND} mode.
     *
     * @return a count of errors
     */
    int countPendingErrors();

    /**
     * Return any errors from previous calls. If there were more errors
     * than could be held in the session's error storage, the overflow state is set to true.
     *
     *
     * @return an object that contains the errors and the overflow status
     */
    RowErrorsAndOverflowStatus getPendingErrors();

    /**
     * Return cumulative write operation metrics since the beginning of the session.
     *
     * @return cumulative write operation metrics since the beginning of the session.
     */
    ResourceMetrics getWriteOpMetrics();
}
