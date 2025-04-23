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

import com.google.common.base.Stopwatch;

import java.util.concurrent.TimeUnit;

/**
 * This is a wrapper class around {@link Stopwatch} used to track a timeout
 * in the future.
 * The watch starts as soon as this object is created with a timeout of 0, which means that
 * there's no timeout.
 * The timeout has been reached once the stopwatch's elapsed time is equal or greater than the
 * provided timeout.
 */
public class TimeoutTracker
{
    private final Stopwatch stopwatch;
    /**
     * timeout in milliseconds
     **/
    private long timeout;

    /**
     * Creates a new tracker, which starts the stopwatch right now.
     */
    public TimeoutTracker()
    {
        this(Stopwatch.createUnstarted());
    }

    /**
     * Creates a new tracker, using the specified stopwatch, and starts it right now.
     * The stopwatch is reset if it was already running.
     *
     * @param stopwatch Specific Stopwatch to use
     */
    public TimeoutTracker(Stopwatch stopwatch)
    {
        if (stopwatch.isRunning()) {
            stopwatch.reset();
        }
        this.stopwatch = stopwatch.start();
    }

    /**
     * Check if we're already past the timeout.
     *
     * @return true if we're past the timeout, otherwise false. Also returns false if no timeout
     * was specified
     */
    public boolean timedOut()
    {
        if (!hasTimeout()) {
            return false;
        }
        return timeout - stopwatch.elapsed(TimeUnit.MILLISECONDS) <= 0;
    }

    public long getMillisBeforeTimeout()
    {
        if (!hasTimeout()) {
            throw new IllegalStateException("This tracker doesn't have a timeout set so it cannot " +
                    "answer getMillisBeforeTimeout()");
        }
        long millisBeforeTimeout = timeout - stopwatch.elapsed(TimeUnit.MILLISECONDS);
        millisBeforeTimeout = millisBeforeTimeout <= 0 ? 1 : millisBeforeTimeout;
        return millisBeforeTimeout;
    }

    public long getElapsedMillis()
    {
        return this.stopwatch.elapsed(TimeUnit.MILLISECONDS);
    }

    /**
     * Tells if a non-zero timeout was set.
     *
     * @return true if the timeout is greater than 0, false otherwise.
     */
    public boolean hasTimeout()
    {
        return timeout != 0;
    }

    /**
     * Utility method to check if sleeping for a specified amount of time would put us past the
     * timeout.
     *
     * @param plannedSleepTimeMillis number of milliseconds for a planned sleep
     * @return if the planned sleeps goes past the timeout.
     */
    public boolean wouldSleepingTimeoutMillis(long plannedSleepTimeMillis)
    {
        if (!hasTimeout()) {
            return false;
        }
        return getMillisBeforeTimeout() - plannedSleepTimeMillis <= 0;
    }

    /**
     * Sets the timeout to 0 (no timeout) and restarts the stopwatch from scratch.
     */
    public void reset()
    {
        timeout = 0;
        stopwatch.reset();
        stopwatch.start();
    }

    /**
     * Get the timeout (in milliseconds).
     *
     * @return the current timeout
     */
    public long getTimeout()
    {
        return timeout;
    }

    /**
     * Set a new timeout for this tracker. It cannot be smaller than 0,
     * and if it is 0 then it means that there is no timeout (which is the default behavior).
     * This method won't call reset().
     *
     * @param timeout a number of milliseconds greater or equal to 0
     * @throws IllegalArgumentException if the timeout is lesser than 0
     */
    public void setTimeout(long timeout)
    {
        if (timeout < 0) {
            throw new IllegalArgumentException("The timeout must be greater or equal to 0, " +
                    "the passed value is " + timeout);
        }
        this.timeout = timeout;
    }

    @Override
    public String toString()
    {
        StringBuilder buf = new StringBuilder("TimeoutTracker(timeout=");
        buf.append(timeout);
        buf.append(", elapsed=").append(stopwatch.elapsed(TimeUnit.MILLISECONDS));
        buf.append(")");
        return buf.toString();
    }
}
