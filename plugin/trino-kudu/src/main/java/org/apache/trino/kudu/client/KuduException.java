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

import com.stumbleupon.async.DeferredGroupException;
import com.stumbleupon.async.TimeoutException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.io.IOException;

/**
 * The parent class of all exceptions sent by the Kudu client. This is the only exception you will
 * see if you're using the non-async API, such as {@link KuduSession} instead of
 * {@link AsyncKuduSession}.
 * Each instance of this class has a {@link Status} which gives more information about the error.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public abstract class KuduException
        extends IOException
{
    private final Status status;

    /**
     * Constructor.
     *
     * @param status object containing the reason for the exception
     * trace.
     */
    KuduException(Status status)
    {
        super(status.getMessage());
        this.status = status;
    }

    /**
     * Constructor.
     *
     * @param status object containing the reason for the exception
     * @param cause The exception that caused this one to be thrown.
     */
    KuduException(Status status, Throwable cause)
    {
        super(status.getMessage(), cause);
        this.status = status;
    }

    /**
     * Get the Status object for this exception.
     *
     * @return a status object indicating the reason for the exception
     */
    public Status getStatus()
    {
        return status;
    }

    /**
     * When exceptions are thrown by the asynchronous Kudu client, the stack trace is
     * typically deep within the internals of the Kudu client and/or Netty.
     * Thus, when the synchronous Kudu client wraps and
     * throws the exception,
     * we suppress that stack trace and replace it with the stack trace of the user's
     * calling thread. The original stack trace is added to the {@link KuduException}
     * as a suppressed exception (see Throwable#addSuppressed(Throwable)) of
     * this
     */
    @InterfaceAudience.Public
    @InterfaceStability.Evolving
    public static class OriginalException
            extends Throwable
    {
        private OriginalException(Throwable e)
        {
            super("Original asynchronous stack trace");
            setStackTrace(e.getStackTrace());
        }
    }

    /**
     * Inspects the given exception and transforms it into a KuduException.
     *
     * @param e generic exception we want to transform
     * @return a KuduException that's easier to handle
     */
    static KuduException transformException(Exception e)
    {
        // The message may be null.
        String message = e.getMessage() == null ? "" : e.getMessage();
        if (e instanceof KuduException) {
            // The exception thrown inside the async code has a stack trace
            // that doesn't correspond to where the user actually called
            // some synchronous method. This can be very confusing for
            // users, so we'll reset the stack trace back the call frame
            // where we are transforming it.
            e.addSuppressed(new OriginalException(e));
            StackTraceElement[] stack = new Exception().getStackTrace();
            e.setStackTrace(stack);
            return (KuduException) e;
        }
        else if (e instanceof DeferredGroupException) {
            // The cause of a DeferredGroupException is the first exception it sees, we're just going to
            // use it as our main exception. DGE doesn't let us see the other exceptions anyways.
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                return transformException((Exception) cause);
            }
            // Else fall down into a generic exception at the end.
        }
        else if (e instanceof TimeoutException) {
            Status statusTimeout = Status.TimedOut(message);
            return new NonRecoverableException(statusTimeout, e);
        }
        else if (e instanceof InterruptedException) {
            // Need to reset the interrupt flag since we caught it but aren't handling it.
            Thread.currentThread().interrupt();

            Status statusAborted = Status.Aborted(message);
            return new NonRecoverableException(statusAborted, e);
        }
        Status status = Status.IOError(message);
        return new NonRecoverableException(status, e);
    }
}
