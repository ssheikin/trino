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

/**
 * This exception notifies the application to throttle its use of Kudu.
 * p
 * Since all APIs of {@link AsyncKuduSession} are asynchronous and non-blocking,
 * it's possible that the application would produce RPCs at a rate higher
 * than Kudu is able to handle.  When this happens, {@link AsyncKuduSession}
 * will typically do some buffering up to a certain point beyond which RPCs
 * will fail-fast with this exception, to prevent the application from
 * running itself out of memory.
 * p
 * This exception is expected to be handled by having the application
 * throttle or pause itself for a short period of time before retrying the
 * RPC that failed with this exception as well as before sending other RPCs.
 * The reason this exception inherits from {@link NonRecoverableException}
 * instead of {@link RecoverableException} is that the usual course of action
 * when handling a {@link RecoverableException} is to retry right away, which
 * would defeat the whole purpose of this exception.  Here, we want the
 * application to <b>retry after a reasonable delay</b> as well as <b>throttle
 * the pace of creation of new RPCs</b>.  What constitutes a "reasonable
 * delay" depends on the nature of RPCs and rate at which they're produced.
 * p
 * One effective strategy to handle this exception is to set a flag to true
 * when this exception is first emitted that causes the application to pause
 * or throttle its use of Kudu.  Then you can retry the RPC that failed
 * (which is accessible through {@link #getFailedRpc}) and add a callback to
 * it in order to unset the flag once the RPC completes successfully.
 * Note that low-throughput applications will typically rarely (if ever)
 * hit this exception, so they don't need complex throttling logic.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@SuppressWarnings("serial")
public final class PleaseThrottleException
        extends RecoverableException

        implements HasFailedRpcException
{
    /**
     * The RPC that was failed with this exception.
     */
    private final transient Operation rpc;

    /**
     * A deferred one can wait on before retrying the failed RPC.
     */
    private final transient Deferred deferred;

    /**
     * Constructor.
     *
     * @param status status object containing the reason for the exception
     * @param cause The exception that requires the application to throttle
     * itself (can be {@code null})
     * @param rpc The RPC that was made to fail with this exception
     * @param deferred A deferred one can wait on before retrying the failed RPC
     */
    PleaseThrottleException(Status status,
                            KuduException cause,
                            Operation rpc,
                            Deferred deferred)
    {
        super(status, cause);
        this.rpc = rpc;
        this.deferred = deferred;
    }

    /**
     * The RPC that was made to fail with this exception.
     */
    @Override
    public Operation getFailedRpc()
    {
        return rpc;
    }

    /**
     * Returns a deferred one can wait on before retrying the failed RPC.
     *
     * @since 1.3
     */
    public Deferred getDeferred()
    {
        return deferred;
    }
}
