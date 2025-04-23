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

import static com.google.common.base.Preconditions.checkArgument;

@InterfaceAudience.Private
public class BeginTransactionResponse
        extends KuduRpcResponse
{
    private final long txnId;
    private final int keepaliveMillis;

    /**
     * @param elapsedMillis time in milliseconds since RPC creation to now
     * @param serverUUID UUID of the server that sent the response
     * @param txnId identifier of the new transaction
     * @param keepaliveMillis keepalive interval for the newly started transaction
     */
    BeginTransactionResponse(
            long elapsedMillis, String serverUUID, long txnId, int keepaliveMillis)
    {
        super(elapsedMillis, serverUUID);
        checkArgument(txnId > AsyncKuduClient.INVALID_TXN_ID);
        checkArgument(keepaliveMillis >= 0);
        this.txnId = txnId;
        this.keepaliveMillis = keepaliveMillis;
    }

    /**
     * @return the identifier of the started transaction
     */
    public long txnId()
    {
        return txnId;
    }

    /**
     * @return the keepalive interval for the started transaction (milliseconds)
     */
    public int keepaliveMillis()
    {
        return keepaliveMillis;
    }
}
