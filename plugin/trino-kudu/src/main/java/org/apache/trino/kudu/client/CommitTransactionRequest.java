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

import com.google.common.collect.ImmutableList;
import org.apache.kudu.shaded.com.google.protobuf.Message;
import org.apache.kudu.shaded.io.netty.util.Timer;
import org.apache.kudu.transactions.TxnManager;
import org.apache.trino.kudu.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.kudu.transactions.TxnManager.CommitTransactionResponsePB;

/**
 * A wrapper class for kudu.transactions.TxnManagerService.CommitTransaction RPC.
 */
@InterfaceAudience.Private
class CommitTransactionRequest
        extends KuduRpc<CommitTransactionResponse>
{
    private static final List<Integer> featureFlags = ImmutableList.of();
    final long txnId;

    CommitTransactionRequest(
            KuduTable masterTable, Timer timer, long timeoutMillis, long txnId)
    {
        super(masterTable, timer, timeoutMillis);
        checkArgument(txnId > AsyncKuduClient.INVALID_TXN_ID);
        this.txnId = txnId;
    }

    @Override
    Message createRequestPB()
    {
        final TxnManager.CommitTransactionRequestPB.Builder b =
                TxnManager.CommitTransactionRequestPB.newBuilder();
        b.setTxnId(txnId);
        return b.build();
    }

    @Override
    String serviceName()
    {
        return TXN_MANAGER_SERVICE_NAME;
    }

    @Override
    String method()
    {
        return "CommitTransaction";
    }

    @Override
    Pair<CommitTransactionResponse, Object> deserialize(
            final CallResponse callResponse, String serverUUID)
            throws KuduException
    {
        final CommitTransactionResponsePB.Builder b =
                CommitTransactionResponsePB.newBuilder();
        readProtobuf(callResponse.getPBMessage(), b);
        CommitTransactionResponse response = new CommitTransactionResponse(
                timeoutTracker.getElapsedMillis(), serverUUID);
        return new Pair<>(response, b.hasError() ? b.getError() : null);
    }

    @Override
    Collection<Integer> getRequiredFeatures()
    {
        return featureFlags;
    }
}
