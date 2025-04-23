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

import org.apache.kudu.shaded.com.google.protobuf.Message;
import org.apache.kudu.shaded.io.netty.util.Timer;
import org.apache.trino.kudu.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import static org.apache.kudu.master.Master.IsAlterTableDoneRequestPB;
import static org.apache.kudu.master.Master.IsAlterTableDoneResponsePB;
import static org.apache.kudu.master.Master.TableIdentifierPB;

/**
 * RPC used to check if an alter is running for the specified table
 */
@InterfaceAudience.Private
class IsAlterTableDoneRequest
        extends KuduRpc<IsAlterTableDoneResponse>
{
    private final TableIdentifierPB.Builder tableId;

    IsAlterTableDoneRequest(KuduTable masterTable,
                            TableIdentifierPB.Builder tableId,
                            Timer timer,
                            long timeoutMillis)
    {
        super(masterTable, timer, timeoutMillis);
        this.tableId = tableId;
    }

    @Override
    Message createRequestPB()
    {
        final IsAlterTableDoneRequestPB.Builder builder =
                IsAlterTableDoneRequestPB.newBuilder();
        builder.setTable(tableId);
        return builder.build();
    }

    @Override
    String serviceName()
    {
        return MASTER_SERVICE_NAME;
    }

    @Override
    String method()
    {
        return "IsAlterTableDone";
    }

    @Override
    Pair<IsAlterTableDoneResponse, Object> deserialize(final CallResponse callResponse,
                                                       String tsUUID)
            throws KuduException
    {
        final IsAlterTableDoneResponsePB.Builder respBuilder = IsAlterTableDoneResponsePB.newBuilder();
        readProtobuf(callResponse.getPBMessage(), respBuilder);
        IsAlterTableDoneResponse resp = new IsAlterTableDoneResponse(timeoutTracker.getElapsedMillis(),
                tsUUID,
                respBuilder.getDone());
        return new Pair<IsAlterTableDoneResponse, Object>(
                resp, respBuilder.hasError() ? respBuilder.getError() : null);
    }
}
