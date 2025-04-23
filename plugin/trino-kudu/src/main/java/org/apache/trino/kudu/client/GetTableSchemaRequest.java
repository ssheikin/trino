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
import org.apache.kudu.master.Master;
import org.apache.kudu.shaded.com.google.protobuf.ByteString;
import org.apache.kudu.shaded.com.google.protobuf.Message;
import org.apache.kudu.shaded.io.netty.util.Timer;
import org.apache.trino.kudu.Schema;
import org.apache.trino.kudu.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.kudu.master.Master.GetTableSchemaRequestPB;
import static org.apache.kudu.master.Master.GetTableSchemaResponsePB;
import static org.apache.kudu.master.Master.TableIdentifierPB;

/**
 * RPC to fetch a table's schema
 */
@InterfaceAudience.Private
public class GetTableSchemaRequest
        extends KuduRpc<GetTableSchemaResponse>
{
    private final String id;
    private final String name;
    private final List<Integer> requiredFeatures;

    GetTableSchemaRequest(KuduTable masterTable,
                          String id,
                          String name,
                          Timer timer,
                          long timeoutMillis,
                          boolean requiresAuthzTokenSupport)
    {
        super(masterTable, timer, timeoutMillis);
        checkArgument(id != null ^ name != null,
                "Only one of table ID or the table name should be provided");
        this.id = id;
        this.name = name;
        this.requiredFeatures = requiresAuthzTokenSupport ?
                ImmutableList.of(Master.MasterFeatures.GENERATE_AUTHZ_TOKEN_VALUE) :
                ImmutableList.of();
    }

    @Override
    Message createRequestPB()
    {
        final GetTableSchemaRequestPB.Builder builder =
                GetTableSchemaRequestPB.newBuilder();
        TableIdentifierPB.Builder identifierBuilder = TableIdentifierPB.newBuilder();
        if (id != null) {
            identifierBuilder.setTableId(ByteString.copyFromUtf8(id));
        }
        else {
            requireNonNull(name);
            identifierBuilder.setTableName(name);
        }
        builder.setTable(identifierBuilder.build());
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
        return "GetTableSchema";
    }

    @Override
    Pair<GetTableSchemaResponse, Object> deserialize(CallResponse callResponse,
                                                     String tsUUID)
            throws KuduException
    {
        final GetTableSchemaResponsePB.Builder respBuilder = GetTableSchemaResponsePB.newBuilder();
        readProtobuf(callResponse.getPBMessage(), respBuilder);
        Schema schema = ProtobufHelper.pbToSchema(respBuilder.getSchema());
        GetTableSchemaResponse response = new GetTableSchemaResponse(
                timeoutTracker.getElapsedMillis(),
                tsUUID,
                schema,
                respBuilder.getTableId().toStringUtf8(),
                respBuilder.getTableName(),
                respBuilder.getNumReplicas(),
                ProtobufHelper.pbToPartitionSchema(respBuilder.getPartitionSchema(), schema),
                respBuilder.hasAuthzToken() ? respBuilder.getAuthzToken() : null,
                respBuilder.getExtraConfigsMap(),
                respBuilder.hasOwner() ? respBuilder.getOwner() : "",
                respBuilder.hasComment() ? respBuilder.getComment() : "");
        return new Pair<GetTableSchemaResponse, Object>(
                response, respBuilder.hasError() ? respBuilder.getError() : null);
    }

    @Override
    Collection<Integer> getRequiredFeatures()
    {
        return requiredFeatures;
    }
}
