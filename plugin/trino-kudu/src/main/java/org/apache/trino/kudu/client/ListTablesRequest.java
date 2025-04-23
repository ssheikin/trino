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

import org.apache.kudu.master.Master;
import org.apache.kudu.shaded.com.google.protobuf.Message;
import org.apache.kudu.shaded.io.netty.util.Timer;
import org.apache.trino.kudu.client.ListTablesResponse.TableInfo;
import org.apache.trino.kudu.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import java.util.ArrayList;
import java.util.List;

@InterfaceAudience.Private
class ListTablesRequest
        extends KuduRpc<ListTablesResponse>
{
    private final String nameFilter;

    private final boolean showSoftDeleted;

    ListTablesRequest(KuduTable masterTable,
                      String nameFilter,
                      boolean showSoftDeleted,
                      Timer timer,
                      long timeoutMillis)
    {
        super(masterTable, timer, timeoutMillis);
        this.nameFilter = nameFilter;
        this.showSoftDeleted = showSoftDeleted;
    }

    @Override
    Message createRequestPB()
    {
        final Master.ListTablesRequestPB.Builder builder =
                Master.ListTablesRequestPB.newBuilder();
        if (nameFilter != null) {
            builder.setNameFilter(nameFilter);
        }
        builder.setShowSoftDeleted(showSoftDeleted);
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
        return "ListTables";
    }

    @Override
    Pair<ListTablesResponse, Object> deserialize(CallResponse callResponse,
                                                 String tsUUID)
            throws KuduException
    {
        final Master.ListTablesResponsePB.Builder respBuilder =
                Master.ListTablesResponsePB.newBuilder();
        readProtobuf(callResponse.getPBMessage(), respBuilder);
        int tablesCount = respBuilder.getTablesCount();
        List<TableInfo> tableInfos = new ArrayList<>(tablesCount);
        for (Master.ListTablesResponsePB.TableInfo infoPb : respBuilder.getTablesList()) {
            tableInfos.add(new TableInfo(infoPb.getId().toStringUtf8(), infoPb.getName()));
        }
        ListTablesResponse response = new ListTablesResponse(timeoutTracker.getElapsedMillis(),
                tsUUID, tableInfos);
        return new Pair<ListTablesResponse, Object>(
                response, respBuilder.hasError() ? respBuilder.getError() : null);
    }
}
