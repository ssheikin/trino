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

import java.util.ArrayList;
import java.util.List;

import static org.apache.kudu.master.Master.ListTabletServersRequestPB;
import static org.apache.kudu.master.Master.ListTabletServersResponsePB;

@InterfaceAudience.Private
public class ListTabletServersRequest
        extends KuduRpc<ListTabletServersResponse>
{
    public ListTabletServersRequest(KuduTable masterTable,
                                    Timer timer,
                                    long timeoutMillis)
    {
        super(masterTable, timer, timeoutMillis);
    }

    @Override
    Message createRequestPB()
    {
        return ListTabletServersRequestPB.getDefaultInstance();
    }

    @Override
    String serviceName()
    {
        return MASTER_SERVICE_NAME;
    }

    @Override
    String method()
    {
        return "ListTabletServers";
    }

    @Override
    Pair<ListTabletServersResponse, Object> deserialize(CallResponse callResponse,
                                                        String tsUUID)
            throws KuduException
    {
        final ListTabletServersResponsePB.Builder respBuilder =
                ListTabletServersResponsePB.newBuilder();
        readProtobuf(callResponse.getPBMessage(), respBuilder);
        int serversCount = respBuilder.getServersCount();
        List<String> servers = new ArrayList<>(serversCount);
        for (ListTabletServersResponsePB.Entry entry : respBuilder.getServersList()) {
            servers.add(entry.getRegistration().getRpcAddresses(0).getHost());
        }
        ListTabletServersResponse response =
                new ListTabletServersResponse(timeoutTracker.getElapsedMillis(),
                        tsUUID,
                        serversCount,
                        servers);
        return new Pair<ListTabletServersResponse, Object>(
                response, respBuilder.hasError() ? respBuilder.getError() : null);
    }
}
