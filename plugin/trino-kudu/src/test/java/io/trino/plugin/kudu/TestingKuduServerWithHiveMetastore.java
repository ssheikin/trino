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
package io.trino.plugin.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.containers.HiveHadoop;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.testcontainers.containers.Network;

import java.io.File;

import static io.trino.plugin.kudu.KuduTestTable.create;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;

public class TestingKuduServerWithHiveMetastore
        implements AutoCloseable
{
    private static final File HIVE_EVENT_LISTENER_PROVIDER = new File("target/kudu-hive.jar");

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final TestingKuduServer kuduServer;
    private final HiveHadoop hiveHadoop;
    private final KuduClient kuduClient;

    public TestingKuduServerWithHiveMetastore(String kuduVersion)
    {
        Network network = closer.register(Network.newNetwork());
        hiveHadoop = closer.register(
                HiveHadoop.builder()
                        .withFilesToMount(ImmutableMap.of(
                                "/usr/hdp/3.1.0.0-78/hive/lib/kudu-hive.jar", HIVE_EVENT_LISTENER_PROVIDER.getAbsolutePath(),
                                "/etc/hive/conf/hive-site.xml", getPathFromClassPathResource("hive-site.xml")))
                        .withNetwork(network)
                        .build());
        hiveHadoop.start();
        kuduServer = closer.register(TestingKuduServer.builder()
                .setKuduVersion(kuduVersion)
                .setExternalNetwork(network)
                .withExtraMasterArgs(ImmutableList.of("--hive_metastore_uris=thrift://hadoop-master:9083"))
                .build());

        kuduClient = closer.register(new KuduClient.KuduClientBuilder(kuduServer.getMasterAddress().toString()).build());

        exposeHiveSchemaOnKudu("default");
    }

    public TestingKuduServer getKuduServer()
    {
        return kuduServer;
    }

    public void exposeHiveSchemaOnKudu(String schemaName)
    {
        hiveHadoop.runOnHive("CREATE DATABASE IF NOT EXISTS " + schemaName);
        create(kuduClient, schemaName + ".temp", ImmutableList.of(new KuduTestColumn(BIGINT, "id", 0, true)));
    }

    public void dropSchemaOnHive(String schemaName)
            throws KuduException
    {
        kuduClient.deleteTable(schemaName + ".temp");
        hiveHadoop.runOnHive("DROP DATABASE IF EXISTS %s CASCADE".formatted(schemaName));
    }

    public KuduClient getKuduClient()
    {
        return kuduClient;
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }
}
