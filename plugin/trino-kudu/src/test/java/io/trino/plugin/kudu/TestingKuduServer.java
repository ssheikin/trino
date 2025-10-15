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
import com.google.common.net.HostAndPort;
import io.trino.plugin.base.util.AutoCloseableCloser;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.ToxiproxyContainer;

import java.util.List;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TestingKuduServer
        implements AutoCloseable
{
    private static final String KUDU_IMAGE = "apache/kudu";
    public static final String EARLIEST_TAG = "1.13.0";
    public static final String LATEST_TAG = "1.17";

    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7050;

    private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.4.0";
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";

    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private final GenericContainer<?> master;

    /**
     * Kudu tablets needs to know the host/mapped port it will be bound to in order to configure --rpc_advertised_addresses
     * However when using non-fixed ports in testcontainers, we only know the mapped port after the container starts up
     * In order to workaround this, create a proxy to forward traffic from the host to the underlying tablets
     * Since the ToxiProxy container starts up *before* kudu, we know the mapped port when configuring the kudu tablets
     */
    private TestingKuduServer(String kuduVersion, Optional<Network> externalNetwork, List<String> extraMasterArgs, List<String> extraTServerArgs)
    {
        Network network = externalNetwork.orElseGet(() -> closer.register(Network.newNetwork()));
        String masterContainerAlias = "kudu-master";

        this.master = closer.register(new GenericContainer<>(format("%s:%s", KUDU_IMAGE, kuduVersion))
                .withExposedPorts(KUDU_MASTER_PORT)
                .withCommand("master")
                .withEnv("MASTER_ARGS", "--default_num_replicas=1 --unlock_unsafe_flags --use_hybrid_clock=false %s".formatted(String.join(" ", extraMasterArgs)))
                .withNetwork(network)
                .withNetworkAliases(masterContainerAlias));

        @SuppressWarnings("deprecation")
        ToxiproxyContainer toxiProxy = closer.register(new ToxiproxyContainer(TOXIPROXY_IMAGE)
                .withNetwork(network)
                .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS));
        toxiProxy.start();

        String instanceName = "kudu-tserver";
        @SuppressWarnings("deprecation")
        ToxiproxyContainer.ContainerProxy proxy = toxiProxy.getProxy(instanceName, KUDU_TSERVER_PORT);
        String tServerArgs = "--fs_wal_dir=/var/lib/kudu/tserver --logtostderr --use_hybrid_clock=false --unlock_unsafe_flags --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:%s %s"
                .formatted(instanceName, KUDU_TSERVER_PORT, TOXIPROXY_NETWORK_ALIAS, proxy.getOriginalProxyPort(), String.join(" ", extraTServerArgs));
        GenericContainer tabletServer = closer.register(new GenericContainer<>(format("%s:%s", KUDU_IMAGE, kuduVersion))
                .withExposedPorts(KUDU_TSERVER_PORT)
                .withCommand("tserver")
                .withEnv("KUDU_MASTERS", format("%s:%s", masterContainerAlias, KUDU_MASTER_PORT))
                .withEnv("TSERVER_ARGS", tServerArgs)
                .withNetwork(network)
                .withNetworkAliases(instanceName)
                .waitingFor(new KuduTabletWaitStrategy(master))
                .dependsOn(master));

        master.start();
        tabletServer.start();
    }

    public HostAndPort getMasterAddress()
    {
        // Do not use master.getHost(), it returns "localhost" which the kudu client resolves to:
        // localhost/127.0.0.1, localhost/0:0:0:0:0:0:0:1
        // Instead explicitly list only the ipv4 loopback address 127.0.0.1
        return HostAndPort.fromParts("127.0.0.1", master.getMappedPort(KUDU_MASTER_PORT));
    }

    @Override
    public void close()
            throws Exception
    {
        closer.close();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private String kuduVersion = LATEST_TAG;
        private Optional<Network> externalNetwork = Optional.empty();
        private List<String> extraMasterArgs = ImmutableList.of();
        private List<String> extraTServerArgs = ImmutableList.of();

        private Builder() {}

        public Builder setKuduVersion(String kuduVersion)
        {
            this.kuduVersion = requireNonNull(kuduVersion, "kuduVersion is null");
            return this;
        }

        public Builder setExternalNetwork(Network externalNetwork)
        {
            this.externalNetwork = Optional.of(externalNetwork);
            return this;
        }

        public Builder withExtraMasterArgs(List<String> extraMasterArgs)
        {
            this.extraMasterArgs = ImmutableList.copyOf(requireNonNull(extraMasterArgs, "extraMasterArgs is null"));
            return this;
        }

        public Builder withExtraTServerArgs(List<String> extraTServerArgs)
        {
            this.extraTServerArgs = ImmutableList.copyOf(requireNonNull(extraTServerArgs, "extraTServerArgs is null"));
            return this;
        }

        public TestingKuduServer build()
        {
            return new TestingKuduServer(kuduVersion, externalNetwork, extraMasterArgs, extraTServerArgs);
        }
    }
}
