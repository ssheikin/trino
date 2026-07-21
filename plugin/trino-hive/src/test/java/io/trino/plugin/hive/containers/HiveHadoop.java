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
package io.trino.plugin.hive.containers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HostAndPort;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.testing.TestingProperties;
import io.trino.testing.containers.BaseTestContainer;
import io.trino.testing.containers.PrintingLogConsumer;
import org.testcontainers.containers.Network;

import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HiveHadoop
        extends BaseTestContainer
{
    private static final Logger log = Logger.get(HiveHadoop.class);

    public static final String HIVE3_IMAGE = "ghcr.io/trinodb/testing/hdp3.1-hive:" + TestingProperties.getDockerImagesVersion();

    public static final String HOST_NAME = "hadoop-master";

    public static final int HIVE_METASTORE_PORT = 9083;

    public static Builder builder()
    {
        return new Builder();
    }

    private final Function<String, String> runOnHive;

    private HiveHadoop(
            String image,
            String hostName,
            Set<Integer> ports,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int startupRetryLimit,
            Optional<Function<String, String>> runOnHive)
    {
        super(image,
                hostName,
                ports,
                filesToMount,
                envVars,
                network,
                startupRetryLimit);
        this.runOnHive = runOnHive.orElseGet(() -> this::runOnHiveViaBeeline);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        String runCmd = "/usr/local/hadoop-run.sh";
        copyResourceToContainer("containers/hive_hadoop/hadoop-run.sh", runCmd);
        withRunCommand(
                ImmutableList.of(
                        "/bin/bash",
                        runCmd));
        withLogConsumer(new PrintingLogConsumer("Hadoop"));
    }

    @Override
    public void start()
    {
        super.start();
        log.info("Hive container started with addresses for metastore: %s", getHiveMetastoreEndpoint());
        assertEventually(new Duration(2, MINUTES), new Duration(1, SECONDS), () -> runOnHive("SELECT 1"));
        log.info("Hive server is available");
    }

    public String runOnHive(String query)
    {
        return runOnHive.apply(query);
    }

    private String runOnHiveViaBeeline(String query)
    {
        return executeInContainerFailOnError("beeline", "-u", "jdbc:hive2://localhost:10000/default", "-n", "hive", "-e", query);
    }

    public String runOnMetastore(String query)
    {
        return executeInContainerFailOnError("mysql", "-D", "metastore", "-uroot", "-proot", "--batch", "--column-names=false", "-e", query).replaceAll("\n$", "");
    }

    public URI getHiveMetastoreEndpoint()
    {
        HostAndPort address = getMappedHostAndPortForExposedPort(HIVE_METASTORE_PORT);
        return URI.create("thrift://" + address.getHost() + ":" + address.getPort());
    }

    public static class Builder
            extends BaseTestContainer.Builder<HiveHadoop.Builder, HiveHadoop>
    {
        private Optional<Function<String, String>> runOnHive = Optional.empty();

        private Builder()
        {
            this.image = HIVE3_IMAGE;
            this.hostName = HOST_NAME;
            this.exposePorts = ImmutableSet.of(HIVE_METASTORE_PORT);
        }

        // Allows callers using a Hive image incompatible with the default beeline-based
        // runOnHive (and the readiness check in start(), which runs through it) to substitute
        // their own implementation.
        public Builder withRunOnHive(Function<String, String> runOnHive)
        {
            this.runOnHive = Optional.of(requireNonNull(runOnHive, "runOnHive is null"));
            return this;
        }

        @Override
        public HiveHadoop build()
        {
            return new HiveHadoop(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit, runOnHive);
        }
    }
}
