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

import com.google.common.collect.ImmutableMap;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TestKuduPlugin
{
    @Test
    public void testCreateConnector()
    {
        Plugin plugin = new KuduPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create(
                "test",
                ImmutableMap.of(
                        "kudu.client.master-addresses", "localhost:7051",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()).shutdown();
    }

    @Test
    public void testKuduIdentifierMappingWithHiveSchemaEmulation()
    {
        Plugin plugin = new KuduPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        assertThatThrownBy(() -> factory.create(
                "test",
                ImmutableMap.of(
                        "kudu.client.master-addresses", "localhost:7051",
                        "kudu.schema-emulation.type", "hive_metastore",
                        "case-insensitive-name-matching", "true",
                        "bootstrap.quiet", "true"),
                new TestingConnectorContext()))
                .hasMessageContaining("1) Error: Configuration property 'case-insensitive-name-matching' was not used");
    }

    @Test
    void testGetSecuritySensitivePropertyNames()
    {
        Plugin plugin = new KuduPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        Map<String, String> config = ImmutableMap.of(
                "non-existent-property", "value",
                "bootstrap.quiet", "true",
                "kudu.client.master-addresses", "localhost:7051");

        Set<String> sensitiveProperties = factory.getSecuritySensitivePropertyNames("catalog", config, new TestingConnectorContext());

        assertThat(sensitiveProperties).containsExactlyInAnyOrder("non-existent-property");
    }
}
