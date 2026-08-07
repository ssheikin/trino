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
package io.trino.plugin.session;

import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.SystemSessionProperties.QUERY_MAX_RUN_TIME;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestSessionPropertyManagerWithAccessControl
{
    private static final String DEFAULT_VALUE = "1h";
    private static final String USER_OVERRIDE_VALUE = "24h";

    private static final String MOCK_CATALOG = "mock";
    private static final String CATALOG_PROPERTY = "some_property";
    private static final String QUALIFIED_CATALOG_PROPERTY = MOCK_CATALOG + "." + CATALOG_PROPERTY;
    private static final String DEFAULT_CATALOG_VALUE = "default_value";
    private static final String USER_OVERRIDE_CATALOG_VALUE = "user_value";

    private static final String ACCESS_CONTROL_CONFIG =
            """
            {
              "system_session_properties": [
                {
                  "user": "privileged_user",
                  "allow": true
                },
                {
                  "allow": false
                }
              ],
              "catalog_session_properties": [
                {
                  "user": "catalog_privileged_user",
                  "allow": true
                },
                {
                  "allow": false
                }
              ]
            }
            """;

    private static final String SESSION_PROPERTY_CONFIG =
            """
            [
              {
                "user": "privileged_user|restricted_user",
                "sessionProperties": {
                  "%s": "%s"
                }
              },
              {
                "user": "catalog_privileged_user|catalog_restricted_user",
                "sessionProperties": {
                  "%s": "%s"
                }
              }
            ]
            """.formatted(QUERY_MAX_RUN_TIME, DEFAULT_VALUE, QUALIFIED_CATALOG_PROPERTY, DEFAULT_CATALOG_VALUE);

    @TempDir
    private static Path tempDir;

    @AutoClose
    private DistributedQueryRunner queryRunner;

    @BeforeAll
    void setup()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .setSystemAccessControl("file", Map.of(
                        "security.config-file", writeConfig("access-control.json", ACCESS_CONTROL_CONFIG)))
                .build();

        queryRunner.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                .withSessionProperty(stringProperty(CATALOG_PROPERTY, "Test catalog session property", null, false))
                .build()));
        queryRunner.createCatalog(MOCK_CATALOG, "mock");

        queryRunner.installPlugin(new SessionPropertyConfigurationManagerPlugin());
        queryRunner.getSessionPropertyDefaults().setConfigurationManager("file", Map.of(
                "session-property-manager.config-file", writeConfig("session-property-config.json", SESSION_PROPERTY_CONFIG)));
    }

    @Test
    void testDefaultApplies()
    {
        assertThat(getSessionPropertyValue(session("privileged_user").build(), QUERY_MAX_RUN_TIME))
                .isEqualTo(DEFAULT_VALUE);
    }

    @Test
    void testUserCanOverrideDefault()
    {
        Session session = session("privileged_user")
                .setSystemProperty(QUERY_MAX_RUN_TIME, USER_OVERRIDE_VALUE)
                .build();

        assertThat(getSessionPropertyValue(session, QUERY_MAX_RUN_TIME))
                .isEqualTo(USER_OVERRIDE_VALUE);
    }

    @Test
    void testUserCanOverrideDefaultBySetSession()
    {
        MaterializedResult result = queryRunner.execute(
                session("privileged_user").build(),
                "SET SESSION " + QUERY_MAX_RUN_TIME + " = '" + USER_OVERRIDE_VALUE + "'");
        assertThat(result.getSetSessionProperties())
                .containsEntry(QUERY_MAX_RUN_TIME, USER_OVERRIDE_VALUE);
    }

    @Test
    void testQueryFailsForRestrictedUser()
    {
        // The injected default is validated against the end user's privileges,
        // so a user without the grant cannot run any query at all once a default matches their session
        // This leaves administrators stuck:
        // without a grant to set the property, every query of the user fails;
        // with the grant, the user is free to override the configured value, so it cannot serve as a guardrail.
        assertThatThrownBy(() -> queryRunner.execute(session("restricted_user").build(), "SELECT 1"))
                .hasMessageContaining("Access Denied: Cannot set system session property " + QUERY_MAX_RUN_TIME);
    }

    @Test
    void testSetSessionDeniedForRestrictedUser()
    {
        assertThatThrownBy(() -> queryRunner.execute(session("restricted_user").build(), "SET SESSION " + QUERY_MAX_RUN_TIME + " = '" + USER_OVERRIDE_VALUE + "'"))
                .hasMessageContaining("Cannot set system session property " + QUERY_MAX_RUN_TIME);
    }

    @Test
    void testCatalogDefaultApplies()
    {
        assertThat(getSessionPropertyValue(session("catalog_privileged_user").build(), QUALIFIED_CATALOG_PROPERTY))
                .isEqualTo(DEFAULT_CATALOG_VALUE);
    }

    @Test
    void testUserCanOverrideCatalogDefault()
    {
        Session session = session("catalog_privileged_user")
                .setCatalogSessionProperty(MOCK_CATALOG, CATALOG_PROPERTY, USER_OVERRIDE_CATALOG_VALUE)
                .build();

        assertThat(getSessionPropertyValue(session, QUALIFIED_CATALOG_PROPERTY))
                .isEqualTo(USER_OVERRIDE_CATALOG_VALUE);
    }

    @Test
    void testQueryFailsForCatalogRestrictedUser()
    {
        // Same problem as testQueryFailsForRestrictedUser, for a catalog session property default
        assertThatThrownBy(() -> queryRunner.execute(session("catalog_restricted_user").build(), "SELECT 1"))
                .hasMessageContaining("Access Denied: Cannot set catalog session property " + CATALOG_PROPERTY);
    }

    @Test
    void testSetCatalogSessionDeniedForRestrictedUser()
    {
        assertThatThrownBy(() -> queryRunner.execute(session("catalog_restricted_user").build(), "SET SESSION " + QUALIFIED_CATALOG_PROPERTY + " = '" + USER_OVERRIDE_CATALOG_VALUE + "'"))
                .hasMessageContaining("Cannot set catalog session property " + CATALOG_PROPERTY);
    }

    private String getSessionPropertyValue(Session session, String propertyName)
    {
        MaterializedResult result = queryRunner.execute(session, "SHOW SESSION LIKE '" + propertyName + "'");
        return (String) result.getMaterializedRows().stream()
                .filter(row -> row.getField(0).equals(propertyName))
                .collect(onlyElement())
                .getField(1);
    }

    private static String writeConfig(String fileName, String content)
            throws IOException
    {
        Path path = tempDir.resolve(fileName);
        Files.writeString(path, content);
        return path.toString();
    }

    private static Session.SessionBuilder session(String user)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(user));
    }
}
