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
package io.trino.security;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logging;
import io.trino.jdbc.TrinoConnection;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.testing.TestingAccessControlManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestImpersonation
{
    private TestingTrinoServer server;
    private final TestingSystemSecurityMetadata securityMetadata = new TestingSystemSecurityMetadata();
    private TestingAccessControlManager accessControl;

    @BeforeAll
    public void setup()
            throws Exception
    {
        Logging.initialize();
        server = TestingTrinoServer.builder()
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .toInstance(securityMetadata);
                }).build();
        server.installPlugin(new MemoryPlugin());
        server.createCatalog("memory", "memory");
        accessControl = server.getAccessControl();
    }

    @ParameterizedTest
    @MethodSource("roles")
    @Timeout(10)
    public void testImpersonationAllowedByRole(String roleName)
            throws Exception
    {
        securityMetadata.reset();
        accessControl.reset();

        try (TrinoConnection connection = createConnection("memory", "default", "alice", Optional.empty()).unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(getCurrentUser(connection)).isEqualTo("alice");
            securityMetadata.createRole(null, "invalid_role", Optional.empty());
            securityMetadata.grantRoles(
                    null,
                    ImmutableSet.of("invalid_role"),
                    ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                    false,
                    Optional.empty());
            denyImpersonation();
            statement.execute("SET ROLE invalid_role");
            assertThatThrownBy(() -> statement.execute("SET SESSION AUTHORIZATION john"))
                    .hasMessageContaining("User alice cannot impersonate user john");

            securityMetadata.createRole(null, "alice_role", Optional.empty());
            securityMetadata.grantRoles(
                    null,
                    ImmutableSet.of("alice_role"),
                    ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                    false,
                    Optional.empty());

            statement.execute("SET ROLE alice_role");
            statement.execute("SET SESSION AUTHORIZATION john");

            assertThat(getCurrentUser(connection)).isEqualTo("john");
            // here we simply verify that those queries succeed
            statement.execute("SHOW SCHEMAS IN memory");
            // more than 1 to make sure that _impersonation catalog role is not lost
            statement.execute("SHOW CATALOGS");
            statement.execute("SHOW SCHEMAS IN memory");
        }
    }

    @Test
    @Timeout(10)
    public void testImpersonationDisallowedWhenRoleIsNone()
            throws Exception
    {
        securityMetadata.reset();
        accessControl.reset();

        try (TrinoConnection connection = createConnection("memory", "default", "alice", Optional.empty()).unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(getCurrentUser(connection)).isEqualTo("alice");
            securityMetadata.createRole(null, "alice_role", Optional.empty());
            denyImpersonation();
            securityMetadata.grantRoles(
                    null,
                    ImmutableSet.of("alice_role"),
                    ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                    false,
                    Optional.empty());
            statement.execute("SET ROLE NONE");

            assertThatThrownBy(() -> statement.execute("SET SESSION AUTHORIZATION john"))
                    .hasMessageContaining("User alice cannot impersonate user john");
        }
    }

    @Test
    @Timeout(10)
    public void testImpersonateWhenRoleIsDefineByConnection()
            throws Exception
    {
        securityMetadata.reset();

        securityMetadata.createRole(null, "alice_role", Optional.empty());
        securityMetadata.grantRoles(
                null,
                ImmutableSet.of("alice_role"),
                ImmutableSet.of(new TrinoPrincipal(USER, "alice")),
                false,
                Optional.empty());

        try (TrinoConnection connection = createConnection("memory", "default", "alice", Optional.of("system:alice_role"))
                .unwrap(TrinoConnection.class);
                Statement statement = connection.createStatement()) {
            assertThat(getCurrentUser(connection)).isEqualTo("alice");
            denyImpersonation();
            statement.execute("SET SESSION AUTHORIZATION john");

            assertThat(getCurrentUser(connection)).isEqualTo("john");
            // here we simply verify that those queries succeed
            statement.execute("SHOW SCHEMAS IN memory");
            // more than 1 to make sure that _impersonation catalog role is not lost
            statement.execute("SHOW CATALOGS");
            statement.execute("SHOW SCHEMAS IN memory");
        }
    }

    private Connection createConnection(String catalog, String schema, String user, Optional<String> role)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/%s/%s", server.getAddress(), catalog, schema);
        if (role.isPresent()) {
            Properties properties = new Properties();
            properties.put("user", user);
            properties.put("roles", role.get());
            return DriverManager.getConnection(url, properties);
        }
        else {
            return DriverManager.getConnection(url, user, null);
        }
    }

    private static String getCurrentUser(Connection connection)
            throws SQLException
    {
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT current_user")) {
            while (rs.next()) {
                return rs.getString(1);
            }
        }

        throw new RuntimeException("Failed to get CURRENT_USER");
    }

    private Stream<String> roles()
    {
        return Stream.of("alice_role", "ALL");
    }

    private void denyImpersonation()
    {
        accessControl.denyImpersonation((identity, _) ->
                identity.getEnabledRoles()
                        .stream()
                        .anyMatch(role -> role.equalsIgnoreCase("alice_role")));
    }
}
