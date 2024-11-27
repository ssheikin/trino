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
package io.trino.plugin.base.authtolocal.ldap;

import io.airlift.bootstrap.Bootstrap;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.plugin.base.authtolocal.AuthToLocalModule;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLdapBasedAuthToLocal
{
    private TestingLdapServerForAuthToLocal ldapServerForAuthToLocal;
    private AuthToLocal authToLocal;

    @BeforeAll
    public void setup()
            throws Exception
    {
        ldapServerForAuthToLocal = new TestingLdapServerForAuthToLocal();

        authToLocal = new Bootstrap(new AuthToLocalModule())
                .setRequiredConfigurationProperties(ldapServerForAuthToLocal.getAuthToLocalConfiguration())
                .initialize()
                .getInstance(AuthToLocal.class);
    }

    @AfterAll
    public void close()
            throws Exception
    {
        try (var _ = ldapServerForAuthToLocal) {
            ldapServerForAuthToLocal = null;
        }
    }

    @Test
    public void testUserMapping()
            throws Exception
    {
        try (var _ = ldapServerForAuthToLocal.createUserWithGivenName("charlie", "mapped-charlie")) {
            Assertions.assertThat(authToLocal.translate(ConnectorIdentity.forUser("charlie").build()))
                    .isEqualTo("mapped-charlie");
        }
    }

    @Test
    public void testMissingLdapAttribute()
            throws Exception
    {
        try (var _ = ldapServerForAuthToLocal.createUser("bob")) {
            assertThatThrownBy(() -> authToLocal.translate(ConnectorIdentity.forUser("bob").build()))
                    .isInstanceOf(TrinoException.class)
                    .hasMessage("Attribute givenName is missing");
        }
    }

    @Test
    public void testInvalidUser()
    {
        assertThatThrownBy(() -> authToLocal.translate(ConnectorIdentity.forUser("missing_user").build()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("User missing_user is missing");
    }

    @Test
    public void testMultipleLdapObjectsMatching()
    {
        Map<String, String> authToLocalConfiguration = new HashMap<>(ldapServerForAuthToLocal.getAuthToLocalConfiguration());
        authToLocalConfiguration.put("auth-to-local.ldap.user-search-filter", "(objectClass=inetOrgPerson)");
        authToLocalConfiguration.put("auth-to-local.ldap.attribute", "uid");
        AuthToLocal authToLocal = new Bootstrap(new AuthToLocalModule())
                .setRequiredConfigurationProperties(authToLocalConfiguration)
                .initialize()
                .getInstance(AuthToLocal.class);
        assertThatThrownBy(() -> authToLocal.translate(ConnectorIdentity.forUser("random_user").build()))
                .isInstanceOf(TrinoException.class)
                .hasMessage("More than one LDAP object matches for the ldap query");
    }

    @Test
    public void testContainsSpecialCharacters()
    {
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("The quick brown fox jumped over the lazy dogs"))
                .as("English pangram")
                .isEqualTo(false);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("Pchnąć w tę łódź jeża lub ośm skrzyń fig"))
                .as("Perfect polish pangram")
                .isEqualTo(false);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("いろはにほへと ちりぬるを わかよたれそ つねならむ うゐのおくやま けふこえて あさきゆめみし ゑひもせす（ん）"))
                .as("Japanese hiragana pangram - Iroha")
                .isEqualTo(false);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("*"))
                .as("LDAP wildcard")
                .isEqualTo(true);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("   John Doe"))
                .as("Beginning with whitespace")
                .isEqualTo(true);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("John Doe  \r"))
                .as("Ending with whitespace")
                .isEqualTo(true);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("Hi (This) = is * a \\ test # ç à ô"))
                .as("Multiple special characters")
                .isEqualTo(true);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("John\u0000Doe"))
                .as("NULL character")
                .isEqualTo(true);
        assertThat(LdapBasedAuthToLocal.containsSpecialCharacters("John Doe <john.doe@company.com>"))
                .as("Angle brackets")
                .isEqualTo(true);
    }
}
