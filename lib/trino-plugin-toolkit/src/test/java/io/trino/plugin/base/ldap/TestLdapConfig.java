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
package io.trino.plugin.base.ldap;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.airlift.units.Duration.succinctDuration;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLdapConfig
{
    @Test
    public void testDefault()
    {
        assertRecordedDefaults(recordDefaults(LdapClientConfig.class)
                .setLdapUrl(null)
                .setAllowInsecure(false)
                .setKeystorePath(null)
                .setKeystorePassword(null)
                .setTrustStorePath(null)
                .setTruststorePassword(null)
                .setIgnoreReferrals(false)
                .setLdapConnectionTimeout(succinctDuration(1, MINUTES))
                .setLdapReadTimeout(succinctDuration(1, MINUTES))
                .setLdapSearchTimeLimit(0)
                .setLdapSearchCountLimit(0)
                .setLdapSearchScope(LdapSearchScope.SUBTREE)
                .setBinaryAttributes(List.of()));
    }

    @Test
    public void testExplicitConfig()
            throws IOException
    {
        Path trustStoreFile = Files.createTempFile(null, null);
        Path keyStoreFile = Files.createTempFile(null, null);

        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("ldap.url", "ldaps://localhost:636")
                .put("ldap.allow-insecure", "true")
                .put("ldap.ssl.keystore.path", keyStoreFile.toString())
                .put("ldap.ssl.keystore.password", "12345")
                .put("ldap.ssl.truststore.path", trustStoreFile.toString())
                .put("ldap.ssl.truststore.password", "54321")
                .put("ldap.ignore-referrals", "true")
                .put("ldap.timeout.connect", "3m")
                .put("ldap.timeout.read", "4m")
                .put("ldap.search.time-limit", "30000")
                .put("ldap.search.count-limit", "1000")
                .put("ldap.search.scope", "ONELEVEL")
                .put("ldap.binary-attributes", "userCertificate,photo,thumbnailPhoto")
                .buildOrThrow();

        LdapClientConfig expected = new LdapClientConfig()
                .setLdapUrl("ldaps://localhost:636")
                .setAllowInsecure(true)
                .setKeystorePath(keyStoreFile.toFile())
                .setKeystorePassword("12345")
                .setTrustStorePath(trustStoreFile.toFile())
                .setTruststorePassword("54321")
                .setIgnoreReferrals(true)
                .setLdapConnectionTimeout(new Duration(3, TimeUnit.MINUTES))
                .setLdapReadTimeout(new Duration(4, TimeUnit.MINUTES))
                .setLdapSearchTimeLimit(30000)
                .setLdapSearchCountLimit(1000)
                .setLdapSearchScope(LdapSearchScope.ONELEVEL)
                .setBinaryAttributes(List.of("userCertificate", "photo", "thumbnailPhoto"));

        assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        assertValidates(new LdapClientConfig()
                .setLdapUrl("ldaps://localhost"));

        assertValidates(new LdapClientConfig()
                .setLdapUrl("ldap://localhost")
                .setAllowInsecure(true));

        assertFailsValidation(
                new LdapClientConfig()
                        .setLdapUrl("ldap://")
                        .setAllowInsecure(false),
                "urlConfigurationValid",
                "Connecting to the LDAP server without SSL enabled requires `ldap.allow-insecure=true`",
                AssertTrue.class);

        assertFailsValidation(new LdapClientConfig().setLdapUrl("localhost"), "ldapUrl", "Invalid LDAP server URL. Expected ldap:// or ldaps://", Pattern.class);
        assertFailsValidation(new LdapClientConfig().setLdapUrl("ldaps:/localhost"), "ldapUrl", "Invalid LDAP server URL. Expected ldap:// or ldaps://", Pattern.class);

        assertFailsValidation(new LdapClientConfig(), "ldapUrl", "must not be null", NotNull.class);
        assertFailsValidation(new LdapClientConfig().setLdapConnectionTimeout(null), "ldapConnectionTimeout", "must not be null", NotNull.class);
        assertFailsValidation(new LdapClientConfig().setLdapReadTimeout(null), "ldapReadTimeout", "must not be null", NotNull.class);
    }

    @Test
    public void testSearchScopeEnum()
    {
        LdapClientConfig config = new LdapClientConfig()
                .setLdapUrl("ldaps://localhost");

        // Valid scopes
        config.setLdapSearchScope(LdapSearchScope.SUBTREE);
        assertThat(config.getLdapSearchScope()).isEqualTo(LdapSearchScope.SUBTREE);

        config.setLdapSearchScope(LdapSearchScope.ONELEVEL);
        assertThat(config.getLdapSearchScope()).isEqualTo(LdapSearchScope.ONELEVEL);

        config.setLdapSearchScope(LdapSearchScope.OBJECT);
        assertThat(config.getLdapSearchScope()).isEqualTo(LdapSearchScope.OBJECT);

        // Verify JNDI values are correct
        assertThat(LdapSearchScope.SUBTREE.getJndiValue()).isEqualTo(2);  // SearchControls.SUBTREE_SCOPE
        assertThat(LdapSearchScope.ONELEVEL.getJndiValue()).isEqualTo(1);  // SearchControls.ONELEVEL_SCOPE
        assertThat(LdapSearchScope.OBJECT.getJndiValue()).isEqualTo(0);  // SearchControls.OBJECT_SCOPE
    }

    @Test
    public void testBinaryAttributesConfiguration()
    {
        LdapClientConfig config = new LdapClientConfig()
                .setLdapUrl("ldaps://localhost");

        // Default is empty
        assertThat(config.getBinaryAttributes()).isEmpty();

        // Single attribute
        config.setBinaryAttributes(List.of("userCertificate"));
        assertThat(config.getBinaryAttributes()).containsExactly("userCertificate");

        // Multiple attributes
        config.setBinaryAttributes(List.of("userCertificate", "photo", "thumbnailPhoto"));
        assertThat(config.getBinaryAttributes()).containsExactly("userCertificate", "photo", "thumbnailPhoto");

        // Empty list
        config.setBinaryAttributes(List.of());
        assertThat(config.getBinaryAttributes()).isEmpty();
    }
}
