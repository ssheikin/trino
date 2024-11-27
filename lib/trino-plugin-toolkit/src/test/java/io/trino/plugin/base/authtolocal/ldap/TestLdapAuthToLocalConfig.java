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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestLdapAuthToLocalConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(LdapAuthToLocalConfig.class)
                .setBindDistinguishedName(null)
                .setBindPassword(null)
                .setUserBaseDistinguishedName(null)
                .setUserSearchPatterns(null)
                .setLdapAttribute(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("auth-to-local.ldap.bind-dn", "user@example.com")
                .put("auth-to-local.ldap.bind-password", "password")
                .put("auth-to-local.ldap.user-base-dn", "dc=trino")
                .put("auth-to-local.ldap.user-search-filter", "{USER}@example.com")
                .put("auth-to-local.ldap.attribute", "another-username")
                .buildOrThrow();

        LdapAuthToLocalConfig expected = new LdapAuthToLocalConfig()
                .setBindDistinguishedName("user@example.com")
                .setBindPassword("password")
                .setUserBaseDistinguishedName("dc=trino")
                .setUserSearchPatterns("{USER}@example.com")
                .setLdapAttribute("another-username");

        assertFullMapping(properties, expected);
    }
}
