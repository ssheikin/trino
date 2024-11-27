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
import com.google.common.io.Closer;
import io.trino.plugin.base.authtolocal.ldap.TestingOpenLdapServer.DisposableSubContext;
import org.testcontainers.containers.Network;

import javax.naming.NamingException;

import java.util.Arrays;
import java.util.Map;

import static java.lang.String.format;

public class TestingLdapServerForAuthToLocal
        implements AutoCloseable
{
    private final Closer closer = Closer.create();
    private final TestingOpenLdapServer ldapServer;
    private final DisposableSubContext organization;
    private final DisposableSubContext serviceUser;
    private final DisposableSubContext defaultUser;

    public TestingLdapServerForAuthToLocal()
            throws Exception
    {
        Network network = Network.newNetwork();
        closer.register(network::close);

        ldapServer = new TestingOpenLdapServer(network);
        closer.register(ldapServer);
        ldapServer.start();

        organization = ldapServer.createOrganization();
        serviceUser = ldapServer.createUser(organization, "serviceuser", "servicepassword");

        defaultUser = createUserWithGivenName("alice", "alice");
    }

    public DisposableSubContext createUserWithGivenName(String userName, String mappedUserName)
            throws NamingException
    {
        LdapObjectDefinition user = LdapObjectDefinition.builder(userName)
                .setDistinguishedName(format("uid=%s,%s", userName, organization.getDistinguishedName()))
                .setAttributes(ImmutableMap.of(
                        "cn", userName,
                        "sn", userName,
                        "givenName", mappedUserName))
                .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                .build();
        return ldapServer.createDisposableSubContext(user);
    }

    public DisposableSubContext createUser(String userName)
            throws Exception
    {
        return ldapServer.createUser(organization, userName, userName);
    }

    public Map<String, String> getAuthToLocalConfiguration()
    {
        return getAuthToLocalConfigurationWithAttribute("givenName");
    }

    public Map<String, String> getAuthToLocalConfigurationWithAttribute(String attribute)
    {
        return ImmutableMap.<String, String>builder()
                .put("auth-to-local.type", "LDAP")
                .put("ldap.url", ldapServer.getLdapUrl())
                .put("ldap.allow-insecure", "true")
                .put("auth-to-local.ldap.bind-dn", serviceUser.getDistinguishedName())
                .put("auth-to-local.ldap.bind-password", "servicepassword")
                .put("auth-to-local.ldap.user-base-dn", "dc=trino,dc=testldap,dc=com")
                .put("auth-to-local.ldap.user-search-filter", "(&(objectClass=inetOrgPerson)(uid=${USER}))")
                .put("auth-to-local.ldap.attribute", attribute)
                .buildOrThrow();
    }

    @Override
    public void close()
            throws Exception
    {
        defaultUser.close();
        serviceUser.close();
        organization.close();
        closer.close();
    }
}
