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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.inject.Inject;
import io.trino.plugin.base.authtolocal.AuthToLocal;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapQuery;
import io.trino.plugin.base.ldap.LdapQuery.LdapQueryBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.ConnectorIdentity;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.SearchResult;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class LdapBasedAuthToLocal
        implements AuthToLocal
{
    private static final CharMatcher SPECIAL_CHARACTERS = CharMatcher.anyOf(",=+<>#;*()\"\\\u0000");
    private static final CharMatcher WHITESPACE = CharMatcher.anyOf(" \r");

    private final LdapClient client;
    private final String bindDistinguishedName;
    private final String bindPassword;
    private final String userBaseDistinguishedName;

    private final String userSearchPattern;
    private final String ldapAttribute;

    @Inject
    public LdapBasedAuthToLocal(LdapClient client, LdapAuthToLocalConfig config)
    {
        this.client = requireNonNull(client, "client is null");
        requireNonNull(config, "config is null");
        this.bindDistinguishedName = config.getBindDistinguishedName();
        this.bindPassword = config.getBindPassword();
        this.userBaseDistinguishedName = config.getUserBaseDistinguishedName();
        this.ldapAttribute = config.getLdapAttribute();
        this.userSearchPattern = config.getUserSearchPatterns();
    }

    @Override
    public String translate(ConnectorIdentity identity)
    {
        if (containsSpecialCharacters(identity.getUser())) {
            throw new TrinoException(
                    LdapErrorCode.INVALID_USER_NAME,
                    format("Username %s contains a special LDAP character", identity.getUser()));
        }
        LdapQuery ldapQuery = new LdapQueryBuilder()
                .withSearchBase(userBaseDistinguishedName)
                .withSearchFilter(replaceUser(userSearchPattern, identity.getUser()))
                .withAttributes(ldapAttribute)
                .build();
        try (var _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            return client.executeLdapQuery(
                    bindDistinguishedName,
                    bindPassword,
                    ldapQuery,
                    searchResults -> {
                        if (searchResults.hasMore()) {
                            SearchResult result = searchResults.next();
                            Attribute attribute = result.getAttributes().get(ldapAttribute);
                            if (attribute == null) {
                                throw new TrinoException(LdapErrorCode.ATTRIBUTE_NOT_FOUND, format("Attribute %s is missing", ldapAttribute));
                            }
                            if (searchResults.hasMore()) {
                                throw new TrinoException(LdapErrorCode.MORE_ELEMENTS_FOUND, "More than one LDAP object matches for the ldap query");
                            }
                            return attribute.get().toString();
                        }
                        throw new TrinoException(LdapErrorCode.USER_NOT_FOUND, format("User %s is missing", identity.getUser()));
                    });
        }
        catch (NamingException e) {
            throw new TrinoException(LdapErrorCode.UNABLE_TO_EXECUTE_QUERY, e);
        }
    }

    private static String replaceUser(String pattern, String user)
    {
        return pattern.replace("${USER}", user);
    }

    /**
     * Returns {@code true} when parameter contains a character that has a special meaning in
     * LDAP search or bind name (DN).
     * <p>
     * Based on <a href="https://www.owasp.org/index.php/Preventing_LDAP_Injection_in_Java">Preventing_LDAP_Injection_in_Java</a> and
     * {@link javax.naming.ldap.Rdn#escapeValue(Object) escapeValue} method.
     */
    @VisibleForTesting
    static boolean containsSpecialCharacters(String user)
    {
        if (WHITESPACE.indexIn(user) == 0 || WHITESPACE.lastIndexIn(user) == user.length() - 1) {
            return true;
        }
        return SPECIAL_CHARACTERS.matchesAnyOf(user);
    }
}
