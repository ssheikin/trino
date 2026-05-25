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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttribute;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;

import java.util.Arrays;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

final class LdapUtil
{
    private LdapUtil() {}

    @SuppressWarnings("BanJNDI")
    public static String addLdapDefinition(LdapObjectDefinition ldapObjectDefinition, DirContext context)
    {
        requireNonNull(ldapObjectDefinition, "LDAP Object Definition is null");

        Attributes entries = new BasicAttributes();
        Attribute objectClass = new BasicAttribute("objectClass");

        ldapObjectDefinition.getAttributes()
                .forEach((key, value) -> entries.put(new BasicAttribute(key, value)));

        ldapObjectDefinition.getObjectClasses()
                .forEach(objectClass::add);
        entries.put(objectClass);

        try {
            context.createSubcontext(ldapObjectDefinition.getDistinguishedName(), entries);
        }
        catch (NamingException e) {
            throw new RuntimeException("LDAP Entry addition failed", e);
        }

        return ldapObjectDefinition.getDistinguishedName();
    }

    public static LdapObjectDefinition buildLdapOrganizationObject(String name, String baseDistinguisedName)
    {
        return LdapObjectDefinition.builder(name)
                .setDistinguishedName(format("ou=%s,%s", name, baseDistinguisedName))
                .setAttributes(ImmutableMap.of("ou", name))
                .setObjectClasses(ImmutableList.of("top", "organizationalUnit"))
                .build();
    }

    public static LdapObjectDefinition buildLdapUserObject(String organizationName, String userName, String password)
    {
        return LdapObjectDefinition.builder(userName)
                .setDistinguishedName(format("uid=%s,%s", userName, organizationName))
                .setAttributes(ImmutableMap.of(
                        "cn", userName,
                        "sn", userName,
                        "userPassword", password))
                .setObjectClasses(Arrays.asList("person", "inetOrgPerson"))
                .build();
    }
}
