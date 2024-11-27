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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import jakarta.validation.constraints.NotNull;

public class LdapAuthToLocalConfig
{
    private String bindDistinguishedName;
    private String bindPassword;
    private String userBaseDistinguishedName;
    private String userSearchPattern;
    private String ldapAttribute;

    @Config("auth-to-local.ldap.bind-dn")
    @ConfigDescription("Bind distinguished name. Example: CN=User Name,OU=CITY_OU,OU=STATE_OU,DC=domain,DC=domain_root")
    public LdapAuthToLocalConfig setBindDistinguishedName(String bindDistinguishedName)
    {
        this.bindDistinguishedName = bindDistinguishedName;
        return this;
    }

    @NotNull
    public String getBindDistinguishedName()
    {
        return bindDistinguishedName;
    }

    @Config("auth-to-local.ldap.bind-password")
    @ConfigDescription("Bind password used. Example: password1234")
    @ConfigSecuritySensitive
    public LdapAuthToLocalConfig setBindPassword(String bindPassword)
    {
        this.bindPassword = bindPassword;
        return this;
    }

    @NotNull
    public String getBindPassword()
    {
        return bindPassword;
    }

    public String getUserBaseDistinguishedName()
    {
        return userBaseDistinguishedName;
    }

    @Config("auth-to-local.ldap.user-base-dn")
    @ConfigDescription("Base distinguished name of the user. Example: dc=example,dc=com")
    public LdapAuthToLocalConfig setUserBaseDistinguishedName(String userBaseDistinguishedName)
    {
        this.userBaseDistinguishedName = userBaseDistinguishedName;
        return this;
    }

    @Config("auth-to-local.ldap.user-search-filter")
    @ConfigDescription("Custom user search query. Example: &(objectClass=user)(memberOf=cn=group)(user=username)")
    public LdapAuthToLocalConfig setUserSearchPatterns(String userSearchPattern)
    {
        this.userSearchPattern = userSearchPattern;
        return this;
    }

    @NotNull
    public String getUserSearchPatterns()
    {
        return userSearchPattern;
    }

    @Config("auth-to-local.ldap.attribute")
    @ConfigDescription("Attribute to be used for mapping")
    public LdapAuthToLocalConfig setLdapAttribute(String ldapAttribute)
    {
        this.ldapAttribute = ldapAttribute;
        return this;
    }

    @NotNull
    public String getLdapAttribute()
    {
        return ldapAttribute;
    }
}
