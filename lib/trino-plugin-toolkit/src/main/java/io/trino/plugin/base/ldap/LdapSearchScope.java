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

import javax.naming.directory.SearchControls;

/**
 * LDAP search scope definitions that map to JNDI SearchControls constants.
 */
public enum LdapSearchScope
{
    /**
     * Search the base entry and all its descendants (entire subtree).
     */
    SUBTREE(SearchControls.SUBTREE_SCOPE),

    /**
     * Search only the immediate children of the base entry (one level down).
     */
    ONELEVEL(SearchControls.ONELEVEL_SCOPE),

    /**
     * Search only the base entry itself.
     */
    OBJECT(SearchControls.OBJECT_SCOPE);

    private final int jndiValue;

    LdapSearchScope(int jndiValue)
    {
        this.jndiValue = jndiValue;
    }

    /**
     * Returns the JNDI SearchControls constant value for this scope.
     */
    public int getJndiValue()
    {
        return jndiValue;
    }
}
