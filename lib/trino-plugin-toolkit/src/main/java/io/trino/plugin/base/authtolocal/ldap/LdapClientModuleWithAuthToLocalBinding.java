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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import io.trino.plugin.base.authtolocal.AuthToLocalModule.AuthToLocalBinding;
import io.trino.plugin.base.ldap.JdkLdapClient;
import io.trino.plugin.base.ldap.LdapClient;
import io.trino.plugin.base.ldap.LdapClientConfig;

import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class LdapClientModuleWithAuthToLocalBinding
        implements Module
{
    private final Optional<AuthToLocalBinding> authToLocalBinding;

    public LdapClientModuleWithAuthToLocalBinding(Optional<AuthToLocalBinding> authToLocalBinding)
    {
        this.authToLocalBinding = requireNonNull(authToLocalBinding, "authToLocalBinding is null");
    }

    @Override
    public void configure(Binder binder)
    {
        if (authToLocalBinding.isPresent()) {
            AuthToLocalBinding binding = authToLocalBinding.orElseThrow();
            configBinder(binder).bindConfig(LdapClientConfig.class, binding.getAnnotation(), binding.getPrefix());
            Provider<LdapClientConfig> ldapClientConfigProvider = binder.getProvider(Key.get(LdapClientConfig.class, binding.getAnnotation()));
            binder.bind(Key.get(LdapClient.class, binding.getAnnotation()))
                    .toProvider(() -> new JdkLdapClient(ldapClientConfigProvider.get()))
                    .in(Scopes.SINGLETON);
            return;
        }
        configBinder(binder).bindConfig(LdapClientConfig.class);
        binder.bind(LdapClient.class).to(JdkLdapClient.class).in(Scopes.SINGLETON);
    }
}
