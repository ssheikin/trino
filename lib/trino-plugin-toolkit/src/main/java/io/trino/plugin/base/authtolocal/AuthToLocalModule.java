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
package io.trino.plugin.base.authtolocal;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.authtolocal.ldap.LdapBasedAuthToLocalModule;
import io.trino.plugin.base.authtolocal.rule.RuleBasedAuthToLocalModule;

import java.lang.annotation.Annotation;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class AuthToLocalModule
        extends AbstractConfigurationAwareModule
{
    private final Optional<AuthToLocalBinding> authToLocalBinding;

    public AuthToLocalModule()
    {
        this(Optional.empty());
    }

    public AuthToLocalModule(String prefix, Class<? extends Annotation> annotation)
    {
        this(Optional.of(new AuthToLocalBinding(prefix, annotation)));
    }

    private AuthToLocalModule(Optional<AuthToLocalBinding> authToLocalBinding)
    {
        this.authToLocalBinding = requireNonNull(authToLocalBinding, "authToLocalBinding is null");
    }

    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AuthToLocalConfig.class);
        install(getAuthToLocalModule());
    }

    private Module getAuthToLocalModule()
    {
        AuthToLocalConfig config;
        if (authToLocalBinding.isEmpty()) {
            config = buildConfigObject(AuthToLocalConfig.class);
        }
        else {
            config = buildConfigObject(AuthToLocalConfig.class, authToLocalBinding.get().getPrefix());
        }
        return switch (config.getAuthToLocalType()) {
            case RULE -> new RuleBasedAuthToLocalModule(authToLocalBinding);
            case LDAP -> new LdapBasedAuthToLocalModule(authToLocalBinding);
        };
    }

    public static class AuthToLocalBinding
    {
        private final String prefix;
        private final Class<? extends Annotation> annotation;

        public AuthToLocalBinding(String prefix, Class<? extends Annotation> annotation)
        {
            this.prefix = requireNonNull(prefix, "prefix is null");
            this.annotation = requireNonNull(annotation, "annotation is null");
        }

        public String getPrefix()
        {
            return prefix;
        }

        public Class<? extends Annotation> getAnnotation()
        {
            return annotation;
        }
    }
}
