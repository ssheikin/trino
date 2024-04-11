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
package io.trino.plugin.hive.security;

import com.google.inject.Binder;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.base.security.ConnectorAccessControlModule;
import io.trino.plugin.base.security.FileBasedAccessControlModule;
import io.trino.plugin.base.security.ReadOnlySecurityModule;
import io.trino.spi.TrinoException;

import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class HiveSecurityModule
        extends AbstractConfigurationAwareModule
{
    public enum HiveSecurity
    {
        ALLOW_ALL,
        READ_ONLY,
        FILE,
        SQL_STANDARD,
        SYSTEM,
        RANGER,
        LAKE_FORMATION,
        SENTRY,
        STARBURST,
        /**/
    }

    @Override
    protected void setup(Binder binder)
    {
        install(new ConnectorAccessControlModule());
        install(switch (buildConfigObject(SecurityConfig.class).getSecuritySystem()) {
            case ALLOW_ALL -> new AllowAllSecurityModule();
            case READ_ONLY -> combine(new ReadOnlySecurityModule(), new StaticAccessControlMetadataModule());
            case FILE -> combine(new FileBasedAccessControlModule(), new StaticAccessControlMetadataModule());
            case SQL_STANDARD -> new SqlStandardSecurityModule();
            case SYSTEM -> new SystemSecurityModule();
            case RANGER -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported Hive security: RANGER");
            case LAKE_FORMATION -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported Hive security: LAKE_FORMATION");
            case SENTRY -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported Hive security: SENTRY");
            case STARBURST -> throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unsupported Hive security: STARBURST");
        });
    }

    private static class StaticAccessControlMetadataModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(AccessControlMetadataFactory.class).toInstance(metastore -> new AccessControlMetadata() {});
        }
    }
}
