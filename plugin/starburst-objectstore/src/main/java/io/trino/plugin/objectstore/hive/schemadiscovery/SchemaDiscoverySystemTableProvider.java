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
package io.trino.plugin.objectstore.hive.schemadiscovery;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.starburst.schema.discovery.SchemaDiscoveryConfig;
import io.starburst.schema.discovery.SchemaDiscoveryControllerFactory;
import io.starburst.schema.discovery.trino.system.table.DiscoveryLocationAccessControlAdapter;
import io.starburst.schema.discovery.trino.system.table.SchemaDiscoverySystemTable;
import io.starburst.schema.discovery.trino.system.table.ShallowDiscoverySystemTable;
import io.trino.plugin.base.classloader.ClassLoaderSafeSystemTable;
import io.trino.plugin.base.connector.SystemTableProvider;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.security.LocationAccessControl;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SchemaDiscoverySystemTableProvider
        implements SystemTableProvider
{
    private final ObjectMapper objectMapper;
    private final DiscoveryLocationAccessControlAdapter discoveryLocationAccessControlAdapter;
    private final SchemaDiscoveryControllerFactory controllerFactory;
    private final int maxBucketQuantity;

    @Inject
    public SchemaDiscoverySystemTableProvider(
            ObjectMapper objectMapper,
            LocationAccessControl locationAccessControl,
            SchemaDiscoveryControllerFactory controllerFactory,
            SchemaDiscoveryConfig config)
    {
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
        requireNonNull(locationAccessControl, "locationAccessControl is null");
        this.discoveryLocationAccessControlAdapter = locationAccessControl::checkCanUseLocation;
        this.controllerFactory = requireNonNull(controllerFactory, "controllerFactory is null");
        maxBucketQuantity = config.getMaxBucketQuantity();
    }

    @Override
    public Optional<SchemaTableName> getSourceTableName(SchemaTableName table)
    {
        return Optional.empty();
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorMetadata metadata, ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName.equals(SchemaDiscoverySystemTable.SCHEMA_TABLE_NAME)) {
            SchemaDiscoverySystemTable systemTable = new SchemaDiscoverySystemTable(controllerFactory, objectMapper, discoveryLocationAccessControlAdapter, maxBucketQuantity);
            return Optional.of(new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
        }
        if (tableName.equals(ShallowDiscoverySystemTable.SCHEMA_TABLE_NAME)) {
            ShallowDiscoverySystemTable systemTable = new ShallowDiscoverySystemTable(controllerFactory, objectMapper, discoveryLocationAccessControlAdapter);
            return Optional.of(new ClassLoaderSafeSystemTable(systemTable, getClass().getClassLoader()));
        }
        return Optional.empty();
    }
}
