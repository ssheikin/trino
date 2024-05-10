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

package io.trino.spi.connector.metastore;

import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class UnimplementedMetastore
        implements Metastore
{
    @Override
    public List<Table> getTables(ClusterCatalogName clusterCatalogName, String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Table> getTable(ClusterCatalogName clusterCatalogName, SchemaTableName schemaTableName, RelationType type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(Table table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int updateTableMetadataLocation(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            String newMetadataLocation,
            String previousMetadataLocation)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int renameTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName fromSchemaTableName,
            SchemaTableName toSchemaTableName,
            RelationType type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int dropTable(ClusterCatalogName catalogName, SchemaTableName schemaTableName, RelationType type)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int updateTable(ClusterCatalogName clusterCatalogName, SchemaTableName schemaTableName, Table newTable)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, String> getSchemaProperties(ClusterCatalogName clusterCatalogName, String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createSchema(ClusterCatalogName clusterCatalogName, String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Schema> getSchemas(ClusterCatalogName clusterCatalogName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean dropSchema(ClusterCatalogName clusterCatalogName, String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean schemaExists(ClusterCatalogName clusterCatalogName, String schemaName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addSchemaProperties(ClusterCatalogName clusterCatalogName, String schemaName, Map<String, String> properties)
    {
        throw new UnsupportedOperationException();
    }
}
