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

import com.google.errorprone.annotations.FormatMethod;
import io.trino.spi.connector.RelationType;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public interface Metastore
{
    List<Table> getTables(
            ClusterCatalogName clusterCatalogName,
            String schemaName)
            throws MetastoreFailureException;

    Optional<Table> getTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            RelationType type)
            throws MetastoreFailureException;

    void createTable(Table table)
            throws MetastoreFailureException, AlreadyExistsException;

    int updateTableMetadataLocation(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            String newMetadataLocation,
            String previousMetadataLocation)
            throws MetastoreFailureException, NotFoundException;

    int renameTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName fromSchemaTableName,
            SchemaTableName toSchemaTableName,
            RelationType type)
            throws MetastoreFailureException, NotFoundException;

    int dropTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            RelationType type)
            throws MetastoreFailureException, NotFoundException;

    int updateTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            Table newTable)
            throws MetastoreFailureException, NotFoundException;

    Map<String, String> getSchemaProperties(ClusterCatalogName clusterCatalogName, String schemaName)
            throws MetastoreFailureException;

    void createSchema(
            ClusterCatalogName clusterCatalogName,
            String schemaName)
            throws MetastoreFailureException, AlreadyExistsException;

    List<Schema> getSchemas(ClusterCatalogName clusterCatalogName)
            throws MetastoreFailureException;

    void addSchemaProperties(
            ClusterCatalogName clusterCatalogName,
            String schemaName,
            Map<String, String> properties)
            throws MetastoreFailureException, AlreadyExistsException;

    boolean dropSchema(ClusterCatalogName clusterCatalogName, String schemaName)
            throws MetastoreFailureException, NotFoundException;

    boolean schemaExists(ClusterCatalogName clusterCatalogName, String schemaName)
            throws MetastoreFailureException;

    record Table(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            Optional<String> metadataLocation,
            Optional<String> previousMetadataLocation,
            Optional<String> viewDefinition,
            Optional<String> viewComment,
            Optional<String> owner,
            RelationType type)
    {
        public static final int MAX_VIEW_DEFINITION_LENGTH = 409600;

        public Table
        {
            requireNonNull(clusterCatalogName, "clusterCatalogName is null");
            requireNonNull(schemaTableName, "schemaTableName is null");
            requireNonNull(metadataLocation, "metadataLocation is null");
            requireNonNull(previousMetadataLocation, "previousMetadataLocation is null");
            requireNonNull(viewDefinition, "viewDefinition is null");
            requireNonNull(viewComment, "viewComment is null");
            requireNonNull(owner, "owner is null");
            requireNonNull(type, "type is null");
            validate(type, viewDefinition, metadataLocation);
        }

        private void validate(RelationType type, Optional<String> viewDefinition, Optional<String> metadataLocation)
        {
            switch (type) {
                case VIEW -> {
                    checkState(viewDefinition, "viewDefinition");
                    if (viewDefinition.get().length() > MAX_VIEW_DEFINITION_LENGTH) {
                        throw new IllegalStateException("View definition can't be longer than " + MAX_VIEW_DEFINITION_LENGTH);
                    }
                }
                case TABLE -> checkState(metadataLocation, "metadataLocation");
                case MATERIALIZED_VIEW -> {
                    checkState(viewDefinition, "viewDefinition");
                    checkState(metadataLocation, "metadataLocation");
                }
            }
        }
    }

    private static void checkState(Optional<String> value, String fieldName)
    {
        if (value.isEmpty()) {
            throw new IllegalStateException(fieldName + " must be present");
        }
    }

    record Schema(String name)
    {
        public Schema
        {
            name = name.toLowerCase(ENGLISH);
        }
    }

    record ClusterCatalogName(String clusterName, String catalogName)
    {
        public ClusterCatalogName
        {
            clusterName = clusterName.toLowerCase(ENGLISH);
            catalogName = catalogName.toLowerCase(ENGLISH);
        }
    }

    class MetastoreException
            extends RuntimeException
    {
        public MetastoreException(String message)
        {
            super(message);
        }

        public MetastoreException(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    class NotFoundException
            extends MetastoreException
    {
        @FormatMethod
        public NotFoundException(String message, Object... args)
        {
            super(format(message, args));
        }

        @FormatMethod
        public NotFoundException(Throwable cause, String message, Object... args)
        {
            super(format(message, args), cause);
        }
    }

    class AlreadyExistsException
            extends MetastoreException
    {
        @FormatMethod
        public AlreadyExistsException(String message, Object... args)
        {
            super(format(message, args));
        }

        @FormatMethod
        public AlreadyExistsException(Throwable cause, String message, Object... args)
        {
            super(format(message, args), cause);
        }
    }

    class MetastoreFailureException
            extends MetastoreException
    {
        @FormatMethod
        public MetastoreFailureException(String message, Object... args)
        {
            super(format(message, args));
        }

        @FormatMethod
        public MetastoreFailureException(Throwable cause, String message, Object... args)
        {
            super(format(message, args), cause);
        }
    }
}
