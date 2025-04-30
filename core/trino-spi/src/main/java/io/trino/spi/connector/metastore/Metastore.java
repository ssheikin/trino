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
import java.util.function.Function;

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

    List<Table> findTablesByProperty(String propertyKey, PropertyMatch propertyMatch);

    default void createTable(Table table)
            throws MetastoreFailureException, AlreadyExistsException
    {
        createTable(table, Map.of());
    }

    void createTable(Table table, Map<String, String> properties)
            throws MetastoreFailureException, AlreadyExistsException;

    void renameTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName fromSchemaTableName,
            SchemaTableName toSchemaTableName,
            RelationType type)
            throws MetastoreFailureException, NotFoundException;

    void dropTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            RelationType type)
            throws MetastoreFailureException, NotFoundException;

    void updateTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            Table newTable)
            throws MetastoreFailureException, NotFoundException;

    default void updateTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            Table newTable,
            Map<String, String> newProperties)
            throws MetastoreFailureException, NotFoundException
    {
        updateTable(clusterCatalogName, schemaTableName, newTable, _ -> newProperties);
    }

    void updateTable(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            Table newTable,
            Function<Map<String, String>, Map<String, String>> propertiesTransformer);

    Map<String, String> getTableProperties(ClusterCatalogName clusterCatalogName, SchemaTableName schemaTableName)
            throws MetastoreFailureException;

    default void setTableProperties(ClusterCatalogName clusterCatalogName, SchemaTableName schemaTableName, Map<String, String> properties)
            throws MetastoreFailureException, NotFoundException
    {
        setTableProperties(clusterCatalogName, schemaTableName, _ -> properties);
    }

    void setTableProperties(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            Function<Map<String, String>, Map<String, String>> propertiesTransformer)
            throws MetastoreFailureException, NotFoundException;

    List<Schema> getSchemas(ClusterCatalogName clusterCatalogName)
            throws MetastoreFailureException;

    boolean schemaExists(ClusterCatalogName clusterCatalogName, String schemaName)
            throws MetastoreFailureException;

    default void createSchema(
            ClusterCatalogName clusterCatalogName,
            String schemaName)
            throws MetastoreFailureException, AlreadyExistsException
    {
        createSchema(clusterCatalogName, schemaName, Map.of());
    }

    void createSchema(
            ClusterCatalogName clusterCatalogName,
            String schemaName,
            Map<String, String> properties)
            throws MetastoreFailureException, AlreadyExistsException;

    boolean dropSchema(ClusterCatalogName clusterCatalogName, String schemaName)
            throws MetastoreFailureException, NotFoundException;

    Map<String, String> getSchemaProperties(ClusterCatalogName clusterCatalogName, String schemaName)
            throws MetastoreFailureException;

    default void setSchemaProperties(ClusterCatalogName clusterCatalogName, String schemaName, Map<String, String> properties)
            throws MetastoreFailureException, NotFoundException
    {
        setSchemaProperties(clusterCatalogName, schemaName, _ -> properties);
    }

    void setSchemaProperties(
            ClusterCatalogName clusterCatalogName,
            String schemaName,
            Function<Map<String, String>, Map<String, String>> propertiesTransformer)
            throws MetastoreFailureException, NotFoundException;

    record Table(
            ClusterCatalogName clusterCatalogName,
            SchemaTableName schemaTableName,
            Optional<String> viewDefinition,
            Optional<String> owner,
            RelationType type)
    {
        public static final int MAX_VIEW_DEFINITION_LENGTH = 409600;

        public Table
        {
            requireNonNull(clusterCatalogName, "clusterCatalogName is null");
            requireNonNull(schemaTableName, "schemaTableName is null");
            requireNonNull(viewDefinition, "viewDefinition is null");
            requireNonNull(owner, "owner is null");
            requireNonNull(type, "type is null");
            validateViewDefinition(type, viewDefinition);
        }

        private void validateViewDefinition(RelationType type, Optional<String> viewDefinition)
        {
            switch (type) {
                case VIEW, MATERIALIZED_VIEW -> {
                    if (viewDefinition.isEmpty()) {
                        throw new IllegalStateException("View definition must be present");
                    }
                    if (viewDefinition.get().length() > MAX_VIEW_DEFINITION_LENGTH) {
                        throw new IllegalStateException("View definition can't be longer than " + MAX_VIEW_DEFINITION_LENGTH);
                    }
                }
                case TABLE -> {
                    if (viewDefinition.isPresent()) {
                        throw new IllegalStateException("View definition cannot be present");
                    }
                }
            }
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

    sealed interface PropertyMatch
            permits PropertyMatch.MatchByPrefix, PropertyMatch.Reverse
    {
        default PropertyMatch reverse()
        {
            return new Reverse(this);
        }

        /**
         * Match all values that the given value is a prefix of.
         */
        record MatchByPrefix(String... prefixes)
                implements PropertyMatch
        {
            public MatchByPrefix
            {
                requireNonNull(prefixes, "prefixes is null");
            }
        }

        /**
         * Reverses the search, treating the stored values as the search predicate.
         */
        record Reverse(PropertyMatch match)
                implements PropertyMatch
        {
            public Reverse
            {
                requireNonNull(match, "match is null");
            }
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
