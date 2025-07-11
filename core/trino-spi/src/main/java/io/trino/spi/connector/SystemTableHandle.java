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
package io.trino.spi.connector;

import io.trino.spi.predicate.TupleDomain;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record SystemTableHandle(String schemaName, String tableName, TupleDomain<ColumnHandle> constraint)
        implements ConnectorTableHandle
{
    public SystemTableHandle
    {
        requireNonNull(schemaName, "schemaName is null");
        if (!schemaName.equals(schemaName.toLowerCase(ENGLISH))) {
            throw new IllegalArgumentException("schemaName is not lowercase: %s".formatted(schemaName));
        }
        requireNonNull(tableName, "tableName is null");
        if (!tableName.equals(tableName.toLowerCase(ENGLISH))) {
            throw new IllegalArgumentException("tableName is not lowercase: %s".formatted(tableName));
        }
        requireNonNull(constraint, "constraint is null");
    }

    public static SystemTableHandle fromSchemaTableName(SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        return new SystemTableHandle(tableName.getSchemaName(), tableName.getTableName(), TupleDomain.all());
    }

    public SchemaTableName schemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + "." + tableName;
    }
}
