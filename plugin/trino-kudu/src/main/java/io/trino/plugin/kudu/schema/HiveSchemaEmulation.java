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
package io.trino.plugin.kudu.schema;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.kudu.KuduClientWrapper;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import org.apache.trino.kudu.client.KuduException;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.kudu.KuduClientSession.DEFAULT_SCHEMA;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

public class HiveSchemaEmulation
        implements SchemaEmulation
{
    private static final Splitter SPLITTER = Splitter.on('.');

    @Override
    public void createSchema(KuduClientWrapper client, String schemaName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Creating schema in Kudu connector not allowed if schema emulation is set to HIVE_METASTORE");
    }

    @Override
    public boolean existsSchema(KuduClientWrapper client, String schemaName)
    {
        return ImmutableSet.copyOf(listSchemaNames(client)).contains(schemaName);
    }

    @Override
    public void dropSchema(KuduClientWrapper client, String schemaName, boolean cascade)
    {
        throw new TrinoException(NOT_SUPPORTED, "Deleting default schema not allowed.");
    }

    @Override
    public List<String> listSchemaNames(KuduClientWrapper client)
    {
        try {
            return client.getTablesList().getTablesList().stream()
                    .map(name -> SPLITTER.splitToList(name).getFirst())
                    .collect(toImmutableList());
        }
        catch (KuduException e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public String toRawName(SchemaTableName schemaTableName)
    {
        return schemaTableName.toString();
    }

    @Override
    public SchemaTableName fromRawName(String rawName)
    {
        List<String> parts = SPLITTER.splitToList(rawName);
        return new SchemaTableName(parts.getFirst(), parts.getLast());
    }

    @Override
    public String getPrefixForTablesOfSchema(String schemaName)
    {
        return "";
    }

    @Override
    public List<String> filterTablesForDefaultSchema(List<String> rawTables)
    {
        return rawTables.stream()
                .filter(table -> table.startsWith(DEFAULT_SCHEMA))
                .collect(toImmutableList());
    }
}
