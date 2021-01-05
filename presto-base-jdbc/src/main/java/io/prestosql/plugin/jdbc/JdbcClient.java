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
package io.prestosql.plugin.jdbc;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.connector.TableScanRedirectApplicationResult;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

public interface JdbcClient
{
    default boolean schemaExists(ConnectorSession session, String schema)
    {
        return getSchemaNames(session).contains(schema);
    }

    Set<String> getSchemaNames(ConnectorSession session);

    List<SchemaTableName> getTableNames(ConnectorSession session, Optional<String> schema);

    Optional<JdbcTableHandle> getTableHandle(ConnectorSession session, SchemaTableName schemaTableName);

    List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle);

    Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle);

    /**
     * Bulk variant of {@link #toPrestoType(ConnectorSession, Connection, JdbcTypeHandle)}.
     */
    List<ColumnMapping> getColumnMappings(ConnectorSession session, List<JdbcTypeHandle> typeHandles);

    WriteMapping toWriteMapping(ConnectorSession session, Type type);

    default boolean supportsGroupingSets()
    {
        return true;
    }

    default boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<List<ColumnHandle>> groupingSets)
    {
        return true;
    }

    default Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return Optional.empty();
    }

    ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle);

    Connection getConnection(ConnectorSession session, JdbcSplit split)
            throws SQLException;

    default void abortReadConnection(Connection connection)
            throws SQLException
    {
        // most drivers do not need this
    }

    PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException;

    boolean supportsLimit();

    boolean isLimitGuaranteed(ConnectorSession session);

    default void setColumnComment(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column, Optional<String> comment)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support setting column comments");
    }

    void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column);

    void dropColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle column);

    void renameColumn(ConnectorSession session, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName);

    void renameTable(ConnectorSession session, JdbcTableHandle handle, SchemaTableName newTableName);

    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void commitCreateTable(ConnectorSession session, JdbcOutputTableHandle handle);

    JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns);

    void finishInsertTable(ConnectorSession session, JdbcOutputTableHandle handle);

    void dropTable(ConnectorSession session, JdbcTableHandle jdbcTableHandle);

    void rollbackCreateTable(ConnectorSession session, JdbcOutputTableHandle handle);

    String buildInsertSql(JdbcOutputTableHandle handle, List<WriteFunction> columnWriters);

    Connection getConnection(ConnectorSession session, JdbcOutputTableHandle handle)
            throws SQLException;

    PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException;

    TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain);

    void createSchema(ConnectorSession session, String schemaName);

    void dropSchema(ConnectorSession session, String schemaName);

    default Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        return Optional.empty();
    }

    String quoted(String name);

    String quoted(RemoteTableName remoteTableName);

    Map<String, Object> getTableProperties(ConnectorSession session, JdbcTableHandle tableHandle);

    default Optional<TableScanRedirectApplicationResult> getTableScanRedirection(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return Optional.empty();
    }
}
