/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.starburst.schema.discovery.formats.orc.OrcDataSourceFactory;
import io.starburst.schema.discovery.formats.parquet.ParquetDataSourceFactory;
import io.starburst.schema.discovery.generation.Dialect;
import io.starburst.schema.discovery.io.DiscoveryTrinoFileSystem;
import io.starburst.schema.discovery.models.IdentifierConstraint;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.connector.ConnectorSession;

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

public class SchemaDiscoveryControllerFactory
{
    private final Executor executor;
    private final TrinoFileSystemFactory trinoFileSystemFactory;
    private final OrcDataSourceFactory orcDataSourceFactory;
    private final ParquetDataSourceFactory parquetDataSourceFactory;
    private final IdentifierConstraint identifierConstraint;
    private final Dialect dialect;

    @Inject
    public SchemaDiscoveryControllerFactory(
            SchemaDiscoveryConfig config,
            @ForSchemaDiscovery ExecutorService executorService,
            TrinoFileSystemFactory trinoFileSystemFactory,
            OrcDataSourceFactory orcDataSourceFactory,
            ParquetDataSourceFactory parquetDataSourceFactory,
            IdentifierConstraint identifierConstraint,
            @ForSchemaDiscovery Dialect dialect)
    {
        this.executor = new BoundedExecutor(requireNonNull(executorService, "executorService is null"), config.getSchemaDiscoveryConcurrency());
        this.trinoFileSystemFactory = requireNonNull(trinoFileSystemFactory, "trinoFileSystemFactory is null");
        this.orcDataSourceFactory = requireNonNull(orcDataSourceFactory, "orcDataSourceFactory is null");
        this.parquetDataSourceFactory = requireNonNull(parquetDataSourceFactory, "parquetDataSourceFactory is null");
        this.identifierConstraint = requireNonNull(identifierConstraint, "identifierConstraint is null");
        this.dialect = requireNonNull(dialect, "dialect is null");
    }

    public SchemaDiscoveryController createSchemaDiscoveryController(ConnectorSession session)
    {
        return new SchemaDiscoveryController(
                _ -> new DiscoveryTrinoFileSystem(trinoFileSystemFactory.create(session)),
                parquetDataSourceFactory,
                orcDataSourceFactory,
                dialect,
                identifierConstraint,
                executor);
    }

    public IdentifierConstraint getIdentifierConstraint()
    {
        return identifierConstraint;
    }
}
