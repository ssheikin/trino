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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Min;

public class SchemaDiscoveryConfig
{
    private int schemaDiscoveryConcurrency = 8;

    @Min(1)
    public int getSchemaDiscoveryConcurrency()
    {
        return schemaDiscoveryConcurrency;
    }

    @Config("schema-discovery.concurrency")
    @ConfigDescription("Maximum number of parallel jobs used for scanning files by schema discovery")
    public SchemaDiscoveryConfig setSchemaDiscoveryConcurrency(int schemaDiscoveryConcurrency)
    {
        this.schemaDiscoveryConcurrency = schemaDiscoveryConcurrency;
        return this;
    }
}
