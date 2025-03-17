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

import com.google.common.collect.ImmutableMap;
import io.starburst.schema.discovery.models.IdentifierConstraint;
import jakarta.validation.constraints.Min;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;

public class TestSchemaDiscoveryConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SchemaDiscoveryConfig.class)
                .setSchemaDiscoveryConcurrency(8)
                .setIdentifierConstraint(IdentifierConstraint.ENFORCED_ALPHANUMERIC)
                .setMaxBucketQuantity(10));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("schema-discovery.concurrency", "16")
                .put("schema-discovery.max-buckets", "15")
                .put("schema-discovery.identifier-constraint", "VALID_IN_TRINO")
                .buildOrThrow();

        SchemaDiscoveryConfig expected = new SchemaDiscoveryConfig()
                .setSchemaDiscoveryConcurrency(16)
                .setIdentifierConstraint(IdentifierConstraint.VALID_IN_TRINO)
                .setMaxBucketQuantity(15);

        assertFullMapping(properties, expected);
    }

    @Test
    public void testConcurrencyConfiguration()
    {
        SchemaDiscoveryConfig invalidConfig = new SchemaDiscoveryConfig()
                .setSchemaDiscoveryConcurrency(0);
        assertFailsValidation(
                invalidConfig,
                "schemaDiscoveryConcurrency",
                "must be greater than or equal to 1",
                Min.class);
        invalidConfig = new SchemaDiscoveryConfig()
                .setMaxBucketQuantity(0);
        assertFailsValidation(
                invalidConfig,
                "maxBucketQuantity",
                "must be greater than or equal to 1",
                Min.class);

        SchemaDiscoveryConfig validConfig = new SchemaDiscoveryConfig()
                .setSchemaDiscoveryConcurrency(1);
        assertValidates(validConfig);
    }
}
