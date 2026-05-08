/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.license;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public enum StarburstFeature
{
    AGENTIC_LAYER("agentic-layer", "Agentic layer", false),
    AI_WORKFLOWS("ai-workflows", "AI workflows", false),
    DYNAMODB("dynamodb", "Starburst DynamoDB connector", true),
    MAPR("mapr", "MapR support in Hive connector", false),
    MCP("mcp", "MCP server", false),
    PORTAL("portal", "Starburst Portal", false),
    ROUTING("routing", "Starburst Routing", false),
    SALESFORCE("salesforce", "Starburst Salesforce connector", true),
    SPARK("spark", "Spark", false),
    SPLUNK("splunk", "Starburst Splunk connector", true),
    WARP_SPEED("warp-speed", "Warp Speed", false),
    DELL("dell", "Dell Data Lakehouse", false, ImmutableSet.of(AGENTIC_LAYER, AI_WORKFLOWS, MAPR, MCP, PORTAL, ROUTING, SPARK, WARP_SPEED)),
    /**/;

    private final String featureName;
    private final String displayName;
    private final boolean thirdParty;
    private final Set<StarburstFeature> includedFeatures;

    StarburstFeature(String featureName, String displayName, boolean thirdParty)
    {
        this(featureName, displayName, thirdParty, ImmutableSet.of());
    }

    StarburstFeature(String featureName, String displayName, boolean thirdParty, Set<StarburstFeature> includedFeatures)
    {
        this.featureName = requireNonNull(featureName, "featureName is null");
        this.displayName = requireNonNull(displayName, "displayName is null");
        this.thirdParty = thirdParty;
        this.includedFeatures = ImmutableSet.copyOf(requireNonNull(includedFeatures, "includedFeatures is null"));
    }

    public String getFeatureName()
    {
        return featureName;
    }

    public String getDisplayName()
    {
        return displayName;
    }

    public boolean isThirdParty()
    {
        return thirdParty;
    }

    public Set<StarburstFeature> effectiveFeatures()
    {
        return ImmutableSet.<StarburstFeature>builder()
                .add(this)
                .addAll(includedFeatures.stream().flatMap(feature -> feature.effectiveFeatures().stream()).collect(toImmutableSet()))
                .build();
    }
}
