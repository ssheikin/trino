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

package org.apache.trino.kudu.client;

import org.apache.kudu.Common;
import org.apache.kudu.master.Master;
import org.apache.trino.kudu.Schema;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * This is a builder class for all the options that can be provided while creating a table.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class CreateTableOptions
{
    private final List<PartialRow> splitRows = new ArrayList<>();
    private final List<RangePartition> rangePartitions = new ArrayList<>();
    private final List<RangePartitionWithCustomHashSchema> customRangePartitions =
            new ArrayList<>(); // range partitions with custom hash schemas
    private Master.CreateTableRequestPB.Builder pb = Master.CreateTableRequestPB.newBuilder();
    private boolean wait = true;
    private boolean isPbGenerationDone;

    public CreateTableOptions addHashPartitions(List<String> columns, int buckets)
    {
        addHashPartitions(columns, buckets, 0);
        return this;
    }

    public CreateTableOptions addHashPartitions(List<String> columns, int buckets, int seed)
    {
        Common.PartitionSchemaPB.HashBucketSchemaPB.Builder hashBucket =
                pb.getPartitionSchemaBuilder().addHashSchemaBuilder();
        for (String column : columns) {
            hashBucket.addColumnsBuilder().setName(column);
        }
        hashBucket.setNumBuckets(buckets);
        hashBucket.setSeed(seed);
        return this;
    }

    public CreateTableOptions setRangePartitionColumns(List<String> columns)
    {
        Common.PartitionSchemaPB.RangeSchemaPB.Builder rangePartition =
                pb.getPartitionSchemaBuilder().getRangeSchemaBuilder();
        for (String column : columns) {
            rangePartition.addColumnsBuilder().setName(column);
        }
        return this;
    }

    public CreateTableOptions addRangePartition(PartialRow lower,
                                                PartialRow upper)
    {
        return addRangePartition(lower, upper,
                RangePartitionBound.INCLUSIVE_BOUND,
                RangePartitionBound.EXCLUSIVE_BOUND);
    }

    public CreateTableOptions addRangePartition(PartialRow lower,
                                                PartialRow upper,
                                                RangePartitionBound lowerBoundType,
                                                RangePartitionBound upperBoundType)
    {
        rangePartitions.add(new RangePartition(lower, upper, lowerBoundType, upperBoundType));
        return this;
    }

    /**
     * Add range partition with custom hash schema.
     *
     * @param rangePartition range partition with custom hash schema
     * @return this CreateTableOptions object modified accordingly
     */
    public CreateTableOptions addRangePartition(RangePartitionWithCustomHashSchema rangePartition)
    {
        if (!splitRows.isEmpty()) {
            throw new IllegalArgumentException(
                    "no range partitions with custom hash schema are allowed when using " +
                            "split rows to define range partitioning for a table");
        }
        customRangePartitions.add(rangePartition);
        pb.getPartitionSchemaBuilder().addCustomHashSchemaRanges(rangePartition.toPB());
        return this;
    }

    /**
     * Add a range partition split. The split row must fall in a range partition,
     * and causes the range partition to split into two contiguous range partitions.
     * The row may be reused or modified safely after this call without changing
     * the split point.
     *
     * @param row a key row for the split point
     * @return this instance
     */
    public CreateTableOptions addSplitRow(PartialRow row)
    {
        if (!customRangePartitions.isEmpty()) {
            throw new IllegalArgumentException(
                    "no split rows are allowed to define range partitioning for a table " +
                            "when range partitions with custom hash schema are present");
        }
        splitRows.add(new PartialRow(row));
        return this;
    }

    /**
     * Sets the number of replicas that each tablet will have. If not specified, it uses the
     * server-side default which is usually 3 unless changed by an administrator.
     *
     * @param numReplicas the number of replicas to use
     * @return this instance
     */
    public CreateTableOptions setNumReplicas(int numReplicas)
    {
        pb.setNumReplicas(numReplicas);
        return this;
    }

    public CreateTableOptions setDimensionLabel(String dimensionLabel)
    {
        checkArgument(dimensionLabel != null,
                "dimension label must not be null");
        pb.setDimensionLabel(dimensionLabel);
        return this;
    }

    public CreateTableOptions setExtraConfigs(Map<String, String> extraConfig)
    {
        pb.putAllExtraConfigs(extraConfig);
        return this;
    }

    public CreateTableOptions setWait(boolean wait)
    {
        this.wait = wait;
        return this;
    }

    public CreateTableOptions setOwner(String owner)
    {
        pb.setOwner(owner);
        return this;
    }

    /**
     * Set the table comment.
     *
     * @param comment the table comment
     * @return this instance
     */
    public CreateTableOptions setComment(String comment)
    {
        pb.setComment(comment);
        return this;
    }

    Master.CreateTableRequestPB.Builder getBuilder()
    {
        if (isPbGenerationDone) {
            return pb;
        }

        if (!splitRows.isEmpty() && !customRangePartitions.isEmpty()) {
            throw new IllegalArgumentException(
                    "no split rows are allowed to define range partitioning for a table " +
                            "when range partitions with custom hash schema are present");
        }
        if (customRangePartitions.isEmpty()) {
            if (!splitRows.isEmpty() || !rangePartitions.isEmpty()) {
                pb.setSplitRowsRangeBounds(new Operation.OperationsEncoder()
                        .encodeRangePartitions(rangePartitions, splitRows));
            }
        }
        else {
            // With the presence of a range with custom hash schema when the
            // table-wide hash schema is used for a particular range, add proper
            // element into PartitionSchemaPB::custom_hash_schema_ranges to satisfy
            // the convention used by the backend. Do so for all the ranges with
            // table-wide hash schemas.
            for (RangePartition p : rangePartitions) {
                Common.PartitionSchemaPB.RangeWithHashSchemaPB.Builder b =
                        pb.getPartitionSchemaBuilder().addCustomHashSchemaRangesBuilder();
                // Set the hash schema for the range.
                for (Common.PartitionSchemaPB.HashBucketSchemaPB hashSchema :
                        pb.getPartitionSchemaBuilder().getHashSchemaList()) {
                    b.addHashSchema(hashSchema);
                }
                b.setRangeBounds(
                        new Operation.OperationsEncoder().encodeLowerAndUpperBounds(
                                p.lowerBound, p.upperBound, p.lowerBoundType, p.upperBoundType));
            }
        }
        isPbGenerationDone = true;
        return pb;
    }

    List<Integer> getRequiredFeatureFlags(Schema schema)
    {
        List<Integer> requiredFeatureFlags = new ArrayList<>();
        if (schema.hasAutoIncrementingColumn()) {
            requiredFeatureFlags.add(
                    Integer.valueOf(Master.MasterFeatures.AUTO_INCREMENTING_COLUMN_VALUE));
        }
        if (schema.hasImmutableColumns()) {
            requiredFeatureFlags.add(
                    Integer.valueOf(Master.MasterFeatures.IMMUTABLE_COLUMN_ATTRIBUTE_VALUE));
        }
        if (!rangePartitions.isEmpty() || !customRangePartitions.isEmpty()) {
            requiredFeatureFlags.add(Integer.valueOf(Master.MasterFeatures.RANGE_PARTITION_BOUNDS_VALUE));
        }
        if (!customRangePartitions.isEmpty()) {
            requiredFeatureFlags.add(
                    Integer.valueOf(Master.MasterFeatures.RANGE_SPECIFIC_HASH_SCHEMA_VALUE));
        }

        return requiredFeatureFlags;
    }

    boolean shouldWait()
    {
        return wait;
    }
}
