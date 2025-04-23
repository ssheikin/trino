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
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import java.util.List;

/**
 * This class represents a range partition with custom hash bucketing schema.
 * p
 * See also RangePartition.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RangePartitionWithCustomHashSchema
        extends RangePartition
{
    // Using the corresponding PB type to represent this range with its custom
    // hash schema.
    private Common.PartitionSchemaPB.RangeWithHashSchemaPB.Builder pb =
            Common.PartitionSchemaPB.RangeWithHashSchemaPB.newBuilder();

    public RangePartitionWithCustomHashSchema(
            PartialRow lowerBound,
            PartialRow upperBound,
            RangePartitionBound lowerBoundType,
            RangePartitionBound upperBoundType)
    {
        super(lowerBound, upperBound, lowerBoundType, upperBoundType);
        pb.setRangeBounds(
                new Operation.OperationsEncoder().encodeLowerAndUpperBounds(
                        lowerBound, upperBound, lowerBoundType, upperBoundType));
    }

    public RangePartition addHashPartitions(
            List<String> columns, int numBuckets, int seed)
    {
        Common.PartitionSchemaPB.HashBucketSchemaPB.Builder b =
                pb.addHashSchemaBuilder();
        for (String column : columns) {
            b.addColumnsBuilder().setName(column);
        }
        b.setNumBuckets(numBuckets);
        b.setSeed(seed);
        return this;
    }

    public Common.PartitionSchemaPB.RangeWithHashSchemaPB toPB()
    {
        return pb.build();
    }
}
