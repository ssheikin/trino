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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class represents a range partition schema with table-wide hash schema.
 * p
 * See also RangePartitionWithCustomHashSchema.
 */
@InterfaceAudience.LimitedPrivate({"kudu-backup", "Test"})
@InterfaceStability.Evolving
public class RangePartition
{
    final PartialRow lowerBound;
    final PartialRow upperBound;
    final RangePartitionBound lowerBoundType;
    final RangePartitionBound upperBoundType;

    public RangePartition(PartialRow lowerBound,
                          PartialRow upperBound,
                          RangePartitionBound lowerBoundType,
                          RangePartitionBound upperBoundType)
    {
        requireNonNull(lowerBound);
        requireNonNull(upperBound);
        checkArgument(
                lowerBound.getSchema().equals(upperBound.getSchema()));
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.lowerBoundType = lowerBoundType;
        this.upperBoundType = upperBoundType;
    }

    public PartialRow getLowerBound()
    {
        return lowerBound;
    }

    public RangePartitionBound getLowerBoundType()
    {
        return lowerBoundType;
    }

    public PartialRow getUpperBound()
    {
        return upperBound;
    }

    public RangePartitionBound getUpperBoundType()
    {
        return upperBoundType;
    }
}
