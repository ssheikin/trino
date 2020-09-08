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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Stores information about Acid properties of a partition.
 */
public class AcidInfo
{
    private final String partitionLocation;
    private final List<DeleteDeltaInfo> deleteDeltas;

    @JsonCreator
    public AcidInfo(
            @JsonProperty("partitionLocation") String partitionLocation,
            @JsonProperty("deleteDeltas") List<DeleteDeltaInfo> deleteDeltas)
    {
        this.partitionLocation = requireNonNull(partitionLocation, "partitionLocation is null");
        this.deleteDeltas = ImmutableList.copyOf(requireNonNull(deleteDeltas, "deleteDeltas is null"));
        checkArgument(!deleteDeltas.isEmpty(), "deleteDeltas is empty");
    }

    @JsonProperty
    public String getPartitionLocation()
    {
        return partitionLocation;
    }

    @JsonProperty
    public List<DeleteDeltaInfo> getDeleteDeltas()
    {
        return deleteDeltas;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AcidInfo that = (AcidInfo) o;
        return partitionLocation.equals(that.partitionLocation) &&
                deleteDeltas.equals(that.deleteDeltas);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionLocation, deleteDeltas);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partitionLocation", partitionLocation)
                .add("deleteDeltas", deleteDeltas)
                .toString();
    }

    public static class DeleteDeltaInfo
    {
        private final String directoryName;

        @JsonCreator
        public DeleteDeltaInfo(@JsonProperty("directoryName") String directoryName)
        {
            this.directoryName = directoryName;
        }

        @JsonProperty
        public String getDirectoryName()
        {
            return directoryName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DeleteDeltaInfo that = (DeleteDeltaInfo) o;
            return Objects.equals(directoryName, that.directoryName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(directoryName);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("directoryName", directoryName)
                    .toString();
        }
    }

    public static Builder builder(Path partitionPath)
    {
        return new Builder(partitionPath);
    }

    public static Builder builder(AcidInfo acidInfo)
    {
        return new Builder(acidInfo);
    }

    public static class Builder
    {
        private final Path partitionLocation;
        private final ImmutableList.Builder<DeleteDeltaInfo> deleteDeltaInfoBuilder = ImmutableList.builder();

        private Builder(Path partitionPath)
        {
            partitionLocation = requireNonNull(partitionPath, "partitionPath is null");
        }

        private Builder(AcidInfo acidInfo)
        {
            partitionLocation = new Path(acidInfo.getPartitionLocation());
            deleteDeltaInfoBuilder.addAll(acidInfo.deleteDeltas);
        }

        public Builder addDeleteDelta(Path deleteDeltaPath)
        {
            requireNonNull(deleteDeltaPath, "deleteDeltaPath is null");
            Path partitionPathFromDeleteDelta = deleteDeltaPath.getParent();
            checkArgument(
                    partitionLocation.equals(partitionPathFromDeleteDelta),
                    "Partition location in DeleteDelta '%s' does not match stored location '%s'",
                    deleteDeltaPath.getParent().toString(),
                    partitionLocation);

            deleteDeltaInfoBuilder.add(new DeleteDeltaInfo(deleteDeltaPath.getName()));
            return this;
        }

        public Optional<AcidInfo> build()
        {
            List<DeleteDeltaInfo> deleteDeltas = deleteDeltaInfoBuilder.build();
            if (deleteDeltas.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(new AcidInfo(partitionLocation.toString(), deleteDeltas));
        }
    }
}
