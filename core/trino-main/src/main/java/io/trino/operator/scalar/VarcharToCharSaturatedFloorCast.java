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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import it.unimi.dsi.fastutil.ints.IntList;

import static io.trino.operator.scalar.CharacterStringCasts.codePointsToSliceUtf8;
import static io.trino.operator.scalar.CharacterStringCasts.toCodePoints;
import static java.lang.Math.toIntExact;

/**
 * Default saturated floor cast from {@code VARCHAR} to {@code CHAR}: returns the largest
 * {@code char(y)} value that, cast back to varchar, does not exceed the input. {@code CHAR} values
 * are stored without trailing spaces and cast back to {@code VARCHAR} unpadded
 * ({@link CharToVarcharCast}), so an input without trailing spaces (truncated to y code points) is
 * its own floor. Registered unless the {@code deprecated.legacy-varchar-to-char-coercion}
 * configuration property is set, in which case {@link LegacyVarcharToCharSaturatedFloorCast} is
 * registered instead.
 */
public final class VarcharToCharSaturatedFloorCast
{
    private VarcharToCharSaturatedFloorCast() {}

    @ScalarOperator(OperatorType.SATURATED_FLOOR_CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharSaturatedFloorCast(@LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        IntList codePoints = toCodePoints(slice);
        if (codePoints.size() > y) {
            // a strict prefix is always smaller than the original value
            codePoints.size(toIntExact(y));
        }
        if (codePoints.isEmpty() || codePoints.getInt(codePoints.size() - 1) != ' ') {
            return codePointsToSliceUtf8(codePoints);
        }
        // Trailing spaces are not representable in CHAR. The largest representable value below the
        // input replaces the last space with the preceding code point and pads with the maximum
        // code point up to y code points.
        codePoints.set(codePoints.size() - 1, ' ' - 1);
        int toAdd = toIntExact(y) - codePoints.size();
        for (int i = 0; i < toAdd; i++) {
            codePoints.add(Character.MAX_CODE_POINT);
        }
        return codePointsToSliceUtf8(codePoints);
    }
}
