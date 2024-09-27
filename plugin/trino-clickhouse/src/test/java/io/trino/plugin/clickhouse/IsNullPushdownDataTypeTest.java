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
package io.trino.plugin.clickhouse;

import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.clickhouse.IsNullPushdownDataTypeTest.PushdownImplementation.ALWAYS;
import static io.trino.plugin.clickhouse.IsNullPushdownDataTypeTest.PushdownImplementation.CONNECTOR_EXPRESSION_ONLY;
import static io.trino.plugin.clickhouse.IsNullPushdownDataTypeTest.PushdownImplementation.TUPLE_DOMAIN_ONLY;
import static io.trino.plugin.clickhouse.IsNullPushdownDataTypeTest.Scenario.IS_NOT_NULL;
import static io.trino.plugin.clickhouse.IsNullPushdownDataTypeTest.Scenario.IS_NULL;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public final class IsNullPushdownDataTypeTest
{
    private static final int SPECIAL_COLUMNS = 1;

    public static IsNullPushdownDataTypeTest create(PushdownImplementation pushdownImplementation)
    {
        return new IsNullPushdownDataTypeTest(pushdownImplementation);
    }

    private final List<TestCase> testCases = new ArrayList<>();
    private final PushdownImplementation pushdownImplementation;

    private IsNullPushdownDataTypeTest(PushdownImplementation pushdownImplementation)
    {
        this.pushdownImplementation = pushdownImplementation;
    }

    public IsNullPushdownDataTypeTest addRoundTrip(String literal)
    {
        return addRoundTrip(literal, literal);
    }

    public IsNullPushdownDataTypeTest addRoundTrip(String inputLiteral, String expectedLiteral)
    {
        testCases.add(new TestCase(Optional.empty(), Optional.empty(), inputLiteral, Optional.empty(), expectedLiteral));
        return this;
    }

    public IsNullPushdownDataTypeTest addRoundTrip(String inputType, String literal, Type expectedType)
    {
        return addRoundTrip(inputType, literal, expectedType, literal);
    }

    public IsNullPushdownDataTypeTest addRoundTrip(String inputType, String inputLiteral, Type expectedType, String expectedLiteral)
    {
        addRoundTrip(Optional.empty(), inputType, inputLiteral, expectedType, expectedLiteral);
        return this;
    }

    public IsNullPushdownDataTypeTest addRoundTrip(String columnName, String inputType, String inputLiteral, Type expectedType, String expectedLiteral)
    {
        addRoundTrip(Optional.of(columnName), inputType, inputLiteral, expectedType, expectedLiteral);
        return this;
    }

    public IsNullPushdownDataTypeTest addRoundTrip(Optional<String> columnName, String inputType, String inputLiteral, Type expectedType, String expectedLiteral)
    {
        testCases.add(new TestCase(columnName, Optional.of(inputType), inputLiteral, Optional.of(expectedType), expectedLiteral));
        return this;
    }

    public IsNullPushdownDataTypeTest execute(QueryRunner queryRunner, DataSetup dataSetup)
    {
        return execute(queryRunner, queryRunner.getDefaultSession(), dataSetup);
    }

    public IsNullPushdownDataTypeTest execute(QueryRunner queryRunner, Session session, DataSetup dataSetup)
    {
        checkState(!testCases.isEmpty(), "No test cases");
        for (int specialColumn = 0; specialColumn < SPECIAL_COLUMNS; specialColumn++) {
            checkArgument(!"NULL".equalsIgnoreCase(testCases.get(specialColumn).inputLiteral()));
        }

        try (TemporaryRelation temporaryRelation = dataSetup.setupTemporaryRelation(unmodifiableList(testCases))) {
            verifySelect(queryRunner, session, temporaryRelation);
            verifyPredicate(queryRunner, session, temporaryRelation, IS_NULL, false, List.of(TUPLE_DOMAIN_ONLY, ALWAYS).contains(pushdownImplementation));
            verifyPredicate(queryRunner, session, temporaryRelation, IS_NULL, true, List.of(CONNECTOR_EXPRESSION_ONLY, ALWAYS).contains(pushdownImplementation));
            verifyPredicate(queryRunner, session, temporaryRelation, IS_NOT_NULL, false, List.of(TUPLE_DOMAIN_ONLY, ALWAYS).contains(pushdownImplementation));
            verifyPredicate(queryRunner, session, temporaryRelation, IS_NOT_NULL, true, List.of(CONNECTOR_EXPRESSION_ONLY, ALWAYS).contains(pushdownImplementation));
        }
        return this;
    }

    private void verifySelect(QueryRunner queryRunner, Session session, TemporaryRelation temporaryRelation)
    {
        @SuppressWarnings("resource") // Closing QueryAssertions would close the QueryRunner
        QueryAssertions queryAssertions = new QueryAssertions(queryRunner);

        QueryAssertions.ResultAssert assertion = assertThat(queryAssertions.query(session, "SELECT * FROM " + temporaryRelation.getName()))
                .result();
        MaterializedResult expected = queryRunner.execute(session, testCases.stream()
                .map(TestCase::expectedLiteral)
                .collect(joining(",", "VALUES ROW(", ")")));

        // Verify types if specified
        for (int column = 0; column < testCases.size(); column++) {
            TestCase testCase = testCases.get(column);
            if (testCase.expectedType().isPresent()) {
                Type expectedType = testCase.expectedType().get();
                assertion.hasType(column, expectedType);
                assertThat(expected.getTypes())
                        .as(format("Expected literal type at column %d (check consistency of expected type and expected literal)", column + 1))
                        .element(column).isEqualTo(expectedType);
            }
        }

        assertion.matches(expected);
    }

    private void verifyPredicate(QueryRunner queryRunner, Session session, TemporaryRelation temporaryRelation, Scenario scenario, boolean connectorExpression, boolean expectPushdown)
    {
        TestCase firstCase = testCases.getFirst();
        String columnName = firstCase.columnName().orElse("col_0");
        String withConnectorExpression = connectorExpression ? " OR %s IS NULL".formatted(columnName) : "";

        String queryWithAll = "SELECT " + columnName + " FROM " + temporaryRelation.getName() + " WHERE " +
                IntStream.range(SPECIAL_COLUMNS, testCases.size())
                        .mapToObj(column -> getPredicate(column, scenario))
                        .collect(joining(" AND "))
                + withConnectorExpression;

        @SuppressWarnings("resource") // Closing QueryAssertions would close the QueryRunner
        QueryAssertions queryAssertions = new QueryAssertions(queryRunner);

        try {
            assertPushdown(expectPushdown,
                    assertResult(scenario.equals(IS_NULL) ? Optional.of(firstCase) : Optional.empty(),
                            assertThat(queryAssertions.query(session, queryWithAll))));
        }
        catch (AssertionError e) {
            for (int column = SPECIAL_COLUMNS; column < testCases.size(); column++) {
                String queryWithSingleColumnPredicate = "SELECT " + columnName + " FROM " + temporaryRelation.getName() + " WHERE " + getPredicate(column, scenario) + withConnectorExpression;
                assertPushdown(expectPushdown,
                        assertResult(scenario.equals(IS_NULL) ? Optional.of(firstCase) : Optional.empty(),
                                assertThat(queryAssertions.query(session, queryWithSingleColumnPredicate))));
            }
            throw new IllegalStateException("Single column assertion should fail for at least one column, if query of all column failed", e);
        }
    }

    private String getPredicate(int column, Scenario scenario)
    {
        checkArgument(column >= SPECIAL_COLUMNS);
        String columnName = testCases.get(column).columnName().orElseGet(() -> "col_" + column);
        checkArgument("NULL".equalsIgnoreCase(testCases.get(column).inputLiteral()));
        return scenario == IS_NULL
                ? columnName + " IS NULL"
                : columnName + " IS NOT NULL";
    }

    private static QueryAssertions.QueryAssert assertResult(Optional<TestCase> firstCase, QueryAssertions.QueryAssert assertion)
    {
        return firstCase.isPresent()
                ? assertion.matches("VALUES %s".formatted(firstCase.get().expectedLiteral()))
                : assertion.returnsEmptyResult();
    }

    private static QueryAssertions.QueryAssert assertPushdown(boolean expectPushdown, QueryAssertions.QueryAssert assertion)
    {
        return expectPushdown
                ? assertion.isFullyPushedDown()
                : assertion.isNotFullyPushedDown(FilterNode.class);
    }

    private record TestCase(
            Optional<String> columnName,
            Optional<String> declaredType,
            String inputLiteral,
            Optional<Type> expectedType,
            String expectedLiteral)
            implements ColumnSetup
    {
        private TestCase
        {
            requireNonNull(columnName, "columnName is null");
            requireNonNull(declaredType, "declaredType is null");
            requireNonNull(expectedType, "expectedType is null");
            requireNonNull(inputLiteral, "inputLiteral is null");
            requireNonNull(expectedLiteral, "expectedLiteral is null");
        }

        @Override
        public Optional<String> getDeclaredType()
        {
            return declaredType;
        }

        @Override
        public String getInputLiteral()
        {
            return inputLiteral;
        }
    }

    enum Scenario
    {
        IS_NULL,
        IS_NOT_NULL,
    }

    enum PushdownImplementation
    {
        NEVER,
        TUPLE_DOMAIN_ONLY,
        CONNECTOR_EXPRESSION_ONLY,
        ALWAYS,
    }
}
