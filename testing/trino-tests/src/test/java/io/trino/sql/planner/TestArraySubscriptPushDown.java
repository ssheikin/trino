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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorColumnHandle;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.SystemSessionProperties.ALLOW_UNSAFE_PUSHDOWN;
import static io.trino.SystemSessionProperties.FILTERING_SEMI_JOIN_TO_INNER;
import static io.trino.SystemSessionProperties.PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Logical.Operator.OR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.any;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestArraySubscriptPushDown
        extends BasePlanTest
{
    private static final String TEST_CATALOG_NAME = "test_catalog";
    private static final String TEST_SCHEMA_NAME = "test_schema";
    private static final ArrayType ARRAY_TYPE = new ArrayType(INTEGER);
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ARRAY_SUBSCRIPT = FUNCTIONS.resolveOperator(OperatorType.SUBSCRIPT, ImmutableList.of(ARRAY_TYPE, BIGINT));
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));
    private static final ResolvedFunction IS_FINITE = FUNCTIONS.resolveFunction("is_finite", fromTypes(BIGINT));

    public TestArraySubscriptPushDown()
    {
        super(ImmutableMap.of(ALLOW_UNSAFE_PUSHDOWN, "true"));
    }

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TEST_SCHEMA_NAME)
                .setSystemProperty(ALLOW_UNSAFE_PUSHDOWN, "true")
                .setSystemProperty("task_concurrency", "1"); // these tests don't handle exchanges from local parallel

        PlanTester planTester = PlanTester.create(sessionBuilder.build());

        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withGetColumns(_ -> ImmutableList.of(
                        new ColumnMetadata("arr", ARRAY_TYPE)))
                .withGetTableHandle((_, _) -> new MockConnectorTableHandle(
                        SchemaTableName.schemaTableName(TEST_SCHEMA_NAME, "array_table"),
                        TupleDomain.all(),
                        Optional.of(ImmutableList.of(new MockConnectorColumnHandle("arr", ARRAY_TYPE)))))
                .withName("mock")
                .build();
        planTester.createCatalog(TEST_CATALOG_NAME, connectorFactory, ImmutableMap.of());
        return planTester;
    }

    @Test
    public void testArraySubscriptPushdownMultiLevel()
    {
        assertPlan("SELECT a.arr[1] + arr_at_1 + b.arr[3] FROM (SELECT arr, arr[1] arr_at_1 FROM array_table) a JOIN array_table b on a.arr[1] = b.arr[2]",
                output(
                        project(
                                ImmutableMap.of("expr", expression(
                                        addExpression(
                                                addExpression(new Reference(INTEGER, "arr_at_1"), new Reference(INTEGER, "arr_at_1")),
                                                new Reference(INTEGER, "arr_at_3")))),
                                join(INNER, builder -> builder
                                        .equiCriteria("arr_at_1", "arr_at_2")
                                        .left(
                                                project(
                                                        ImmutableMap.of("arr_at_1", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "arr_l"), new Constant(BIGINT, 1L)))),
                                                        tableScan("array_table", ImmutableMap.of("arr_l", "arr"))))
                                        .right(
                                                exchange(
                                                        ExchangeNode.Scope.LOCAL,
                                                        project(
                                                                ImmutableMap.of(
                                                                        "arr_at_2", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "arr_r"), new Constant(BIGINT, 2L))),
                                                                        "arr_at_3", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "arr_r"), new Constant(BIGINT, 3L)))),
                                                                tableScan("array_table", ImmutableMap.of("arr_r", "arr")))))))));
    }

    @Test
    public void testArraySubscriptPushdownOnParentColumn()
    {
        assertPlan("WITH t(arr) AS (VALUES ARRAY[1, 2, 3, 4], ARRAY[5, 6, 7, 8]) " +
                        "SELECT a.arr, a.arr[1], b.arr[2] FROM t a CROSS JOIN t b",
                output(ImmutableList.of("a_arr", "a_at_1", "b_at_2"),
                        project(
                                ImmutableMap.of(
                                        "a_at_1",
                                        expression(
                                                arraySubscriptExpression(new Reference(new ArrayType(INTEGER), "a_arr"), new Constant(BIGINT, 1L)))),
                                join(INNER, builder -> builder
                                        .left(
                                                values(
                                                        ImmutableList.of("a_arr")))
                                        .right(
                                                values(
                                                        ImmutableList.of("b_at_2"),
                                                        ImmutableList.of(
                                                                ImmutableList.of(new Constant(INTEGER, 2L)),
                                                                ImmutableList.of(new Constant(INTEGER, 6L)))))))));
    }

    @Test
    public void testArraySubscriptPushdownJoin()
    {
        assertPlan("WITH t(arr) AS (VALUES ARRAY[1, 2, 2, 4])" +
                        "SELECT b.arr[1] " +
                        "FROM t a, t b " +
                        "WHERE a.arr[2] = b.arr[3]",
                disablePushFilterIntoValues(),
                output(
                        project(
                                ImmutableMap.of("b_x", expression(new Reference(INTEGER, "arr_1"))),
                                filter(
                                        new Comparison(EQUAL, new Reference(INTEGER, "arr_2"), new Reference(INTEGER, "arr_3")),
                                        values(
                                                ImmutableList.of("arr_1", "arr_2", "arr_3"),
                                                ImmutableList.of(ImmutableList.of(
                                                        new Constant(INTEGER, 1L),
                                                        new Constant(INTEGER, 2L),
                                                        new Constant(INTEGER, 2L))))))));

        assertPlan("WITH t(arr) AS (VALUES ARRAY[1, 2, 2, 4])" +
                        "SELECT a.arr[1] " +
                        "FROM t a JOIN t b ON a.arr[2] = b.arr[3] " +
                        "WHERE a.arr[4] > INTEGER '5'",
                output(ImmutableList.of("a_y"),
                        values("a_y")));

        assertPlan("WITH t(arr) AS (VALUES ARRAY[1, 2, 2, 4])" +
                        "SELECT b.arr[1] " +
                        "FROM t a JOIN t b ON a.arr[2] = b.arr[3] " +
                        "WHERE a.arr[4] + b.arr[4] < BIGINT '10'",
                disablePushFilterIntoValues(),
                output(
                        ImmutableList.of("b_arr_1"),
                        join(INNER, builder -> builder
                                .left(
                                        project(
                                                filter(
                                                        new Comparison(EQUAL, new Reference(INTEGER, "a_arr_2"), new Constant(INTEGER, 2L)),
                                                        values(
                                                                ImmutableList.of("a_arr_2"),
                                                                ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 2L)))))))
                                .right(
                                        project(
                                                ImmutableMap.of("b_arr_1", expression(new Reference(INTEGER, "b_arr_1"))),
                                                filter(
                                                        new Comparison(EQUAL, new Reference(INTEGER, "b_arr_3"), new Constant(INTEGER, 2L)),
                                                        values(
                                                                ImmutableList.of("b_arr_3", "b_arr_1"),
                                                                ImmutableList.of(ImmutableList.of(new Constant(INTEGER, 2L), new Constant(INTEGER, 1L))))))))));
    }

    @Test
    public void testArraySubscriptPushdownFilter()
    {
        // dereference pushdown + constant folding
        assertPlan("WITH t(arr) AS (VALUES ARRAY[1, 2, 2, 4]) " +
                        "SELECT a.arr[1], b.arr[2] " +
                        "FROM t a CROSS JOIN t b " +
                        "WHERE a.arr[3] = 2 OR IS_FINITE(b.arr[4])",
                disablePushFilterIntoValues(),
                any(
                        project(
                                ImmutableMap.of("a_y", expression(new Reference(INTEGER, "arr_1")), "b_x", expression(new Reference(INTEGER, "arr_2"))),
                                filter(
                                        new Logical(OR, ImmutableList.of(
                                                new Comparison(EQUAL, new Reference(INTEGER, "arr_3"), new Constant(INTEGER, 2L)),
                                                new Call(IS_FINITE, ImmutableList.of(new Cast(new Reference(INTEGER, "arr_4"), BIGINT))))),
                                        values(
                                                ImmutableList.of("arr_2", "arr_4", "arr_1", "arr_3"),
                                                ImmutableList.of(ImmutableList.of(
                                                        new Constant(INTEGER, 2L),
                                                        new Constant(INTEGER, 4L),
                                                        new Constant(INTEGER, 1L),
                                                        new Constant(INTEGER, 2L))))))));
    }

    @Test
    public void testArraySubscriptPushdownSemiJoin()
    {
        assertPlan("SELECT arr[1] FROM array_table WHERE arr[2] IN (SELECT t.arr[3] FROM array_table t)",
                Session.builder(getPlanTester().getDefaultSession())
                        .setSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, "false")
                        .build(),
                anyTree(
                        semiJoin("arr_at_2", "arr_at_3", "semi_join_symbol",
                                project(
                                        ImmutableMap.of(
                                                "arr_at_1", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "arr_source"), new Constant(BIGINT, 1L))),
                                                "arr_at_2", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "arr_source"), new Constant(BIGINT, 2L)))),
                                        tableScan("array_table", ImmutableMap.of("arr_source", "arr"))),
                                exchange(
                                        project(
                                                ImmutableMap.of("arr_at_3", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "arr_filter"), new Constant(BIGINT, 3L)))),
                                                tableScan("array_table", ImmutableMap.of("arr_filter", "arr")))))));
    }

    private Session disablePushFilterIntoValues()
    {
        return Session.builder(getPlanTester().getDefaultSession())
                .setSystemProperty(PUSH_FILTER_INTO_VALUES_MAX_ROW_COUNT, "0")
                .build();
    }

    private static Expression arraySubscriptExpression(Expression base, Expression index)
    {
        return new Call(ARRAY_SUBSCRIPT, ImmutableList.of(base, index));
    }

    private static Expression addExpression(Expression left, Expression right)
    {
        return new Call(ADD_INTEGER, ImmutableList.of(left, right));
    }
}
