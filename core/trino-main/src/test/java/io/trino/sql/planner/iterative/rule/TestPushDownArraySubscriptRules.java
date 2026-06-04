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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.plugin.tpch.TpchColumnHandle;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.assertions.ExpressionMatcher;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.Assignments;
import io.trino.testing.PlanTester;
import io.trino.testing.TestingSession;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.ir.Comparison.Operator.EQUAL;
import static io.trino.sql.ir.Comparison.Operator.GREATER_THAN;
import static io.trino.sql.ir.Comparison.Operator.NOT_EQUAL;
import static io.trino.sql.ir.IrExpressions.not;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.planner.assertions.PlanMatchPattern.expression;
import static io.trino.sql.planner.assertions.PlanMatchPattern.filter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.join;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.strictProject;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.INNER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.type.JsonType.JSON;

public class TestPushDownArraySubscriptRules
        extends BaseRuleTest
{
    private static final ArrayType ARRAY_TYPE = new ArrayType(BIGINT);
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_BIGINT = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
    private static final ResolvedFunction ARRAY_SUBSCRIPT = FUNCTIONS.resolveOperator(OperatorType.SUBSCRIPT, ImmutableList.of(ARRAY_TYPE, BIGINT));
    private static final ResolvedFunction ARRAY_CONCAT = FUNCTIONS.resolveFunction("concat", ImmutableList.of(new TypeDescriptorProvider(ARRAY_TYPE.getTypeDescriptor()), new TypeDescriptorProvider(ARRAY_TYPE.getTypeDescriptor())));

    @Override
    protected RuleTester tester()
    {
        return new RuleTester(PlanTester.create(TestingSession.testSessionBuilder().setSystemProperty("allow_unsafe_pushdown", "true").build()));
    }

    @Test
    public void testDoesNotFire()
    {
        // rule does not fire for symbols
        tester().assertThat(new PushDownArraySubscriptThroughFilter())
                .on(p ->
                        p.filter(
                                new Comparison(GREATER_THAN, new Reference(BIGINT, "x"), new Constant(BIGINT, 5L)),
                                p.values(p.symbol("x"))))
                .doesNotFire();

        // Pushdown is not enabled if dereferences come from an expression that is not a simple dereference chain
        tester().assertThat(new PushDownArraySubscriptThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", BIGINT),
                                        new Call(ADD_BIGINT,
                                                ImmutableList.of(
                                                        arraySubscriptExpression(
                                                                new Call(ARRAY_CONCAT, ImmutableList.of(new Reference(ARRAY_TYPE, "a"), new Reference(ARRAY_TYPE, "b"))),
                                                                new Constant(BIGINT, 0L)),
                                                        new Reference(BIGINT, "b")))),
                                p.project(
                                        Assignments.builder()
                                                .put(p.symbol("a", ARRAY_TYPE), new Reference(ARRAY_TYPE, "a"))
                                                .put(p.symbol("b", ARRAY_TYPE), new Reference(ARRAY_TYPE, "b"))
                                                .put(p.symbol("c", BIGINT), new Reference(BIGINT, "c"))
                                                .build(),
                                        p.values(p.symbol("a", ARRAY_TYPE), p.symbol("b", ARRAY_TYPE), p.symbol("c", BIGINT)))))
                .doesNotFire();

        // Does not fire when base symbols are referenced along with the dereferences
        tester().assertThat(new PushDownArraySubscriptThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", ARRAY_TYPE),
                                        new Reference(ARRAY_TYPE, "a"),
                                        p.symbol("a_first_element"),
                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "a"), new Constant(BIGINT, 0L))),
                                p.project(
                                        Assignments.of(p.symbol("a", ARRAY_TYPE), new Reference(ARRAY_TYPE, "a")),
                                        p.values(p.symbol("a", ARRAY_TYPE)))))
                .doesNotFire();
    }

    @Test
    public void testPushdownDereferenceThroughProject()
    {
        tester().assertThat(new PushDownArraySubscriptThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "array"), new Reference(BIGINT, "y"))),
                                p.project(
                                        Assignments.of(
                                                p.symbol("y"),
                                                new Reference(BIGINT, "y"),
                                                p.symbol("array", ARRAY_TYPE),
                                                new Reference(ARRAY_TYPE, "array")),
                                        p.values(p.symbol("array", ARRAY_TYPE), p.symbol("y")))))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", expression(new Reference(BIGINT, "array_at_y"))),
                                strictProject(
                                        ImmutableMap.of(
                                                "array_at_y", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array"), new Reference(BIGINT, "y"))),
                                                "array", expression(new Reference(ARRAY_TYPE, "array")),
                                                "y", expression(new Reference(BIGINT, "y"))),
                                        values("array", "y"))));

        tester().assertThat(new PushDownArraySubscriptThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(p.symbol("x"), new Call(ADD_BIGINT, ImmutableList.of(
                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "array"), new Reference(BIGINT, "y")),
                                        new Constant(BIGINT, 0L)))),
                                p.project(
                                        Assignments.of(
                                                p.symbol("y"),
                                                new Reference(BIGINT, "y"),
                                                p.symbol("array", ARRAY_TYPE),
                                                new Reference(ARRAY_TYPE, "array")),
                                        p.values(p.symbol("array", ARRAY_TYPE), p.symbol("y")))))
                .matches(
                        strictProject(
                                ImmutableMap.of("x", expression(new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "array_at_index"), new Constant(BIGINT, 0L))))),
                                strictProject(
                                        ImmutableMap.of(
                                                "array_at_index", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array"), new Reference(BIGINT, "y"))),
                                                "y", expression(new Reference(BIGINT, "y")),
                                                "array", expression(new Reference(ARRAY_TYPE, "array"))),
                                        values("array", "y"))));
    }

    @Test
    public void testPushDownDereferenceThroughJoin()
    {
        tester().assertThat(new PushDownArraySubscriptThroughJoin())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("left_x"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L)))
                                        .put(p.symbol("right_y"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L)))
                                        .put(p.symbol("z"), new Reference(BIGINT, "z"))
                                        .build(),
                                p.join(INNER,
                                        p.values(p.symbol("array1", ARRAY_TYPE), p.symbol("unreferenced_symbol")),
                                        p.values(p.symbol("array2", ARRAY_TYPE), p.symbol("z")))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("left_x", expression(new Reference(BIGINT, "x")))
                                        .put("right_y", expression(new Reference(BIGINT, "y")))
                                        .put("z", expression(new Reference(BIGINT, "z")))
                                        .buildOrThrow(),
                                join(INNER, builder -> builder
                                        .left(
                                                strictProject(
                                                        ImmutableMap.of(
                                                                "x", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L))),
                                                                "array1", expression(new Reference(ARRAY_TYPE, "array1")),
                                                                "unreferenced_symbol", expression(new Reference(BIGINT, "unreferenced_symbol"))),
                                                        values("array1", "unreferenced_symbol")))
                                        .right(
                                                strictProject(
                                                        ImmutableMap.<String, ExpressionMatcher>builder()
                                                                .put("y", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L))))
                                                                .put("z", expression(new Reference(BIGINT, "z")))
                                                                .put("array2", expression(new Reference(ARRAY_TYPE, "array2")))
                                                                .buildOrThrow(),
                                                        values("array2", "z"))))));

        // Verify pushdown for filters
        tester().assertThat(new PushDownArraySubscriptThroughJoin())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr"),
                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L)),
                                        p.symbol("expr_2", ARRAY_TYPE),
                                        new Reference(ARRAY_TYPE, "array2")),
                                p.join(INNER,
                                        p.values(p.symbol("array1", ARRAY_TYPE)),
                                        p.values(p.symbol("array2", ARRAY_TYPE)),
                                        new Comparison(
                                                GREATER_THAN,
                                                new Call(ADD_BIGINT, ImmutableList.of(
                                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L)),
                                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L)))),
                                                new Constant(BIGINT, 10L)))))
                .matches(
                        project(
                                ImmutableMap.of(
                                        "expr", expression(new Reference(BIGINT, "indexed_array1")),
                                        "expr_2", expression(new Reference(ARRAY_TYPE, "array2"))),
                                join(INNER, builder -> builder
                                        .filter(new Comparison(
                                                GREATER_THAN,
                                                new Call(ADD_BIGINT, ImmutableList.of(
                                                        new Reference(BIGINT, "indexed_array1"),
                                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L)))),
                                                new Constant(BIGINT, 10L)))
                                        .left(
                                                strictProject(
                                                        ImmutableMap.of(
                                                                "indexed_array1", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L))),
                                                                "array1", expression(new Reference(ARRAY_TYPE, "array1"))),
                                                        values("array1")))
                                        .right(values("array2")))));
    }

    @Test
    public void testPushdownDereferencesThroughSemiJoin()
    {
        tester().assertThat(new PushDownArraySubscriptThroughSemiJoin())
                .on(p ->
                        p.project(
                                Assignments.builder()
                                        .put(p.symbol("array1_1"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L)))
                                        .put(p.symbol("array2_2"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L)))
                                        .build(),
                                p.semiJoin(
                                        p.symbol("array2", ARRAY_TYPE),
                                        p.symbol("filtering_array", ARRAY_TYPE),
                                        p.symbol("match"),
                                        p.values(p.symbol("array1", ARRAY_TYPE), p.symbol("array2", ARRAY_TYPE)),
                                        p.values(p.symbol("filtering_array", ARRAY_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.<String, ExpressionMatcher>builder()
                                        .put("array1_1", expression(new Reference(BIGINT, "expr")))
                                        .put("array2_1", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L))))   // Not pushed down because msg2 is sourceJoinSymbol
                                        .buildOrThrow(),
                                semiJoin(
                                        "array2",
                                        "filtering_array",
                                        "match",
                                        strictProject(
                                                ImmutableMap.of(
                                                        "expr", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L))),
                                                        "array1", expression(new Reference(ARRAY_TYPE, "array1")),
                                                        "array2", expression(new Reference(ARRAY_TYPE, "array2"))),
                                                values("array1", "array2")),
                                        values("filtering_array"))));
    }

    @Test
    public void testExtractDereferencesFromFilterAboveScan()
    {
        TableHandle testTable = new TableHandle(
                TEST_CATALOG_HANDLE,
                new TpchTableHandle("sf1", "orders", 1.0),
                TestingTransactionHandle.create());

        ArrayType nestedArrayType = new ArrayType(ARRAY_TYPE);
        ResolvedFunction nestedArraySubscript = FUNCTIONS.resolveOperator(OperatorType.SUBSCRIPT, ImmutableList.of(nestedArrayType, BIGINT));

        tester().assertThat(new ExtractArraySubscriptFromFilterAboveScan())
                .on(p ->
                        p.filter(
                                new Logical(AND, ImmutableList.of(
                                        new Comparison(NOT_EQUAL, arraySubscriptExpression(new Call(nestedArraySubscript, ImmutableList.of(new Reference(nestedArrayType, "a"), new Constant(BIGINT, 1L))), new Constant(BIGINT, 1L)), new Constant(BIGINT, 5L)),
                                        new Comparison(EQUAL, arraySubscriptExpression(new Reference(ARRAY_TYPE, "b"), new Constant(BIGINT, 2L)), new Constant(BIGINT, 2L)),
                                        not(FUNCTIONS.getMetadata(), new IsNull(new Cast(new Call(nestedArraySubscript, ImmutableList.of(new Reference(nestedArrayType, "a"), new Constant(BIGINT, 3L))), JSON))))),
                                p.tableScan(
                                        testTable,
                                        ImmutableList.of(p.symbol("a", nestedArrayType), p.symbol("b", ARRAY_TYPE)),
                                        ImmutableMap.of(
                                                p.symbol("a", nestedArrayType), new TpchColumnHandle("a", nestedArrayType),
                                                p.symbol("b", ARRAY_TYPE), new TpchColumnHandle("b", ARRAY_TYPE)))))
                .matches(project(
                        filter(
                                new Logical(AND, ImmutableList.of(
                                        new Comparison(NOT_EQUAL, new Reference(BIGINT, "expr"), new Constant(BIGINT, 5L)),
                                        new Comparison(EQUAL, new Reference(BIGINT, "expr_0"), new Constant(BIGINT, 2L)),
                                        not(FUNCTIONS.getMetadata(), new IsNull(new Cast(new Reference(ARRAY_TYPE, "expr_1"), JSON))))),
                                strictProject(
                                        ImmutableMap.of(
                                                "expr", expression(arraySubscriptExpression(new Call(nestedArraySubscript, ImmutableList.of(new Reference(nestedArrayType, "a"), new Constant(BIGINT, 1L))), new Constant(BIGINT, 1L))),
                                                "expr_0", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "b"), new Constant(BIGINT, 2L))),
                                                "expr_1", expression(new Call(nestedArraySubscript, ImmutableList.of(new Reference(nestedArrayType, "a"), new Constant(BIGINT, 3L)))),
                                                "a", expression(new Reference(nestedArrayType, "a")),
                                                "b", expression(new Reference(ARRAY_TYPE, "b"))),
                                        tableScan(
                                                testTable.connectorHandle()::equals,
                                                TupleDomain.all(),
                                                ImmutableMap.of(
                                                        "a", new TpchColumnHandle("a", nestedArrayType)::equals,
                                                        "b", new TpchColumnHandle("b", ARRAY_TYPE)::equals))))));
    }

    @Test
    public void testPushdownDereferenceThroughFilter()
    {
        tester().assertThat(new PushDownArraySubscriptThroughFilter())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr", BIGINT),
                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L)),
                                        p.symbol("expr_2", BIGINT),
                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L))),
                                p.filter(
                                        new Logical(
                                                AND,
                                                ImmutableList.of(
                                                        new Comparison(NOT_EQUAL, arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L)), new Constant(BIGINT, 3L)),
                                                        not(FUNCTIONS.getMetadata(), new IsNull(new Reference(ARRAY_TYPE, "array2"))))),
                                        p.values(p.symbol("array1", ARRAY_TYPE), p.symbol("array2", ARRAY_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr", expression(new Reference(BIGINT, "indexed_array")),
                                        "expr_2", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array2"), new Constant(BIGINT, 2L)))), // not pushed down since predicate contains msg2 reference
                                filter(
                                        new Logical(
                                                AND,
                                                ImmutableList.of(
                                                        new Comparison(NOT_EQUAL, new Reference(BIGINT, "indexed_array"), new Constant(BIGINT, 3L)),
                                                        not(FUNCTIONS.getMetadata(), new IsNull(new Reference(ARRAY_TYPE, "array2"))))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "indexed_array", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "array1"), new Constant(BIGINT, 1L))),
                                                        "array1", expression(new Reference(ARRAY_TYPE, "array1")),
                                                        "array2", expression(new Reference(ARRAY_TYPE, "array2"))),
                                                values("array1", "array2")))));
    }

    @Test
    public void testMultiLevelPushdown()
    {
        tester().assertThat(new PushDownArraySubscriptThroughProject())
                .on(p ->
                        p.project(
                                Assignments.of(
                                        p.symbol("expr_1"),
                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "a"), new Constant(BIGINT, 1L)),
                                        p.symbol("expr_2"),
                                        new Call(
                                                ADD_BIGINT,
                                                ImmutableList.of(
                                                        new Call(
                                                                ADD_BIGINT,
                                                                ImmutableList.of(
                                                                        arraySubscriptExpression(new Reference(ARRAY_TYPE, "a"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "b"), new Constant(BIGINT, 1L))),
                                                                        new Constant(BIGINT, 2L))),
                                                        arraySubscriptExpression(
                                                                new Reference(ARRAY_TYPE, "b"),
                                                                arraySubscriptExpression(new Reference(ARRAY_TYPE, "a"), new Constant(BIGINT, 1L)))))),
                                p.project(
                                        Assignments.identity(ImmutableList.of(p.symbol("a", ARRAY_TYPE), p.symbol("b", ARRAY_TYPE))),
                                        p.values(p.symbol("a", ARRAY_TYPE), p.symbol("b", ARRAY_TYPE)))))
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "expr_1", expression(new Reference(BIGINT, "operator_subscript")),
                                        "expr_2", expression(new Call(
                                                ADD_BIGINT,
                                                ImmutableList.of(
                                                        new Call(ADD_BIGINT, ImmutableList.of(new Reference(BIGINT, "a_1"), new Constant(BIGINT, 2L))),
                                                        new Reference(BIGINT, "b_1"))))),
                                strictProject(
                                        ImmutableMap.of(
                                                "a", expression(new Reference(ARRAY_TYPE, "a")),
                                                "b", expression(new Reference(ARRAY_TYPE, "b")),
                                                "operator_subscript", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "a"), new Constant(BIGINT, 1L))),
                                                "a_1", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "a"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "b"), new Constant(BIGINT, 1L)))),
                                                "b_1", expression(arraySubscriptExpression(new Reference(ARRAY_TYPE, "b"), arraySubscriptExpression(new Reference(ARRAY_TYPE, "a"), new Constant(BIGINT, 1L))))),
                                        values("a", "b"))));
    }

    private static Expression arraySubscriptExpression(Expression base, Expression index)
    {
        return new Call(ARRAY_SUBSCRIPT, ImmutableList.of(base, index));
    }
}
