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

package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.sql.planner.OrderingScheme;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.assertions.TopNRankingSymbolMatcher;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.planner.iterative.rule.test.PlanBuilder;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.WindowNode.Frame;
import io.prestosql.sql.planner.plan.WindowNode.Function;
import io.prestosql.sql.planner.plan.WindowNode.Specification;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.expression;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.filter;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.project;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.topNRanking;
import static io.prestosql.sql.planner.assertions.PlanMatchPattern.values;
import static io.prestosql.sql.tree.FrameBound.Type.CURRENT_ROW;
import static io.prestosql.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static io.prestosql.sql.tree.WindowFrame.Type.RANGE;

public class TestPushPredicateThroughProjectIntoWindow
        extends BaseRuleTest
{
    @Test
    public void testRankingSymbolPruned()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getMetadata(), tester().getQueryRunner().getTypeOperators()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            PlanBuilder.expression("a = 1"),
                            p.project(
                                    Assignments.identity(a),
                                    p.window(
                                            new Specification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rowNumberFunction()),
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNoUpperBoundForRankingSymbol()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getMetadata(), tester().getQueryRunner().getTypeOperators()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            PlanBuilder.expression("a = 1"),
                            p.project(
                                    Assignments.identity(a, ranking),
                                    p.window(
                                            new Specification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rowNumberFunction()),
                                            p.values(a))));
                })
                .doesNotFire();
    }

    @Test
    public void testNonPositiveUpperBoundForRankingSymbol()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getMetadata(), tester().getQueryRunner().getTypeOperators()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            PlanBuilder.expression("a = 1 AND ranking < -10"),
                            p.project(
                                    Assignments.identity(a, ranking),
                                    p.window(
                                            new Specification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rowNumberFunction()),
                                            p.values(a))));
                })
                .matches(values("a", "ranking"));
    }

    @Test
    public void testPredicateNotSatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getMetadata(), tester().getQueryRunner().getTypeOperators()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            PlanBuilder.expression("ranking > 2 AND ranking < 5"),
                            p.project(
                                    Assignments.identity(ranking),
                                    p.window(
                                            new Specification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rowNumberFunction()),
                                            p.values(a))));
                })
                .matches(filter(
                        "ranking > 2 AND ranking < 5",
                        project(
                                ImmutableMap.of("ranking", expression("ranking")),
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", ASC_NULLS_FIRST))
                                                .maxRankingPerPartition(4)
                                                .partial(false),
                                        values(ImmutableList.of("a")))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher()))));
    }

    @Test
    public void testPredicateSatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getMetadata(), tester().getQueryRunner().getTypeOperators()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            PlanBuilder.expression("ranking < 5"),
                            p.project(
                                    Assignments.identity(ranking),
                                    p.window(
                                            new Specification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rowNumberFunction()),
                                            p.values(a))));
                })
                .matches(project(
                        ImmutableMap.of("ranking", expression("ranking")),
                        topNRanking(
                                pattern -> pattern
                                        .specification(
                                                ImmutableList.of(),
                                                ImmutableList.of("a"),
                                                ImmutableMap.of("a", ASC_NULLS_FIRST))
                                        .maxRankingPerPartition(4)
                                        .partial(false),
                                values(ImmutableList.of("a")))
                                .withAlias("ranking", new TopNRankingSymbolMatcher())));
    }

    @Test
    public void testPredicatePartiallySatisfied()
    {
        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getMetadata(), tester().getQueryRunner().getTypeOperators()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            PlanBuilder.expression("ranking < 5 AND a > 0"),
                            p.project(
                                    Assignments.identity(ranking, a),
                                    p.window(
                                            new Specification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rowNumberFunction()),
                                            p.values(a))));
                })
                .matches(filter(
                        "a > 0",
                        project(
                                ImmutableMap.of("ranking", expression("ranking"), "a", expression("a")),
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", ASC_NULLS_FIRST))
                                                .maxRankingPerPartition(4)
                                                .partial(false),
                                        values(ImmutableList.of("a")))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher()))));

        tester().assertThat(new PushPredicateThroughProjectIntoWindow(tester().getMetadata(), tester().getQueryRunner().getTypeOperators()))
                .on(p -> {
                    Symbol a = p.symbol("a");
                    Symbol ranking = p.symbol("ranking");
                    return p.filter(
                            PlanBuilder.expression("ranking < 5 AND ranking % 2 = 0"),
                            p.project(
                                    Assignments.identity(ranking),
                                    p.window(
                                            new Specification(
                                                    ImmutableList.of(),
                                                    Optional.of(new OrderingScheme(ImmutableList.of(a), ImmutableMap.of(a, ASC_NULLS_FIRST)))),
                                            ImmutableMap.of(ranking, rowNumberFunction()),
                                            p.values(a))));
                })
                .matches(filter(
                        "ranking % 2 = 0",
                        project(
                                ImmutableMap.of("ranking", expression("ranking")),
                                topNRanking(
                                        pattern -> pattern
                                                .specification(
                                                        ImmutableList.of(),
                                                        ImmutableList.of("a"),
                                                        ImmutableMap.of("a", ASC_NULLS_FIRST))
                                                .maxRankingPerPartition(4)
                                                .partial(false),
                                        values(ImmutableList.of("a")))
                                        .withAlias("ranking", new TopNRankingSymbolMatcher()))));
    }

    private Function rowNumberFunction()
    {
        return new Function(
                tester().getMetadata().resolveFunction(QualifiedName.of("row_number"), fromTypes()),
                ImmutableList.of(),
                new Frame(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), Optional.empty(), CURRENT_ROW, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty()),
                false);
    }
}
