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

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.SystemSessionProperties.isUnsafePushdownAllowed;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.ArraySubscriptPushdownUtil.extractArraySubscripts;
import static io.trino.sql.planner.plan.Patterns.filter;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 *  Project(D := f1(A[x]), E := f2(B), G := f3(C))
 *      Filter(B[m] = 3 AND A[x] = 5)
 *          Source(A, B, C)
 *  </pre>
 * to:
 * <pre>
 *  Project(D := f1(expr), E := f2(B), G := f3(C))
 *      Filter(expr = 5 AND B[m] = 3)
 *          Project(A, B, C, expr := A[x])
 *              Source(A, B, C)
 * </pre>
 * <p>
 * Pushes down array subscript projections in project node assignments and filter node predicate. If the underlying connector supports
 * array subscript projection pushdown then these projection would be moved to the table scan and reducing the data to be read from the underlying system.
 * This optimization is considered unsafe as this expression `B[m] = 3` could ensure a valid values of `x` would be used for subscript operation.
 */
public class PushDownArraySubscriptThroughFilter
        implements Rule<ProjectNode>
{
    private static final Capture<FilterNode> CHILD = newCapture();

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(filter().capturedAs(CHILD)));
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isUnsafePushdownAllowed(session);
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Rule.Context context)
    {
        FilterNode filterNode = captures.get(CHILD);

        // Pushdown superset of array subscripts expressions from projections and filtering predicate
        List<Expression> expressions = ImmutableList.<Expression>builder()
                .addAll(node.getAssignments().expressions())
                .add(filterNode.getPredicate())
                .build();

        // Extract array subscripts from project node assignments for pushdown
        Set<Call> arraySubscripts = extractArraySubscripts(expressions, false);

        if (arraySubscripts.isEmpty()) {
            return Result.empty();
        }

        // Create new symbols for array subscripts expressions
        Assignments dereferenceAssignments = Assignments.of(arraySubscripts, context.getSymbolAllocator());

        // Rewrite project node assignments using new symbols for array subscripts expressions
        Map<Expression, Reference> mappings = HashBiMap.create(dereferenceAssignments.assignments())
                .inverse()
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));
        Assignments assignments = node.getAssignments().rewrite(expression -> replaceExpression(expression, mappings));

        PlanNode source = filterNode.getSource();

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new FilterNode(
                                context.getIdAllocator().getNextId(),
                                new ProjectNode(
                                        context.getIdAllocator().getNextId(),
                                        source,
                                        Assignments.builder()
                                                .putIdentities(source.getOutputSymbols())
                                                .putAll(dereferenceAssignments)
                                                .build()),
                                replaceExpression(filterNode.getPredicate(), mappings)),
                        assignments));
    }
}
