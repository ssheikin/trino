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
import io.trino.Session;
import io.trino.matching.Capture;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SystemSessionProperties.isUnsafePushdownAllowed;
import static io.trino.matching.Capture.newCapture;
import static io.trino.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.trino.sql.planner.iterative.rule.ArraySubscriptPushdownUtil.extractArraySubscripts;
import static io.trino.sql.planner.iterative.rule.ArraySubscriptPushdownUtil.getArrayBase;
import static io.trino.sql.planner.plan.Patterns.project;
import static io.trino.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 *  Project(c := f(a[x]), d := g(b))
 *    Project(a, b)
 *  </pre>
 * to:
 * <pre>
 *  Project(c := f(symbol), d := g(b))
 *    Project(a, b, symbol := a[x])
 * </pre>
 */
public class PushDownArraySubscriptThroughProject
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    @Override
    public boolean isEnabled(Session session)
    {
        return isUnsafePushdownAllowed(session);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return project()
                .with(source().matching(project().capturedAs(CHILD)));
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        // Extract array subscripts from project node assignments for pushdown
        Set<Call> arraySubscripts = extractArraySubscripts(node.getAssignments().expressions(), false);

        // Exclude array subscript on symbols being synthesized within child
        arraySubscripts = arraySubscripts.stream()
                .filter(expression -> child.getSource().getOutputSymbols().contains(getArrayBase(expression)))
                .collect(toImmutableSet());

        if (arraySubscripts.isEmpty()) {
            return Result.empty();
        }

        // Create new symbols for array subscript expressions
        Assignments dereferenceAssignments = Assignments.of(arraySubscripts, context.getSymbolAllocator());

        // Rewrite project node assignments using new symbols for array subscript expressions
        Map<Expression, Reference> mappings = HashBiMap.create(dereferenceAssignments.assignments())
                .inverse()
                .entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().toSymbolReference()));
        Assignments assignments = node.getAssignments().rewrite(expression -> replaceExpression(expression, mappings));

        return Result.ofPlanNode(
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                child.getSource(),
                                Assignments.builder()
                                        .putAll(child.getAssignments())
                                        .putAll(dereferenceAssignments)
                                        .build()),
                        assignments));
    }
}
