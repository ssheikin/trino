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
import io.trino.sql.ir.Call;
import io.trino.sql.ir.DefaultTraversalVisitor;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Lambda;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.Symbol;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.SymbolsExtractor.extractAll;
import static io.trino.type.BooleanOperators.not;

/**
 * Provides helper methods to push down array subscript in the query plan.
 */
final class ArraySubscriptPushdownUtil
{
    private static final String ARRAY_SUBSCRIPT_OPERATOR = "$operator$subscript";

    private ArraySubscriptPushdownUtil() {}

    public static Set<Call> extractArraySubscripts(Collection<Expression> expressions, boolean allowOverlap)
    {
        Set<Expression> symbolReferencesAndArraySubscripts = expressions.stream()
                .flatMap(expression -> getSymbolReferencesAndArraySubscripts(expression).stream())
                .collect(toImmutableSet());

        // Remove overlap if required
        Set<Expression> candidateExpressions = symbolReferencesAndArraySubscripts;
        if (!allowOverlap) {
            candidateExpressions = symbolReferencesAndArraySubscripts.stream()
                    .filter(expression -> not(prefixExists(expression, symbolReferencesAndArraySubscripts)))
                    .collect(toImmutableSet());
        }

        // Retain row subscript expressions
        return candidateExpressions.stream()
                .filter(expression -> expression instanceof Call)
                .map(Call.class::cast)
                .filter(ArraySubscriptPushdownUtil::isArraySubscriptOperator)
                .collect(toImmutableSet());
    }

    public static Symbol getArrayBase(Expression expression)
    {
        return extractAll(expression).getFirst();
    }

    private static List<Expression> getSymbolReferencesAndArraySubscripts(Expression expression)
    {
        ImmutableList.Builder<Expression> builder = ImmutableList.builder();

        new DefaultTraversalVisitor<ImmutableList.Builder<Expression>>()
        {
            @Override
            protected Void visitCall(Call node, ImmutableList.Builder<Expression> context)
            {
                if (isArraySubscriptChain(node)) {
                    context.add(node);
                    return null;
                }
                return super.visitCall(node, context);
            }

            @Override
            protected Void visitReference(Reference node, ImmutableList.Builder<Expression> context)
            {
                context.add(node);
                return null;
            }

            // Avoid extracting subscript when applied as a part of lambda expression
            @Override
            protected Void visitLambda(Lambda node, ImmutableList.Builder<Expression> context)
            {
                return null;
            }
        }.process(expression, builder);

        return builder.build();
    }

    public static boolean isArraySubscriptChain(Expression expression)
    {
        return expression instanceof Call call
                && isArraySubscriptOperator(call)
                && (call.arguments().getFirst() instanceof Reference || isArraySubscriptChain(call.arguments().getFirst()));
    }

    private static boolean prefixExists(Expression expression, Set<Expression> expressions)
    {
        Expression current = expression;

        while (current instanceof Call call && isArraySubscriptOperator(call)) {
            current = call.arguments().getFirst();
            if (expressions.contains(current)) {
                return true;
            }
        }

        verify(current instanceof Reference);
        return false;
    }

    private static boolean isArraySubscriptOperator(Call call)
    {
        return call.function().name().functionName().equals(ARRAY_SUBSCRIPT_OPERATOR);
    }
}
