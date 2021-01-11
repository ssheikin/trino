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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.prestosql.RowPagesBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.SortOrder;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.type.BlockTypeOperators;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static io.prestosql.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.TopNRankingOperator.TopNRankingOperatorFactory;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestTopNRankingOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private JoinCompiler joinCompiler;
    private TypeOperators typeOperators = new TypeOperators();
    private BlockTypeOperators blockTypeOperators = new BlockTypeOperators(typeOperators);

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed(getClass().getSimpleName() + "-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        joinCompiler = new JoinCompiler(typeOperators);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider
    public Object[][] partial()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testPartitioned(boolean hashEnabled)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), VARCHAR, DOUBLE);
        List<Page> input = rowPagesBuilder
                .row("a", 0.3)
                .row("b", 0.2)
                .row("c", 0.1)
                .row("c", 0.91)
                .pageBreak()
                .row("a", 0.4)
                .pageBreak()
                .row("a", 0.5)
                .row("a", 0.6)
                .row("b", 0.7)
                .row("b", 0.8)
                .pageBreak()
                .row("b", 0.9)
                .build();

        TopNRankingOperatorFactory operatorFactory = new TopNRankingOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(VARCHAR),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                typeOperators,
                blockTypeOperators);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, VARCHAR, BIGINT)
                .row(0.3, "a", 1L)
                .row(0.4, "a", 2L)
                .row(0.5, "a", 3L)
                .row(0.2, "b", 1L)
                .row(0.7, "b", 2L)
                .row(0.8, "b", 3L)
                .row(0.1, "c", 1L)
                .row(0.91, "c", 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(dataProvider = "partial")
    public void testUnPartitioned(boolean partial)
    {
        List<Page> input = rowPagesBuilder(VARCHAR, DOUBLE)
                .row("a", 0.3)
                .row("b", 0.2)
                .row("c", 0.1)
                .row("c", 0.91)
                .pageBreak()
                .row("a", 0.4)
                .pageBreak()
                .row("a", 0.5)
                .row("a", 0.6)
                .row("b", 0.7)
                .row("b", 0.8)
                .pageBreak()
                .row("b", 0.9)
                .build();

        TopNRankingOperatorFactory operatorFactory = new TopNRankingOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(),
                ImmutableList.of(),
                Ints.asList(1),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                partial,
                Optional.empty(),
                10,
                joinCompiler,
                typeOperators,
                blockTypeOperators);

        MaterializedResult expected;
        if (partial) {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, VARCHAR)
                    .row(0.1, "c")
                    .row(0.2, "b")
                    .row(0.3, "a")
                    .build();
        }
        else {
            expected = resultBuilder(driverContext.getSession(), DOUBLE, VARCHAR, BIGINT)
                    .row(0.1, "c", 1L)
                    .row(0.2, "b", 2L)
                    .row(0.3, "a", 3L)
                    .build();
        }

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMemoryReservationYield()
    {
        Type type = BIGINT;
        List<Page> input = createPagesWithDistinctHashKeys(type, 1_000, 500);

        OperatorFactory operatorFactory = new TopNRankingOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type),
                ImmutableList.of(0),
                ImmutableList.of(0),
                ImmutableList.of(type),
                Ints.asList(0),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                3,
                false,
                Optional.empty(),
                10,
                joinCompiler,
                typeOperators,
                blockTypeOperators);

        // get result with yield; pick a relatively small buffer for heaps
        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(
                input,
                type,
                operatorFactory,
                operator -> ((TopNRankingOperator) operator).getCapacity(),
                1_000_000);
        assertGreaterThan(result.getYieldCount(), 3);
        assertGreaterThan(result.getMaxReservedBytes(), 5L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            assertEquals(page.getChannelCount(), 2);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(page.getBlock(1).getByte(i, 0), 1);
                count++;
            }
        }
        assertEquals(count, 1_000 * 500);
    }
}
