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
package io.trino.plugin.warp.log;

import io.airlift.log.Logger;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.spi.catalog.CatalogName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShapingLoggerTest
{
    private final String catalogName = "c";
    private final ShapingLoggerFactory shapingLoggerFactory = new ShapingLoggerFactory(new CatalogName(catalogName), new SharedConfig());
    private Logger logger;

    @BeforeEach
    public void beforeEach()
    {
        logger = mock(Logger.class);
    }

    @Test
    public void testSimple()
    {
        ShapingLogger shapingLogger = shapingLoggerFactory.getInstance(this.getClass(), logger, 2, Duration.ZERO, 1, ShapingLogger.MODE.FORMAT);

        shapingLogger.info("%s", "test");
        verify(logger, never()).info(eq("test"));

        when(logger.isInfoEnabled()).thenReturn(true);
        shapingLogger.info("%s", "test");
        shapingLogger.info("%s", "test");
        verify(logger, times(1))
                .info(eq("catalog[%s]: %s".formatted(catalogName, "test")));
        verify(logger, times(1))
                .info(eq("catalog[%s]: %s - skipped 1 times"));
    }
}
