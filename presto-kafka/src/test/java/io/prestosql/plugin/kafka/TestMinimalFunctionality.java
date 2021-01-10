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
package io.prestosql.plugin.kafka;

import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.kafka.BasicTestingKafka;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.Test;

import java.util.stream.LongStream;

import static io.prestosql.plugin.kafka.util.TestUtils.createEmptyTopicDescription;
import static java.util.UUID.randomUUID;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
        extends AbstractTestQueryFramework
{
    private BasicTestingKafka testingKafka;
    private String topicName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        testingKafka = closeAfterClass(new BasicTestingKafka());
        topicName = "test_" + randomUUID().toString().replaceAll("-", "_");
        QueryRunner queryRunner = KafkaQueryRunner.builder(testingKafka)
                .setExtraTopicDescription(ImmutableMap.<SchemaTableName, KafkaTopicDescription>builder()
                        .put(createEmptyTopicDescription(topicName, new SchemaTableName("default", topicName)))
                        .build())
                .setExtraKafkaProperties(ImmutableMap.<String, String>builder()
                        .put("kafka.messages-per-split", "100")
                        .build())
                .build();
        return queryRunner;
    }

    @Test
    public void testTopicExists()
    {
        assertTrue(getQueryRunner().listTables(getSession(), "kafka", "default").contains(QualifiedObjectName.valueOf("kafka.default." + topicName)));
    }

    @Test
    public void testTopicHasData()
    {
        assertQuery("SELECT count(*) FROM default." + topicName, "VALUES 0");

        testingKafka.sendMessages(LongStream.range(0, 100000)
                .mapToObj(id -> new ProducerRecord<>(topicName, id, ImmutableMap.of("id", Long.toString(id), "value", randomUUID().toString()))));

        assertQuery("SELECT count(*) FROM default." + topicName, "VALUES 100000L");
    }
}
