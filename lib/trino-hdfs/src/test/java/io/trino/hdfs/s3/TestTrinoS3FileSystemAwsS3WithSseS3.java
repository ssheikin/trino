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
package io.trino.hdfs.s3;

import org.apache.hadoop.conf.Configuration;

/**
 * Tests file system operations on AWS S3 storage.
 * <p>
 * Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 */
public class TestTrinoS3FileSystemAwsS3WithSseS3
        extends TestTrinoS3FileSystemAwsS3
{
    public TestTrinoS3FileSystemAwsS3WithSseS3()
    {
        super();
    }

    @Override
    protected Configuration s3Configuration()
    {
        Configuration configuration = super.s3Configuration();
        configuration.set("trino.s3.sse.type", "S3");
        configuration.set("trino.s3.sse.enabled", "true");
        configuration.set("trino.s3.streaming.part-size", String.valueOf(PART_SIZE));
        return configuration;
    }
}
