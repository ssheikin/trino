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

import com.amazonaws.util.BinaryUtils;
import org.apache.hadoop.conf.Configuration;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;

import static java.util.Objects.requireNonNull;

/**
 * Tests file system operations on AWS S3 storage.
 * <p>
 * Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 */
public class TestTrinoS3FileSystemAwsS3WithSSEC
        extends AbstractTestTrinoS3FileSystem
{
    private static final String CUSTOMER_KEY = generateCustomerKey();
    private final String bucketName;
    private final String s3Endpoint;

    public TestTrinoS3FileSystemAwsS3WithSSEC()
    {
        super();
        bucketName = requireNonNull(System.getenv("S3_BUCKET"), "Environment S3_BUCKET was not set");
        s3Endpoint = requireNonNull(System.getenv("S3_BUCKET_ENDPOINT"), "Environment S3_BUCKET_ENDPOINT was not set");
    }

    @Override
    protected String getBucketName()
    {
        return bucketName;
    }

    @Override
    protected Configuration s3Configuration()
    {
        Configuration configuration = new Configuration(false);
        configuration.set("fs.s3.endpoint", s3Endpoint);
        configuration.set("trino.s3.sse.type", "CUSTOMER");
        configuration.set("trino.s3.sse.customer-key", CUSTOMER_KEY);
        configuration.set("trino.s3.sse.enabled", "true");
        return configuration;
    }

    private static String generateCustomerKey()
    {
        try {
            KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
            keyGenerator.init(256, new SecureRandom());
            SecretKey secretKey = keyGenerator.generateKey();
            return BinaryUtils.toBase64(secretKey.getEncoded());
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
