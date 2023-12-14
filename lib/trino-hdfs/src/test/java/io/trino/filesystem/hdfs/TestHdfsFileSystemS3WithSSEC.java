
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
package io.trino.filesystem.hdfs;

import com.amazonaws.util.BinaryUtils;
import io.airlift.units.DataSize;
import io.trino.filesystem.AbstractTestTrinoFileSystem;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.hdfs.ConfigurationInitializer;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.TrinoHdfsFileSystemStats;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.hdfs.s3.HiveS3Config;
import io.trino.hdfs.s3.TrinoS3ConfigurationInitializer;
import io.trino.hdfs.s3.TrinoS3SseType;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Set;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsFileSystemS3WithSSEC
        extends AbstractTestTrinoFileSystem
{
    private static final String CUSTOMER_KEY = generateCustomerKey();

    private final String bucketName;
    private final String s3Endpoint;
    private final String accessKey;
    private final String secretKey;

    private HdfsEnvironment hdfsEnvironment;
    private HdfsContext hdfsContext;
    private TrinoFileSystem fileSystem;

    public TestHdfsFileSystemS3WithSSEC()
    {
        bucketName = environmentVariable("EMPTY_S3_BUCKET");
        s3Endpoint = environmentVariable("S3_BUCKET_ENDPOINT");
        accessKey = environmentVariable("AWS_ACCESS_KEY_ID");
        secretKey = environmentVariable("AWS_SECRET_ACCESS_KEY");
    }

    @BeforeAll
    void beforeAll()
    {
        HiveS3Config s3Config = new HiveS3Config()
                .setS3AwsAccessKey(accessKey)
                .setS3AwsSecretKey(secretKey)
                .setS3SseCustomerKey(CUSTOMER_KEY)
                .setS3SslEnabled(true)
                .setS3SseEnabled(true)
                .setS3SseType(TrinoS3SseType.CUSTOMER)
                .setS3Endpoint(s3Endpoint)
                .setS3PathStyleAccess(true)
                .setS3StreamingPartSize(DataSize.valueOf("5.5MB"));

        HdfsConfig hdfsConfig = new HdfsConfig();
        ConfigurationInitializer s3Initializer = new TrinoS3ConfigurationInitializer(s3Config);
        HdfsConfigurationInitializer initializer = new HdfsConfigurationInitializer(hdfsConfig, Set.of(s3Initializer));
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(initializer, emptySet());
        hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        hdfsContext = new HdfsContext(ConnectorIdentity.ofUser("test"));

        fileSystem = new HdfsFileSystem(hdfsEnvironment, hdfsContext, new TrinoHdfsFileSystemStats());
    }

    private static String environmentVariable(String name)
    {
        return requireNonNull(System.getenv(name), "Environment variable not set: " + name);
    }

    @AfterEach
    void afterEach()
            throws IOException
    {
        Path root = new Path(getRootLocation().toString());
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, root);
        for (FileStatus status : fs.listStatus(root)) {
            fs.delete(status.getPath(), true);
        }
    }

    @Override
    protected final boolean isHierarchical()
    {
        return false;
    }

    @Override
    protected TrinoFileSystem getFileSystem()
    {
        return fileSystem;
    }

    @Override
    protected Location getRootLocation()
    {
        return Location.of("s3://%s/".formatted(bucketName));
    }

    @Override
    protected final boolean supportsCreateExclusive()
    {
        return false;
    }

    @Override
    protected boolean normalizesListFilesResult()
    {
        return true;
    }

    @Override
    protected boolean seekPastEndOfFileFails()
    {
        return false;
    }

    @Override
    protected void verifyFileSystemIsEmpty()
    {
        try {
            Path root = new Path(getRootLocation().toString());
            FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, root);
            assertThat(fs.listStatus(root)).isEmpty();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected boolean isCreateExclusive()
    {
        return false;
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
