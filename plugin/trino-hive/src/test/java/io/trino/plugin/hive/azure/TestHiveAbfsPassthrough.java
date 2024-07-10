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
package io.trino.plugin.hive.azure;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static io.trino.plugin.hive.containers.HiveHadoop.HIVE3_IMAGE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.containers.TestContainers.getPathFromClassPathResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class TestHiveAbfsPassthrough
        extends AbstractTestQueryFramework
{
    private static final String CATALOG_NAME = "hive";
    private static final String SCHEMA_NAME = "default";
    private static final String TEST_TABLE_NAME = "test_table";
    private static final String TESTED_SUBDIR = "testdir";

    private final String account;
    private final String accessKey;
    private final String clientSecret;
    private final String adlsDirectory;

    private HiveHadoop hiveHadoop;

    public TestHiveAbfsPassthrough()
    {
        String container = requireProperty("hive.hadoop2.azure-abfs-container");
        this.account = requireProperty("hive.hadoop2.azure-abfs-account");
        this.accessKey = requireProperty("hive.hadoop2.azure-abfs-access-key");
        this.clientSecret = requireProperty("hive.hadoop2.azure-abfs-client-secret");
        adlsDirectory = format("abfss://%s@%s.dfs.core.windows.net/test-%s", container, account, randomUUID());
    }

    private static String requireProperty(String variable)
    {
        return requireNonNull(System.getProperty(variable), "property variable not set: " + variable);
    }

    @AfterAll
    public final void tearDownHadoop()
    {
        if (adlsDirectory != null && hiveHadoop != null) {
            hiveHadoop.executeInContainerFailOnError("hadoop", "fs", "-rm", "-f", "-r", adlsDirectory);
        }
    }

    @Override
    protected final QueryRunner createQueryRunner()
            throws Exception
    {
        Path hadoopCoreSiteXmlTempFile = createHadoopCoreSiteXmlTempFileWithAbfsSettings();
        hiveHadoop = closeAfterClass(HiveHadoop.builder()
                .withNetwork(Network.newNetwork())
                .withImage(HIVE3_IMAGE)
                .withFilesToMount(ImmutableMap.of(
                        "/tmp/" + TESTED_SUBDIR, getPathFromClassPathResource("io/trino/plugin/hive/azure/testing/resources/data"),
                        "/etc/hadoop/conf/core-site.xml", hadoopCoreSiteXmlTempFile.toString()))
                .build());
        hiveHadoop.start();

        hiveHadoop.executeInContainerFailOnError("hadoop", "fs", "-mkdir", "-p", adlsDirectory);
        hiveHadoop.executeInContainerFailOnError("hadoop", "fs", "-copyFromLocal", "/tmp/" + TESTED_SUBDIR, adlsDirectory);
        hiveHadoop.executeInContainerFailOnError("/usr/bin/hive", "-e", format("\"CREATE EXTERNAL TABLE %s(t_bigint bigint) LOCATION '%s/%s'\"", TEST_TABLE_NAME, adlsDirectory, TESTED_SUBDIR));

        DistributedQueryRunner queryRunner = HiveQueryRunner.builder(AzureAdSupport.createDefaultUserSession())
                .setHiveProperties(ImmutableMap.<String, String>builder()
                        .put("hive.metastore", "thrift")
                        .put("hive.metastore.uri", hiveHadoop.getHiveMetastoreEndpoint().toString())
                        .put("fs.hadoop.enabled", "false")
                        .put("fs.native-azure.enabled", "true")
                        .put("azure.use-oauth-passthrough-token", "true")
                        .put("hive.security", "allow-all")
                        .buildOrThrow())
                .build();
        return queryRunner;
    }

    @Test
    public void testQuery()
    {
        assertQuery(
                format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TEST_TABLE_NAME),
                """
                        VALUES  3, 14, 15, -- test_table.csv
                         92, 65, 35, -- test_table.csv.gz
                         89, 79, 32, -- test_table.csv.bz2
                         38, 46, 26  -- test_table.csv.lz4""");
    }

    @Test
    public void testUsersSwitching()
            throws Exception
    {
        String sqlSelect = format("SELECT * FROM %s.%s.%s", CATALOG_NAME, SCHEMA_NAME, TEST_TABLE_NAME);

        assertQuerySucceeds(sqlSelect);
        assertQueryFails(nonAuthorizedUserSession(), sqlSelect, ".*[Ff]ailed to list directory.*");
        assertQueryFails(noTokenUserSession(), sqlSelect, "Unable to find Azure AD authentication token");
    }

    private Session nonAuthorizedUserSession()
            throws Exception
    {
        return AzureAdSupport.createAzureUserSession(
                "74ca121e-7246-4bd9-991c-25b39762926d",
                clientSecret,
                "https://starburstdata.com/74ca121e-7246-4bd9-991c-25b39762927d/.default");
    }

    private Session noTokenUserSession()
    {
        return testSessionBuilder().setIdentity(Identity.ofUser("user")).build();
    }

    private Path createHadoopCoreSiteXmlTempFileWithAbfsSettings()
            throws Exception
    {
        String abfsSpecificCoreSiteXmlContent = Resources.toString(Resources.getResource("conf/core-site.xml.abfs-template"), UTF_8)
                .replace("%ABFS_ACCESS_KEY%", accessKey)
                .replace("%ABFS_ACCOUNT%", account);

        FileAttribute<Set<PosixFilePermission>> posixFilePermissions = PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
        Path coreSiteXml = Files.createTempFile("core-site", ".xml", posixFilePermissions);
        Files.write(coreSiteXml, abfsSpecificCoreSiteXmlContent.getBytes(UTF_8));

        return coreSiteXml;
    }
}
