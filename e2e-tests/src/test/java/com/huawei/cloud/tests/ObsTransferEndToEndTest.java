package com.huawei.cloud.tests;

import com.huawei.cloud.fixtures.HuaweiParticipant;
import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.transfer.obs.TestFunctions;
import com.obs.services.ObsClient;
import io.restassured.http.ContentType;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.eclipse.edc.junit.extensions.EdcClassRuntimesExtension;
import org.eclipse.edc.junit.extensions.EdcRuntimeExtension;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.junit.testfixtures.TestUtils.getFileFromResourceName;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@Testcontainers
@EndToEndTest
public class ObsTransferEndToEndTest {
    public static final String MINIO_DOCKER_IMAGE = "bitnami/minio";
    public static final int MINIO_CONTAINER_PORT = 9000;
    public static final String TESTFILE_NAME = "testfile.json";
    protected static final HuaweiParticipant CONSUMER = HuaweiParticipant.Builder.newInstance()
            .name("consumer")
            .id("consumer")
            .apiKey("password")
            .build();
    protected static final HuaweiParticipant PROVIDER = HuaweiParticipant.Builder.newInstance()
            .name("provider")
            .id("provider")
            .apiKey("password")
            .build();
    private static final String CONSUMER_AK = "consumer-ak";
    private static final String CONSUMER_SK = "consumer-sk";
    private static final String PROVIDER_AK = "provider-ak";
    private static final String PROVIDER_SK = "provider-sk";
    @RegisterExtension
    static EdcClassRuntimesExtension runtimes = new EdcClassRuntimesExtension(
            new EdcRuntimeExtension(
                    ":launchers",
                    "consumer",
                    CONSUMER.controlPlaneConfiguration()
            ),
            new EdcRuntimeExtension(
                    ":launchers",
                    "provider",
                    PROVIDER.controlPlaneConfiguration()
            )
    );

    @Container
    private final GenericContainer<?> providerContainer = new GenericContainer<>(MINIO_DOCKER_IMAGE)
            .withEnv("MINIO_ROOT_USER", PROVIDER_AK)
            .withEnv("MINIO_ROOT_PASSWORD", PROVIDER_SK)
            .withExposedPorts(MINIO_CONTAINER_PORT);

    @Container
    private final GenericContainer<?> consumerContainer = new GenericContainer<>(MINIO_DOCKER_IMAGE)
            .withEnv("MINIO_ROOT_USER", CONSUMER_AK)
            .withEnv("MINIO_ROOT_PASSWORD", CONSUMER_SK)
            .withExposedPorts(MINIO_CONTAINER_PORT);
    private ObsClient providerClient;
    private ObsClient consumerClient;
    private String consumerEndpoint;
    private String providerEndpoint;

    @BeforeEach
    void setup() {
        consumerEndpoint = "http://localhost:%s".formatted(consumerContainer.getMappedPort(MINIO_CONTAINER_PORT));
        providerEndpoint = "http://localhost:%s".formatted(providerContainer.getMappedPort(MINIO_CONTAINER_PORT));

        providerClient = TestFunctions.createClient(PROVIDER_AK, PROVIDER_SK, providerEndpoint);
        consumerClient = TestFunctions.createClient(CONSUMER_AK, CONSUMER_SK, consumerEndpoint);
    }

    @Test
    void transfer_singleFile() throws IOException {
        var id = UUID.randomUUID().toString();
        var srcBucket = "src-" + id;
        var destBucket = "dest-" + id;
        providerClient.createBucket(srcBucket);
        var f = getFileFromResourceName(TESTFILE_NAME);
        providerClient.putObject(srcBucket, TESTFILE_NAME, f);
        var fileSize = Files.size(f.toPath());

        consumerClient.createBucket(destBucket);

        var flowRequest = createFlowRequest(destBucket, consumerEndpoint, srcBucket, TESTFILE_NAME, providerEndpoint).build();
        var url = PROVIDER.getControlEndpoint().getUrl().toString() + "/transfer";

        given().when()
                .baseUri(url)
                .contentType(ContentType.JSON)
                .body(flowRequest)
                .post()
                .then()
                .statusCode(200);

        await().pollInterval(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(consumerClient.listObjects(destBucket).getObjects())
                        .isNotEmpty()
                        .allSatisfy(obsObject -> assertThat(obsObject.getObjectKey()).isEqualTo(TESTFILE_NAME))
                        .allSatisfy(obsObject -> assertThat(obsObject.getMetadata().getContentLength()).isEqualTo(fileSize)));
    }

    @Test
    void transfer_multipleFilesWithPrefix() throws IOException {
        var id = UUID.randomUUID().toString();
        var srcBucket = "src-" + id;
        var destBucket = "dest-" + id;
        providerClient.createBucket(srcBucket);
        var f = getFileFromResourceName(TESTFILE_NAME);
        providerClient.putObject(srcBucket, "file1.json", f);
        providerClient.putObject(srcBucket, "file2.json", f);
        providerClient.putObject(srcBucket, "file3.json", f);
        var fileSize = Files.size(f.toPath());

        consumerClient.createBucket(destBucket);

        var flowRequest = createFlowRequest(destBucket, consumerEndpoint, srcBucket, "file", providerEndpoint).build();
        var url = PROVIDER.getControlEndpoint().getUrl().toString() + "/transfer";

        given().when()
                .baseUri(url)
                .contentType(ContentType.JSON)
                .body(flowRequest)
                .post()
                .then()
                .statusCode(200);

        await().pollInterval(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(consumerClient.listObjects(destBucket).getObjects())
                        .isNotEmpty()
                        .hasSize(3)
                        .allSatisfy(obsObject -> assertThat(obsObject.getObjectKey()).startsWith("file"))
                        .allSatisfy(obsObject -> assertThat(obsObject.getObjectKey()).endsWith(".json"))
                        .allSatisfy(obsObject -> assertThat(obsObject.getMetadata().getContentLength()).isEqualTo(fileSize)));
    }

    private DataFlowRequest.Builder createFlowRequest(String consumerBucket, String consumerEndpoint, String providerBucket, String providerObjectKey, String providerEndpoint) {
        return DataFlowRequest.Builder.newInstance()
                .id("test-request")
                .sourceDataAddress(DataAddress.Builder.newInstance()
                        .type(ObsBucketSchema.TYPE)
                        .keyName(TESTFILE_NAME)
                        .property(ObsBucketSchema.BUCKET_NAME, providerBucket)
                        .property(ObsBucketSchema.KEY_PREFIX, providerObjectKey)
                        .property(ObsBucketSchema.ACCESS_KEY_ID, PROVIDER_AK)
                        .property(ObsBucketSchema.SECRET_ACCESS_KEY, PROVIDER_SK)
                        .property(ObsBucketSchema.ENDPOINT, providerEndpoint)
                        .build()
                )
                .destinationDataAddress(DataAddress.Builder.newInstance()
                        .type(ObsBucketSchema.TYPE)
                        .keyName(TESTFILE_NAME)
                        .property(ObsBucketSchema.BUCKET_NAME, consumerBucket)
                        .property(ObsBucketSchema.ACCESS_KEY_ID, CONSUMER_AK)
                        .property(ObsBucketSchema.SECRET_ACCESS_KEY, CONSUMER_SK)
                        .property(ObsBucketSchema.ENDPOINT, consumerEndpoint)
                        .build()
                )
                .processId("test-process-id")
                .trackable(false);
    }

}
