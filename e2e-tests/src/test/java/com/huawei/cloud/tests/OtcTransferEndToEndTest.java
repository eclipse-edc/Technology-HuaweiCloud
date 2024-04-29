package com.huawei.cloud.tests;

import com.huawei.cloud.fixtures.HuaweiParticipant;
import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.OtcTest;
import com.obs.services.ObsClient;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import org.eclipse.edc.junit.extensions.EdcClassRuntimesExtension;
import org.eclipse.edc.junit.extensions.EdcRuntimeExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.edc.connector.controlplane.test.system.utils.PolicyFixtures.inForceDatePolicy;
import static org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates.COMPLETED;
import static org.eclipse.edc.jsonld.spi.JsonLdKeywords.TYPE;
import static org.eclipse.edc.junit.testfixtures.TestUtils.getFileFromResourceName;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;


@OtcTest
public class OtcTransferEndToEndTest {

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

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String OBS_OTC_CLOUD_URL = "https://obs.eu-de.otc.t-systems.com";


    static EdcRuntimeExtension providerRuntime = new EdcRuntimeExtension(
            ":launchers:e2e-test",
            "consumer",
            CONSUMER.controlPlaneConfiguration()
    );

    static EdcRuntimeExtension consumerRuntime = new EdcRuntimeExtension(
            ":launchers:e2e-test",
            "provider",
            PROVIDER.controlPlaneConfiguration()
    );

    @RegisterExtension
    static EdcClassRuntimesExtension runtimes = new EdcClassRuntimesExtension(
            providerRuntime,
            consumerRuntime
    );

    private String id;
    private String sourceBucket;
    private String destBucket;
    private ObsClient providerClient;
    private ObsClient consumerClient;

    @BeforeEach
    void setup() {
        id = UUID.randomUUID().toString();
        sourceBucket = "src-" + id;
        destBucket = "dest-" + id;
        providerClient = providerRuntime.getContext().getService(ObsClientProvider.class).obsClient(OBS_OTC_CLOUD_URL);
        consumerClient = consumerRuntime.getContext().getService(ObsClientProvider.class).obsClient(OBS_OTC_CLOUD_URL);
    }

    @Test
    void obsToObsTransfer() {
        var assetId = "file-asset-" + id;

        var prefix = "testfile";
        providerClient.createBucket(sourceBucket);
        var f = getFileFromResourceName(TESTFILE_NAME);
        providerClient.putObject(sourceBucket, TESTFILE_NAME, f);

        createResourcesOnProvider(assetId, sourceAddress(sourceBucket, prefix));

        var transferProcessId = CONSUMER.requestAsset(PROVIDER, assetId, noPrivateProperty(), obsSink(destBucket, prefix));

        await().atMost(TIMEOUT).untilAsserted(() -> {
            var state = CONSUMER.getTransferProcessState(transferProcessId);
            assertThat(state).isEqualTo(COMPLETED.name());
        });

        assertThat(consumerClient.listObjects(destBucket).getObjects())
                .isNotEmpty()
                .allSatisfy(obsObject -> assertThat(obsObject.getObjectKey()).isEqualTo(TESTFILE_NAME));

    }

    @AfterEach
    void cleanup() {
        cleanResource(providerClient, sourceBucket);
        cleanResource(consumerClient, destBucket);
    }


    void cleanResource(ObsClient obsClient, String bucketName) {
        if (obsClient.headBucket(bucketName)) {
            obsClient.listObjects(bucketName).getObjects()
                    .forEach(obj -> obsClient.deleteObject(bucketName, obj.getObjectKey()));
            obsClient.deleteBucket(bucketName);
        }
    }

    private JsonObject noPrivateProperty() {
        return Json.createObjectBuilder().build();
    }

    private JsonObject obsSink(String providerBucket, String providerObjectKey) {
        return Json.createObjectBuilder()
                .add(TYPE, EDC_NAMESPACE + "DataAddress")
                .add(EDC_NAMESPACE + "type", ObsBucketSchema.TYPE)
                .add(EDC_NAMESPACE + "properties", Json.createObjectBuilder()
                        .add(ObsBucketSchema.ENDPOINT, OBS_OTC_CLOUD_URL)
                        .add(ObsBucketSchema.KEY_PREFIX, providerObjectKey)
                        .add(ObsBucketSchema.BUCKET_NAME, providerBucket)
                        .build())
                .build();
    }

    private Map<String, Object> sourceAddress(String providerBucket, String providerObjectKey) {
        return Map.of(
                "type", ObsBucketSchema.TYPE,
                "keyName", TESTFILE_NAME,
                ObsBucketSchema.BUCKET_NAME, providerBucket,
                ObsBucketSchema.KEY_PREFIX, providerObjectKey,
                ObsBucketSchema.ENDPOINT, OBS_OTC_CLOUD_URL);
    }

    private void createResourcesOnProvider(String assetId, Map<String, Object> dataAddressProperties) {
        PROVIDER.createAsset(assetId, Map.of("description", "description"), dataAddressProperties);
        var policy = inForceDatePolicy("gteq", "contractAgreement+0s", "lteq", "contractAgreement+100s");
        var policyDefinition = PROVIDER.createPolicyDefinition(policy);
        PROVIDER.createContractDefinition(assetId, UUID.randomUUID().toString(), policyDefinition, policyDefinition);
    }

}
