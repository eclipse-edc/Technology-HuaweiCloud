/*
 *  Copyright (c) 2024 Huawei Technologies
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Huawei Technologies - initial API and implementation
 *
 */

package com.huawei.cloud.tests;

import com.huawei.cloud.fixtures.HuaweiParticipant;
import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.ObsClientProviderImpl;
import com.huawei.cloud.obs.OtcTest;
import com.obs.services.ObsClient;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObject;
import org.eclipse.edc.connector.dataplane.spi.Endpoint;
import org.eclipse.edc.connector.dataplane.spi.iam.PublicEndpointGeneratorService;
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
        providerClient = providerRuntime.getService(ObsClientProvider.class).obsClient(OBS_OTC_CLOUD_URL);
        consumerClient = consumerRuntime.getService(ObsClientProvider.class).obsClient(OBS_OTC_CLOUD_URL);
        var providerClientProviderImp = (ObsClientProviderImpl) providerRuntime.getService(ObsClientProvider.class);
        providerClientProviderImp.getVault().storeSecret("publickey", PUBLIC_KEY);
        providerClientProviderImp.getVault().storeSecret("privatekey", PRIVATE_KEY);
        var consumerClientProviderImp = (ObsClientProviderImpl) consumerRuntime.getService(ObsClientProvider.class);
        consumerClientProviderImp.getVault().storeSecret("publickey", PUBLIC_KEY);
        consumerClientProviderImp.getVault().storeSecret("privatekey", PRIVATE_KEY);
        var providerEndpointGeneratorService=(PublicEndpointGeneratorService) providerRuntime.getService(PublicEndpointGeneratorService.class);
        var consumerEndpointGeneratorService=(PublicEndpointGeneratorService) consumerRuntime.getService(PublicEndpointGeneratorService.class);
        var endpoint = new Endpoint("endpoint", "obs");
        providerEndpointGeneratorService.addGeneratorFunction("HttpData", dataAddress1 -> endpoint);
        consumerEndpointGeneratorService.addGeneratorFunction("HttpData",dataAddress1 -> endpoint);
    }

    @Test
    void obsToObsTransfer() {
        var assetId = "file-asset-" + id;

        var prefix = "testfile";
        providerClient.createBucket(sourceBucket);
        var f = getFileFromResourceName(TESTFILE_NAME);
        providerClient.putObject(sourceBucket, TESTFILE_NAME, f);

        JsonArrayBuilder allowedSourceTypes = Json.createArrayBuilder();
        allowedSourceTypes.add(Json.createValue("OBS"));
        allowedSourceTypes.add(Json.createValue("HttpData"));
        JsonArrayBuilder allowedDestTypes = Json.createArrayBuilder();
        allowedDestTypes.add(Json.createValue("OBS"));
        allowedDestTypes.add(Json.createValue("HttpData"));
        JsonArrayBuilder allowedTransferTypes = Json.createArrayBuilder();
        allowedTransferTypes.add(Json.createValue("HttpData-PULL"));
        allowedTransferTypes.add(Json.createValue("HttpData-PUSH"));

        JsonObject dataPlaneRequestBody = Json.createObjectBuilder().add("@context", Json.createObjectBuilder().add("@vocab", "https://w3id.org/edc/v0.0.1/ns/"))
                .add("@id", "http-pull-provider-dataplane")
                .add("url", PROVIDER.getControlEndpoint().getUrl().toString().concat("/transfer"))
                .add("allowedSourceTypes", allowedSourceTypes.build())
                .add("allowedDestTypes", allowedDestTypes.build())
                .add("allowedTransferTypes", allowedTransferTypes)
                .add("properties", "").build();

        ValidatableResponse validatableResponse = PROVIDER.getManagementEndpoint().baseRequest().contentType(ContentType.JSON).body(dataPlaneRequestBody).when().post("/v2/dataplanes").then();
        createResourcesOnProvider(assetId, sourceAddress(sourceBucket, prefix));

        var transferType = "HttpData-PULL";
        var transferProcessId = CONSUMER.requestAsset(PROVIDER, assetId, noPrivateProperty(), obsSink(destBucket, prefix), transferType);

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

    private static final String PUBLIC_KEY = """
            -----BEGIN CERTIFICATE-----
            MIIDazCCAlOgAwIBAgIUZ3/sZXYzW4PjmOXKrZn6WBmUJ+4wDQYJKoZIhvcNAQEL
            BQAwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM
            GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0yMjAyMjMxNTA2MDNaFw0zMjAy
            MjExNTA2MDNaMEUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEw
            HwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEiMA0GCSqGSIb3DQEB
            AQUAA4IBDwAwggEKAoIBAQDBl6XaJnXTL+6DWip3aBhU+MzmY4d1V9hbTm1tiZ3g
            E0VbUrvGO3LoYaxpPv6zFmsg3uJv6JxVAde7EddidN0ITHB9cQNdAfdUJ5njmsGS
            PbdQuOQTHw0aG7/QvTI/nsvfEE6e0lbV/0e7DHacZT/+OztBH1RwkG2ymM94Hf8H
            I6x7q6yfRTAZOqeOMrPCYTcluAgE9NskoPvjX5qASakBtXISKIsOU84N0/2HDN3W
            EGMXvoHUQu6vrij6BwiwxKaw1AKwWENKoga775bPXN3M+JTSaIKE7dZbKzvx0Zi0
            h5X+bxc3BJi3Z/CsUBCzE+Y0SFetOiYmyl/2YmnneYoVAgMBAAGjUzBRMB0GA1Ud
            DgQWBBTvK1wVERwjni4B2vdH7KtEJeVWFzAfBgNVHSMEGDAWgBTvK1wVERwjni4B
            2vdH7KtEJeVWFzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBCwUAA4IBAQBn
            QHiPA7OBYukHd9gS7c0HXE+fsWcS3GZeLqcHfQQnV3pte1vTmu9//IVW71wNCJ1/
            rySRyODPQoPehxEcyHwupNZSzXK//nPlTdSgjMfFxscvt1YndyQLQYCfyOJMixAe
            Aqrb14GTFHUUrdor0PyElhkULjkOXUrSIsdBrfWrwLTkelE8NK3tb5ZG8KPzD9Jy
            +NwEPPr9d+iHkUkM7EFWw/cl56wka9ryBb97RI7DqbO6/j6OXHMk4GByxKv7DSIR
            IvF9/Dw20qytajtaHV0pluFcOBuFc0NfiDvCaQlbTsfjzbc6UmZWbOi9YOJl3VQ/
            g3h+15GuzbsSzOCOEYOT
            -----END CERTIFICATE-----
            """;

    private static final String PRIVATE_KEY = """
            -----BEGIN PRIVATE KEY-----
            MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDBl6XaJnXTL+6D
            Wip3aBhU+MzmY4d1V9hbTm1tiZ3gE0VbUrvGO3LoYaxpPv6zFmsg3uJv6JxVAde7
            EddidN0ITHB9cQNdAfdUJ5njmsGSPbdQuOQTHw0aG7/QvTI/nsvfEE6e0lbV/0e7
            DHacZT/+OztBH1RwkG2ymM94Hf8HI6x7q6yfRTAZOqeOMrPCYTcluAgE9NskoPvj
            X5qASakBtXISKIsOU84N0/2HDN3WEGMXvoHUQu6vrij6BwiwxKaw1AKwWENKoga7
            75bPXN3M+JTSaIKE7dZbKzvx0Zi0h5X+bxc3BJi3Z/CsUBCzE+Y0SFetOiYmyl/2
            YmnneYoVAgMBAAECggEBAJHXiN6bctAyn+DcoHlsNkhtVw+Jk5bXIutGXjHTJtiU
            K//siAGC78IZMyXmi0KndPVCdBwShROVW8xWWIiXuZxy2Zvm872xqX4Ah3JsN7/Q
            NrXdVBUDo38zwIGkxqIfIz9crZ4An+J/eq5zaTfRHzCLtswMqjRS2hFeBY5cKrBY
            4bkSDGTP/c5cP7xS/UwaiTR2Ptd41f4zTyd4l5rl30TYHpazQNlbdxcOV4jh2Rnp
            E0+cFEvEfeagVq7RmfBScKG5pk4qcRG0q2QHMyK5y00hdYvhdRjSgN7xIDkeO5B8
            s8/tSLU78nCl2gA9IKxTXYLitpISwZ81Q04mEAKRRtECgYEA+6lKnhn//aXerkLo
            ZOLOjWQZhh005jHdNxX7DZqLpTrrfxc8v15KWUkAK1H0QHqYvfPrbbsBV1MY1xXt
            sKmkeu/k8fJQzCIvFN4K2J5W5kMfq9PSw5d3XPeDaQuXUVaxBVp0gzPEPHmkKRbA
            AkUqY0oJwA9gMKf8dK+flmLZfbsCgYEAxO4Roj2G46/Oox1GEZGxdLpiMpr9rEdR
            JlSZ9kMGfddNLV7sFp6yPXDcyc/AOqeNj7tw1MyoT3Ar454+V0q83EZzCXvs4U6f
            jUrfFcoVWIwf9AV/J4KWzMIzfqPIeNwqymZKd6BrZgcXXvAEPWt27mwO4a1GhC4G
            oZv0t3lAsm8CgYAQ8C0IhSF4tgBN5Ez19VoHpDQflbmowLRt77nNCZjajyOokyzQ
            iI0ig0pSoBp7eITtTAyNfyew8/PZDi3IVTKv35OeQTv08VwP4H4EZGve5aetDf3C
            kmBDTpl2qYQOwnH5tUPgTMypcVp+NXzI6lTXB/WuCprjy3qvc96e5ZpT3wKBgQC8
            Xny/k9rTL/eYTwgXBiWYYjBL97VudUlKQOKEjNhIxwkrvQBXIrWbz7lh0Tcu49al
            BcaHxru4QLO6pkM7fGHq0fh3ufJ8EZjMrjF1xjdk26Q05o0aXe+hLKHVIRVBhlfo
            ArB4fRo+HcpdJXjox0KcDQCvHe+1v9DYBTWvymv4QQKBgBy3YH7hKz35DcXvA2r4
            Kis9a4ycuZqTXockO4rkcIwC6CJp9JbHDIRzig8HYOaRqmZ4a+coqLmddXr2uOF1
            7+iAxxG1KzdT6uFNd+e/j2cdUjnqcSmz49PRtdDswgyYhoDT+W4yVGNQ4VuKg6a3
            Z3pC+KTdoHSKeA2FyAGnSUpD
            -----END PRIVATE KEY-----
            """;

}
