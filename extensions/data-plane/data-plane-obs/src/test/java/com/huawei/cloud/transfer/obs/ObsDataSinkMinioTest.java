package com.huawei.cloud.transfer.obs;

import com.obs.services.ObsClient;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.lang.reflect.Method;
import java.util.UUID;

@Testcontainers
@EndToEndTest
public class ObsDataSinkMinioTest extends ObsDataSinkTestBase {
    public static final String MINIO_DOCKER_IMAGE = "bitnami/minio";
    public static final String USER = "USER";
    public static final String PASSWORD = "PASSWORD";

    @Container
    private final GenericContainer<?> minioContainer = new GenericContainer<>(MINIO_DOCKER_IMAGE)
            .withEnv("MINIO_ROOT_USER", USER)
            .withEnv("MINIO_ROOT_PASSWORD", PASSWORD)
            .withExposedPorts(9000);
    private ObsClient obsClient;


    @Override
    protected @NotNull String createBucket(TestInfo testInfo) {
        var bn = testInfo.getTestMethod().map(Method::getName).orElseGet(() -> UUID.randomUUID().toString())
                .replace("_", "-")
                .toLowerCase();
        getObsClient().createBucket(bn);
        return bn;
    }

    @Override
    protected ObsClient getObsClient() {
        if (obsClient == null) {
            var url = "http://localhost:%s".formatted(minioContainer.getMappedPort(9000));
            obsClient = TestFunctions.createClient(USER, PASSWORD, url);
        }
        return obsClient;
    }


}
