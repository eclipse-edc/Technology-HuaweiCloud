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

package com.huawei.cloud.provision.obs;

import com.huawei.cloud.obs.ObsSecretToken;
import com.huawei.cloud.obs.TestFunctions;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.obs.services.ObsClient;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

@Testcontainers
@EndToEndTest
public class ObsProvisionerMinioTest extends ObsProvisionerTestBase {

    private static final String MINIO_DOCKER_IMAGE = "bitnami/minio";
    private static final String USER = "USER";
    private static final String PASSWORD = "PASSWORD";

    @Container
    private final GenericContainer<?> minioContainer = new GenericContainer<>(MINIO_DOCKER_IMAGE)
            .withEnv("MINIO_ROOT_USER", USER)
            .withEnv("MINIO_ROOT_PASSWORD", PASSWORD)
            .withExposedPorts(9000);
    private ObsClient obsClient;

    @Override
    protected ObsClient getObsClient() {
        if (obsClient == null) {
            obsClient = getObsClient(USER, PASSWORD);
        }
        return obsClient;
    }

    @Override
    protected IamClient getIamClient() {
        var client = TestFunctions.createIamClient(USER, PASSWORD, url());
        client = spy(client);

        var credential = new Credential()
                .withAccess(USER)
                .withSecret(PASSWORD)
                .withExpiresAt("2016-08-16T15:23:01Z")
                .withSecuritytoken("TOKEN");

        doReturn(new CreateTemporaryAccessKeyByTokenResponse().withCredential(credential))
                .when(client)
                .createTemporaryAccessKeyByToken(any());
        return client;
    }

    @Override
    protected ObsClient getObsClient(ObsSecretToken token) {
        return getObsClient(token.ak(), token.sk());
    }

    protected ObsClient getObsClient(String ak, String sk) {
        if (obsClient == null) {
            obsClient = TestFunctions.createClient(ak, sk, url());
        }
        return obsClient;
    }

    private String url() {
        return "http://localhost:%s".formatted(minioContainer.getMappedPort(9000));
    }
}
