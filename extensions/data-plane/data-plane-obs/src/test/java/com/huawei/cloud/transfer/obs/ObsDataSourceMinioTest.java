/*
 *
 *   Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft
 *
 *   See the NOTICE file(s) distributed with this work for additional
 *   information regarding copyright ownership.
 *
 *   This program and the accompanying materials are made available under the
 *   terms of the Apache License, Version 2.0 which is available at
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 *   WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *   License for the specific language governing permissions and limitations
 *   under the License.
 *
 *   SPDX-License-Identifier: Apache-2.0
 *
 */

package com.huawei.cloud.transfer.obs;

import com.obs.services.ObsClient;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import java.lang.reflect.Method;
import java.util.UUID;

public class ObsDataSourceMinioTest extends ObsDataSourceTestBase {

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
    ObsClient getClient() {
        if (obsClient == null) {
            var url = "http://localhost:%s".formatted(minioContainer.getMappedPort(9000));
            obsClient = TestFunctions.createClient(USER, PASSWORD, url);
        }
        return obsClient;
    }

    @Override
    protected @NotNull String createBucket(TestInfo testInfo) {
        var bn = testInfo.getTestMethod().map(Method::getName).orElseGet(() -> UUID.randomUUID().toString())
                .replace("_", "-")
                .toLowerCase();
        getClient().createBucket(bn);
        return bn;
    }
}
