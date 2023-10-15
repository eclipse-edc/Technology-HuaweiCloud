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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.UUID;

public class ObsDataSourceOtcTest extends ObsDataSourceTestBase {
    public static final String OTC_CLOUD_URL = "https://obs.eu-de.otc.t-systems.com";
    public static final String BUCKET_NAME = "obs-sink-itest-" + UUID.randomUUID();
    private ObsClient obsClient;

    @NotNull
    @Override
    protected String createBucket(TestInfo testInfo) {
        if (!getClient().headBucket(BUCKET_NAME)) {
            getClient().createBucket(BUCKET_NAME);
        }
        return BUCKET_NAME;
    }

    @Override
    protected ObsClient getClient() {
        if (obsClient == null) {
            obsClient = TestFunctions.createClient(OTC_CLOUD_URL);
        }
        return obsClient;
    }

    @AfterEach
    void cleanup() {
        obsClient.listObjects(BUCKET_NAME)
                .getObjects()
                .forEach(obj -> getClient().deleteObject(BUCKET_NAME, obj.getObjectKey()));
        obsClient.deleteBucket(BUCKET_NAME);
        try {
            obsClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
