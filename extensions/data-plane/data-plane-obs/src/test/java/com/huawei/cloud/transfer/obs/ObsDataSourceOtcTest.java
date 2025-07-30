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

package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.OtcTest;
import com.huawei.cloud.obs.TestFunctions;
import com.obs.services.ObsClient;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.util.UUID;

@OtcTest
@Disabled("only for release 0.13.2")
class ObsDataSourceOtcTest extends ObsDataSourceTestBase {
    public static final String OTC_CLOUD_URL = "https://obs.eu-de.otc.t-systems.com";
    public static final String BUCKET_NAME = "obs-sink-itest-" + UUID.randomUUID();
    private ObsClient obsClient;

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

    @Override
    protected ObsClient getClient() {
        if (obsClient == null) {
            obsClient = TestFunctions.createClient(OTC_CLOUD_URL);
        }
        return obsClient;
    }

    @NotNull
    @Override
    protected String createBucket(TestInfo testInfo) {
        if (!getClient().headBucket(BUCKET_NAME)) {
            getClient().createBucket(BUCKET_NAME);
        }
        return BUCKET_NAME;
    }
}
