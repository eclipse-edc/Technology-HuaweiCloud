package com.huawei.cloud.transfer.obs;

import com.obs.services.ObsClient;
import org.eclipse.edc.junit.annotations.EndToEndTest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInfo;

import java.util.UUID;

@EndToEndTest
class ObsDataSinkOtcTest extends ObsDataSinkTestBase {

    public static final String OTC_CLOUD_URL = "https://obs.eu-de.otc.t-systems.com";
    public static final String BUCKET_NAME = "obs-sink-itest-" + UUID.randomUUID();
    private ObsClient obsClient;

    @NotNull
    @Override
    protected String createBucket(TestInfo testInfo) {
        if (!getObsClient().headBucket(BUCKET_NAME)) {
            getObsClient().createBucket(BUCKET_NAME);
        }
        return BUCKET_NAME;
    }

    @Override
    protected ObsClient getObsClient() {
        if (obsClient == null) {
            obsClient = TestFunctions.createClient(OTC_CLOUD_URL);
        }
        return obsClient;
    }

    @AfterEach
    void cleanup() {
        getObsClient().listObjects(BUCKET_NAME).getObjects()
                .forEach(obj -> getObsClient().deleteObject(BUCKET_NAME, obj.getObjectKey()));
        getObsClient().deleteBucket(BUCKET_NAME);
    }
}
