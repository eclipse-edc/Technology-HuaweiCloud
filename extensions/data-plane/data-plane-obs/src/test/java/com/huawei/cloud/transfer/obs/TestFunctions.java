package com.huawei.cloud.transfer.obs;

import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.EnvironmentVariableObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import org.testcontainers.shaded.org.checkerframework.checker.nullness.qual.Nullable;

public class TestFunctions {
    public static ObsClient createClient(@Nullable String endpoint) {
        var config = createConfig(endpoint);
        // OBS_ACCESS_KEY_ID + OBS_SECRET_ACCESS_KEY
        return new ObsClient(new EnvironmentVariableObsCredentialsProvider(), config);
    }

    public static ObsClient createClient(String ak, String sk, @Nullable String endpoint) {
        var config = createConfig(endpoint);

        return new ObsClient(new BasicObsCredentialsProvider(ak, sk), config);
    }

    public static ObsConfiguration createConfig(String endpoint) {
        var config = new ObsConfiguration();
        config.setEndPoint(endpoint);
        config.setMaxErrorRetry(3);
        config.setConnectionTimeout(30000);
        config.setPathStyle(true); //otherwise the bucketname gets prepended
        return config;
    }
}
