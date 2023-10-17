package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.EnvironmentVariableObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import org.eclipse.edc.spi.types.domain.DataAddress;

public class TestFunctions {
    public static final String VALID_ENDPOINT = "https://foo.bar";
    public static final String VALID_BUCKET_NAME = "validBucketName";
    public static final String VALID_ACCESS_KEY_ID = "validAccessKeyId";
    public static final String VALID_SECRET_ACCESS_KEY = "validSecretAccessKey";

    public static ObsClient createClient(String endpoint) {
        var config = createConfig(endpoint);
        // OBS_ACCESS_KEY_ID + OBS_SECRET_ACCESS_KEY
        return new ObsClient(new EnvironmentVariableObsCredentialsProvider(), config);
    }

    public static ObsClient createClient(String ak, String sk, String endpoint) {
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

    public static DataAddress dataAddressWithoutCredentials() {
        return DataAddress.Builder.newInstance()
                .type(ObsBucketSchema.TYPE)
                .keyName("aKey")
                .property(ObsBucketSchema.BUCKET_NAME, VALID_BUCKET_NAME)
                .property(ObsBucketSchema.ENDPOINT, VALID_ENDPOINT)
                .build();
    }

    public static DataAddress dataAddressWithCredentials() {
        return DataAddress.Builder.newInstance()
                .type(ObsBucketSchema.TYPE)
                .keyName("aKey")
                .property(ObsBucketSchema.BUCKET_NAME, VALID_BUCKET_NAME)
                .property(ObsBucketSchema.ENDPOINT, VALID_ENDPOINT)
                .property(ObsBucketSchema.ACCESS_KEY_ID, VALID_ACCESS_KEY_ID)
                .property(ObsBucketSchema.SECRET_ACCESS_KEY, VALID_SECRET_ACCESS_KEY)
                .build();
    }
}
