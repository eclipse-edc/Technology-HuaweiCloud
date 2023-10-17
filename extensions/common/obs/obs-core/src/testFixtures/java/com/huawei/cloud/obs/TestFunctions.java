package com.huawei.cloud.obs;

import com.huaweicloud.sdk.core.auth.GlobalCredentials;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.EnvironmentVariableObsCredentialsProvider;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.ArrayList;
import java.util.List;

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

    public static ObsClient createClient(ObsSecretToken secretToken, String endpoint) {
        var config = createConfig(endpoint);

        return new ObsClient(new BasicObsCredentialsProvider(secretToken.ak(), secretToken.sk(), secretToken.securityToken()), config);
    }

    public static ObsConfiguration createConfig(String endpoint) {
        var config = new ObsConfiguration();
        config.setEndPoint(endpoint);
        config.setMaxErrorRetry(3);
        config.setConnectionTimeout(30000);
        config.setPathStyle(true); //otherwise the bucketname gets prepended
        return config;
    }

    public static IamClient createIamClient(String ak, String sk, String endpoint) {
        var endpoints = new ArrayList<>(List.of(endpoint));
        return IamClient.newBuilder().withEndpoints(endpoints).withCredential(new GlobalCredentials().withAk(ak).withSk(sk)).build();
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
