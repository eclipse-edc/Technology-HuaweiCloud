package com.huawei.cloud.provision.obs;

import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.ObsClient;
import com.obs.services.model.ObjectMetadata;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.policy.model.Policy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public abstract class ObsProvisionerTestBase {

    private final ObsClientProvider provider = mock();
    protected String bucketName;
    protected ObsProvisioner provisioner;

    private ObsClient obsClient;

    @BeforeEach
    void setup() {
        provisioner = new ObsProvisioner(provider, RetryPolicy.ofDefaults(), mock(), 900);
        bucketName = "obs-provisioner-itest-" + UUID.randomUUID();
        obsClient = getObsClient();
        var iamClient = getIamClient();
        when(provider.obsClient(any())).thenReturn(obsClient);
        when(provider.iamClient()).thenReturn(iamClient);

    }

    @Test
    void provision() throws ExecutionException, InterruptedException {
        var endpoint = "http://endpoint";
        var objectName = "objectname";
        var definition = ObsResourceDefinition.Builder.newInstance()
                .endpoint(endpoint)
                .bucketName(bucketName)
                .transferProcessId(UUID.randomUUID().toString())
                .id(UUID.randomUUID().toString())
                .build();

        var result = provisioner.provision(definition, Policy.Builder.newInstance().build()).get();

        assertThat(result.succeeded()).isTrue();

        var newClient = getObsClient((ObsSecretToken) result.getContent().getSecretToken());


        putObject(newClient, bucketName, objectName);


        var objects = obsClient.listObjects(bucketName);
        assertThat(objects.getBucketName()).isEqualTo(bucketName);

        assertThat(objects.getObjects()).hasSize(1).satisfiesOnlyOnce(obsObject -> {
            assertThat(obsObject.getObjectKey()).isEqualTo(objectName);
        });

    }

    @Test
    void deprovision() throws ExecutionException, InterruptedException {
        var objectName = "objectname";

        obsClient.createBucket(bucketName);

        putObject(obsClient, bucketName, objectName);


        var definition = ObsProvisionedResource.Builder.newInstance()
                .bucketName(bucketName)
                .endpoint("http://test")
                .transferProcessId(UUID.randomUUID().toString())
                .resourceDefinitionId(UUID.randomUUID().toString())
                .resourceName("resource")
                .id(UUID.randomUUID().toString())
                .build();

        var result = provisioner.deprovision(definition, Policy.Builder.newInstance().build()).get();

        assertThat(result.succeeded()).isTrue();

        assertThat(obsClient.headBucket(bucketName)).isFalse();

    }

    @AfterEach
    void cleanup() {
        if (obsClient.headBucket(bucketName)) {
            obsClient.listObjects(bucketName).getObjects()
                    .forEach(obj -> obsClient.deleteObject(bucketName, obj.getObjectKey()));
            obsClient.deleteBucket(bucketName);
        }
    }

    protected abstract ObsClient getObsClient();

    protected abstract ObsClient getObsClient(ObsSecretToken token);

    protected abstract IamClient getIamClient();

    protected void putObject(ObsClient client, String bucketName, String objectName) {
        putObject(client, bucketName, objectName, "Hello EDC");
    }

    protected void putObject(ObsClient newClient, String bucketName, String objectName, String contentString) {
        var content = contentString.getBytes();

        var metadata = new ObjectMetadata();
        metadata.setContentLength((long) content.length);

        newClient.putObject(bucketName, objectName, new ByteArrayInputStream(content), metadata);
    }

}
