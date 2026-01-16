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

package com.huaweicloud.provision.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huawei.cloud.provision.obs.ObsBucketDeprovisioner;
import com.huawei.cloud.provision.obs.ObsBucketProvisioner;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.ObsClient;
import com.obs.services.model.ObjectMetadata;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.boot.vault.InMemoryVault;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
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
    private final Vault vault = new InMemoryVault(mock(), null);
    private final TypeManager typeManager = new JacksonTypeManager();
    protected String bucketName;
    protected ObsBucketProvisioner provisioner;
    protected ObsBucketDeprovisioner deprovisioner;

    private ObsClient obsClient;

    @BeforeEach
    void setup() {
        provisioner = new ObsBucketProvisioner(provider, RetryPolicy.ofDefaults(), mock(Monitor.class), 900, vault, typeManager);
        deprovisioner = new ObsBucketDeprovisioner(RetryPolicy.ofDefaults(), provider, mock(Monitor.class), vault);
        bucketName = "obs-provisioner-itest-" + UUID.randomUUID();
        obsClient = getObsClient();
        var iamClient = getIamClient();
        when(provider.obsClient(any())).thenReturn(obsClient);
        when(provider.iamClient()).thenReturn(iamClient);

    }

    @Test
    void provision() throws ExecutionException, InterruptedException {
        var objectName = "objectname";
        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                        .property(ObsBucketSchema.BUCKET_NAME, bucketName)
                        .build())
                .flowId("test")
                .id(UUID.randomUUID().toString())
                .build();

        var result = provisioner.provision(definition);
        var newClient = getObsClient(typeManager.readValue(vault.resolveSecret(result.get().getContent().getDataAddress().getKeyName()), ObsSecretToken.class));
        putObject(newClient, bucketName, objectName);
        assertThat(result.get().succeeded()).isTrue();
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


        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                        .property(ObsBucketSchema.BUCKET_NAME, bucketName)
                        .build())
                .flowId("test")
                .id(UUID.randomUUID().toString())
                .build();

        var result = deprovisioner.deprovision(definition);

        assertThat(result.get().succeeded()).isTrue();

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

