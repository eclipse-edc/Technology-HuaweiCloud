/*
 *  Copyright (c) 2020, 2021 Microsoft Corporation
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Microsoft Corporation - initial API and implementation
 *
 */

package com.huawei.cloud.provision.obs;

import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.CreateBucketRequest;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.HeaderResponse;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsBucket;
import com.obs.services.model.ObsObject;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObsProvisionerTest {

    private final IamClient iamClient = mock(IamClient.class);
    private final ObsClient s3Client = mock(ObsClient.class);
    private final ObsClientProvider clientProvider = mock(ObsClientProvider.class);
    private ObsProvisioner provisioner;

    @BeforeEach
    void setUp() {
        when(clientProvider.iamClient()).thenReturn(iamClient);
        when(clientProvider.obsClient(anyString())).thenReturn(s3Client);

        provisioner = new ObsProvisioner(clientProvider, RetryPolicy.ofDefaults(), mock(Monitor.class), 900);
    }

    @Test
    void verify_basic_provision() {

        var bucket = "test";
        var endpoint = "http://endpoint";
        var credentials = new Credential()
                .withAccess("accessKeyId").withSecret("secretAccessKey").withSecuritytoken("sessionToken")
                .withExpiresAt(Instant.now().toString());


        var createBucketResponse = new ObsBucket();
        var tokenResponse = new CreateTemporaryAccessKeyByTokenResponse().withCredential(credentials);
        var createBucketRequest = new CreateBucketRequest(bucket);

        when(s3Client.createBucket(createBucketRequest)).thenReturn(createBucketResponse);
        when(iamClient.createTemporaryAccessKeyByToken(any())).thenReturn(tokenResponse);

        var definition = ObsResourceDefinition.Builder.newInstance()
                .id("test")
                .endpoint(endpoint)
                .bucketName(bucket)
                .transferProcessId("test")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var response = provisioner.provision(definition, policy).join().getContent();

        assertThat(response.getResource()).isInstanceOf(ObsProvisionedResource.class);
        assertThat(response.getSecretToken()).isInstanceOfSatisfying(ObsSecretToken.class, secretToken -> {
            assertThat(secretToken.ak()).isEqualTo("accessKeyId");
            assertThat(secretToken.sk()).isEqualTo("secretAccessKey");
            assertThat(secretToken.securityToken()).isEqualTo("sessionToken");
        });
        verify(iamClient).createTemporaryAccessKeyByToken(any());
        verify(s3Client).createBucket(isA(CreateBucketRequest.class));
    }

    @Test
    void verify_basic_deprovision() {

        var bucket = "test";
        var endpoint = "http://endpoint";
        var objectKey = "key";

        var object = new ObsObject();
        object.setObjectKey(objectKey);
        var listing = new ObjectListing.Builder()
                .bucketName(bucket)
                .objectSummaries(List.of(object))
                .builder();

        when(s3Client.listObjects(bucket)).thenReturn(listing);
        when(s3Client.deleteObjects(any())).thenReturn(new DeleteObjectsResult());
        when(s3Client.deleteBucket(bucket)).thenReturn(new HeaderResponse());

        var provisionedResource = ObsProvisionedResource.Builder.newInstance()
                .id("test")
                .resourceDefinitionId(UUID.randomUUID().toString())
                .endpoint(endpoint)
                .bucketName(bucket)
                .resourceName("name")
                .transferProcessId("test")
                .build();

        var policy = Policy.Builder.newInstance().build();

        var response = provisioner.deprovision(provisionedResource, policy).join().getContent();

        assertThat(response.isError()).isFalse();
        verify(s3Client).listObjects(bucket);
        verify(s3Client).deleteObjects(isA(DeleteObjectsRequest.class));
        verify(s3Client).deleteBucket(bucket);
    }

    @Test
    void deprovision_shouldReturnFutureError_whenObjectDeletionFails() {
        var bucket = "test";
        var endpoint = "http://endpoint";
        var objectKey = "key";

        var object = new ObsObject();
        object.setObjectKey(objectKey);
        var listing = new ObjectListing.Builder()
                .bucketName(bucket)
                .objectSummaries(List.of(object))
                .builder();

        when(s3Client.listObjects(bucket)).thenReturn(listing);
        when(s3Client.deleteObjects(any())).thenThrow(new ObsException("any"));

        var provisionedResource = ObsProvisionedResource.Builder.newInstance()
                .id("test")
                .resourceDefinitionId(UUID.randomUUID().toString())
                .endpoint(endpoint)
                .bucketName(bucket)
                .resourceName("name")
                .transferProcessId("test")
                .build();

        var policy = Policy.Builder.newInstance().build();

        var response = provisioner.deprovision(provisionedResource, policy);

        assertThat(response).failsWithin(1, SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(ObsException.class)
                .withMessageContaining("any");
    }

    @Test
    void deprovision_shouldReturnFutureError_whenDeleteBucketFails() {
        var bucket = "test";
        var endpoint = "http://endpoint";
        var objectKey = "key";

        var object = new ObsObject();
        object.setObjectKey(objectKey);
        var listing = new ObjectListing.Builder()
                .bucketName(bucket)
                .objectSummaries(List.of(object))
                .builder();

        when(s3Client.listObjects(bucket)).thenReturn(listing);
        when(s3Client.deleteBucket(bucket)).thenThrow(new ObsException("any"));

        var provisionedResource = ObsProvisionedResource.Builder.newInstance()
                .id("test")
                .resourceDefinitionId(UUID.randomUUID().toString())
                .endpoint(endpoint)
                .bucketName(bucket)
                .resourceName("name")
                .transferProcessId("test")
                .build();

        var policy = Policy.Builder.newInstance().build();

        var response = provisioner.deprovision(provisionedResource, policy);

        assertThat(response).failsWithin(1, SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(ObsException.class)
                .withMessageContaining("any");
    }

    @Test
    void deprovision_should_return_failed_future_on_error() {
        var bucket = "test";
        var endpoint = "http://endpoint";

        when(s3Client.listObjects(bucket)).thenThrow(new ObsException("any"));

        var provisionedResource = ObsProvisionedResource.Builder.newInstance()
                .id("test")
                .resourceDefinitionId(UUID.randomUUID().toString())
                .endpoint(endpoint)
                .bucketName(bucket)
                .resourceName("name")
                .transferProcessId("test")
                .build();

        var policy = Policy.Builder.newInstance().build();

        var response = provisioner.deprovision(provisionedResource, policy);

        assertThat(response).failsWithin(1, SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(ObsException.class)
                .withMessageContaining("any");
    }

    @Test
    void provision_should_return_failed_future_on_error() {
        when(s3Client.createBucket(isA(CreateBucketRequest.class))).thenThrow(new ObsException("any"));
        var definition = ObsResourceDefinition.Builder.newInstance()
                .id("test").endpoint("http://test")
                .bucketName("test")
                .transferProcessId("test")
                .build();

        var policy = Policy.Builder.newInstance().build();

        var response = provisioner.provision(definition, policy);

        assertThat(response).failsWithin(1, SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(ObsException.class)
                .withMessageContaining("any");
    }


}
