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
import com.huawei.cloud.provision.obs.ObsBucketProvisioner;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.CreateBucketRequest;
import com.obs.services.model.ObsBucket;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ObsBucketProvisionerTest {

    private final IamClient iamClient = mock(IamClient.class);
    private final ObsClient obsClient = mock(ObsClient.class);
    private final ObsClientProvider clientProvider = mock(ObsClientProvider.class);
    private final Vault vault = mock(Vault.class);
    private ObsBucketProvisioner provisioner;

    @BeforeEach
    void setUp() {
        when(clientProvider.iamClient()).thenReturn(iamClient);
        when(clientProvider.obsClient(anyString())).thenReturn(obsClient);

        provisioner = new ObsBucketProvisioner(clientProvider, RetryPolicy.ofDefaults(), mock(Monitor.class), 900, vault, new JacksonTypeManager());
    }

    @Test
    void verify_basic_provision() {
        var bucket = "test";
        var credentials = new Credential()
                .withAccess("accessKeyId").withSecret("secretAccessKey").withSecuritytoken("sessionToken")
                .withExpiresAt(Instant.now().toString());

        var createBucketResponse = new ObsBucket();
        var tokenResponse = new CreateTemporaryAccessKeyByTokenResponse().withCredential(credentials);
        var createBucketRequest = new CreateBucketRequest(bucket);

        when(obsClient.createBucket(createBucketRequest)).thenReturn(createBucketResponse);
        when(iamClient.createTemporaryAccessKeyByToken(any())).thenReturn(tokenResponse);

        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                        .property(ObsBucketSchema.BUCKET_NAME, "test")
                        .build())
                .flowId("test")
                .build();

        var response = provisioner.provision(definition).join().getContent();

        assertThat(response.getDataAddress()).isInstanceOf(DataAddress.class);
        verify(iamClient).createTemporaryAccessKeyByToken(any());
        verify(obsClient).createBucket(isA(CreateBucketRequest.class));
    }

    @Test
    void provision_should_return_failed_future_on_error() {
        when(obsClient.createBucket(isA(CreateBucketRequest.class))).thenThrow(new ObsException("any"));
        var definition = ProvisionResource.Builder.newInstance()
                .dataAddress(DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                        .property(ObsBucketSchema.BUCKET_NAME, "test")
                        .build())
                .flowId("test")
                .build();

        var response = provisioner.provision(definition);

        assertThat(response).failsWithin(1, SECONDS)
                .withThrowableThat()
                .withCauseInstanceOf(ObsException.class)
                .withMessageContaining("any");
    }
}
