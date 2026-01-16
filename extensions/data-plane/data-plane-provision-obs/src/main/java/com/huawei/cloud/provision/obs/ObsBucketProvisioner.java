/*
 *  Copyright (c) 2026 Huawei Technologies
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

package com.huawei.cloud.provision.obs;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenRequest;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenRequestBody;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenResponse;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import com.huaweicloud.sdk.iam.v3.model.IdentityToken;
import com.huaweicloud.sdk.iam.v3.model.ServicePolicy;
import com.huaweicloud.sdk.iam.v3.model.ServiceStatement;
import com.huaweicloud.sdk.iam.v3.model.TokenAuth;
import com.huaweicloud.sdk.iam.v3.model.TokenAuthIdentity;
import com.obs.services.ObsClient;
import com.obs.services.model.CreateBucketRequest;
import com.obs.services.model.ObsBucket;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionedResource;
import org.eclipse.edc.connector.dataplane.spi.provision.Provisioner;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

public class ObsBucketProvisioner implements Provisioner {

    public static final String VERSION = "1.1";
    public static final String ACTION_ITEM = "obs:object:PutObject";
    // TODO restrict to region?
    public static final String RESOURCE_ITEM = "obs:*:*:object:%s/*";
    private final ObsClientProvider clientProvider;
    private final RetryPolicy<Object> retryPolicy;
    private final Monitor monitor;
    private final int tokenDuration;
    private final Vault vault;
    private final TypeManager typeManager;

    public ObsBucketProvisioner(ObsClientProvider clientProvider, RetryPolicy<Object> retryPolicy, Monitor monitor, int tokenDuration, Vault vault, TypeManager typeManager) {
        this.clientProvider = clientProvider;
        this.retryPolicy = retryPolicy;
        this.monitor = monitor;
        this.tokenDuration = tokenDuration;
        this.vault = vault;
        this.typeManager = typeManager;
    }

    @Override
    public String supportedType() {
        return "";
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionedResource>> provision(ProvisionResource provisionResource) {
        var bucketName = provisionResource.getDataAddress().getStringProperty(ObsBucketSchema.BUCKET_NAME);
        var obsClient = clientProvider.obsClient(bucketName);
        var iamClient = clientProvider.iamClient();

        monitor.debug("ObsProvisionPipeline: create bucket " + bucketName);
        try {
            createBucket(obsClient, bucketName);
            var tokenResponse = requestTemporaryToken(iamClient, bucketName);
            return completedFuture(provisionSucceeded(provisionResource, tokenResponse.getCredential())
            );
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    private ObsBucket createBucket(ObsClient obsClient, String bucketName) {
        var request = new CreateBucketRequest(bucketName);
        return Failsafe.with(retryPolicy).get(() -> obsClient.createBucket(request));
    }

    private CreateTemporaryAccessKeyByTokenResponse requestTemporaryToken(IamClient iamClient, String bucketName) {

        var request = new CreateTemporaryAccessKeyByTokenRequest().withBody(
                new CreateTemporaryAccessKeyByTokenRequestBody()
                        .withAuth(new TokenAuth().withIdentity((identity -> tokenIdentity(identity, bucketName)))));

        return Failsafe.with(retryPolicy).get(() -> iamClient.createTemporaryAccessKeyByToken(request));
    }

    private void tokenIdentity(TokenAuthIdentity identity, String bucketName) {
        identity.addMethodsItem(TokenAuthIdentity.MethodsEnum.TOKEN)
                .withToken(new IdentityToken().withDurationSeconds(tokenDuration))
                .withPolicy(new ServicePolicy().withVersion(VERSION)
                        .addStatementItem(new ServiceStatement()
                                .withEffect(ServiceStatement.EffectEnum.ALLOW)
                                .addActionItem(ACTION_ITEM)
                                .addResourceItem(format(RESOURCE_ITEM, bucketName))));
    }

    private StatusResult<ProvisionedResource> provisionSucceeded(ProvisionResource provisionResource, Credential credentials) {
        var keyName = "resourceDefinition-" + provisionResource.getId() + "-secret-" + UUID.randomUUID();
        var expiration = Instant.parse(credentials.getExpiresAt());
        var secretToken = new ObsSecretToken(credentials.getAccess(), credentials.getSecret(), credentials.getSecuritytoken(), expiration.toEpochMilli());
        try {
            vault.storeSecret(keyName, typeManager.getMapper().writeValueAsString(secretToken));
        } catch (JsonProcessingException e) {
            return StatusResult.failure(ResponseStatus.FATAL_ERROR, "Cannot serialize secret token: " + e.getMessage());
        }
        monitor.debug("ObsBucketProvisioner: Bucket request submitted: " + provisionResource.getDataAddress().getStringProperty(ObsBucketSchema.BUCKET_NAME));
        var response = ProvisionedResource.Builder.from(provisionResource).dataAddress(
                DataAddress.Builder.newInstance()
                        .properties(provisionResource.getDataAddress().getProperties())
                        .keyName(keyName)
                        .build())
                .build();
        return StatusResult.success(response);
    }

}
