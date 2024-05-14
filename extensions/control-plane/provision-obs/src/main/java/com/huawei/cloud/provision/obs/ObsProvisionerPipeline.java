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

package com.huawei.cloud.provision.obs;

import com.huawei.cloud.obs.ObsClientProvider;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenRequest;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenRequestBody;
import com.huaweicloud.sdk.iam.v3.model.CreateTemporaryAccessKeyByTokenResponse;
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
import org.eclipse.edc.spi.monitor.Monitor;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

public class ObsProvisionerPipeline {

    public static final String VERSION = "1.1";
    public static final String ACTION_ITEM = "obs:object:PutObject";
    // TODO restrict to region?
    public static final String RESOURCE_ITEM = "obs:*:*:object:%s/*";

    private final RetryPolicy<Object> retryPolicy;
    private final ObsClientProvider clientProvider;
    private final Monitor monitor;
    private final int tokenDuration;

    private ObsProvisionerPipeline(RetryPolicy<Object> retryPolicy, ObsClientProvider clientProvider,
                                   Monitor monitor, int tokenDuration) {
        this.retryPolicy = retryPolicy;
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.tokenDuration = tokenDuration;
    }

    /**
     * Performs a non-blocking provisioning operation.
     */
    public CompletableFuture<ObsProvisionResponse> provision(ObsResourceDefinition resourceDefinition) {
        var obsClient = clientProvider.obsClient(resourceDefinition.getEndpoint());
        var iamClient = clientProvider.iamClient();
        
        monitor.debug("ObsProvisionPipeline: create bucket " + resourceDefinition.getBucketName());
        try {
            createBucket(obsClient, resourceDefinition.getBucketName());
            var response = requestTemporaryToken(iamClient, resourceDefinition.getBucketName());
            return completedFuture(new ObsProvisionResponse(response.getCredential()));
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

    static class Builder {
        private final RetryPolicy<Object> retryPolicy;
        private int tokenDuration;
        private Monitor monitor;
        private ObsClientProvider clientProvider;

        private Builder(RetryPolicy<Object> retryPolicy) {
            this.retryPolicy = retryPolicy;
        }

        public static Builder newInstance(RetryPolicy<Object> policy) {
            return new Builder(policy);
        }

        public Builder tokenDuration(int tokenDuration) {
            this.tokenDuration = tokenDuration;
            return this;
        }

        public Builder monitor(Monitor monitor) {
            this.monitor = monitor;
            return this;
        }

        public Builder clientProvider(ObsClientProvider clientProvider) {
            this.clientProvider = clientProvider;
            return this;
        }

        public ObsProvisionerPipeline build() {
            Objects.requireNonNull(retryPolicy);
            Objects.requireNonNull(clientProvider);
            Objects.requireNonNull(monitor);
            return new ObsProvisionerPipeline(retryPolicy, clientProvider, monitor, tokenDuration);
        }
    }

}
