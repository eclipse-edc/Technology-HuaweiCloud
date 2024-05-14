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
import com.obs.services.ObsClient;
import com.obs.services.model.DeleteObjectsRequest;
import com.obs.services.model.DeleteObjectsResult;
import com.obs.services.model.HeaderResponse;
import com.obs.services.model.KeyAndVersion;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.spi.monitor.Monitor;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.joining;

public class ObsDeprovisionPipeline {

    private final RetryPolicy<Object> retryPolicy;
    private final ObsClientProvider clientProvider;
    private final Monitor monitor;

    public ObsDeprovisionPipeline(RetryPolicy<Object> retryPolicy, ObsClientProvider clientProvider, Monitor monitor) {
        this.retryPolicy = retryPolicy;
        this.clientProvider = clientProvider;
        this.monitor = monitor;
    }

    /**
     * Performs a deprovisioning operation.
     */
    public CompletableFuture<DeprovisionedResource> deprovision(ObsProvisionedResource resource) {
        var obsClient = clientProvider.obsClient(resource.getEndpoint());
        var bucketName = resource.getBucketName();

        monitor.debug("ObsDeprovisionPipeline: list objects");
        try {
            var objects = listObjects(obsClient, bucketName);
            deleteObjects(obsClient, bucketName, objects);
            deleteBucket(obsClient, bucketName);
            return completedFuture(DeprovisionedResource.Builder.newInstance().provisionedResourceId(resource.getId()).build());
        } catch (Exception e) {
            return failedFuture(e);
        }
    }

    private ObjectListing listObjects(ObsClient obsClient, String bucketName) {
        return Failsafe.with(retryPolicy).get(() -> obsClient.listObjects(bucketName));
    }

    private HeaderResponse deleteBucket(ObsClient obsClient, String bucketName) {
        return Failsafe.with(retryPolicy).get(() -> {
            monitor.debug("ObsDeprovisionPipeline: delete bucket");
            return obsClient.deleteBucket(bucketName);
        });
    }

    private DeleteObjectsResult deleteObjects(ObsClient obsClient, String bucketName, ObjectListing objectListing) {
        var deleteRequest = objectListing.getObjects()
                .stream()
                .map(ObsObject::getObjectKey)
                .reduce(new DeleteObjectsRequest(), (request, key) -> {
                    request.addKeyAndVersion(key);
                    return request;
                }, (request, al) -> request);

        deleteRequest.setBucketName(bucketName);

        var ids = Arrays.stream(deleteRequest.getKeyAndVersions())
                .map(KeyAndVersion::getKey)
                .collect(joining(", "));

        monitor.debug("ObsDeprovisionPipeline: delete bucket contents: " + ids);

        return Failsafe.with(retryPolicy).get(() -> obsClient.deleteObjects(deleteRequest));
    }

    static class Builder {
        private final RetryPolicy<Object> retryPolicy;
        private Monitor monitor;
        private ObsClientProvider clientProvider;

        private Builder(RetryPolicy<Object> retryPolicy) {
            this.retryPolicy = retryPolicy;
        }

        public static Builder newInstance(RetryPolicy<Object> policy) {
            return new Builder(policy);
        }

        public Builder monitor(Monitor monitor) {
            this.monitor = monitor;
            return this;
        }

        public Builder clientProvider(ObsClientProvider clientProvider) {
            this.clientProvider = clientProvider;
            return this;
        }

        public ObsDeprovisionPipeline build() {
            Objects.requireNonNull(retryPolicy);
            Objects.requireNonNull(clientProvider);
            Objects.requireNonNull(monitor);
            return new ObsDeprovisionPipeline(retryPolicy, clientProvider, monitor);
        }
    }
}
