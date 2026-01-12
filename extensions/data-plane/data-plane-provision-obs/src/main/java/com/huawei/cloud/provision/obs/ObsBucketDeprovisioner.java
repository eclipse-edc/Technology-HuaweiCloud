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

import com.huawei.cloud.obs.ObsBucketSchema;
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
import org.eclipse.edc.connector.dataplane.spi.provision.DeprovisionedResource;
import org.eclipse.edc.connector.dataplane.spi.provision.Deprovisioner;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.security.Vault;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.joining;

public class ObsBucketDeprovisioner implements Deprovisioner {

    private final RetryPolicy<Object> retryPolicy;
    private final ObsClientProvider clientProvider;
    private final Monitor monitor;
    private final Vault vault;

    public ObsBucketDeprovisioner(RetryPolicy<Object> retryPolicy, ObsClientProvider clientProvider, Monitor monitor, Vault vault) {
        this.retryPolicy = retryPolicy;
        this.clientProvider = clientProvider;
        this.monitor = monitor;
        this.vault = vault;
    }


    @Override
    public String supportedType() {
        return "";
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(ProvisionResource provisionResource) {
        var bucketName = provisionResource.getDataAddress().getStringProperty(ObsBucketSchema.BUCKET_NAME);
        var obsClient = clientProvider.obsClient(bucketName);

        monitor.debug("ObsDeprovisionPipeline: list objects");
        try {
            var objects = listObjects(obsClient, bucketName);
            deleteObjects(obsClient, bucketName, objects);
            deleteBucket(obsClient, bucketName);
            return completedFuture(StatusResult.success(DeprovisionedResource.Builder.from(provisionResource).build()));
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

    private CompletableFuture<?> deleteSecret(ProvisionResource resource) {
        return Failsafe.with(retryPolicy).getStageAsync(() -> {
            monitor.debug("delete secret from vault");
            var response = vault.deleteSecret(resource.getDataAddress().getKeyName());
            return CompletableFuture.completedFuture(response);
        });
    }
}
