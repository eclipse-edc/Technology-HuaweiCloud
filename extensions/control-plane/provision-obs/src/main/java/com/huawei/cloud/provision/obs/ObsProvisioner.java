package com.huawei.cloud.provision.obs;


import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huaweicloud.sdk.iam.v3.model.Credential;
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class ObsProvisioner implements Provisioner<ObsResourceDefinition, ObsProvisionedResource> {

    private final ObsClientProvider clientProvider;

    private final RetryPolicy<Object> retryPolicy;

    private final Monitor monitor;
    private final int tokenDuration;

    public ObsProvisioner(ObsClientProvider clientProvider, RetryPolicy<Object> retryPolicy, Monitor monitor, int tokenDuration) {
        this.clientProvider = clientProvider;
        this.retryPolicy = retryPolicy;
        this.monitor = monitor;
        this.tokenDuration = tokenDuration;
    }

    @Override
    public boolean canProvision(ResourceDefinition resourceDefinition) {
        return resourceDefinition instanceof ObsResourceDefinition;
    }

    @Override
    public boolean canDeprovision(ProvisionedResource resourceDefinition) {
        return resourceDefinition instanceof ObsProvisionedResource;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionResponse>> provision(ObsResourceDefinition resourceDefinition, Policy policy) {
        return ObsProvisionerPipeline.Builder.newInstance(retryPolicy)
                .clientProvider(clientProvider)
                .tokenDuration(tokenDuration)
                .monitor(monitor)
                .build()
                .provision(resourceDefinition)
                .thenApply(result -> provisionSuccedeed(resourceDefinition, result.credential()));
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(ObsProvisionedResource provisionedResource, Policy policy) {
        return ObsDeprovisionPipeline.Builder.newInstance(retryPolicy)
                .clientProvider(clientProvider)
                .monitor(monitor)
                .build().deprovision(provisionedResource)
                .thenApply(ignore -> StatusResult.success(DeprovisionedResource.Builder.newInstance().provisionedResourceId(provisionedResource.getId()).build()));
    }

    private StatusResult<ProvisionResponse> provisionSuccedeed(ObsResourceDefinition resourceDefinition, Credential credentials) {
        var resource = ObsProvisionedResource.Builder.newInstance()
                .id(resourceDefinition.getBucketName())
                .resourceDefinitionId(resourceDefinition.getId())
                .hasToken(true)
                .endpoint(resourceDefinition.getEndpoint())
                .bucketName(resourceDefinition.getBucketName())
                .transferProcessId(resourceDefinition.getTransferProcessId())
                .resourceName(resourceDefinition.getBucketName())
                .build();


        var expiration = Instant.parse(credentials.getExpiresAt());
        var secretToken = new ObsSecretToken(credentials.getAccess(), credentials.getSecret(), credentials.getSecuritytoken(), expiration.toEpochMilli());

        monitor.debug("ObsBucketProvisioner: Bucket request submitted: " + resourceDefinition.getBucketName());
        var response = ProvisionResponse.Builder.newInstance().resource(resource).secretToken(secretToken).build();
        return StatusResult.success(response);
    }
}
