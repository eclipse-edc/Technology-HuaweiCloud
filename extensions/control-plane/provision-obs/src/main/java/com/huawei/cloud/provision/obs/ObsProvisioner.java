package com.huawei.cloud.provision.obs;

import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.response.StatusResult;

import java.util.concurrent.CompletableFuture;

public class ObsProvisioner implements Provisioner<ObsResourceDefinition, ObsProvisionedResource> {
    @Override
    public boolean canProvision(ResourceDefinition resourceDefinition) {
        return false;
    }

    @Override
    public boolean canDeprovision(ProvisionedResource resourceDefinition) {
        return false;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionResponse>> provision(ObsResourceDefinition resourceDefinition, Policy policy) {
        return null;
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(ObsProvisionedResource provisionedResource, Policy policy) {
        return null;
    }
}
