package com.huawei.cloud.provision.obs;

import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;

import java.util.Objects;

public class ObsResourceDefinition extends ResourceDefinition {

    private String endpoint;
    private String bucketName;

    private ObsResourceDefinition() {

    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getBucketName() {
        return bucketName;
    }

    @Override
    public Builder toBuilder() {
        return initializeBuilder(new Builder())
                .bucketName(bucketName)
                .endpoint(endpoint);
    }

    public static class Builder extends ResourceDefinition.Builder<ObsResourceDefinition, Builder> {

        private Builder() {
            super(new ObsResourceDefinition());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder endpoint(String endpoint) {
            resourceDefinition.endpoint = endpoint;
            return this;
        }

        public Builder bucketName(String bucketName) {
            resourceDefinition.bucketName = bucketName;
            return this;
        }

        @Override
        protected void verify() {
            super.verify();
            Objects.requireNonNull(resourceDefinition.endpoint, "endpoint");
            Objects.requireNonNull(resourceDefinition.bucketName, "bucketName");
        }
    }
}
