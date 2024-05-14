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

package com.huawei.cloud.obs;

import com.obs.services.IObsCredentialsProvider;

import java.util.Objects;

/**
 * Configuration for the {@link ObsClientProvider}
 */
public class ObsClientProviderConfiguration {

    private String iamEndpoint;

    private IObsCredentialsProvider credentialsProvider;

    private ObsClientProviderConfiguration() {
    }

    /**
     * Returns the configured IAM endpoint
     *
     * @return The endpoint
     */
    public String getIamEndpoint() {
        return iamEndpoint;
    }

    /**
     * Returns the configured {@link IObsCredentialsProvider}
     *
     * @return The provider
     */
    public IObsCredentialsProvider getCredentialsProvider() {
        return credentialsProvider;
    }

    public static class Builder {

        private final ObsClientProviderConfiguration configuration = new ObsClientProviderConfiguration();

        private Builder() {

        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder iamEndpoint(String iamEndpoint) {
            configuration.iamEndpoint = iamEndpoint;
            return this;
        }

        public Builder credentialsProvider(IObsCredentialsProvider credentialsProvider) {
            configuration.credentialsProvider = credentialsProvider;
            return this;
        }

        public ObsClientProviderConfiguration build() {
            Objects.requireNonNull(configuration.credentialsProvider, "Credential provider required");

            return configuration;
        }
    }
}
