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

import com.huaweicloud.sdk.core.auth.GlobalCredentials;
import com.huaweicloud.sdk.iam.v3.IamClient;
import com.obs.services.IObsCredentialsProvider;
import com.obs.services.OBSCredentialsProviderChain;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.String.format;

public class ObsClientProviderImpl implements ObsClientProvider {

    private final ObsClientProviderConfiguration configuration;
    private final Map<String, ObsClient> clients = new ConcurrentHashMap<>();

    private final Monitor monitor;

    private IamClient iamClient;

    private Vault vault;

    public ObsClientProviderImpl(ObsClientProviderConfiguration configuration, Monitor monitor) {
        this.configuration = configuration;
        this.monitor = monitor;
    }

    public ObsClientProviderImpl(ObsClientProviderConfiguration configuration, Monitor monitor, Vault vault) {
        this.configuration = configuration;
        this.monitor = monitor;
        this.vault = vault;
    }

    @NotNull
    private static ObsConfiguration createObsConfiguration(String endpoint) {
        var config = new ObsConfiguration();
        config.setPathStyle(true); //otherwise the bucketname gets prepended
        config.setEndPoint(endpoint);
        return config;
    }

    @Override
    public ObsClient obsClient(String endpoint) {
        return clients.computeIfAbsent(endpoint, this::createClient);
    }

    @Override
    public ObsClient obsClient(String endpoint, IObsCredentialsProvider credentialsProvider) {
        var config = createObsConfiguration(endpoint);
        return new ObsClient(credentialsProvider, config);
    }

    @Override
    public IamClient iamClient() {
        return getOrCreateIamclient(configuration);
    }

    @Override
    public void shutdown() {
        clients.entrySet().forEach(this::closeClient);
        clients.clear();
    }

    public ObsClientProviderConfiguration getConfiguration() {
        return configuration;
    }

    private ObsClient createClient(String endpoint) {
        var config = createObsConfiguration(endpoint);
        return new ObsClient(configuration.getCredentialsProvider(), config);
    }

    /**
     * Get or create a {@link IamClient}. The lazy initialization is due the fact that the IamClient
     * does not work like {@link ObsClient} which uses provider for credentials. For feeding here the credential
     * to the IamClient we call {@link IObsCredentialsProvider#getSecurityKey()} which in case of {@link OBSCredentialsProviderChain}
     * could cause process hangs in testing phase.
     *
     * @param configuration The input configuration
     * @return The {@link IamClient}
     */
    private IamClient getOrCreateIamclient(ObsClientProviderConfiguration configuration) {
        if (iamClient != null) {
            return iamClient;
        }
        synchronized (this) {
            if (iamClient == null) {
                var secKey = configuration.getCredentialsProvider().getSecurityKey();

                var credentials = new GlobalCredentials()
                        .withAk(secKey.getAccessKey())
                        .withSk(secKey.getSecretKey());

                var endpoints = new ArrayList<String>();
                endpoints.add(configuration.getIamEndpoint());
                iamClient = IamClient.newBuilder()
                        .withEndpoints(endpoints)
                        .withCredential(credentials)
                        .build();
            }
            return iamClient;
        }
    }

    public Vault getVault() {
        return vault;
    }

    private void closeClient(Map.Entry<String, ObsClient> clientEntry) {
        try {
            clientEntry.getValue().close();
        } catch (IOException e) {
            monitor.severe(format("Failed to close client with endpoint: %s", clientEntry.getKey()));
        }
    }
}
