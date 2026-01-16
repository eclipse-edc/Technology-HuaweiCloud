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
import dev.failsafe.RetryPolicy;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionerManager;
import org.eclipse.edc.connector.dataplane.spi.provision.ResourceDefinitionGeneratorManager;
import org.eclipse.edc.runtime.metamodel.annotation.Configuration;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

@Extension(value = DataPlaneProvisionObsExtension.NAME)
public class DataPlaneProvisionObsExtension implements ServiceExtension {

    public static final String NAME = "Data Plane Provision HUAWEI Cloud OBS";
    @Inject
    private Vault vault;
    @Inject
    private ObsClientProvider clientProvider;
    @Inject
    private ProvisionerManager provisionerManager;
    @Inject
    private RetryPolicy<Object> retryPolicy;
    @Inject
    private ResourceDefinitionGeneratorManager resourceDefinitionGeneratorManager;
    @Inject
    private TypeManager typeManager;
    @Configuration
    ObsBucketProvisionerConfiguration obsBucketProvisionerConfiguration;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        resourceDefinitionGeneratorManager.registerConsumerGenerator(new ObsConsumerProvisionResourceGenerator());

        provisionerManager.register(new ObsBucketProvisioner(clientProvider,
                retryPolicy, context.getMonitor().withPrefix("ObsBucketProvisioner"), obsBucketProvisionerConfiguration.tokenDuration(), vault, typeManager));
        provisionerManager.register(new ObsBucketDeprovisioner(retryPolicy, clientProvider,
                context.getMonitor().withPrefix("ObsBucketDeprovisioner"), vault));
    }
}
