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
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProvisionManager;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ResourceManifestGenerator;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;


@Extension(ObsProvisionExtension.NAME)
public class ObsProvisionExtension implements ServiceExtension {

    public static final String NAME = "OBS Provision";

    @Setting(value = "Duration in seconds of the temporary token", defaultValue = "" + ObsProvisionExtension.PROVISIONER_DEFAULT_TOKEN_DURATION)
    private static final String PROVISION_TOKEN_DURATION = "edc.obs.provision.token.duration";

    private static final int PROVISIONER_DEFAULT_TOKEN_DURATION = 60 * 30;
    @Inject
    private ObsClientProvider clientProvider;

    @Inject
    private ProvisionManager provisionManager;

    @Inject
    private RetryPolicy<Object> retryPolicy;

    @Inject
    private ResourceManifestGenerator resourceManifestGenerator;

    @Inject
    private Monitor monitor;

    @Inject
    private TypeManager typeManager;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {

        var tokenDuration = context.getConfig().getInteger(PROVISION_TOKEN_DURATION, PROVISIONER_DEFAULT_TOKEN_DURATION);
        provisionManager.register(new ObsProvisioner(clientProvider, retryPolicy, monitor, tokenDuration));
        resourceManifestGenerator.registerGenerator(new ObsConsumerResourceDefinitionGenerator());

        registerTypes(typeManager);
    }

    private void registerTypes(TypeManager typeManager) {
        typeManager.registerTypes(ObsProvisionedResource.class, ObsResourceDefinition.class);
    }

}
