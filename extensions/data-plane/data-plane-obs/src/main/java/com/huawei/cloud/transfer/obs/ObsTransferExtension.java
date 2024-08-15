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

package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsClientProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

import java.util.concurrent.Executors;

@Extension(ObsTransferExtension.NAME)
public class ObsTransferExtension implements ServiceExtension {

    public static final String NAME = "Huawei OBS Data Transfer Extension";

    @Inject
    private PipelineService pipelineService;
    @Inject
    private TypeManager typeManager;
    @Inject
    private Vault vault;
    @Inject
    private ObsClientProvider clientProvider;

    @Override
    public void initialize(ServiceExtensionContext context) {

        var sourceFactory = new ObsDataSourceFactory(vault, typeManager, clientProvider);
        pipelineService.registerFactory(sourceFactory);

        var executor = Executors.newFixedThreadPool(10);
        var sinkFactory = new ObsDataSinkFactory(vault, typeManager, context.getMonitor(), executor, clientProvider);
        pipelineService.registerFactory(sinkFactory);
    }

    @Override
    public String name() {
        return NAME;
    }
}
