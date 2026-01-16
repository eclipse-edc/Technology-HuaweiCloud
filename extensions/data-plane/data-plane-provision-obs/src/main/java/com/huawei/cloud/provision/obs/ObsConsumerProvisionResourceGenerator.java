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
import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.connector.dataplane.spi.provision.ResourceDefinitionGenerator;

/**
 * Generates OBS buckets on the consumer (requesting connector) that serve as data destinations.
 */
public class ObsConsumerProvisionResourceGenerator implements ResourceDefinitionGenerator {


    @Override
    public String supportedType() {
        return ObsBucketSchema.TYPE;
    }

    @Override
    public ProvisionResource generate(DataFlow dataFlow) {
        return ProvisionResource.Builder.newInstance()
                .flowId(dataFlow.getId())
                .type(ObsBucketSchema.TYPE)
                .dataAddress(dataFlow.getDestination())
                .build();
    }
}
