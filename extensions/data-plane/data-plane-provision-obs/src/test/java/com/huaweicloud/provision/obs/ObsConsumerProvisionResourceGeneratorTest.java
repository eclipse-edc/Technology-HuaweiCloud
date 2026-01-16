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

package com.huaweicloud.provision.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.provision.obs.ObsConsumerProvisionResourceGenerator;
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.dataplane.spi.DataFlow;
import org.eclipse.edc.connector.dataplane.spi.provision.ProvisionResource;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;


public class ObsConsumerProvisionResourceGeneratorTest {

    private static final String BUCKET = "test-name";
    private static final String ENDPOINT = "http://endpoint";
    private ObsConsumerProvisionResourceGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new ObsConsumerProvisionResourceGenerator();
    }

    @Test
    void generate() {
        var destination = DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                .property(ObsBucketSchema.BUCKET_NAME, BUCKET)
                .property(ObsBucketSchema.ENDPOINT, ENDPOINT)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var dataflow = DataFlow.Builder.newInstance()
                .destination(destination)
                .assetId(asset.getId())
                .id(UUID.randomUUID().toString())
                .build();

        var definition = generator.generate(dataflow);

        assertThat(definition).isInstanceOf(ProvisionResource.class);
        assertThat(definition.getDataAddress().getProperties().get(ObsBucketSchema.BUCKET_NAME)).isEqualTo(BUCKET);
        assertThat(definition.getDataAddress().getProperties().get(ObsBucketSchema.ENDPOINT)).isEqualTo(ENDPOINT);
    }

    @Test
    void generate_noDataRequestAsParameter() {
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(null));
    }
}
