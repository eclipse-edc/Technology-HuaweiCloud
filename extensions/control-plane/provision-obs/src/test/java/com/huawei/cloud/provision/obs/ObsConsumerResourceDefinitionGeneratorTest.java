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
import org.eclipse.edc.connector.controlplane.asset.spi.domain.Asset;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;


public class ObsConsumerResourceDefinitionGeneratorTest {

    private static final String BUCKET = "test-name";
    private static final String ENDPOINT = "http://endpoint";
    private ObsConsumerResourceDefinitionGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new ObsConsumerResourceDefinitionGenerator();
    }

    @Test
    void generate() {
        var destination = DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                .property(ObsBucketSchema.BUCKET_NAME, BUCKET)
                .property(ObsBucketSchema.ENDPOINT, ENDPOINT)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.generate(transferProcess, policy);

        assertThat(definition).isInstanceOf(ObsResourceDefinition.class);
        var objectDef = (ObsResourceDefinition) definition;
        assertThat(objectDef.getBucketName()).isEqualTo(BUCKET);
        assertThat(objectDef.getEndpoint()).isEqualTo(ENDPOINT);
        assertThat(objectDef.getId()).satisfies(UUID::fromString);
    }

    @Test
    void generate_noDataRequestAsParameter() {
        var policy = Policy.Builder.newInstance().build();
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(null, policy));
    }

    @Test
    void generate_noBucketNameSpecified() {
        var destination = DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                .property(ObsBucketSchema.ENDPOINT, ENDPOINT)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(transferProcess, policy));
    }

    @Test
    void generate_noEndpointNameSpecified() {
        var destination = DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                .property(ObsBucketSchema.BUCKET_NAME, BUCKET)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(transferProcess, policy));
    }

    @Test
    void canGenerate() {
        var destination = DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE)
                .property(ObsBucketSchema.BUCKET_NAME, BUCKET)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.canGenerate(transferProcess, policy);

        assertThat(definition).isTrue();
    }

    @Test
    void canGenerateIsNotTypeObsSchema() {
        var destination = DataAddress.Builder.newInstance().type("aNonOBSBucketSchema")
                .property(ObsBucketSchema.BUCKET_NAME, BUCKET)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var transferProcess = TransferProcess.Builder.newInstance()
                .dataDestination(destination)
                .assetId(asset.getId())
                .correlationId("process-id")
                .build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.canGenerate(transferProcess, policy);
        assertThat(definition).isFalse();
    }
}
