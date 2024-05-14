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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ObsResourceDefinitionTest {

    @Test
    void toBuilder_verifyEqualResourceDefinition() {
        var definition = ObsResourceDefinition.Builder.newInstance()
                .id("id")
                .transferProcessId("tp-id")
                .endpoint("http://localhost")
                .bucketName("bucket")
                .build();
        var builder = definition.toBuilder();
        var rebuiltDefinition = builder.build();

        assertThat(rebuiltDefinition).usingRecursiveComparison().isEqualTo(definition);
    }

}
