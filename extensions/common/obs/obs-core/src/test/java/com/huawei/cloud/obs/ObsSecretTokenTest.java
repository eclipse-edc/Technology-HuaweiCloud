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

import org.eclipse.edc.json.JacksonTypeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ObsSecretTokenTest {

    private ObsSecretToken secretToken;

    @Test
    void verifyDeserialize() throws IOException {
        var mapper = new JacksonTypeManager().getMapper();
        var writer = new StringWriter();

        mapper.writeValue(writer, secretToken);

        var deserialized = mapper.readValue(writer.toString(), ObsSecretToken.class);

        assertNotNull(deserialized);
        assertThat(deserialized).isEqualTo(secretToken);
    }

    @BeforeEach
    void setUp() {
        secretToken = new ObsSecretToken("ak", "sk", "st", 10L);
    }
}
