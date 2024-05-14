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

import org.eclipse.edc.json.JacksonTypeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.StringWriter;

import static java.util.UUID.randomUUID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ObsProvisionedResourceTest {

    private ObsProvisionedResource provisionedResource;

    @Test
    void verifyDeserialize() throws IOException {
        var mapper = new JacksonTypeManager().getMapper();

        StringWriter writer = new StringWriter();
        mapper.writeValue(writer, provisionedResource);

        ObsProvisionedResource deserialized = mapper.readValue(writer.toString(), ObsProvisionedResource.class);

        assertNotNull(deserialized);
        assertEquals("http://local", deserialized.getEndpoint());
        assertEquals("bucket", deserialized.getBucketName());
    }

    @BeforeEach
    void setUp() {
        provisionedResource = ObsProvisionedResource.Builder.newInstance()
                .id(randomUUID().toString())
                .transferProcessId("123")
                .resourceDefinitionId(randomUUID().toString())
                .resourceName("resource")
                .endpoint("http://local")
                .bucketName("bucket")
                .build();
    }
}
