package com.huawei.cloud.provision.obs;

import org.eclipse.edc.spi.types.TypeManager;
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
        var mapper = new TypeManager().getMapper();

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
