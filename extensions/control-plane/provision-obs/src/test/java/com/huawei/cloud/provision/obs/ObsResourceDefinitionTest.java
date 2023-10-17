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
