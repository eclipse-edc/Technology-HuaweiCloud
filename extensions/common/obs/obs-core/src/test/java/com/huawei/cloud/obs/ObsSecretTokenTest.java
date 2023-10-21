package com.huawei.cloud.obs;

import org.eclipse.edc.spi.types.TypeManager;
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
        var mapper = new TypeManager().getMapper();
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
