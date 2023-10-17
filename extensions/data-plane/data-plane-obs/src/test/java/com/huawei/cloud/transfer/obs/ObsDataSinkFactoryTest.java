package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import org.eclipse.edc.junit.assertions.AbstractResultAssert;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.huawei.cloud.transfer.obs.TestFunctions.dataAddressWithCredentials;
import static com.huawei.cloud.transfer.obs.TestFunctions.dataAddressWithoutCredentials;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObsDataSinkFactoryTest {

    private final Vault vaultMock = mock();
    private final ExecutorService executor = Executors.newFixedThreadPool(1);
    private final ObsDataSinkFactory factory = new ObsDataSinkFactory(vaultMock, new TypeManager(), mock(), executor);

    @BeforeEach
    void setUp() {
    }

    @Test
    void canHandle() {
        var dataAddress = DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE).build();
        assertThat(factory.canHandle(createRequest(dataAddress))).isTrue();

        var dataAddress2 = DataAddress.Builder.newInstance().type("invalid-type").build();
        assertThat(factory.canHandle(createRequest(dataAddress2))).isFalse();
    }

    @Test
    void validate_success() {
        var addr = DataAddress.Builder.newInstance()
                .type(ObsBucketSchema.TYPE)
                .keyName("test-keyname")
                .property(ObsBucketSchema.BUCKET_NAME, "test-bucket")
                .property(ObsBucketSchema.ENDPOINT, "test-endpoint")
                .build();
        AbstractResultAssert.assertThat(factory.validateRequest(createRequest(addr))).isSucceeded();
    }

    @Test
    void validate_whenBucketNameMissing_shouldFail() {
        var addr = DataAddress.Builder.newInstance()
                .type(ObsBucketSchema.TYPE)
                .keyName("test-keyname")
                .property(ObsBucketSchema.ENDPOINT, "test-endpoint")
                .build();
        AbstractResultAssert.assertThat(factory.validateRequest(createRequest(addr)))
                .isFailed()
                .detail().contains("Missing or invalid value for key %s".formatted(ObsBucketSchema.BUCKET_NAME));
    }

    @Test
    void validate_whenEndpointMissing_shouldFail() {
        var addr = DataAddress.Builder.newInstance()
                .type(ObsBucketSchema.TYPE)
                .keyName("test-keyname")
                .property(ObsBucketSchema.BUCKET_NAME, "test-bucket")
                .build();
        AbstractResultAssert.assertThat(factory.validateRequest(createRequest(addr)))
                .isFailed()
                .detail().contains("Missing or invalid value for key %s".formatted(ObsBucketSchema.ENDPOINT));
    }

    @Test
    void createSink_whenNotValid() {
        var addr = DataAddress.Builder.newInstance()
                .type(ObsBucketSchema.TYPE)
                .keyName("test-keyname")
                .property(ObsBucketSchema.BUCKET_NAME, "test-bucket")
                .build();
        assertThatThrownBy(() -> factory.createSink(createRequest(addr))).isInstanceOf(EdcException.class)
                .hasMessage("Missing or invalid value for key %s".formatted(ObsBucketSchema.ENDPOINT));
    }

    @Test
    void createSink_credentialsOnDataAddress() {
        var dest = dataAddressWithCredentials();
        var sink = factory.createSink(createRequest(dest));
        assertThat(sink).isNotNull().isInstanceOf(ObsDataSink.class);
        verify(vaultMock).resolveSecret(eq(dest.getKeyName()));
    }

    @Test
    void createSink_credentialsFromVault() {
        var dest = dataAddressWithoutCredentials();
        when(vaultMock.resolveSecret("aKey")).thenReturn("""
                {
                  "ak": "test-ak",
                  "sk": "test-sk"
                }
                """);
        var sink = factory.createSink(createRequest(dest));
        assertThat(sink).isNotNull().isInstanceOf(ObsDataSink.class);
        verify(vaultMock).resolveSecret(eq(dest.getKeyName()));
    }

    @Disabled("Untestable, as there is no (easy) way to mock the env var access")
    @Test
    void createSink_credentialsFromEnv() {
        // no-op
    }

    private DataFlowRequest createRequest(DataAddress destination) {
        return DataFlowRequest.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE).build())
                .destinationDataAddress(destination)
                .build();
    }
}