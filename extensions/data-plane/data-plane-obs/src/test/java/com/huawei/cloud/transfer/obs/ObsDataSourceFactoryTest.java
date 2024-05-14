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

package com.huawei.cloud.transfer.obs;

import com.huawei.cloud.obs.ObsBucketSchema;
import com.huawei.cloud.obs.ObsClientProvider;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.UUID;
import java.util.stream.Stream;

import static com.huawei.cloud.obs.TestFunctions.VALID_ACCESS_KEY_ID;
import static com.huawei.cloud.obs.TestFunctions.VALID_BUCKET_NAME;
import static com.huawei.cloud.obs.TestFunctions.VALID_ENDPOINT;
import static com.huawei.cloud.obs.TestFunctions.VALID_SECRET_ACCESS_KEY;
import static com.huawei.cloud.obs.TestFunctions.dataAddressWithCredentials;
import static com.huawei.cloud.obs.TestFunctions.dataAddressWithoutCredentials;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObsDataSourceFactoryTest {
    public static final ObsClientProvider CLIENT_PROVIDER = mock();
    private final TypeManager typeManager = new JacksonTypeManager();
    private final Vault vault = mock(Vault.class);
    private final ObsDataSourceFactory factory = new ObsDataSourceFactory(vault, typeManager, CLIENT_PROVIDER);

    @Test
    void canHandle() {
        var dataAddress = DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE).build();

        var result = factory.canHandle(createRequest(dataAddress));

        assertThat(result).isTrue();
    }

    @Test
    void canHandle_unexpectedType() {
        var dataAddress = DataAddress.Builder.newInstance().type("any").build();

        var result = factory.canHandle(createRequest(dataAddress));

        assertThat(result).isFalse();
    }

    @Test
    void validate_validProperties() {
        var source = dataAddressWithCredentials();
        var request = createRequest(source);

        var result = factory.validateRequest(request);

        assertThat(result.succeeded()).isTrue();
    }

    @ParameterizedTest
    @ArgumentsSource(InvalidInputs.class)
    void validate_mandatoryPropertyMissing_shouldFail(String bucketName, String endpoint, String accessKeyId, String secretAccessKey) {
        var source = DataAddress.Builder.newInstance()
                .type(ObsBucketSchema.TYPE)
                .property(ObsBucketSchema.BUCKET_NAME, bucketName)
                .property(ObsBucketSchema.ENDPOINT, endpoint)
                .property(ObsBucketSchema.ACCESS_KEY_ID, accessKeyId)
                .property(ObsBucketSchema.SECRET_ACCESS_KEY, secretAccessKey)
                .build();

        var request = createRequest(source);

        var result = factory.validateRequest(request);

        assertThat(result.failed()).isTrue();
    }

    @Test
    void createSource_credentialsOnDataAddress() {
        var source = dataAddressWithCredentials();
        var request = createRequest(source);
        var dataSource = factory.createSource(request);
        assertThat(dataSource).isNotNull().isInstanceOf(ObsDataSource.class);
    }


    @Test
    void createSource_credentialsFromVault() {
        var source = dataAddressWithoutCredentials();
        when(vault.resolveSecret("aKey")).thenReturn("""
                {
                  "edctype": "dataspaceconnector:obssecrettoken",
                  "ak": "test-ak",
                  "sk": "test-sk"
                }
                """);

        var req = createRequest(source);
        var src = factory.createSource(req);

        assertThat(src).isNotNull().isInstanceOf(ObsDataSource.class);
        verify(vault).resolveSecret(eq(source.getKeyName()));
    }

    @Disabled("Untestable, as there is no (easy) way to mock the env var access")
    @Test
    void createSource_credentialsFromEnv() {
        var source = dataAddressWithoutCredentials();

        var req = createRequest(source);
        var src = factory.createSource(req);

        assertThat(src).isNotNull().isInstanceOf(ObsDataSource.class);
    }

    /**
     * It is imperative that no OBS_ACCESS_KEY_ID or OBS_SECRET_ACCESS_KEY env vars are set for this test!
     */
    @Test
    void createSource_noCredentials_shouldFail() {
        var source = dataAddressWithoutCredentials();
        var req = createRequest(source);
        when(CLIENT_PROVIDER.obsClient(anyString(), any())).thenThrow(IllegalArgumentException.class);
        assertThatThrownBy(() -> factory.createSource(req)).isInstanceOf(IllegalArgumentException.class);
    }

    private DataFlowStartMessage createRequest(DataAddress source) {
        return DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(source)
                .destinationDataAddress(DataAddress.Builder.newInstance().type(ObsBucketSchema.TYPE).build())
                .build();
    }

    private static class InvalidInputs implements ArgumentsProvider {

        @Override
        public Stream<? extends Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(VALID_BUCKET_NAME, " ", VALID_ACCESS_KEY_ID, VALID_SECRET_ACCESS_KEY),
                    Arguments.of(" ", VALID_ENDPOINT, VALID_ACCESS_KEY_ID, VALID_SECRET_ACCESS_KEY)
            );
        }
    }
}