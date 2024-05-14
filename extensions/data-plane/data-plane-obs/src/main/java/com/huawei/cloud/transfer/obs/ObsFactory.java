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

import com.huawei.cloud.obs.ObsClientProvider;
import com.huawei.cloud.obs.ObsSecretToken;
import com.huawei.cloud.transfer.obs.validation.ObsDataAddressCredentialsValidator;
import com.obs.services.BasicObsCredentialsProvider;
import com.obs.services.EnvironmentVariableObsCredentialsProvider;
import com.obs.services.IObsCredentialsProvider;
import com.obs.services.ObsClient;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.Validator;

import static com.huawei.cloud.obs.ObsBucketSchema.ACCESS_KEY_ID;
import static com.huawei.cloud.obs.ObsBucketSchema.ENDPOINT;
import static com.huawei.cloud.obs.ObsBucketSchema.SECRET_ACCESS_KEY;

public abstract class ObsFactory {
    private final Validator<DataAddress> credentials = new ObsDataAddressCredentialsValidator();
    private final Vault vault;
    private final TypeManager typeManager;
    private final ObsClientProvider clientProvider;

    protected ObsFactory(Vault vault, TypeManager typeManager, ObsClientProvider clientProvider) {
        this.vault = vault;
        this.typeManager = typeManager;
        this.clientProvider = clientProvider;
    }

    protected ObsClient createObsClient(DataAddress dataAddress) {
        var endpoint = dataAddress.getStringProperty(ENDPOINT);
        var secret = vault.resolveSecret(dataAddress.getKeyName());
        IObsCredentialsProvider provider;

        if (secret != null) { // AK/SK was stored in vault ->interpret secret as JSON
            var token = typeManager.readValue(secret, ObsSecretToken.class);
            provider = new BasicObsCredentialsProvider(token.ak(), token.sk(), token.securityToken());
        } else if (credentials.validate(dataAddress).succeeded()) { //AK and SK are stored directly on data address
            var ak = dataAddress.getStringProperty(ACCESS_KEY_ID);
            var sk = dataAddress.getStringProperty(SECRET_ACCESS_KEY);
            provider = new BasicObsCredentialsProvider(ak, sk);
        } else { // no credentials provided, assume there are env vars
            provider = new EnvironmentVariableObsCredentialsProvider();
        }

        return clientProvider.obsClient(endpoint, provider);
    }
}
