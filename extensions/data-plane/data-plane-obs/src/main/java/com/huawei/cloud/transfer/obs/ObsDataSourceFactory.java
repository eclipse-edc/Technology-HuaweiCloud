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
import com.huawei.cloud.transfer.obs.validation.ObsDataAddressValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.Validator;
import org.jetbrains.annotations.NotNull;

import static com.huawei.cloud.obs.ObsBucketSchema.BUCKET_NAME;
import static com.huawei.cloud.obs.ObsBucketSchema.KEY_PREFIX;

public class ObsDataSourceFactory extends ObsFactory implements DataSourceFactory {

    private final Validator<DataAddress> validation = new ObsDataAddressValidator();

    public ObsDataSourceFactory(Vault vault, TypeManager typeManager, ObsClientProvider clientProvider) {
        super(vault, typeManager, clientProvider);
    }

    @Override
    public boolean canHandle(DataFlowStartMessage request) {
        return ObsBucketSchema.TYPE.equals(request.getSourceDataAddress().getType());
    }

    @Override
    public DataSource createSource(DataFlowStartMessage request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var source = request.getSourceDataAddress();

        return ObsDataSource.Builder.newInstance()
                .bucketName(source.getStringProperty(BUCKET_NAME))
                .client(createObsClient(source))
                .keyPrefix(source.getStringProperty(KEY_PREFIX, null))
                .build();
    }


    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage request) {
        var source = request.getSourceDataAddress();

        return validation.validate(source).toResult();
    }

}
