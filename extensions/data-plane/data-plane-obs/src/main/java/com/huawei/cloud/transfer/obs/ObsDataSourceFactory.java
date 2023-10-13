package com.huawei.cloud.transfer.obs;

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.jetbrains.annotations.NotNull;

public class ObsDataSourceFactory implements DataSourceFactory {
    @Override
    public boolean canHandle(DataFlowRequest request) {
        return false;
    }

    @Override
    public DataSource createSource(DataFlowRequest request) {
        return null;
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        return null;
    }
}
