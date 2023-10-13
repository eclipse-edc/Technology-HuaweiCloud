package com.huawei.cloud.transfer.obs;


import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;

import java.util.stream.Stream;

public class ObsDataSource implements DataSource {
    @Override
    public StreamResult<Stream<Part>> openPartStream() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
