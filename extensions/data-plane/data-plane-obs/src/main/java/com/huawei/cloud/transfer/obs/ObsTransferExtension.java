package com.huawei.cloud.transfer.obs;

import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

@Extension(ObsTransferExtension.NAME)
public class ObsTransferExtension implements ServiceExtension {

    public static final String NAME = "Huawei OBS Data Transfer Extension";

    @Inject
    private PipelineService pipelineService;

    @Override
    public void initialize(ServiceExtensionContext context) {

        var sourceFactory = new ObsDataSourceFactory();
        pipelineService.registerFactory(sourceFactory);

        var sinkFactory = new ObsDataSinkFactory();
        pipelineService.registerFactory(sinkFactory);
    }

    @Override
    public String name() {
        return NAME;
    }
}
