package com.huawei.cloud.transfer.obs;

import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

import java.util.concurrent.Executors;

@Extension(ObsTransferExtension.NAME)
public class ObsTransferExtension implements ServiceExtension {

    public static final String NAME = "Huawei OBS Data Transfer Extension";

    @Inject
    private PipelineService pipelineService;
    @Inject
    private TypeManager typeManager;
    @Inject
    private Vault vault;

    @Override
    public void initialize(ServiceExtensionContext context) {

        var sourceFactory = new ObsDataSourceFactory(vault, typeManager);
        pipelineService.registerFactory(sourceFactory);

        var executor = Executors.newFixedThreadPool(10);
        var sinkFactory = new ObsDataSinkFactory(vault, typeManager, context.getMonitor(), executor);
        pipelineService.registerFactory(sinkFactory);
    }

    @Override
    public String name() {
        return NAME;
    }
}
