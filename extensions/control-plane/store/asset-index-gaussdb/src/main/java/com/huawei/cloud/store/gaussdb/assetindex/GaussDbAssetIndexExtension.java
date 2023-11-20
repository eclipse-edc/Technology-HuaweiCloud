package com.huawei.cloud.store.gaussdb.assetindex;

import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.spi.system.ServiceExtension;

@Extension(GaussDbAssetIndexExtension.NAME)
public class GaussDbAssetIndexExtension implements ServiceExtension {

    public static final String NAME = "Huawei GaussDB Asset Index Extension";

    @Override
    public String name() {
        return NAME;
    }

}
