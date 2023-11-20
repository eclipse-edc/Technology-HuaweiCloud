package com.huawei.cloud.store.gaussdb.assetindex;

import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.spi.system.ServiceExtension;

@Extension(GaussDbContractNegotiationStoreExtension.NAME)
public class GaussDbContractNegotiationStoreExtension implements ServiceExtension {

    public static final String NAME = "Huawei GaussDB ContractNegotiation Store Extension";

    @Override
    public String name() {
        return NAME;
    }

}
