package com.huawei.cloud.store.gaussdb.contractdefinition;


import org.eclipse.edc.connector.controlplane.store.sql.contractdefinition.schema.ContractDefinitionStatements;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.spi.system.ServiceExtension;

import static com.huawei.cloud.store.gaussdb.contractdefinition.GaussDbContractDefinitionStoreExtension.NAME;

@Extension(NAME)
public class GaussDbContractDefinitionStoreExtension implements ServiceExtension {

    public static final String NAME = "Huawei GaussDB ContractDefinition Store Extension";

    @Provider
    public ContractDefinitionStatements createGaussDbContractDefStore() {
        return new GaussDbStatements();
    }

    @Override
    public String name() {
        return NAME;
    }
}
