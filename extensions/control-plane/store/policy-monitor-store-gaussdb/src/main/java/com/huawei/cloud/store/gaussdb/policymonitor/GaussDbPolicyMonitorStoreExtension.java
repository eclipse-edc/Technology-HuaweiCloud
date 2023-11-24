package com.huawei.cloud.store.gaussdb.policymonitor;

import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.spi.system.ServiceExtension;

import static com.huawei.cloud.store.gaussdb.policymonitor.GaussDbPolicyMonitorStoreExtension.NAME;

@Extension(NAME)
public class GaussDbPolicyMonitorStoreExtension implements ServiceExtension {
    public static final String NAME = "Huawei GaussDB PolicyMonitor Store Extension";

    @Override
    public String name() {
        return NAME;
    }

}
