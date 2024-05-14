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
