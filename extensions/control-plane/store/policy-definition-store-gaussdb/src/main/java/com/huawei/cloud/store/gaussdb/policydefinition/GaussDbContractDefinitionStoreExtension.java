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

package com.huawei.cloud.store.gaussdb.policydefinition;


import org.eclipse.edc.connector.controlplane.store.sql.policydefinition.store.schema.SqlPolicyStoreStatements;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.spi.system.ServiceExtension;

@Extension(GaussDbContractDefinitionStoreExtension.NAME)
public class GaussDbContractDefinitionStoreExtension implements ServiceExtension {

    public static final String NAME = "Huawei GaussDB ContractDefinition Store Extension";

    @Provider
    public SqlPolicyStoreStatements createGaussDbContractDefStore() {
        return new GaussDbStatements();
    }

    @Override
    public String name() {
        return NAME;
    }
}
