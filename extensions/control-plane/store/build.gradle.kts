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

plugins {
    // Apply the java-library plugin for API and implementation separation.
    `java-library`
}


dependencies {

    api(project(":extensions:control-plane:store:asset-index-gaussdb"))
    api(project(":extensions:control-plane:store:contract-definition-store-gaussdb"))
    api(project(":extensions:control-plane:store:contract-negotiation-store-gaussdb"))
    api(project(":extensions:control-plane:store:data-plane-instance-store-gaussdb"))
    api(project(":extensions:control-plane:store:policy-definition-store-gaussdb"))
    api(project(":extensions:control-plane:store:policy-monitor-store-gaussdb"))
    api(project(":extensions:control-plane:store:transfer-process-store-gaussdb"))
}


