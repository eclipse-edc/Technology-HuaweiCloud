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

// this is needed to have access to snapshot builds of plugins
pluginManagement {
    repositories {
        mavenLocal()
        maven {
            url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
        }
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenLocal()
        maven {
            url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
        }
        mavenCentral()
    }
}

rootProject.name = "components"
include(":launchers:e2e-test")
include(":launchers:huawei-cloud-runtime")
include(":extensions")
include(":extensions:common:obs:obs-core")
include(":extensions:control-plane:provision-obs")
include(":extensions:data-plane:data-plane-obs")

// GaussDB
include(":extensions:control-plane:store:asset-index-gaussdb")
include(":extensions:control-plane:store:contract-definition-store-gaussdb")
include(":extensions:control-plane:store:contract-negotiation-store-gaussdb")
include(":extensions:control-plane:store:transfer-process-store-gaussdb")
include(":extensions:control-plane:store:data-plane-instance-store-gaussdb")
include(":extensions:control-plane:store:policy-monitor-store-gaussdb")
include(":extensions:common:gaussdb:gaussdb-core")
include(":extensions:control-plane:store:policy-definition-store-gaussdb")
include(":e2e-tests")
