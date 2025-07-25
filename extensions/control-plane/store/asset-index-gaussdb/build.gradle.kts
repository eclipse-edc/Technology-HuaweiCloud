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
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)
    api(libs.edc.lib.util)
    api(libs.huawei.dws.jdbc)
    implementation(libs.failsafe.core)
    // sql libs
    implementation(libs.edc.sql.assetindex)
    implementation(libs.edc.sql.lib)
    implementation(libs.edc.spi.datasource.transaction)

    testImplementation(libs.edc.junit)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(libs.edc.spi.core))
    testImplementation(testFixtures(project(":extensions:common:gaussdb:gaussdb-core")))

}
