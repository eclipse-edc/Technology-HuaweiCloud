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
    api(libs.edc.sql.transferprocess)
    api(libs.edc.sql.lease)
    api(libs.huawei.dws.jdbc)

    implementation(libs.failsafe.core)
    implementation(libs.edc.sql.lib)

    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(libs.edc.spi.transfer))
    testImplementation(testFixtures(libs.edc.sql.test.fixtures))
    testImplementation(testFixtures(project(":extensions:common:gaussdb:gaussdb-core")))

}
