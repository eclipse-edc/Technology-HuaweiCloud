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
    `java-test-fixtures`
    `maven-publish`
}

dependencies {
    api(libs.edc.controlplane.spi)
    api(libs.edc.spi.datasource.transaction)
    testFixturesApi(libs.huawei.dws.jdbc)
    testFixturesApi(libs.testcontainers.junit)

    testFixturesApi(libs.edc.junit)
    testFixturesApi(libs.edc.sql.core)
    testFixturesApi(libs.junit.jupiter.api)
}


