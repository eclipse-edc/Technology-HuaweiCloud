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
}

dependencies {
    api(libs.edc.spi.transfer)
    implementation(libs.huawei.obs)
    implementation(libs.huawei.iam)
    implementation(libs.edc.spi.core)
    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testFixturesApi(libs.huawei.obs)
    testFixturesApi(libs.huawei.iam)
    testFixturesApi(libs.edc.junit)
    testFixturesApi(libs.junit.jupiter.api)
}
