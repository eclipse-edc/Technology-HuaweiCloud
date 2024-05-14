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
    api(project(":extensions:common:obs:obs-core"))
    api(libs.huawei.obs)
    implementation(libs.edc.lib.util)
    implementation(libs.edc.spi.core)
    implementation(libs.edc.spi.validation)
    implementation(libs.edc.spi.dataplane)
    implementation(libs.edc.core.dataPlane.util)
    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(project(":extensions:common:obs:obs-core")))
}
