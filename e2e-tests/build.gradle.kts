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
    testImplementation(libs.edc.junit)
    testImplementation(testFixtures(libs.edc.testfixtures.managementapi))
    testImplementation(libs.restAssured)
    testImplementation(libs.awaitility)
    testImplementation(libs.awaitility)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(project(":extensions:common:obs:obs-core")))
    testCompileOnly(project(":launchers:e2e-test"))
}

// do not publish
edcBuild {
    publish.set(false)
}
