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
    id("application")
    id("com.github.johnrengelman.shadow") version "8.1.1"
}


dependencies {

    runtimeOnly(project(":extensions:control-plane:store"))
    runtimeOnly(project(":extensions:control-plane:provision-obs"))
    runtimeOnly(project(":extensions:data-plane:data-plane-obs"))
    runtimeOnly(libs.edc.sql.pool.apachecommons)
    runtimeOnly(libs.edc.core.controlplane)
    runtimeOnly(libs.edc.core.dataplane)
    runtimeOnly(libs.edc.core.connector)
    runtimeOnly(libs.edc.config.filesystem)
    runtimeOnly(libs.edc.auth.tokenbased)

    runtimeOnly(libs.edc.api.management)
    runtimeOnly(libs.edc.api.controlplane)
    runtimeOnly(libs.edc.dpf.api)
    runtimeOnly(libs.edc.dpf.selector.api)
    runtimeOnly(libs.edc.api.management.config)
    runtimeOnly(libs.edc.api.observability)
    runtimeOnly(libs.edc.dsp)
    runtimeOnly(libs.edc.spi.jwt)
    runtimeOnly(libs.bundles.edc.dpf)
    runtimeOnly(libs.edc.iam.mock)
    runtimeOnly(libs.edc.ext.http)
    runtimeOnly(libs.edc.controlplane.callback.dispatcher.event)
    runtimeOnly(libs.edc.controlplane.callback.dispatcher.http)
    runtimeOnly(libs.edc.core.controlplane.apiclient)


    implementation(libs.edc.spi.core)
    runtimeOnly(libs.edc.core.controlplane)
    testImplementation(libs.edc.junit)
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    exclude("**/pom.properties", "**/pom.xm")
    mergeServiceFiles()
    archiveFileName.set("hds-connector.jar")
}

application {
    mainClass.set("org.eclipse.edc.boot.system.runtime.BaseRuntime")
}

