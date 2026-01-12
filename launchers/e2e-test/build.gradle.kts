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
    alias(libs.plugins.shadow)
}


dependencies {

    runtimeOnly(project(":extensions:control-plane:provision-obs"))
    runtimeOnly(project(":extensions:data-plane:data-plane-obs"))

    runtimeOnly(libs.edc.core.connector)
    runtimeOnly(libs.edc.core.dataplane)
    runtimeOnly(libs.edc.control.api.configuration)
    runtimeOnly(libs.edc.core.controlplane.apiclient)
    runtimeOnly(libs.edc.dpf.iam)
    runtimeOnly(libs.edc.ext.http)
    runtimeOnly(libs.edc.jsonld)
    runtimeOnly(libs.edc.core.token)
    runtimeOnly(libs.edc.core.controlplane)
    runtimeOnly(libs.edc.dsp)
    runtimeOnly(libs.edc.iam.mock)
    runtimeOnly(libs.edc.api.management)
    runtimeOnly(libs.edc.controlplane.edr.core.store)
    runtimeOnly(libs.edc.dpf.self.registration)
    runtimeOnly(libs.bundles.edc.dpf)
    runtimeOnly(libs.edc.api.controlplane)
    runtimeOnly(libs.edc.core.runtime)
    runtimeOnly(libs.edc.dpf.signaling.client)
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
    mergeServiceFiles()
    archiveFileName.set("hds-connector.jar")
}

application {
    mainClass.set("org.eclipse.edc.boot.system.runtime.BaseRuntime")
}

