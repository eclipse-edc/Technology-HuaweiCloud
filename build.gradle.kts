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

val hcScmUrl: String by project
val hcScmConnection: String by project


buildscript {
    dependencies {
        val version = "0.14.0-SNAPSHOT"
        classpath("org.eclipse.edc.edc-build:org.eclipse.edc.edc-build.gradle.plugin:${version}")
    }
}

allprojects {
    apply(plugin = "org.eclipse.edc.edc-build")

    configure<org.eclipse.edc.plugins.edcbuild.extensions.BuildExtension> {

        pom {
            scmUrl.set(hcScmConnection)
            scmConnection.set(hcScmConnection)
            groupId = project.group.toString()
        }
    }

    configure<CheckstyleExtension> {
        configFile = rootProject.file("resources/checkstyle-config.xml")
        configDirectory.set(rootProject.file("resources"))
    }

}
