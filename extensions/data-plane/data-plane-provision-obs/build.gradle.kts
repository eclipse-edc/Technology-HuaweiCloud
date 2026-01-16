plugins {
    id("java")
}

group = "org.eclipse.edc.huawei"
version = "0.16.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    api(libs.edc.spi.dataplane)
    api(project(":extensions:common:obs:obs-core"))
    api(libs.huawei.iam)
    api(libs.huawei.obs)

    testImplementation(libs.testcontainers.junit)
    testImplementation(libs.edc.lib.json)
    testImplementation(testFixtures(project(":extensions:common:obs:obs-core")))
    testImplementation(libs.edc.junit)
    testImplementation(libs.edc.boot.lib)
}

tasks.test {
    useJUnitPlatform()
}