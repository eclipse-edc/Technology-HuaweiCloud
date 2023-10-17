plugins {
    `java-library`
}

dependencies {
    implementation(libs.edc.spi.transfer)
    api(project(":extensions:common:obs:obs-core"))
    api(libs.huawei.iam)
    api(libs.huawei.obs)

    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(project(":extensions:common:obs:obs-core")))
}
