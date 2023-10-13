plugins {
    `java-library`
}

dependencies {
    implementation(libs.huawei.obs)
    implementation(libs.edc.spi.core)
    implementation(libs.edc.spi.dataplane)
    implementation(libs.edc.core.dataPlane.util)
    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
}
