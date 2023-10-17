plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.transfer)
    implementation(libs.huawei.obs)
    implementation(libs.edc.spi.core)
    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
}
