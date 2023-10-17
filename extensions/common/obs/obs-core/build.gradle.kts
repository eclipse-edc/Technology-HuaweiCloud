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
