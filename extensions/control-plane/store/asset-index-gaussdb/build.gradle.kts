plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)
    api(libs.edc.util)
    api(libs.huawei.dws.jdbc)
    implementation(libs.failsafe.core)
    // sql libs
    implementation(libs.edc.sql.assetindex)
    implementation(libs.edc.sql.core)
    implementation(libs.edc.spi.datasource.transaction)

    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(libs.edc.spi.core))
    testImplementation(testFixtures(project(":extensions:common:gaussdb:gaussdb-core")))

}
