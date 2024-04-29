plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)
    api(libs.edc.lib.util)
    api(libs.huawei.dws.jdbc)
    implementation(libs.failsafe.core)
    // sql libs
    implementation(libs.edc.sql.dataplane.instance)
    implementation(libs.edc.sql.core)
    implementation(libs.edc.spi.datasource.transaction)

    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(libs.edc.spi.core))
    testImplementation(testFixtures(libs.edc.dpf.selector.spi))
    testImplementation(testFixtures(project(":extensions:common:gaussdb:gaussdb-core")))

}
