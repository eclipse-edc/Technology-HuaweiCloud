plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)
    api(libs.edc.util)
    api(libs.edc.sql.contractdefstore)
    implementation(libs.failsafe.core)
    implementation(libs.edc.sql.core)

    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(libs.edc.spi.contract))
    testImplementation(testFixtures(project(":extensions:common:gaussdb:gaussdb-test")))

}
