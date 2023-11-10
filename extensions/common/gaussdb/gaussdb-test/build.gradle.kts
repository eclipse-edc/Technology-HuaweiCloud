plugins {
    `java-library`
    `java-test-fixtures`
    `maven-publish`
}

dependencies {
    api(libs.edc.controlplane.spi)
    api(libs.huawei.dws.jdbc)
    testFixturesApi(libs.testcontainers.junit)

    testFixturesApi(libs.edc.util)
    testFixturesApi(libs.edc.junit)
    testFixturesApi(libs.edc.sql.core)
    testFixturesApi(libs.junit.jupiter.api)
}


