plugins {
    `java-library`
}

dependencies {
    implementation(libs.edc.spi.transfer)
    testImplementation(libs.edc.junit)
    testImplementation(libs.junit.jupiter.api)
    testImplementation(libs.assertj)
}
