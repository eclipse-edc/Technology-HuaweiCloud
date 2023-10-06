plugins {
    `java-library`
}

dependencies {
    testImplementation(libs.edc.junit)
    testImplementation(testFixtures(libs.edc.testfixtures.managementapi))
    testImplementation(libs.restAssured)
    testImplementation(libs.awaitility)
    testImplementation(libs.awaitility)
}

// do not publish
edcBuild {
    publish.set(false)
}
