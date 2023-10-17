plugins {
    `java-library`
}

dependencies {
    testImplementation(libs.edc.junit)
    testImplementation(testFixtures(libs.edc.testfixtures.managementapi))
    testImplementation(libs.restAssured)
    testImplementation(libs.awaitility)
    testImplementation(libs.awaitility)
    testImplementation(libs.testcontainers.junit)
    testImplementation(testFixtures(project(":extensions:data-plane:data-plane-obs")))
}

// do not publish
edcBuild {
    publish.set(false)
}
