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
    testImplementation(testFixtures(project(":extensions:common:obs:obs-core")))
    testCompileOnly(project(":launchers"))
}

// do not publish
edcBuild {
    publish.set(false)
}
