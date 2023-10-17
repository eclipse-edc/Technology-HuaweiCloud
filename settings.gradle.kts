// this is needed to have access to snapshot builds of plugins
pluginManagement {
    repositories {
        mavenLocal()
        maven {
            url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
        }
        mavenCentral()
        gradlePluginPortal()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenLocal()
        maven {
            url = uri("https://oss.sonatype.org/content/repositories/snapshots/")
        }
        mavenCentral()
    }
}

rootProject.name = "components"
include(":launchers")
include(":extensions")
include(":extensions:common:obs:obs-core")
include(":extensions:control-plane:provision-obs")
include(":extensions:data-plane:data-plane-obs")
include(":e2e-tests")
