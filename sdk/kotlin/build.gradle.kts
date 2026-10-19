plugins {
    kotlin("jvm") version "2.1.0"
    `maven-publish`
}

group = "io.fairshare"
version = "0.1.0"

repositories {
    mavenLocal()
    mavenCentral()
}

val ktorVersion = "3.0.3"

dependencies {
    api("io.fairshare:fairshare-client:0.1.0")
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    testImplementation(kotlin("test"))
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("redis.clients:jedis:5.2.0")
}

kotlin { jvmToolchain(17) }

tasks.test { useJUnitPlatform() }
