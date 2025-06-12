import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    java
    kotlin("jvm") version "2.1.21"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
    id("jvm-test-suite")
}

group = "dev.gamov.streams"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

// Define versions in one place
val kafkaVersion = "3.9.0"
val flinkVersion = "1.20.1"
val confluentVersion = "7.9.1"
val avroVersion = "1.12.0"
val junitVersion = "5.12.2"
val logbackVersion = "1.5.18"
val slf4jVersion = "2.0.17"
val testcontainersVersion = "1.21.0"

// Define source sets
sourceSets {
    main {
        java {
            srcDir("src/main/java")
            srcDir("build/generated-main-avro-java")
        }
        resources {
            srcDir("src/main/resources")
        }
    }
    create("integrationTest") {
        java {
            srcDir("src/integrationTest/java")
        }
        resources {
            srcDir("src/integrationTest/resources")
        }
        compileClasspath += sourceSets.main.get().output
        runtimeClasspath += sourceSets.main.get().output
    }
}

// Create a configuration for integration tests
val integrationTestImplementation by configurations.getting {
    extendsFrom(configurations.implementation.get())
}

val integrationTestRuntimeOnly by configurations.getting {
    extendsFrom(configurations.runtimeOnly.get())
}

dependencies {
    // Kafka Streams
    implementation("org.apache.kafka:kafka-streams:$kafkaVersion")
    implementation("io.confluent:kafka-streams-avro-serde:$confluentVersion")

    // Flink
    implementation("org.apache.flink:flink-streaming-java:$flinkVersion")
    implementation("org.apache.flink:flink-avro-confluent-registry:$flinkVersion")
    implementation("org.apache.flink:flink-table-api-java:$flinkVersion")
    implementation("org.apache.flink:flink-table-runtime:$flinkVersion")
    implementation("org.apache.flink:flink-connector-kafka:3.4.0-1.20")
    implementation("org.apache.flink:flink-connector-base:$flinkVersion")
    implementation("org.apache.flink:flink-table-api-java-bridge:$flinkVersion")
    implementation("org.apache.flink:flink-table-planner-loader:$flinkVersion")
    implementation("org.apache.flink:flink-clients:$flinkVersion")

    // Avro
    implementation("org.apache.avro:avro:$avroVersion")

    // Jackson for Avro
    implementation("com.fasterxml.jackson.core:jackson-core:2.19.0")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.19.0")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.19.0")

    // Logging
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("ch.qos.logback:logback-core:$logbackVersion")
    implementation("org.fusesource.jansi:jansi:2.4.2")  // For colorful console output

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
    testImplementation("org.apache.flink:flink-test-utils:$flinkVersion")
    testImplementation("org.apache.flink:flink-runtime:$flinkVersion")
    testImplementation("org.apache.flink:flink-streaming-java:$flinkVersion:tests")

    // Integration Testing
    integrationTestImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    integrationTestRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    // Explicitly declare the test framework implementation dependencies to avoid deprecation warning
    integrationTestRuntimeOnly("org.junit.platform:junit-platform-launcher")

    // TestContainers for integration tests
    integrationTestImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    integrationTestImplementation("org.testcontainers:junit-jupiter:$testcontainersVersion")
    integrationTestImplementation("org.testcontainers:kafka:$testcontainersVersion")
    integrationTestImplementation("com.github.docker-java:docker-java-api:3.5.1")

    // Awaitility for better async testing
    integrationTestImplementation("org.awaitility:awaitility:4.3.0")

    // AssertJ for fluent assertions
    integrationTestImplementation("org.assertj:assertj-core:3.27.3")
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

// Configure common test settings
fun Test.configureTestLogging() {
    systemProperty("jansi.passthrough", "true")

    // Display test class and test name during test execution
    testLogging {
        events("started", "passed", "skipped", "failed", "standard_out", "standard_error")
        showStandardStreams = true
        showExceptions = true
        showCauses = true
        showStackTraces = true
        displayGranularity = 2
    }
}

// Configure the default test suite with explicit framework dependencies
testing {
    suites {
        // Configure the default test suite
        val test by getting(JvmTestSuite::class) {
            useJUnitJupiter(junitVersion)

            targets {
                all {
                    testTask.configure {
                        configureTestLogging()

                        // Exclude integration tests from the standard test task
                        filter {
                            excludeTestsMatching("*IntegrationTest")
                        }
                    }
                }
            }
        }
    }
}

// Configure the integration test task manually
val integrationTest = tasks.register<Test>("integrationTest") {
    description = "Runs integration tests."
    group = "verification"

    useJUnitPlatform()
    configureTestLogging()

    testClassesDirs = sourceSets["integrationTest"].output.classesDirs
    classpath = sourceSets["integrationTest"].runtimeClasspath

    shouldRunAfter(tasks.test)
}

// Add integration tests to the check task
tasks.check {
    dependsOn(integrationTest)
}

tasks.withType<Copy> {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }
}

tasks.withType<JavaExec> {
    systemProperty("jansi.passthrough", "true")
}

avro {
    stringType.set("String")
    fieldVisibility.set("PRIVATE")
}

// Avro schema generation
tasks.register<Copy>("copyAvroSchemas") {
    from("src/main/avro")
    into("build/avro")
}

tasks.named("generateAvroJava") {
    dependsOn("copyAvroSchemas")
    inputs.dir("src/main/avro")
    outputs.dir("build/generated-main-avro-java")
}

tasks.named("compileJava") {
    dependsOn("generateAvroJava")
}
