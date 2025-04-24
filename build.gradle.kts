plugins {
    java
    kotlin("jvm") version "1.9.0"
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1"
}

group = "com.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

val kafkaVersion = "3.4.0"
val flinkVersion = "1.20.0"
val confluentVersion = "7.5.0"
val avroVersion = "1.11.1"
val junitVersion = "5.9.2"
val logbackVersion = "1.5.18"
val slf4jVersion = "2.0.9"

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

    // Avro
    implementation("org.apache.avro:avro:$avroVersion")

    // Jackson for Avro
    implementation("com.fasterxml.jackson.core:jackson-core:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.15.2")
    implementation("com.fasterxml.jackson.core:jackson-annotations:2.15.2")

    // Logging
    implementation("org.slf4j:slf4j-api:$slf4jVersion")
    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("ch.qos.logback:logback-core:$logbackVersion")
    implementation("org.fusesource.jansi:jansi:2.4.1")  // For colorful console output

    // Testing
    testImplementation("org.junit.jupiter:junit-jupiter-api:$junitVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:$junitVersion")
    testImplementation("org.apache.kafka:kafka-streams-test-utils:$kafkaVersion")
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}

tasks.withType<Test> {
    useJUnitPlatform()
    systemProperty("jansi.passthrough", "true")
}

tasks.withType<Copy> {
    duplicatesStrategy = DuplicatesStrategy.INCLUDE
}

tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
    kotlinOptions {
        jvmTarget = "11"
    }
}

tasks.withType<JavaExec> {
    systemProperty("jansi.passthrough", "true")
}

avro {
    stringType.set("String")
    fieldVisibility.set("PRIVATE")
}

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
}

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
