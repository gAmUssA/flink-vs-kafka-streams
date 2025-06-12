# =======================================================================
# 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟
#
#  ███████╗██╗     ██╗███╗   ██╗██╗  ██╗    ██╗   ██╗███████╗
#  ██╔════╝██║     ██║████╗  ██║██║ ██╔╝    ██║   ██║██╔════╝
#  █████╗  ██║     ██║██╔██╗ ██║█████╔╝     ██║   ██║███████╗
#  ██╔══╝  ██║     ██║██║╚██╗██║██╔═██╗     ╚██╗ ██╔╝╚════██║
#  ██║     ███████╗██║██║ ╚████║██║  ██╗     ╚████╔╝ ███████║
#  ╚═╝     ╚══════╝╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝      ╚═══╝  ╚══════╝
#
#  ██╗  ██╗ █████╗ ███████╗██╗  ██╗ █████╗     
#  ██║ ██╔╝██╔══██╗██╔════╝██║ ██╔╝██╔══██╗    
#  █████╔╝ ███████║█████╗  █████╔╝ ███████║    
#  ██╔═██╗ ██╔══██║██╔══╝  ██╔═██╗ ██╔══██║    
#  ██║  ██╗██║  ██║██║     ██║  ██╗██║  ██║    
#  ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝    
#
#  ███████╗████████╗██████╗ ███████╗ █████╗ ███╗   ███╗███████╗
#  ██╔════╝╚══██╔══╝██╔══██╗██╔════╝██╔══██╗████╗ ████║██╔════╝
#  ███████╗   ██║   ██████╔╝█████╗  ███████║██╔████╔██║███████╗
#  ╚════██║   ██║   ██╔══██╗██╔══╝  ██╔══██║██║╚██╔╝██║╚════██║
#  ███████║   ██║   ██║  ██║███████╗██║  ██║██║ ╚═╝ ██║███████║
#  ╚══════╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═╝     ╚═╝╚══════╝
#                                                                
# 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟 🌟
# =======================================================================

# Makefile for Flink vs Kafka Streams Data Generator

# Color definitions
RESET := \033[0m
BOLD := \033[1m
RED := \033[31m
GREEN := \033[32m
YELLOW := \033[33m
BLUE := \033[34m
MAGENTA := \033[35m
CYAN := \033[36m
WHITE := \033[37m
BG_BLUE := \033[44m

# Variables
JAR_FILE = build/libs/flink-vs-kafka-streams-1.0-SNAPSHOT.jar
MAIN_CLASS = dev.gamov.streams.generator.DataGeneratorApp
BOOTSTRAP_SERVERS = localhost:64671
SCHEMA_REGISTRY = http://localhost:65154

# Default values for data generation
CLICK_COUNT = 100
PAGE_COUNT = 10
USER_COUNT = 10
PARTITIONS = 1
REPLICATION_FACTOR = 1

# Help target
.PHONY: help
help:
	@echo "$(BOLD)$(BG_BLUE)🚀 Flink vs Kafka Streams Data Generator Makefile 🚀$(RESET)"
	@echo ""
	@echo "$(BOLD)$(YELLOW)📋 Usage:$(RESET)"
	@echo "  $(BOLD)$(GREEN)make build$(RESET)              - 🔨 Build the application"
	@echo "  $(BOLD)$(RED)make clean$(RESET)              - 🧹 Clean build artifacts"
	@echo "  $(BOLD)$(BLUE)make run$(RESET)                - ▶️  Run the data generator with default settings"
	@echo "  $(BOLD)$(BLUE)make run-custom$(RESET)         - ⚙️  Run with custom settings (use with parameters)"
	@echo "  $(BOLD)$(BLUE)make run-large$(RESET)          - 📊 Generate a large dataset (100 clicks, 10 users, 8 pages)"
	@echo "  $(BOLD)$(BLUE)make run-no-topics$(RESET)      - 🚫 Run without creating topics"
	@echo "  $(BOLD)$(CYAN)make help$(RESET)               - ❓ Show this help message"
	@echo ""
	@echo "$(BOLD)$(MAGENTA)🔧 Parameters for run-custom:$(RESET)"
	@echo "  $(YELLOW)BOOTSTRAP_SERVERS$(RESET)       - Kafka bootstrap servers (default: $(CYAN)localhost:9092$(RESET))"
	@echo "  $(YELLOW)SCHEMA_REGISTRY$(RESET)         - Schema Registry URL (default: $(CYAN)http://localhost:8081$(RESET))"
	@echo "  $(YELLOW)CLICK_COUNT$(RESET)             - Number of clicks to generate (default: $(CYAN)10$(RESET))"
	@echo "  $(YELLOW)PAGE_COUNT$(RESET)              - Number of pages to generate (default: $(CYAN)5$(RESET))"
	@echo "  $(YELLOW)USER_COUNT$(RESET)              - Number of users to generate (default: $(CYAN)3$(RESET))"
	@echo "  $(YELLOW)PARTITIONS$(RESET)              - Number of partitions for topics (default: $(CYAN)1$(RESET))"
	@echo "  $(YELLOW)REPLICATION_FACTOR$(RESET)      - Replication factor for topics (default: $(CYAN)1$(RESET))"
	@echo ""
	@echo "$(BOLD)$(GREEN)💡 Examples:$(RESET)"
	@echo "  $(CYAN)make run-custom CLICK_COUNT=50 USER_COUNT=5$(RESET)"
	@echo "  $(CYAN)make run-custom BOOTSTRAP_SERVERS=kafka1:9092,kafka2:9092 SCHEMA_REGISTRY=http://schema-registry:8081$(RESET)"

# 🔨 Build the application
.PHONY: build
build:
	@echo "$(BOLD)$(GREEN)🔨 Building the application...$(RESET)"
	@./gradlew clean build -x integrationTest
	@echo "$(BOLD)$(GREEN)✅ Build completed successfully!$(RESET)"

# 🧹 Clean build artifacts
.PHONY: clean
clean:
	@echo "$(BOLD)$(RED)🧹 Cleaning build artifacts...$(RESET)"
	@./gradlew clean
	@echo "$(BOLD)$(RED)✅ Clean completed successfully!$(RESET)"

# ▶️ Run the data generator with default settings
.PHONY: run
run: build
	@echo "$(BOLD)$(BLUE)▶️ Running data generator with default settings...$(RESET)"
	@./gradlew run
	@echo "$(BOLD)$(BLUE)✅ Data generation completed!$(RESET)"

# ⚙️ Run with custom settings
.PHONY: run-custom
run-custom: build
	@echo "$(BOLD)$(BLUE)⚙️ Running data generator with custom settings...$(RESET)"
	@echo "$(CYAN)   Click Count: $(CLICK_COUNT)$(RESET)"
	@echo "$(CYAN)   Page Count: $(PAGE_COUNT)$(RESET)"
	@echo "$(CYAN)   User Count: $(USER_COUNT)$(RESET)"
	@./gradlew run --args="-b $(BOOTSTRAP_SERVERS) -s $(SCHEMA_REGISTRY) -c $(CLICK_COUNT) -p $(PAGE_COUNT) -u $(USER_COUNT) --partitions=$(PARTITIONS) --replication-factor=$(REPLICATION_FACTOR)"
	@echo "$(BOLD)$(BLUE)✅ Custom data generation completed!$(RESET)"

# 📊 Generate a large dataset
.PHONY: run-large
run-large: build
	@echo "$(BOLD)$(MAGENTA)📊 Generating large dataset (100 clicks, 10 users, 8 pages)...$(RESET)"
	@./gradlew run --args="-c 100 -u 10 -p 8"
	@echo "$(BOLD)$(MAGENTA)✅ Large dataset generation completed!$(RESET)"

# 🚫 Run without creating topics
.PHONY: run-no-topics
run-no-topics: build
	@echo "$(BOLD)$(YELLOW)🚫 Running without creating Kafka topics...$(RESET)"
	@./gradlew run --args="--create-topics=false"
	@echo "$(BOLD)$(YELLOW)✅ Data generation without topic creation completed!$(RESET)"
