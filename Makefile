# Traffic Monitor - Makefile
# Only non-cargo tasks (service management, deployment, database operations)

# Project configuration
PROJECT_NAME := traffic-monitor
PROJECT_DIR := $(shell pwd)
BINARY := target/release/$(PROJECT_NAME)
INSTALL_DIR := /usr/local/bin

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[1;33m
BLUE := \033[0;34m
CYAN := \033[0;36m
NC := \033[0m

# Default target
.DEFAULT_GOAL := help

# ============================================
# Service Management (cargo cannot do this)
# ============================================

start: ## Start server in background
	@echo "$(BLUE)Starting server...$(NC)"
	@./run.sh start

stop: ## Stop the server
	@echo "$(BLUE)Stopping server...$(NC)"
	@./run.sh stop

restart: ## Restart the server
	@echo "$(BLUE)Restarting server...$(NC)"
	@./run.sh restart

status: ## Check server status
	@./run.sh status

logs: ## View server logs
	@tail -f $(PROJECT_DIR)/logs/traffic_monitor.log

# ============================================
# System Installation (cargo cannot do this)
# ============================================

install: ## Install binary to system
	@echo "$(BLUE)Installing binary to $(INSTALL_DIR)...$(NC)"
	@if [ ! -f $(BINARY) ]; then \
		echo "$(RED)Error: Binary not found. Run 'cargo build --release' first$(NC)"; \
		exit 1; \
	fi
	@sudo cp $(BINARY) $(INSTALL_DIR)/
	@sudo chmod +x $(INSTALL_DIR)/$(PROJECT_NAME)
	@echo "$(GREEN)✓ Binary installed to $(INSTALL_DIR)/$(PROJECT_NAME)$(NC)"

uninstall: ## Remove binary from system
	@echo "$(BLUE)Uninstalling binary...$(NC)"
	@sudo rm -f $(INSTALL_DIR)/$(PROJECT_NAME)
	@echo "$(GREEN)✓ Binary removed$(NC)"

# ============================================
# Database Operations (cargo cannot do this)
# ============================================

db-stats: ## Show database statistics
	@echo "$(BLUE)Database Statistics:$(NC)"
	@sqlite3 $(PROJECT_DIR)/data/traffic_monitor.db \
		"SELECT 'Raw data count:', COUNT(*) FROM traffic_history; \
		 SELECT '10s aggregated count:', COUNT(*) FROM traffic_history_10s; \
		 SELECT '1m aggregated count:', COUNT(*) FROM traffic_history_1m; \
		 SELECT 'Namespace count:', COUNT(DISTINCT namespace) FROM traffic_history;" \
		|| echo "$(YELLOW)Database not found or empty$(NC)"

db-clean: ## Clean old database records
	@echo "$(BLUE)Cleaning old database records...$(NC)"
	@sqlite3 $(PROJECT_DIR)/data/traffic_monitor.db \
		"DELETE FROM traffic_history WHERE timestamp_ms < (strftime('%s','now') - 300) * 1000; \
		 DELETE FROM traffic_history_10s WHERE timestamp_ms < (strftime('%s','now') - 3600) * 1000; \
		 DELETE FROM traffic_history_1m WHERE timestamp_ms < (strftime('%s','now') - 10800) * 1000;" \
		&& echo "$(GREEN)✓ Database cleaned$(NC)" \
		|| echo "$(YELLOW)Database not found$(NC)"

db-backup: ## Backup database
	@echo "$(BLUE)Backing up database...$(NC)"
	@mkdir -p $(PROJECT_DIR)/backups
	@cp $(PROJECT_DIR)/data/traffic_monitor.db \
		$(PROJECT_DIR)/backups/traffic_monitor_$$(date +%Y%m%d_%H%M%S).db \
		&& echo "$(GREEN)✓ Database backed up$(NC)" \
		|| echo "$(YELLOW)Database not found$(NC)"



# ============================================
# Development Helpers (convenience wrappers)
# ============================================

dev: ## Run in development mode with auto-reload (requires cargo-watch)
	@echo "$(BLUE)Running in development mode...$(NC)"
	@cargo watch -x run

test-api: ## Run API tests
	@echo "$(BLUE)Running API tests...$(NC)"
	@./test_api.sh

# ============================================
# Information
# ============================================

info: ## Show project information
	@echo "$(CYAN)Project Information:$(NC)"
	@echo "  Project:     $(PROJECT_NAME)"
	@echo "  Directory:   $(PROJECT_DIR)"
	@echo "  Binary:      $(BINARY)"
	@echo "  Install dir: $(INSTALL_DIR)"
	@echo ""
	@echo "$(CYAN)Rust Information:$(NC)"
	@cargo --version
	@rustc --version
	@echo ""
	@echo "$(CYAN)Binary Size:$(NC)"
	@if [ -f $(BINARY) ]; then \
		ls -lh $(BINARY) | awk '{print "  Size: " $$5}'; \
	else \
		echo "  Binary not built yet. Run: cargo build --release"; \
	fi

# ============================================
# Help
# ============================================

help: ## Show this help message
	@echo "$(CYAN)Traffic Monitor - Makefile Commands$(NC)"
	@echo ""
	@echo "$(YELLOW)Note:$(NC) This Makefile only handles non-cargo tasks."
	@echo "For build/test/fmt commands, use cargo directly:"
	@echo ""
	@echo "$(CYAN)Cargo Commands:$(NC)"
	@echo "  cargo build              # Debug build"
	@echo "  cargo build --release    # Release build"
	@echo "  cargo test               # Run tests"
	@echo "  cargo fmt                # Format code"
	@echo "  cargo clippy             # Run linter"
	@echo "  cargo doc --open         # Generate docs"
	@echo "  cargo clean              # Clean artifacts"
	@echo ""
	@echo "$(CYAN)Service Management:$(NC)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(CYAN)%-15s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(YELLOW)Examples:$(NC)"
	@echo "  cargo build --release    # Build optimized binary"
	@echo "  make start               # Start the server"
	@echo "  make logs                # View logs"
	@echo "  make db-stats            # Check database"
	@echo ""

.PHONY: start stop restart status logs install uninstall \
        db-stats db-clean db-backup \
        dev test-api info help
