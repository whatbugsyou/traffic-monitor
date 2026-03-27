#!/bin/bash
# Traffic Monitor - Run Script

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Project directories
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_NAME="traffic-monitor"
PID_DIR="$PROJECT_DIR/logs"
PID_FILE="$PID_DIR/traffic_monitor.pid"
LOG_FILE="$PROJECT_DIR/logs/traffic_monitor.log"

# Display help information
show_help() {
    echo "Traffic Monitor"
    echo
    echo "Usage: $0 {start|stop|status|restart|build}"
    echo
    echo "Subcommands:"
    echo "  start    Start the server"
    echo "  stop     Stop the server"
    echo "  status   Check server status"
    echo "  restart  Restart the server"
    echo "  build    Build the server"
    echo "  -h       Show this help message"
    echo
    echo "Examples:"
    echo "  $0 start    # Start the server"
    echo "  $0 stop     # Stop the server"
    echo "  $0 status   # Check if running"
    echo "  $0 build    # Build the binary"
}

# Parse command line arguments
parse_args() {
    COMMAND=""
    while [[ $# -gt 0 ]]; do
        case "$1" in
            start|stop|status|restart|build)
                COMMAND="$1"
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                echo "Unknown parameter: $1"
                show_help
                exit 1
                ;;
        esac
    done
}

# Create necessary directories
create_dirs() {
    mkdir -p "$PID_DIR"
    mkdir -p "$PROJECT_DIR/data"
    mkdir -p "$PROJECT_DIR/logs"
}

# Build the server
build_server() {
    echo -e "${BLUE}Building server...${NC}"

    if ! command -v cargo &> /dev/null; then
        echo -e "${RED}Error: Rust/Cargo is not installed${NC}"
        echo -e "${YELLOW}Please install Rust from https://rustup.rs/${NC}"
        exit 1
    fi

    cd "$PROJECT_DIR"

    echo -e "${CYAN}Running: cargo build --release${NC}"
    cargo build --release

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Build successful!${NC}"

        BINARY_SIZE=$(du -h "$PROJECT_DIR/target/release/$BINARY_NAME" | cut -f1)
        echo -e "${GREEN}Binary: $PROJECT_DIR/target/release/$BINARY_NAME${NC}"
        echo -e "${GREEN}Size:   $BINARY_SIZE${NC}"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

# Start the server
start_server() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Starting Traffic Monitor${NC}"
    echo -e "${BLUE}========================================${NC}"

    # Check if already running
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${YELLOW}Server already running (PID: $pid)${NC}"
            echo -e "${YELLOW}Use '$0 stop' to stop it first${NC}"
            return 1
        fi
    fi

    create_dirs

    # Check if binary exists
    local binary_path="$PROJECT_DIR/target/release/$BINARY_NAME"
    if [ ! -f "$binary_path" ]; then
        echo -e "${YELLOW}Binary not found. Building...${NC}"
        build_server
    fi

    # Make sure binary is executable
    chmod +x "$binary_path"

    # Start the server
    echo -e "${BLUE}Starting server...${NC}"
    cd "$PROJECT_DIR"
    nohup "$binary_path" > "$LOG_FILE" 2>&1 &
    local pid=$!

    # Save PID
    echo "$pid" > "$PID_FILE"

    # Wait a moment to check if it started successfully
    sleep 2

    if ps -p "$pid" > /dev/null 2>&1; then
        echo -e "${GREEN}✓ Server started successfully (PID: $pid)${NC}"
        echo -e "${GREEN}========================================${NC}"
        echo -e "${CYAN}Backend API: http://localhost:8080${NC}"
        echo -e "${CYAN}Web UI:      Open web/index.html in browser${NC}"
        echo -e "${CYAN}Log file:    $LOG_FILE${NC}"
        echo -e "${CYAN}PID file:    $PID_FILE${NC}"
        echo ""
        echo -e "${CYAN}API Endpoints:${NC}"
        echo -e "${CYAN}  GET /api/namespaces              - List namespaces${NC}"
        echo -e "${CYAN}  GET /api/current?namespace=<ns>  - Current data${NC}"
        echo -e "${CYAN}  GET /api/history?namespace=<ns>  - Historical data${NC}"
        echo -e "${CYAN}  GET /api/stream?namespace=<ns>   - SSE real-time stream${NC}"
        echo ""
        echo -e "${YELLOW}Use '$0 stop' to stop the server${NC}"
        echo -e "${YELLOW}Use 'tail -f $LOG_FILE' to view logs${NC}"
    else
        echo -e "${RED}✗ Server failed to start${NC}"
        echo -e "${YELLOW}Check log file: $LOG_FILE${NC}"
        rm -f "$PID_FILE"
        exit 1
    fi
}

# Stop the server
stop_server() {
    echo -e "${BLUE}Stopping Traffic Monitor...${NC}"

    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")

        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "${CYAN}Stopping server (PID: $pid)...${NC}"
            kill "$pid" 2>/dev/null

            # Wait for process to stop
            sleep 2

            # Force kill if still running
            if ps -p "$pid" > /dev/null 2>&1; then
                echo -e "${YELLOW}Force killing server...${NC}"
                kill -9 "$pid" 2>/dev/null
                sleep 1
            fi

            echo -e "${GREEN}✓ Server stopped${NC}"
        else
            echo -e "${YELLOW}Server process not running (PID: $pid)${NC}"
        fi

        rm -f "$PID_FILE"
    else
        echo -e "${YELLOW}Server is not running${NC}"
    fi
}

# Check server status
check_status() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Traffic Monitor Status${NC}"
    echo -e "${BLUE}========================================${NC}"

    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE")

        if ps -p "$pid" > /dev/null 2>&1; then
            echo -e "Status: ${GREEN}Running${NC}"
            echo -e "PID:    $pid"
            echo -e "Port:   8080"
            echo -e "CPU:    $(ps -p "$pid" -o %cpu --no-headers 2>/dev/null || echo 'N/A')%"
            echo -e "Memory: $(ps -p "$pid" -o %mem --no-headers 2>/dev/null || echo 'N/A')%"
            echo -e "Log:    $LOG_FILE"

            # Check if API is responding
            if command -v curl &> /dev/null; then
                if curl -s "http://localhost:8080/api/namespaces" > /dev/null 2>&1; then
                    echo -e "API:    ${GREEN}Responding${NC}"
                else
                    echo -e "API:    ${YELLOW}Not responding${NC}"
                fi
            fi
        else
            echo -e "Status: ${RED}Stopped (PID file exists)${NC}"
            rm -f "$PID_FILE"
        fi
    else
        echo -e "Status: ${RED}Stopped${NC}"
    fi

    echo -e "${BLUE}========================================${NC}"
}

# Main program
parse_args "$@"

case "$COMMAND" in
    start)
        start_server
        ;;
    stop)
        stop_server
        ;;
    status)
        check_status
        ;;
    restart)
        stop_server
        sleep 2
        start_server
        ;;
    build)
        build_server
        ;;
    "")
        echo "Error: No command specified"
        echo
        show_help
        exit 1
        ;;
    *)
        echo "Error: Unknown command '$COMMAND'"
        echo
        show_help
        exit 1
        ;;
esac
