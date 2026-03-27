#!/bin/bash
# Traffic Monitor - Build Script
# Native Linux build (supports musl static linking)

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Project configuration
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_NAME="traffic-monitor"

# Print usage
print_usage() {
    echo -e "${BLUE}Usage:${NC}"
    echo -e "  $0 [command]"
    echo ""
    echo -e "${BLUE}Commands:${NC}"
    echo -e "  ${GREEN}release${NC}       Build in release mode (default)"
    echo -e "  ${GREEN}debug${NC}         Build in debug mode"
    echo -e "  ${GREEN}musl${NC}          Build with musl static linking (CentOS 7.6+ compatible)"
    echo -e "  ${GREEN}clean${NC}         Clean and build release"
    echo -e "  ${GREEN}musl-clean${NC}    Clean and build musl"
    echo -e "  ${GREEN}help${NC}          Show this help message"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0              # Build in release mode"
    echo -e "  $0 musl         # Build with static linking"
    echo ""
    echo -e "${CYAN}Deploy to production:${NC}"
    echo -e "  scp target/release/$BINARY_NAME user@server:/path/"
}

# Build summary
print_build_summary() {
    local BINARY_PATH="$1"
    local BUILD_TYPE="$2"

    if [ -f "$BINARY_PATH" ]; then
        BINARY_SIZE=$(du -h "$BINARY_PATH" | cut -f1)

        echo -e "${GREEN}========================================${NC}"
        echo -e "${GREEN}Build Summary:${NC}"
        echo -e "${GREEN}========================================${NC}"
        echo -e "Build Type: ${BUILD_TYPE}"
        echo -e "Binary:     ${BINARY_PATH}"
        echo -e "Size:       ${BINARY_SIZE}"

        # Show file type for binaries
        if command -v file &> /dev/null; then
            FILE_TYPE=$(file "$BINARY_PATH" 2>/dev/null)
            if echo "$FILE_TYPE" | grep -q "statically linked"; then
                echo -e "Linking:    statically linked"
            elif echo "$FILE_TYPE" | grep -q "dynamically linked"; then
                echo -e "Linking:    dynamically linked"
            fi
        fi

        echo ""
        echo -e "${CYAN}Usage:${NC}"
        echo -e "  $BINARY_PATH"
        echo ""
        echo -e "${CYAN}Deploy to production:${NC}"
        echo -e "  scp $BINARY_PATH user@server:/path/"
        echo -e "  ssh user@server 'chmod +x /path/$BINARY_NAME && /path/$BINARY_NAME'"
        echo ""
        echo -e "${YELLOW}Note: Make sure to run from project root directory${NC}"
        echo -e "${YELLOW}      or set correct paths for data/ and web/ directories${NC}"
    else
        echo -e "${RED}Error: Binary not found at $BINARY_PATH${NC}"
        exit 1
    fi
}

# Check Rust installation
check_rust() {
    if ! command -v cargo &> /dev/null; then
        echo -e "${RED}Error: Rust/Cargo is not installed${NC}"
        echo -e "${YELLOW}Please install Rust from https://rustup.rs/${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Rust/Cargo found: $(cargo --version)${NC}"
}

# Build in release mode
build_release() {
    local CLEAN="$1"

    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Traffic Monitor - Release Build${NC}"
    echo -e "${BLUE}========================================${NC}"

    check_rust

    # Navigate to project directory
    cd "$PROJECT_DIR"

    # Clean previous build if requested
    if [ "$CLEAN" == "clean" ]; then
        echo -e "${YELLOW}Cleaning previous build...${NC}"
        cargo clean
    fi

    # Build the project
    echo -e "${BLUE}Building project in release mode...${NC}"
    cargo build --release

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Build successful!${NC}"
        print_build_summary "target/release/$BINARY_NAME" "Release (dynamic)"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

# Build in debug mode
build_debug() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Traffic Monitor - Debug Build${NC}"
    echo -e "${BLUE}========================================${NC}"

    check_rust

    # Navigate to project directory
    cd "$PROJECT_DIR"

    # Build the project
    echo -e "${BLUE}Building project in debug mode...${NC}"
    cargo build

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Build successful!${NC}"
        print_build_summary "target/debug/$BINARY_NAME" "Debug"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

# Build with musl static linking
build_musl() {
    local CLEAN="$1"

    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Traffic Monitor - Musl Static Build${NC}"
    echo -e "${BLUE}========================================${NC}"

    check_rust

    # Check if musl target is installed
    MUSL_TARGET="x86_64-unknown-linux-musl"
    if ! rustup target list | grep -q "$MUSL_TARGET (installed)"; then
        echo -e "${YELLOW}Installing $MUSL_TARGET target...${NC}"
        rustup target add $MUSL_TARGET
    fi
    echo -e "${GREEN}✓ Target $MUSL_TARGET is ready${NC}"

    # Check for musl-gcc
    if command -v musl-gcc &> /dev/null; then
        MUSL_LINKER="musl-gcc"
    elif command -v x86_64-linux-musl-gcc &> /dev/null; then
        MUSL_LINKER="x86_64-linux-musl-gcc"
    else
        echo -e "${YELLOW}musl-gcc not found, attempting to use system linker...${NC}"
        MUSL_LINKER="gcc"
    fi
    echo -e "${GREEN}✓ Using linker: $MUSL_LINKER${NC}"

    # Navigate to project directory
    cd "$PROJECT_DIR"

    # Clean previous build if requested
    if [ "$CLEAN" == "clean" ]; then
        echo -e "${YELLOW}Cleaning previous build...${NC}"
        rm -rf target/$MUSL_TARGET/release
    fi

    # Build the project
    echo -e "${BLUE}Building with musl static linking...${NC}"
    cargo build --release --target $MUSL_TARGET

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Build successful!${NC}"
        print_build_summary "target/$MUSL_TARGET/release/$BINARY_NAME" "Release (musl static)"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

# Parse command line arguments
COMMAND="${1:-release}"

case "$COMMAND" in
    release|"")
        build_release ""
        ;;
    debug)
        build_debug
        ;;
    musl|static)
        build_musl ""
        ;;
    clean)
        build_release "clean"
        ;;
    musl-clean|static-clean)
        build_musl "clean"
        ;;
    help|--help|-h)
        print_usage
        exit 0
        ;;
    *)
        echo -e "${RED}Unknown command: $COMMAND${NC}"
        echo ""
        print_usage
        exit 1
        ;;
esac
