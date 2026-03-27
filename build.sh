#!/bin/bash
# Traffic Monitor - Build Script
# Supports local build and cross-compilation for Linux (musl static)

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
MUSL_TARGET="x86_64-unknown-linux-musl"

# Print usage
print_usage() {
    echo -e "${BLUE}Usage:${NC}"
    echo -e "  $0 [command]"
    echo ""
    echo -e "${BLUE}Commands:${NC}"
    echo -e "  ${GREEN}local${NC}        Build for current platform (default)"
    echo -e "  ${GREEN}musl${NC}         Cross-compile for Linux (static linked, CentOS 7.6+ compatible)"
    echo -e "  ${GREEN}clean${NC}        Clean and build for current platform"
    echo -e "  ${GREEN}musl-clean${NC}   Clean and cross-compile for Linux"
    echo -e "  ${GREEN}help${NC}         Show this help message"
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0              # Build for current platform"
    echo -e "  $0 musl         # Cross-compile for Linux (static)"
    echo ""
    echo -e "${CYAN}Deploy to Linux server:${NC}"
    echo -e "  scp target/x86_64-unknown-linux-musl/release/$BINARY_NAME user@server:/path/"
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

        # Show file type for Linux binaries
        if [[ "$BUILD_TYPE" == *"musl"* ]] && command -v file &> /dev/null; then
            FILE_TYPE=$(file "$BINARY_PATH" 2>/dev/null | grep -o "static.*linked" || echo "")
            if [ -n "$FILE_TYPE" ]; then
                echo -e "Linking:    ${FILE_TYPE}"
            fi
        fi

        echo ""
        echo -e "${CYAN}Usage:${NC}"
        echo -e "  $BINARY_PATH"
        echo ""
        if [[ "$BUILD_TYPE" == *"musl"* ]]; then
            echo -e "${CYAN}Deploy to Linux server:${NC}"
            echo -e "  scp $BINARY_PATH user@server:/path/"
            echo -e "  ssh user@server 'chmod +x /path/$BINARY_NAME && /path/$BINARY_NAME'"
            echo ""
        else
            echo -e "${CYAN}Or use run script:${NC}"
            echo -e "  ./run.sh start"
            echo ""
        fi
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

# Local build function
build_local() {
    local CLEAN="$1"

    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Traffic Monitor - Local Build${NC}"
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
        print_build_summary "target/release/$BINARY_NAME" "Local"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

# Cross-compile for Linux musl
build_musl() {
    local CLEAN="$1"

    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Traffic Monitor - Cross-Compile (Linux musl)${NC}"
    echo -e "${BLUE}========================================${NC}"

    check_rust

    # Check if musl target is installed
    if ! rustup target list | grep -q "$MUSL_TARGET (installed)"; then
        echo -e "${YELLOW}Installing $MUSL_TARGET target...${NC}"
        rustup target add $MUSL_TARGET
    fi
    echo -e "${GREEN}✓ Target $MUSL_TARGET is ready${NC}"

    # Check if musl-cross linker is available
    if ! command -v x86_64-linux-musl-gcc &> /dev/null; then
        echo -e "${RED}Error: musl-cross toolchain not found${NC}"
        echo -e "${YELLOW}Please install it with:${NC}"
        echo -e "  brew install filosottile/musl-cross/musl-cross"
        exit 1
    fi
    echo -e "${GREEN}✓ Cross-compiler found: $(x86_64-linux-musl-gcc --version | head -1)${NC}"

    # Navigate to project directory
    cd "$PROJECT_DIR"

    # Clean previous build if requested
    if [ "$CLEAN" == "clean" ]; then
        echo -e "${YELLOW}Cleaning previous build...${NC}"
        rm -rf target/$MUSL_TARGET/release
    fi

    # Build the project
    echo -e "${BLUE}Cross-compiling for Linux (musl static)...${NC}"
    cargo build --release --target $MUSL_TARGET

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Build successful!${NC}"
        print_build_summary "target/$MUSL_TARGET/release/$BINARY_NAME" "Cross-compile (musl static)"
    else
        echo -e "${RED}Build failed!${NC}"
        exit 1
    fi
}

# Parse command line arguments
COMMAND="${1:-local}"

case "$COMMAND" in
    local|"")
        build_local ""
        ;;
    musl|linux)
        build_musl ""
        ;;
    clean)
        build_local "clean"
        ;;
    musl-clean|linux-clean)
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
