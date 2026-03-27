#!/bin/bash
# Traffic Monitor - Build Script

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project directories
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY_NAME="traffic-monitor"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}Traffic Monitor Rust Server - Build${NC}"
echo -e "${BLUE}========================================${NC}"

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo -e "${RED}Error: Rust/Cargo is not installed${NC}"
    echo -e "${YELLOW}Please install Rust from https://rustup.rs/${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Rust/Cargo found: $(cargo --version)${NC}"

# Navigate to project directory
cd "$PROJECT_DIR"

# Clean previous build (optional)
if [ "$1" == "clean" ]; then
    echo -e "${YELLOW}Cleaning previous build...${NC}"
    cargo clean
fi

# Build the project
echo -e "${BLUE}Building project in release mode...${NC}"
cargo build --release

# Check if build was successful
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Build successful!${NC}"

    # Get binary path
    BINARY_PATH="target/release/$BINARY_NAME"

    if [ -f "$BINARY_PATH" ]; then
        # Get binary size
        BINARY_SIZE=$(du -h "$BINARY_PATH" | cut -f1)

        echo -e "${GREEN}========================================${NC}"
        echo -e "${GREEN}Build Summary:${NC}"
        echo -e "${GREEN}========================================${NC}"
        echo -e "Binary: ${BINARY_PATH}"
        echo -e "Size:   ${BINARY_SIZE}"
        echo -e ""
        echo -e "${CYAN}Usage:${NC}"
        echo -e "  ./target/release/$BINARY_NAME"
        echo -e ""
        echo -e "${CYAN}Or use run script:${NC}"
        echo -e "  ./run.sh start"
        echo -e ""
        echo -e "${YELLOW}Note: Make sure to run from project root directory${NC}"
        echo -e "${YELLOW}      or set correct paths for data/ and web/ directories${NC}"
    else
        echo -e "${RED}Error: Binary not found at $BINARY_PATH${NC}"
        exit 1
    fi
else
    echo -e "${RED}Build failed!${NC}"
    exit 1
fi
