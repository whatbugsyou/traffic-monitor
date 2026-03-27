#!/bin/bash
# Traffic Monitor - Installation Script
# Automates the build and installation process

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Project configuration
PROJECT_NAME="traffic-monitor"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/traffic-monitor"
DATA_DIR="/var/lib/traffic-monitor"
LOG_DIR="/var/log/traffic-monitor"
SERVICE_USER="traffic"

# Display banner
show_banner() {
    echo -e "${CYAN}"
    echo "╔═══════════════════════════════════════════════════════════╗"
    echo "║                  Traffic Monitor Installer                 ║"
    echo "║                    Version 1.0.0                          ║"
    echo "╚═══════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
}

# Display help
show_help() {
    echo "Traffic Monitor Installation Script"
    echo
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --help, -h          Show this help message"
    echo "  --prefix DIR        Installation prefix (default: /usr/local)"
    echo "  --user USER         Service user (default: traffic)"
    echo "  --no-service        Skip systemd service installation"
    echo "  --uninstall         Uninstall Traffic Monitor"
    echo
    echo "Examples:"
    echo "  sudo ./install.sh                    # Standard installation"
    echo "  sudo ./install.sh --prefix /opt      # Custom installation path"
    echo "  sudo ./install.sh --no-service       # Install without systemd service"
    echo "  sudo ./install.sh --uninstall        # Uninstall"
}

# Parse command line arguments
parse_args() {
    INSTALL_SERVICE=true
    UNINSTALL=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --help|-h)
                show_help
                exit 0
                ;;
            --prefix)
                INSTALL_DIR="$2/bin"
                CONFIG_DIR="$2/etc/traffic-monitor"
                DATA_DIR="$2/var/lib/traffic-monitor"
                LOG_DIR="$2/var/log/traffic-monitor"
                shift 2
                ;;
            --user)
                SERVICE_USER="$2"
                shift 2
                ;;
            --no-service)
                INSTALL_SERVICE=false
                shift
                ;;
            --uninstall)
                UNINSTALL=true
                shift
                ;;
            *)
                echo -e "${RED}Error: Unknown option: $1${NC}"
                show_help
                exit 1
                ;;
        esac
    done
}

# Check if running as root
check_root() {
    if [[ $EUID -ne 0 ]]; then
        echo -e "${RED}Error: This script must be run as root${NC}"
        echo -e "${YELLOW}Use: sudo $0${NC}"
        exit 1
    fi
}

# Check system requirements
check_requirements() {
    echo -e "${BLUE}Checking system requirements...${NC}"

    # Check OS
    if [[ ! -f /etc/os-release ]]; then
        echo -e "${RED}Error: Cannot detect operating system${NC}"
        exit 1
    fi

    source /etc/os-release
    echo -e "${GREEN}✓ OS: $PRETTY_NAME${NC}"

    # Check architecture
    ARCH=$(uname -m)
    echo -e "${GREEN}✓ Architecture: $ARCH${NC}"

    # Check for required tools
    local missing_tools=()

    for tool in curl tar; do
        if ! command -v $tool &> /dev/null; then
            missing_tools+=($tool)
        fi
    done

    if [[ ${#missing_tools[@]} -gt 0 ]]; then
        echo -e "${YELLOW}Installing missing tools: ${missing_tools[*]}${NC}"
        apt-get update -qq
        apt-get install -y ${missing_tools[*]}
    fi

    echo -e "${GREEN}✓ All requirements met${NC}"
}

# Install Rust if not present
install_rust() {
    if command -v cargo &> /dev/null; then
        echo -e "${GREEN}✓ Rust is already installed: $(cargo --version)${NC}"
        return 0
    fi

    echo -e "${BLUE}Installing Rust...${NC}"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

    # Source Rust environment
    source $HOME/.cargo/env

    echo -e "${GREEN}✓ Rust installed: $(cargo --version)${NC}"
}

# Install system dependencies
install_dependencies() {
    echo -e "${BLUE}Installing system dependencies...${NC}"

    apt-get update -qq

    # Detect package manager
    if command -v apt-get &> /dev/null; then
        apt-get install -y \
            build-essential \
            pkg-config \
            libssl-dev \
            sqlite3 \
            libsqlite3-dev \
            iproute2 \
            curl
    elif command -v yum &> /dev/null; then
        yum install -y \
            gcc \
            make \
            openssl-devel \
            sqlite \
            sqlite-devel \
            iproute \
            curl
    elif command -v dnf &> /dev/null; then
        dnf install -y \
            gcc \
            make \
            openssl-devel \
            sqlite \
            sqlite-devel \
            iproute \
            curl
    else
        echo -e "${YELLOW}Warning: Package manager not detected. Please install dependencies manually.${NC}"
    fi

    echo -e "${GREEN}✓ Dependencies installed${NC}"
}

# Build the project
build_project() {
    echo -e "${BLUE}Building Traffic Monitor...${NC}"

    cd "$PROJECT_DIR"

    # Check if Cargo.toml exists
    if [[ ! -f Cargo.toml ]]; then
        echo -e "${RED}Error: Cargo.toml not found in $PROJECT_DIR${NC}"
        exit 1
    fi

    # Build release binary
    cargo build --release

    if [[ ! -f target/release/$PROJECT_NAME ]]; then
        echo -e "${RED}Error: Build failed${NC}"
        exit 1
    fi

    echo -e "${GREEN}✓ Build successful${NC}"

    # Show binary info
    local binary_size=$(du -h target/release/$PROJECT_NAME | cut -f1)
    echo -e "${CYAN}Binary size: $binary_size${NC}"
}

# Create service user
create_user() {
    if id -u $SERVICE_USER >/dev/null 2>&1; then
        echo -e "${GREEN}✓ User '$SERVICE_USER' already exists${NC}"
        return 0
    fi

    echo -e "${BLUE}Creating service user '$SERVICE_USER'...${NC}"
    useradd -r -s /bin/false $SERVICE_USER
    echo -e "${GREEN}✓ User created${NC}"
}

# Create directories
create_directories() {
    echo -e "${BLUE}Creating directories...${NC}"

    mkdir -p "$CONFIG_DIR"
    mkdir -p "$DATA_DIR"
    mkdir -p "$LOG_DIR"

    # Set permissions
    chown -R $SERVICE_USER:$SERVICE_USER "$DATA_DIR"
    chown -R $SERVICE_USER:$SERVICE_USER "$LOG_DIR"

    echo -e "${GREEN}✓ Directories created${NC}"
}

# Install binary
install_binary() {
    echo -e "${BLUE}Installing binary...${NC}"

    cp target/release/$PROJECT_NAME "$INSTALL_DIR/"
    chmod +x "$INSTALL_DIR/$PROJECT_NAME"

    echo -e "${GREEN}✓ Binary installed to $INSTALL_DIR/$PROJECT_NAME${NC}"
}

# Install web interface
install_web_interface() {
    echo -e "${BLUE}Installing web interface...${NC}"

    if [[ -d web ]]; then
        mkdir -p "$DATA_DIR/web"
        cp -r web/* "$DATA_DIR/web/"
        chown -R $SERVICE_USER:$SERVICE_USER "$DATA_DIR/web"
        echo -e "${GREEN}✓ Web interface installed${NC}"
    else
        echo -e "${YELLOW}Warning: web/ directory not found${NC}"
    fi
}

# Install systemd service
install_systemd_service() {
    if [[ "$INSTALL_SERVICE" != "true" ]]; then
        echo -e "${YELLOW}Skipping systemd service installation${NC}"
        return 0
    fi

    echo -e "${BLUE}Installing systemd service...${NC}"

    # Create service file
    cat > /etc/systemd/system/$PROJECT_NAME.service <<EOF
[Unit]
Description=Traffic Monitor Server
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$SERVICE_USER
Group=$SERVICE_USER
WorkingDirectory=$DATA_DIR
ExecStart=$INSTALL_DIR/$PROJECT_NAME
ExecReload=/bin/kill -HUP \$MAINPID
Restart=on-failure
RestartSec=5s

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$PROJECT_NAME

# Environment
Environment=RUST_LOG=info
Environment=RUST_BACKTRACE=1

# Security
NoNewPrivileges=false
PrivateTmp=true

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Graceful shutdown
TimeoutStopSec=30
KillMode=mixed
KillSignal=SIGTERM

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd
    systemctl daemon-reload

    # Enable service
    systemctl enable $PROJECT_NAME

    echo -e "${GREEN}✓ Systemd service installed${NC}"
}

# Create configuration file
create_config() {
    echo -e "${BLUE}Creating configuration...${NC}"

    cat > "$CONFIG_DIR/config.env" <<EOF
# Traffic Monitor Configuration
# Edit this file to customize settings

# Server settings
TRAFFIC_MONITOR_HOST=0.0.0.0
TRAFFIC_MONITOR_PORT=8080

# Database settings
TRAFFIC_MONITOR_DB_PATH=$DATA_DIR/traffic_monitor.db

# Log settings
RUST_LOG=info

# Collection settings
TRAFFIC_MONITOR_INTERVAL=1
EOF

    echo -e "${GREEN}✓ Configuration created at $CONFIG_DIR/config.env${NC}"
}

# Print installation summary
print_summary() {
    echo ""
    echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║           Installation Completed Successfully!            ║${NC}"
    echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo -e "${CYAN}Installation Details:${NC}"
    echo -e "  Binary:        $INSTALL_DIR/$PROJECT_NAME"
    echo -e "  Config:        $CONFIG_DIR/config.env"
    echo -e "  Data:          $DATA_DIR"
    echo -e "  Logs:          $LOG_DIR"
    echo ""

    if [[ "$INSTALL_SERVICE" == "true" ]]; then
        echo -e "${CYAN}Service Management:${NC}"
        echo -e "  Start:         systemctl start $PROJECT_NAME"
        echo -e "  Stop:          systemctl stop $PROJECT_NAME"
        echo -e "  Status:        systemctl status $PROJECT_NAME"
        echo -e "  Logs:          journalctl -u $PROJECT_NAME -f"
        echo ""
    fi

    echo -e "${CYAN}Quick Start:${NC}"
    echo -e "  ${YELLOW}Option 1: Using systemd${NC}"
    echo -e "    sudo systemctl start $PROJECT_NAME"
    echo ""
    echo -e "  ${YELLOW}Option 2: Manual start${NC}"
    echo -e "    sudo -u $SERVICE_USER $INSTALL_DIR/$PROJECT_NAME"
    echo ""
    echo -e "${CYAN}Web Interface:${NC}"
    echo -e "  URL:           http://localhost:8080"
    echo -e "  Files:         $DATA_DIR/web/index.html"
    echo ""
    echo -e "${CYAN}API Endpoints:${NC}"
    echo -e "  Namespaces:    http://localhost:8080/api/namespaces"
    echo -e "  Current data:  http://localhost:8080/api/current"
    echo -e "  History:       http://localhost:8080/api/history"
    echo -e "  Real-time:      http://localhost:8080/api/stream"
    echo ""
    echo -e "${YELLOW}Next Steps:${NC}"
    echo -e "  1. Edit configuration: sudo nano $CONFIG_DIR/config.env"
    echo -e "  2. Start the service:  sudo systemctl start $PROJECT_NAME"
    echo -e "  3. Check status:       sudo systemctl status $PROJECT_NAME"
    echo -e "  4. View logs:          sudo journalctl -u $PROJECT_NAME -f"
    echo ""
}

# Uninstall function
uninstall() {
    echo -e "${BLUE}Uninstalling Traffic Monitor...${NC}"

    # Stop service
    if systemctl is-active --quiet $PROJECT_NAME; then
        echo -e "${YELLOW}Stopping service...${NC}"
        systemctl stop $PROJECT_NAME
    fi

    # Disable service
    if systemctl is-enabled --quiet $PROJECT_NAME; then
        echo -e "${YELLOW}Disabling service...${NC}"
        systemctl disable $PROJECT_NAME
    fi

    # Remove service file
    if [[ -f /etc/systemd/system/$PROJECT_NAME.service ]]; then
        rm -f /etc/systemd/system/$PROJECT_NAME.service
        systemctl daemon-reload
    fi

    # Remove binary
    if [[ -f "$INSTALL_DIR/$PROJECT_NAME" ]]; then
        rm -f "$INSTALL_DIR/$PROJECT_NAME"
    fi

    # Ask before removing data
    read -p "Remove data directory $DATA_DIR? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "$DATA_DIR"
        echo -e "${GREEN}✓ Data directory removed${NC}"
    fi

    # Ask before removing logs
    read -p "Remove log directory $LOG_DIR? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -rf "$LOG_DIR"
        echo -e "${GREEN}✓ Log directory removed${NC}"
    fi

    # Remove config
    if [[ -d "$CONFIG_DIR" ]]; then
        rm -rf "$CONFIG_DIR"
    fi

    # Remove user
    if id -u $SERVICE_USER >/dev/null 2>&1; then
        read -p "Remove user '$SERVICE_USER'? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            userdel $SERVICE_USER
            echo -e "${GREEN}✓ User removed${NC}"
        fi
    fi

    echo -e "${GREEN}✓ Uninstallation completed${NC}"
}

# Main installation process
main() {
    show_banner
    parse_args "$@"

    if [[ "$UNINSTALL" == "true" ]]; then
        check_root
        uninstall
        exit 0
    fi

    check_root
    check_requirements
    install_rust
    install_dependencies
    build_project
    create_user
    create_directories
    install_binary
    install_web_interface
    create_config
    install_systemd_service
    print_summary
}

# Run main function
main "$@"
