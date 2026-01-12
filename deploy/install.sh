#!/bin/bash
#
# SmartCopy Agent Installation Script
#
# This script installs and configures SmartCopy agent on a single host.
# For multi-host deployment, use the Ansible playbook instead.
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/smartcopy/smartcopy/main/deploy/install.sh | sudo bash
#
# Or with options:
#   sudo ./install.sh --speed 100g --agent-port 9878 --quic
#
# Options:
#   --speed TIER      Network speed tier: 10g, 100g, 200g, 400g (default: 10g)
#   --agent-port PORT Agent TCP port (default: 9878)
#   --quic            Enable QUIC server
#   --quic-port PORT  QUIC server port (default: 9877)
#   --no-service      Don't create/start systemd service
#   --no-tuning       Don't apply kernel tuning
#   --from-source     Build from source instead of downloading binary
#   --help            Show this help message
#

set -e

# Default values
SPEED="10g"
AGENT_PORT=9878
QUIC_ENABLED=false
QUIC_PORT=9877
ENABLE_SERVICE=true
APPLY_TUNING=true
BUILD_FROM_SOURCE=false
INSTALL_DIR="/usr/local/bin"
CONFIG_DIR="/etc/smartcopy"
DATA_DIR="/var/lib/smartcopy"
LOG_DIR="/var/log/smartcopy"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[OK]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

show_help() {
    head -35 "$0" | tail -25
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --speed)
            SPEED="$2"
            shift 2
            ;;
        --agent-port)
            AGENT_PORT="$2"
            shift 2
            ;;
        --quic)
            QUIC_ENABLED=true
            shift
            ;;
        --quic-port)
            QUIC_PORT="$2"
            shift 2
            ;;
        --no-service)
            ENABLE_SERVICE=false
            shift
            ;;
        --no-tuning)
            APPLY_TUNING=false
            shift
            ;;
        --from-source)
            BUILD_FROM_SOURCE=true
            shift
            ;;
        --help)
            show_help
            ;;
        *)
            log_error "Unknown option: $1"
            ;;
    esac
done

# Validate speed tier
case $SPEED in
    10g|100g|200g|400g)
        ;;
    *)
        log_error "Invalid speed tier: $SPEED. Must be 10g, 100g, 200g, or 400g"
        ;;
esac

# Check if running as root
if [[ $EUID -ne 0 ]]; then
    log_error "This script must be run as root (use sudo)"
fi

echo ""
echo "========================================"
echo "  SmartCopy Agent Installation"
echo "========================================"
echo ""
echo "Configuration:"
echo "  Network Speed:    $SPEED"
echo "  Agent Port:       $AGENT_PORT"
echo "  QUIC Enabled:     $QUIC_ENABLED"
echo "  QUIC Port:        $QUIC_PORT"
echo "  Enable Service:   $ENABLE_SERVICE"
echo "  Apply Tuning:     $APPLY_TUNING"
echo "  Build from Source: $BUILD_FROM_SOURCE"
echo ""

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
    OS_VERSION=$VERSION_ID
else
    log_error "Cannot detect OS. /etc/os-release not found."
fi

log_info "Detected OS: $OS $OS_VERSION"

# Install prerequisites
log_info "Installing prerequisites..."
case $OS in
    ubuntu|debian)
        apt-get update -qq
        apt-get install -y -qq curl ca-certificates build-essential pkg-config libssl-dev ethtool
        ;;
    centos|rhel|fedora|rocky|almalinux)
        yum install -y -q curl ca-certificates gcc gcc-c++ make openssl-devel pkgconfig ethtool
        ;;
    *)
        log_warning "Unknown OS: $OS. Attempting to continue..."
        ;;
esac
log_success "Prerequisites installed"

# Create user and directories
log_info "Creating user and directories..."
if ! getent group smartcopy > /dev/null 2>&1; then
    groupadd --system smartcopy
fi
if ! getent passwd smartcopy > /dev/null 2>&1; then
    useradd --system --gid smartcopy --shell /sbin/nologin --home-dir $DATA_DIR smartcopy
fi

mkdir -p $CONFIG_DIR $DATA_DIR $LOG_DIR $DATA_DIR/certs
chown -R smartcopy:smartcopy $DATA_DIR $LOG_DIR
log_success "User and directories created"

# Install SmartCopy
if $BUILD_FROM_SOURCE; then
    log_info "Building SmartCopy from source..."

    # Install Rust if needed
    if ! command -v rustc &> /dev/null; then
        log_info "Installing Rust..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source $HOME/.cargo/env
    fi

    # Clone and build
    TEMP_DIR=$(mktemp -d)
    git clone https://github.com/smartcopy/smartcopy.git $TEMP_DIR/smartcopy
    cd $TEMP_DIR/smartcopy
    cargo build --release
    cp target/release/smartcopy $INSTALL_DIR/
    rm -rf $TEMP_DIR
    log_success "SmartCopy built and installed"
else
    log_info "Downloading SmartCopy binary..."
    ARCH=$(uname -m)
    case $ARCH in
        x86_64)
            ARCH="x86_64"
            ;;
        aarch64|arm64)
            ARCH="aarch64"
            ;;
        *)
            log_error "Unsupported architecture: $ARCH"
            ;;
    esac

    # For now, build from source as binary releases may not exist
    log_warning "Binary releases not yet available. Building from source..."
    BUILD_FROM_SOURCE=true

    if $BUILD_FROM_SOURCE; then
        # Install Rust if needed
        if ! command -v rustc &> /dev/null; then
            log_info "Installing Rust..."
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
            source $HOME/.cargo/env
        fi

        TEMP_DIR=$(mktemp -d)
        git clone https://github.com/smartcopy/smartcopy.git $TEMP_DIR/smartcopy
        cd $TEMP_DIR/smartcopy
        $HOME/.cargo/bin/cargo build --release
        cp target/release/smartcopy $INSTALL_DIR/
        rm -rf $TEMP_DIR
    fi
    log_success "SmartCopy installed"
fi

chmod 755 $INSTALL_DIR/smartcopy

# Verify installation
if ! $INSTALL_DIR/smartcopy --version > /dev/null 2>&1; then
    log_error "SmartCopy installation verification failed"
fi
log_success "SmartCopy $(${INSTALL_DIR}/smartcopy --version)"

# Apply kernel tuning
if $APPLY_TUNING; then
    log_info "Applying kernel tuning for $SPEED network..."

    case $SPEED in
        10g)
            RMEM_MAX=67108864
            WMEM_MAX=67108864
            TCP_RMEM="4096 87380 67108864"
            TCP_WMEM="4096 65536 67108864"
            BACKLOG=30000
            SOMAXCONN=4096
            ;;
        100g)
            RMEM_MAX=536870912
            WMEM_MAX=536870912
            TCP_RMEM="4096 87380 536870912"
            TCP_WMEM="4096 65536 536870912"
            BACKLOG=250000
            SOMAXCONN=65535
            ;;
        200g)
            RMEM_MAX=1073741824
            WMEM_MAX=1073741824
            TCP_RMEM="4096 87380 1073741824"
            TCP_WMEM="4096 65536 1073741824"
            BACKLOG=500000
            SOMAXCONN=65535
            ;;
        400g)
            RMEM_MAX=2147483647
            WMEM_MAX=2147483647
            TCP_RMEM="4096 87380 2147483647"
            TCP_WMEM="4096 65536 2147483647"
            BACKLOG=1000000
            SOMAXCONN=65535
            ;;
    esac

    cat > /etc/sysctl.d/99-smartcopy.conf << EOF
# SmartCopy High-Speed Network Tuning ($SPEED)
net.core.rmem_max=$RMEM_MAX
net.core.wmem_max=$WMEM_MAX
net.ipv4.tcp_rmem=$TCP_RMEM
net.ipv4.tcp_wmem=$TCP_WMEM
net.core.netdev_max_backlog=$BACKLOG
net.core.somaxconn=$SOMAXCONN
net.ipv4.tcp_congestion_control=bbr
net.ipv4.tcp_mtu_probing=1
net.ipv4.tcp_timestamps=1
net.ipv4.tcp_sack=1
net.ipv4.tcp_window_scaling=1
net.ipv4.tcp_fastopen=3
fs.file-max=2097152
EOF

    sysctl --system > /dev/null 2>&1
    log_success "Kernel tuning applied"
fi

# Create environment file
log_info "Creating configuration files..."
cat > $CONFIG_DIR/smartcopy.env << EOF
# SmartCopy Environment Configuration
RUST_LOG=info
SMARTCOPY_LOG_DIR=$LOG_DIR
SMARTCOPY_DATA_DIR=$DATA_DIR
SMARTCOPY_CERT_DIR=$DATA_DIR/certs
EOF

log_success "Configuration files created"

# Create systemd services
if $ENABLE_SERVICE; then
    log_info "Creating systemd services..."

    # Agent service
    cat > /etc/systemd/system/smartcopy-agent.service << EOF
[Unit]
Description=SmartCopy Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=smartcopy
Group=smartcopy
EnvironmentFile=-$CONFIG_DIR/smartcopy.env
ExecStart=$INSTALL_DIR/smartcopy agent --protocol tcp --port $AGENT_PORT --bind 0.0.0.0
Restart=always
RestartSec=5
StandardOutput=append:$LOG_DIR/agent.log
StandardError=append:$LOG_DIR/agent.error.log
LimitNOFILE=1048576
LimitNPROC=65535
Nice=-10

[Install]
WantedBy=multi-user.target
EOF

    # QUIC service (if enabled)
    if $QUIC_ENABLED; then
        cat > /etc/systemd/system/smartcopy-quic.service << EOF
[Unit]
Description=SmartCopy QUIC Server
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=smartcopy
Group=smartcopy
EnvironmentFile=-$CONFIG_DIR/smartcopy.env
ExecStart=$INSTALL_DIR/smartcopy quic-server --port $QUIC_PORT --bind 0.0.0.0
Restart=always
RestartSec=5
StandardOutput=append:$LOG_DIR/quic.log
StandardError=append:$LOG_DIR/quic.error.log
LimitNOFILE=1048576
LimitNPROC=65535
Nice=-10

[Install]
WantedBy=multi-user.target
EOF
    fi

    systemctl daemon-reload
    systemctl enable smartcopy-agent
    systemctl start smartcopy-agent

    if $QUIC_ENABLED; then
        systemctl enable smartcopy-quic
        systemctl start smartcopy-quic
    fi

    log_success "Systemd services created and started"
fi

# Summary
echo ""
echo "========================================"
echo "  Installation Complete!"
echo "========================================"
echo ""
echo "SmartCopy agent is now running."
echo ""
echo "Service Management:"
echo "  sudo systemctl status smartcopy-agent"
echo "  sudo systemctl restart smartcopy-agent"
echo "  sudo journalctl -u smartcopy-agent -f"
echo ""
if $QUIC_ENABLED; then
    echo "QUIC Server:"
    echo "  sudo systemctl status smartcopy-quic"
    echo "  Listening on: 0.0.0.0:$QUIC_PORT"
    echo ""
fi
echo "Agent TCP Port: $AGENT_PORT"
echo "Configuration: $CONFIG_DIR/"
echo "Logs: $LOG_DIR/"
echo ""
echo "Test connection from client:"
echo "  smartcopy /local/path user@$(hostname):$AGENT_PORT:/remote/path --tcp-agent"
echo ""
echo "View high-speed tuning guide:"
echo "  smartcopy highspeed $SPEED"
echo ""
