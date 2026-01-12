# SmartCopy Agent Deployment Guide

This directory contains scripts and playbooks for deploying SmartCopy agents across your infrastructure.

## Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Manual Installation](#manual-installation)
- [Ansible Deployment](#ansible-deployment)
- [Configuration](#configuration)
- [Verification](#verification)
- [Troubleshooting](#troubleshooting)

## Overview

The SmartCopy agent enables high-performance remote file transfers by:
- Running a daemon on remote hosts for delta sync operations
- Providing QUIC/HTTP3 transport for maximum throughput
- Applying kernel tuning for 10G/100G/200G/400G networks

### Architecture

```
┌──────────────┐         SSH/TCP         ┌──────────────────┐
│   Client     │ ────────────────────▶   │  SmartCopy Agent │
│              │                         │    (Remote)      │
│  smartcopy   │ ◀──── Delta Ops ─────   │                  │
│              │                         │  Port 9878 (TCP) │
└──────────────┘                         │  Port 9877 (QUIC)│
                                         └──────────────────┘
```

## Prerequisites

### Client Requirements
- SmartCopy installed locally
- SSH access to remote hosts (for agent installation)
- Network connectivity to agent ports

### Server Requirements
- Linux (Ubuntu 20.04+, RHEL 8+, Debian 11+)
- 2+ CPU cores (more for high-speed networks)
- 4+ GB RAM (more for high-speed networks)
- Root access for installation

### Network Requirements
| Speed Tier | Min Bandwidth | Recommended NIC | Storage |
|------------|---------------|-----------------|---------|
| 10G | 10 Gbps | Intel X710 | NVMe SSD |
| 100G | 100 Gbps | Mellanox ConnectX-5 | NVMe RAID |
| 200G | 200 Gbps | Mellanox ConnectX-6 | Parallel FS |
| 400G | 400 Gbps | Mellanox ConnectX-7 | Parallel FS |

## Quick Start

### Single Host (One-liner)

```bash
# Install with default settings (10G network)
curl -sSL https://raw.githubusercontent.com/smartcopy/smartcopy/main/deploy/install.sh | sudo bash

# Install for 100G network
curl -sSL https://raw.githubusercontent.com/smartcopy/smartcopy/main/deploy/install.sh | sudo bash -s -- --speed 100g

# Install with QUIC server
curl -sSL https://raw.githubusercontent.com/smartcopy/smartcopy/main/deploy/install.sh | sudo bash -s -- --speed 100g --quic
```

### Multiple Hosts (Ansible)

```bash
# Clone repository
git clone https://github.com/smartcopy/smartcopy.git
cd smartcopy/deploy/ansible

# Configure inventory
cp inventory.example.yml inventory.yml
vim inventory.yml  # Edit with your hosts

# Deploy
ansible-playbook -i inventory.yml playbook.yml
```

## Manual Installation

### Step 1: Install Prerequisites

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install -y curl ca-certificates build-essential pkg-config libssl-dev ethtool git
```

**RHEL/CentOS/Rocky:**
```bash
sudo yum install -y curl ca-certificates gcc gcc-c++ make openssl-devel pkgconfig ethtool git
```

### Step 2: Install Rust (for building from source)

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source $HOME/.cargo/env
```

### Step 3: Build SmartCopy

```bash
git clone https://github.com/smartcopy/smartcopy.git
cd smartcopy
cargo build --release
sudo cp target/release/smartcopy /usr/local/bin/
```

### Step 4: Create User and Directories

```bash
sudo groupadd --system smartcopy
sudo useradd --system --gid smartcopy --shell /sbin/nologin smartcopy

sudo mkdir -p /etc/smartcopy /var/lib/smartcopy /var/log/smartcopy
sudo chown -R smartcopy:smartcopy /var/lib/smartcopy /var/log/smartcopy
```

### Step 5: Apply Kernel Tuning

```bash
# For 100G network - adjust for your speed tier
sudo tee /etc/sysctl.d/99-smartcopy.conf << 'EOF'
net.core.rmem_max=536870912
net.core.wmem_max=536870912
net.ipv4.tcp_rmem=4096 87380 536870912
net.ipv4.tcp_wmem=4096 65536 536870912
net.core.netdev_max_backlog=250000
net.core.somaxconn=65535
net.ipv4.tcp_congestion_control=bbr
net.ipv4.tcp_mtu_probing=1
fs.file-max=2097152
EOF

sudo sysctl --system
```

### Step 6: Create Systemd Service

```bash
sudo tee /etc/systemd/system/smartcopy-agent.service << 'EOF'
[Unit]
Description=SmartCopy Agent
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=smartcopy
Group=smartcopy
ExecStart=/usr/local/bin/smartcopy agent --protocol tcp --port 9878 --bind 0.0.0.0
Restart=always
RestartSec=5
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable smartcopy-agent
sudo systemctl start smartcopy-agent
```

### Step 7: Configure Firewall

```bash
# Ubuntu/Debian (ufw)
sudo ufw allow 9878/tcp
sudo ufw allow 9877/udp  # For QUIC

# RHEL/CentOS (firewalld)
sudo firewall-cmd --permanent --add-port=9878/tcp
sudo firewall-cmd --permanent --add-port=9877/udp
sudo firewall-cmd --reload
```

## Ansible Deployment

### Directory Structure

```
deploy/ansible/
├── inventory.example.yml  # Example inventory
├── playbook.yml           # Main playbook
└── templates/
    ├── 99-smartcopy.conf.j2       # Kernel tuning
    ├── smartcopy-agent.conf.j2    # Agent config
    ├── smartcopy-agent.service.j2 # Systemd service
    ├── smartcopy-quic.service.j2  # QUIC service
    └── smartcopy.env.j2           # Environment
```

### Inventory Configuration

Copy and edit the inventory file:

```yaml
# inventory.yml
all:
  vars:
    smartcopy_network_speed: "100g"
    smartcopy_apply_kernel_tuning: true
    smartcopy_enable_service: true

  children:
    smartcopy_agents:
      hosts:
        node1.example.com:
          ansible_host: 192.168.1.10
          smartcopy_network_speed: "100g"
        node2.example.com:
          ansible_host: 192.168.1.11
          smartcopy_network_speed: "10g"
```

### Running the Playbook

```bash
# Full installation
ansible-playbook -i inventory.yml playbook.yml

# Dry run (check mode)
ansible-playbook -i inventory.yml playbook.yml --check

# Only install (no services)
ansible-playbook -i inventory.yml playbook.yml --tags install

# Only apply kernel tuning
ansible-playbook -i inventory.yml playbook.yml --tags tuning

# Only restart services
ansible-playbook -i inventory.yml playbook.yml --tags service
```

### Available Tags

| Tag | Description |
|-----|-------------|
| `install` | Install SmartCopy binary and prerequisites |
| `configure` | Create configuration files |
| `tuning` | Apply kernel tuning parameters |
| `service` | Manage systemd services |
| `quic` | Configure QUIC server |

## Configuration

### Agent Configuration

The agent supports two protocols:

1. **TCP Mode** (standalone daemon):
   ```bash
   smartcopy agent --protocol tcp --port 9878 --bind 0.0.0.0
   ```

2. **STDIO Mode** (for SSH pipe):
   ```bash
   # Automatically spawned by client via SSH
   ssh user@host smartcopy agent --protocol stdio
   ```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RUST_LOG` | Log level (trace, debug, info, warn, error) | info |
| `SMARTCOPY_THREADS` | Number of worker threads | auto |
| `SMARTCOPY_BUFFER_SIZE` | Buffer size in bytes | 16777216 |
| `SMARTCOPY_STREAMS` | Parallel streams | 4 |

### Kernel Tuning by Speed Tier

| Parameter | 10G | 100G | 200G | 400G |
|-----------|-----|------|------|------|
| `rmem_max` | 64MB | 512MB | 1GB | 2GB |
| `wmem_max` | 64MB | 512MB | 1GB | 2GB |
| `netdev_max_backlog` | 30K | 250K | 500K | 1M |
| `somaxconn` | 4K | 64K | 64K | 64K |

## Verification

### Check Service Status

```bash
sudo systemctl status smartcopy-agent
sudo journalctl -u smartcopy-agent -f
```

### Test Agent Connection

```bash
# From client machine
smartcopy /local/file user@agent-host:9878:/remote/path --tcp-agent

# Or using SSH stdio mode
smartcopy /local/file user@agent-host:/remote/path --use-agent
```

### Verify Kernel Tuning

```bash
# Check TCP buffers
sysctl net.core.rmem_max net.core.wmem_max

# Check congestion control
sysctl net.ipv4.tcp_congestion_control

# Check file descriptor limit
ulimit -n
```

### Network Performance Test

```bash
# On agent host
iperf3 -s

# From client
iperf3 -c agent-host -P 16 -t 60
```

## Troubleshooting

### Agent Won't Start

1. Check logs:
   ```bash
   sudo journalctl -u smartcopy-agent -n 50
   cat /var/log/smartcopy/agent.error.log
   ```

2. Verify binary:
   ```bash
   /usr/local/bin/smartcopy --version
   ```

3. Check port availability:
   ```bash
   ss -tlnp | grep 9878
   ```

### Connection Refused

1. Check firewall:
   ```bash
   sudo iptables -L -n | grep 9878
   ```

2. Verify agent is running:
   ```bash
   ps aux | grep smartcopy
   ```

3. Test from localhost:
   ```bash
   nc -zv localhost 9878
   ```

### Poor Performance

1. Check kernel tuning:
   ```bash
   sysctl -a | grep -E "(rmem|wmem|backlog)"
   ```

2. Verify MTU:
   ```bash
   ip link show | grep mtu
   ```

3. Check for packet loss:
   ```bash
   ethtool -S eth0 | grep -i error
   ```

4. View tuning recommendations:
   ```bash
   smartcopy highspeed 100g
   ```

### QUIC Connection Issues

1. Check UDP port:
   ```bash
   ss -ulnp | grep 9877
   ```

2. Verify certificates:
   ```bash
   ls -la /var/lib/smartcopy/certs/
   ```

3. Check QUIC logs:
   ```bash
   sudo journalctl -u smartcopy-quic -f
   ```

## Security Considerations

### Network Security

- Use firewall rules to restrict access to agent ports
- Consider VPN or private network for sensitive transfers
- QUIC provides TLS 1.3 encryption by default

### File System Security

- Agent runs as unprivileged `smartcopy` user
- Configure `allowed_paths` to restrict accessible directories
- Monitor logs for unauthorized access attempts

### SSH Security

When using SSH stdio mode:
- Agent inherits SSH security (key-based auth, etc.)
- No additional ports need to be opened
- Recommended for internet-facing transfers

## Support

- GitHub Issues: https://github.com/smartcopy/smartcopy/issues
- Documentation: https://github.com/smartcopy/smartcopy/wiki
