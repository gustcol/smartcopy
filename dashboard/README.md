# SmartCopy Dashboard

Real-time monitoring dashboard for SmartCopy file transfers in **large-scale HPC environments**.

## Overview

The SmartCopy Dashboard provides visibility into transfer operations for enterprise deployments where monitoring and auditing are critical. It is designed for:

- Large HPC clusters with hundreds of nodes
- Enterprise data centers with petabyte-scale transfers
- Multi-tenant environments requiring audit trails
- Operations teams needing real-time visibility

> **IMPORTANT**: For small-scale or single-user environments, the SmartCopy CLI provides sufficient functionality without additional overhead.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        DASHBOARD CONTAINER                          │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │                   React Dashboard                             │  │
│  │  - Real-time job monitoring                                   │  │
│  │  - Transfer history & comparison                              │  │
│  │  - Agent status                                               │  │
│  │  - Performance analytics                                      │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                              │                                      │
│                     nginx reverse proxy                             │
│                              │                                      │
└──────────────────────────────┼──────────────────────────────────────┘
                               │
                           /api/*
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    HOST SYSTEM (Native)                             │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              SmartCopy API Server                             │  │
│  │  smartcopy api-server --port 8080 --bind 0.0.0.0             │  │
│  └──────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              SmartCopy Transfers                              │  │
│  │  - Direct disk I/O (no container overhead)                   │  │
│  │  - io_uring, mmap, copy_file_range                           │  │
│  │  - Maximum performance                                        │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Why NOT Run SmartCopy in Docker?

Running SmartCopy inside a Docker container would significantly impact I/O performance:

| Aspect | Native | Docker |
|--------|--------|--------|
| `io_uring` | Full support | Limited/disabled |
| `mmap` | Direct memory mapping | Through overlay filesystem |
| `copy_file_range` | Zero-copy | Additional syscall overhead |
| Disk I/O | Direct | Through storage driver |
| Network | Direct | Through network driver |

**Only the dashboard runs in Docker** - SmartCopy itself runs natively on the host for maximum performance.

## Features

### Real-time Monitoring
- Active transfer jobs with progress bars
- Throughput metrics and ETA
- Connected agent status

### Transfer History
- Complete history of all transfers
- Aggregate statistics (7/30/90 days)
- Success/failure tracking

### Performance Comparison
- Compare multiple transfers side-by-side
- Trend analysis (improving/stable/degrading)
- Anomaly detection (throughput drops, high failure rates)
- Automatic recommendations

### System Information
- CPU, memory, storage details
- io_uring and NUMA support status
- Storage device types and usage

### Prometheus Metrics
- `/api/metrics` endpoint
- Ready for Grafana integration

## Quick Start

### 1. Start SmartCopy API Server (on host)

```bash
# The API server runs natively on the host
smartcopy api-server --port 8080 --bind 0.0.0.0
```

### 2. Start Dashboard (Docker)

```bash
cd dashboard

# Using Docker Compose
docker-compose up -d

# Or build and run manually
docker build -t smartcopy-dashboard .
docker run -d -p 3000:80 \
  -e API_URL=http://host.docker.internal:8080 \
  --name smartcopy-dashboard \
  smartcopy-dashboard
```

### 3. Access Dashboard

Open http://localhost:3000 in your browser.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `API_URL` | SmartCopy API server URL | `http://host.docker.internal:8080` |

### API Server Options

```bash
smartcopy api-server [OPTIONS]

OPTIONS:
    --port <PORT>       Server port [default: 8080]
    --bind <ADDR>       Bind address [default: 127.0.0.1]
    --api-key <KEY>     Optional API key for authentication
    --history <PATH>    History storage path
```

## Development

### Prerequisites

- Node.js 20+
- npm or yarn

### Setup

```bash
cd dashboard
npm install
npm run dev
```

The development server starts at http://localhost:3000 with hot reload.

### Build

```bash
npm run build
```

Build output is in the `dist/` directory.

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/status` | GET | System status and health |
| `/api/jobs` | GET | List active transfer jobs |
| `/api/jobs/{id}` | GET | Get job details |
| `/api/jobs` | POST | Create new transfer job |
| `/api/jobs/{id}` | DELETE | Cancel job |
| `/api/history` | GET | Transfer history |
| `/api/history/stats` | GET | Aggregate statistics |
| `/api/compare` | GET | Compare transfers |
| `/api/agents` | GET | Connected agents |
| `/api/system` | GET | System information |
| `/api/metrics` | GET | Prometheus metrics |

## Integration

### Prometheus

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'smartcopy'
    static_configs:
      - targets: ['smartcopy-host:8080']
    metrics_path: '/api/metrics'
```

### Kubernetes

For Kubernetes deployments, use the provided Helm chart (coming soon) or create a Deployment:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: smartcopy-dashboard
spec:
  replicas: 2
  selector:
    matchLabels:
      app: smartcopy-dashboard
  template:
    spec:
      containers:
      - name: dashboard
        image: smartcopy-dashboard:latest
        ports:
        - containerPort: 80
        env:
        - name: API_URL
          value: "http://smartcopy-api:8080"
```

## License

MIT License - see [LICENSE](../LICENSE) for details.
