//! SmartCopy Dashboard API Server
//!
//! REST API for monitoring and managing SmartCopy operations in large-scale
//! HPC environments. The dashboard is designed for enterprise deployments
//! where visibility into transfer operations is critical.
//!
//! ## Use Cases
//!
//! The dashboard API is recommended for:
//! - Large HPC clusters with hundreds of nodes
//! - Enterprise data centers with petabyte-scale transfers
//! - Multi-tenant environments requiring audit trails
//! - Operations teams needing real-time visibility
//!
//! For small-scale or single-user environments, the CLI interface
//! provides sufficient functionality without additional overhead.
//!
//! ## API Endpoints
//!
//! | Endpoint | Method | Description |
//! |----------|--------|-------------|
//! | `/api/status` | GET | System status and health |
//! | `/api/jobs` | GET | List active transfer jobs |
//! | `/api/jobs/{id}` | GET | Get job details |
//! | `/api/jobs` | POST | Create new transfer job |
//! | `/api/jobs/{id}` | DELETE | Cancel job |
//! | `/api/history` | GET | Transfer history with comparison |
//! | `/api/history/{id}` | GET | Detailed history entry |
//! | `/api/compare` | GET | Compare multiple transfers |
//! | `/api/agents` | GET | List connected agents |
//! | `/api/metrics` | GET | Performance metrics (Prometheus format) |
//! | `/api/system` | GET | System resource information |

mod server;
mod handlers;
mod models;
mod history;

pub use server::*;
pub use handlers::*;
pub use models::*;
pub use history::*;
