//! Read-only local dashboard HTTP server.
//!
//! Wire with `#[path = "dashboard/mod.rs"] pub mod dashboard;`.

mod dashboard_server;

pub use dashboard_server::{
    dashboard_instance_id, is_allowed_dashboard_host, DashboardServer, DashboardServerOptions,
    DASHBOARD_ID_HEADER, DASHBOARD_ID_VALUE, DASHBOARD_INSTANCE_HEADER, DEFAULT_DASHBOARD_HOST,
    DEFAULT_DASHBOARD_PORT,
};
