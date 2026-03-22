use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub database: DatabaseConfig,
    #[serde(default)]
    pub server: ServerConfig,
    pub jwt: JwtConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatabaseConfig {
    pub uri: String,
    pub schemas: Vec<String>,
    pub anon_role: String,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    /// Set to false for PgBouncer transaction-mode compatibility.
    /// Disables connection recycling checks that rely on prepared statements.
    #[serde(default = "default_true")]
    pub prepared_statements: bool,
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_host")]
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    /// "text" (default) or "json" for structured JSON logging.
    #[serde(default = "default_log_format")]
    pub log_format: String,
}

fn default_log_format() -> String {
    "text".to_string()
}

#[derive(Debug, Deserialize)]
pub struct JwtConfig {
    pub secret: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            host: default_host(),
            port: default_port(),
            log_format: default_log_format(),
        }
    }
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}
fn default_port() -> u16 {
    3000
}
fn default_pool_size() -> usize {
    10
}
fn default_true() -> bool {
    true
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
