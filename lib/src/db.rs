use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait DB: Send + Sync {
    fn init(&self) -> Result<()>;
    async fn insert(
        &self,
        table: String,
        key: String,
        values: HashMap<String, String>,
    ) -> Result<()>;
    async fn read(&self, table: &str, key: &str) -> Result<HashMap<String, String>>;
}
