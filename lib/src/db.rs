use std::collections::HashMap;
use std::future::Future;

use anyhow::Result;

pub trait DB: Send + Sync {
    fn init(&self) -> Result<()>;
    fn insert(
        &self,
        table: String,
        key: String,
        values: HashMap<String, String>,
    ) -> impl Future<Output = Result<()>> + Send;
    fn read(
        &self,
        table: &str,
        key: &str,
    ) -> impl Future<Output = Result<HashMap<String, String>>> + Send;
}
