mod core_workload;

pub use core_workload::CoreWorkload;

use crate::db::DB;
use async_trait::async_trait;

#[async_trait]
pub trait Workload {
    async fn do_insert<T: DB>(&self, db: T);
    async fn do_transaction<T: DB>(&self, db: T);
}
