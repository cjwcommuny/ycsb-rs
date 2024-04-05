mod core_workload;

use std::future::Future;
pub use core_workload::CoreWorkload;

use crate::db::DB;

pub trait Workload {
    fn do_insert<T: DB>(&self, db: &T) -> impl Future<Output = ()>;
    fn do_transaction<T: DB>(&self, db: &T) -> impl Future<Output = ()>;
}
