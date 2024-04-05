mod core_workload;

pub use core_workload::CoreWorkload;
use std::future::Future;

use crate::db::DB;

pub trait Workload {
    fn do_insert<T: DB>(&self, db: &T) -> impl Future<Output = ()>;
    fn do_transaction<T: DB>(&self, db: &T) -> impl Future<Output = ()>;
}
