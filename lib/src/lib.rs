use crate::db::DB;
use crate::workload::Workload;
use anyhow::Result;
use std::sync::Arc;
use std::time::Instant;
use workload::CoreWorkload;

pub mod db;
pub mod generator;
pub mod properties;
pub mod workload;

fn load<T: DB + Clone>(wl: Arc<CoreWorkload>, db: T, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_insert(db.clone()).await;
    }
}

async fn run<T: DB + Clone>(wl: Arc<CoreWorkload>, db: T, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_transaction(db.clone()).await;
    }
}

pub fn ycsb_run<T: DB + Clone + 'static>(
    db: T,
    commands: Vec<String>,
    wl: Arc<CoreWorkload>,
    operation_count: usize,
    n_threads: usize,
) -> Result<()> {
    let thread_operation_count = operation_count as usize / n_threads;
    for cmd in commands {
        let start = Instant::now();
        let mut threads = vec![];
        for _ in 0..n_threads {
            let database = db.clone();
            let wl = wl.clone();
            let cmd = cmd.clone();
            let task = tokio::task::spawn(async move {
                let wl = wl.clone();
                let db = db::create_db(&database).unwrap();

                db.init().unwrap();
                match &cmd[..] {
                    "load" => load(wl.clone(), db, thread_operation_count as usize).await,
                    "run" => run(wl.clone(), db, thread_operation_count as usize).await,
                    cmd => panic!("invalid command: {}", cmd),
                };
            });
            threads.push(task);
        }
        join_all(threads).await;
        let runtime = start.elapsed().as_millis();
        println!("[OVERALL], ThreadCount, {}", n_threads);
        println!("[OVERALL], RunTime(ms), {}", runtime);
        let throughput = operation_count as f64 / (runtime as f64 / 1000.0);
        println!("[OVERALL], Throughput(ops/sec), {}", throughput);
    }

    Ok(())
}
