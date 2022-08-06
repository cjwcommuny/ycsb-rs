use crate::db::DB;
use crate::sqlite::SQLite;
use crate::workload::Workload;
use anyhow::{bail, Result};
use futures::future::join_all;
use properties::Properties;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use structopt::StructOpt;
use workload::CoreWorkload;

pub mod db;
pub mod generator;
pub mod properties;
pub mod sqlite;
pub mod workload;

#[derive(StructOpt, Debug)]
#[structopt(name = "ycsb")]
struct Opt {
    #[structopt(name = "COMMANDS")]
    commands: Vec<String>,
    #[structopt(short, long)]
    workload: String,
    #[structopt(short, long, default_value = "1")]
    threads: usize,
}

async fn load<T: DB + Clone>(wl: Arc<CoreWorkload>, db: T, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_insert(db.clone()).await;
    }
}

async fn run<T: DB + Clone>(wl: Arc<CoreWorkload>, db: T, operation_count: usize) {
    for _ in 0..operation_count {
        wl.do_transaction(db.clone()).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let raw_props = fs::read_to_string(&opt.workload)?;

    let props: Properties = toml::from_str(&raw_props)?;

    let props = Arc::new(props);

    let wl = Arc::new(CoreWorkload::new(&props));

    let commands = opt.commands;

    if commands.is_empty() {
        bail!("no command specified");
    }

    let database = SQLite::new(Path::new("test.db"))?;
    let n_threads = opt.threads;
    let operation_count = props.operation_count as usize;
    ycsb_run(database, commands, wl, operation_count, n_threads)
}

fn ycsb_run<T: DB + Clone + 'static>(
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
