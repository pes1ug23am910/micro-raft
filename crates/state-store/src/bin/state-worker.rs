//! Bounded JSONL engine worker. The observer supplies the operation clock and
//! treats a missing response to a mutation as outcome-unknown.
use serde::Deserialize;
use serde_json::json;
use state_store::{LsmOptions, LsmStore, Mutation, RedbStore, StateStore};
use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;
use std::time::Instant;

const MAX_LINE: usize = 72 * 1024 * 1024;

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    Commit {
        index: u64,
        changes: Vec<Mutation>,
    },
    CommitRange {
        first: u64,
        index: u64,
        changes: Vec<Mutation>,
    },
    Get {
        key: Vec<u8>,
    },
    Scan,
    Install {
        index: u64,
        cells: Vec<Mutation>,
    },
    Flush,
    Maintain,
    Statistics,
    Exit,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("state-worker: {error}");
        std::process::exit(1);
    }
}

fn run() -> io::Result<()> {
    let mut args = std::env::args().skip(1);
    let mut engine = None;
    let mut directory = None;
    let mut options = LsmOptions::default();
    while let Some(arg) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| io::Error::other("each argument requires a value"))?;
        match arg.as_str() {
            "--engine" => engine = Some(value),
            "--directory" => directory = Some(PathBuf::from(value)),
            "--memtable-bytes" => {
                options.memtable_bytes = value.parse().map_err(io::Error::other)?
            }
            "--table-bytes" => {
                options.target_table_bytes = value.parse().map_err(io::Error::other)?
            }
            "--level1-bytes" => options.level1_bytes = value.parse().map_err(io::Error::other)?,
            _ => return Err(io::Error::other(format!("unknown option {arg}"))),
        }
    }
    let directory = directory.ok_or_else(|| io::Error::other("--directory required"))?;
    let kind = engine.ok_or_else(|| io::Error::other("--engine required"))?;
    let mut store: Box<dyn StateStore> = match kind.as_str() {
        "lsm" => Box::new(LsmStore::open(&directory, options.clone())?),
        "redb" => Box::new(RedbStore::open(&directory)?),
        _ => return Err(io::Error::other("engine must be lsm or redb")),
    };
    let mut output = io::BufWriter::new(io::stdout().lock());
    send(
        &mut output,
        &json!({"schema_version":1,"ready":true,"engine":kind,"applied_index":store.applied_index(),
        "durability":"synchronous","lsm_options":options,"statistics":store.statistics()?}),
    )?;
    let mut input = io::BufReader::new(io::stdin().lock());
    loop {
        let mut bytes = Vec::new();
        let read = (&mut input)
            .take((MAX_LINE + 1) as u64)
            .read_until(b'\n', &mut bytes)?;
        if read == 0 {
            return Ok(());
        }
        if bytes.len() > MAX_LINE || bytes.last() != Some(&b'\n') {
            send(
                &mut output,
                &json!({"outcome":"rejected","error":"request line exceeds bound or is incomplete"}),
            )?;
            return Err(io::Error::other("invalid request framing"));
        }
        let request: Request = match serde_json::from_slice(&bytes) {
            Ok(request) => request,
            Err(error) => {
                send(
                    &mut output,
                    &json!({"outcome":"rejected","error":error.to_string()}),
                )?;
                continue;
            }
        };
        if matches!(request, Request::Exit) {
            store.flush()?;
            send(
                &mut output,
                &json!({"outcome":"ok","exited":true,"statistics":store.statistics()?}),
            )?;
            return Ok(());
        }
        let mutation = matches!(
            request,
            Request::Commit { .. } | Request::CommitRange { .. } | Request::Install { .. }
        );
        let started = Instant::now();
        let result: io::Result<serde_json::Value> = (|| {
            Ok(match request {
                Request::Commit { index, changes } => {
                    let outcome = store.commit(index, &changes)?;
                    let compacted = store.maintain()?;
                    json!({"outcome":"ok","commit":outcome,"applied_index":store.applied_index(),"compacted":compacted})
                }
                Request::CommitRange {
                    first,
                    index,
                    changes,
                } => {
                    let outcome = store.commit_range(first, index, &changes)?;
                    let compacted = store.maintain()?;
                    json!({"outcome":"ok","commit":outcome,"applied_index":store.applied_index(),"compacted":compacted})
                }
                Request::Get { key } => {
                    let value = store.get(&key)?;
                    json!({"outcome":if value.is_some(){"ok"}else{"absent"},"value":value,"applied_index":store.applied_index()})
                }
                Request::Scan => {
                    json!({"outcome":"ok","rows":store.scan()?,"applied_index":store.applied_index()})
                }
                Request::Install { index, cells } => {
                    store.install(index, &cells)?;
                    json!({"outcome":"ok","applied_index":store.applied_index()})
                }
                Request::Flush => {
                    store.flush()?;
                    json!({"outcome":"ok"})
                }
                Request::Maintain => json!({"outcome":"ok","compacted":store.maintain()?}),
                Request::Statistics => json!({"outcome":"ok","statistics":store.statistics()?}),
                Request::Exit => unreachable!(),
            })
        })();
        let elapsed = started.elapsed().as_nanos();
        match result {
            Ok(mut response) => {
                response["engine_elapsed_ns"] = json!(elapsed);
                send(&mut output, &response)?;
            }
            Err(error) => {
                let rejected = error.kind() == io::ErrorKind::InvalidInput;
                send(
                    &mut output,
                    &json!({"outcome":if rejected{"rejected"}else if mutation{"unknown"}else{"error"},"error":error.to_string(),"engine_elapsed_ns":elapsed,"fatal":!rejected}),
                )?;
                if !rejected {
                    return Err(error);
                }
            }
        }
    }
}

fn send(output: &mut impl Write, response: &serde_json::Value) -> io::Result<()> {
    serde_json::to_writer(&mut *output, response).map_err(io::Error::other)?;
    output.write_all(b"\n")?;
    output.flush()
}
