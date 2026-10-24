//! Diagnostic: count Tokio task polls by spawn site. Built only with the
//! `task-polls` feature and `--cfg tokio_unstable`. A task polled again was
//! woken again, so polls per message by spawn site map the handoffs a message
//! takes through the broker. Counts go to `FIBRIL_TASK_POLLS_PATH` as
//! `{"file:line": polls}` once a second. Polls of 1 ms or more go to
//! `FIBRIL_LONG_POLLS_PATH`, when set, as
//! `{"file:line": [over 1 ms, over 4 ms, total us, max us]}`, since one long
//! poll holds its worker and delays every task queued behind it.
use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::Location;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

type Site = &'static Location<'static>;

/// Polls a thread counts before merging into the shared map, so the hook takes
/// no lock on most polls.
const MERGE_EVERY: u64 = 256;

fn shared() -> &'static Mutex<HashMap<usize, (Site, u64)>> {
    static SHARED: OnceLock<Mutex<HashMap<usize, (Site, u64)>>> = OnceLock::new();
    SHARED.get_or_init(Default::default)
}

thread_local! {
    static LOCAL: RefCell<(u64, HashMap<usize, (Site, u64)>)> = RefCell::new((0, HashMap::new()));
    static POLL_STARTED: RefCell<Option<Instant>> = const { RefCell::new(None) };
}

type LongPolls = HashMap<usize, (Site, [u64; 4])>;

fn long_polls() -> &'static Mutex<LongPolls> {
    static LONG: OnceLock<Mutex<LongPolls>> = OnceLock::new();
    LONG.get_or_init(Default::default)
}

/// Records the poll's duration if it held the worker for 1 ms or more. Long
/// polls are rare, so they go straight to the shared map.
pub fn finish(meta: &tokio::runtime::TaskMeta<'_>) {
    let Some(started) = POLL_STARTED.with(|s| s.borrow_mut().take()) else {
        return;
    };
    let us = started.elapsed().as_micros() as u64;
    if us < 1000 {
        return;
    }
    let site = meta.spawned_at();
    let mut long = long_polls().lock().unwrap_or_else(|e| e.into_inner());
    let entry = &mut long.entry(site as *const _ as usize).or_insert((site, [0; 4])).1;
    entry[0] += 1;
    if us >= 4000 {
        entry[1] += 1;
    }
    entry[2] += us;
    entry[3] = entry[3].max(us);
}

fn merge(local: &mut HashMap<usize, (Site, u64)>) {
    let mut shared = shared().lock().unwrap_or_else(|e| e.into_inner());
    for (key, (site, n)) in local.drain() {
        shared.entry(key).or_insert((site, 0)).1 += n;
    }
}

pub fn count(meta: &tokio::runtime::TaskMeta<'_>) {
    POLL_STARTED.with(|s| *s.borrow_mut() = Some(Instant::now()));
    let site = meta.spawned_at();
    LOCAL.with(|local| {
        let mut local = local.borrow_mut();
        let (polls, counts) = &mut *local;
        counts.entry(site as *const _ as usize).or_insert((site, 0)).1 += 1;
        *polls += 1;
        if *polls % MERGE_EVERY == 0 {
            merge(counts);
        }
    });
}

pub fn start_writer() {
    let Ok(path) = std::env::var("FIBRIL_TASK_POLLS_PATH") else {
        return;
    };
    let long_path = std::env::var("FIBRIL_LONG_POLLS_PATH").ok();
    std::thread::Builder::new()
        .name("task-polls".into())
        .spawn(move || loop {
            std::thread::sleep(Duration::from_secs(1));
            let snapshot: HashMap<String, u64> = shared()
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .map(|(site, n)| (format!("{}:{}", site.file(), site.line()), *n))
                .collect();
            if let Ok(json) = serde_json::to_vec(&snapshot) {
                let tmp = format!("{path}.tmp");
                if std::fs::write(&tmp, json).is_ok() {
                    let _ = std::fs::rename(&tmp, &path);
                }
            }
            if let Some(long_path) = &long_path {
                let long: HashMap<String, [u64; 4]> = long_polls()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .values()
                    .map(|(site, v)| (format!("{}:{}", site.file(), site.line()), *v))
                    .collect();
                if let Ok(json) = serde_json::to_vec(&long) {
                    let tmp = format!("{long_path}.tmp");
                    if std::fs::write(&tmp, json).is_ok() {
                        let _ = std::fs::rename(&tmp, long_path);
                    }
                }
            }
        })
        .expect("failed to spawn the task poll writer");
}
