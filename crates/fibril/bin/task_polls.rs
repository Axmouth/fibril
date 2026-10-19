//! Diagnostic: count Tokio task polls by spawn site. Built only with the
//! `task-polls` feature and `--cfg tokio_unstable`. A task polled again was
//! woken again, so polls per message by spawn site map the handoffs a message
//! takes through the broker. Counts go to `FIBRIL_TASK_POLLS_PATH` as
//! `{"file:line": polls}` once a second.
use std::cell::RefCell;
use std::collections::HashMap;
use std::panic::Location;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

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
}

fn merge(local: &mut HashMap<usize, (Site, u64)>) {
    let mut shared = shared().lock().unwrap_or_else(|e| e.into_inner());
    for (key, (site, n)) in local.drain() {
        shared.entry(key).or_insert((site, 0)).1 += n;
    }
}

pub fn count(meta: &tokio::runtime::TaskMeta<'_>) {
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
        })
        .expect("failed to spawn the task poll writer");
}
