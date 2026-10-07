//! On-disk layout of a Fibril node, rooted at `server.data_dir`.
//!
//! ```text
//! <data_dir>/
//!   fibril-node        layout marker: magic, layout version, node id
//!   node/              setup_complete, config-overlay.toml, cluster.secret, tls/, tls-provided/
//!   coordination/      default Ganglion data dir, plus a coordination-layout marker
//!   storage/           Stroma root
//!   cache/             disposable, safe to delete while the node is stopped
//!     recovery-inspection/
//! ```
//!
//! A data dir is either fresh (missing, empty, or holding only `lost+found`)
//! or carries a `fibril-node` marker with a known version and this node's id.
//! Anything else is refused, so an older layout or another node's data is
//! never opened by accident. There is no migration from older layouts.
//!
//! Small files here are replaced atomically: write a temporary file next to
//! the target, sync it, rename it over the target and sync the directory.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

pub use keratin_log::util::fsync_dir;

/// Marker file at the data dir root.
pub const NODE_MARKER_FILE: &str = "fibril-node";
/// First value in the node marker, identifying the file.
pub const NODE_MARKER_MAGIC: &str = "fibril-node";
/// Node layout version this build reads and writes.
pub const NODE_LAYOUT_VERSION: u64 = 2;

/// Node-owned files: setup state, secrets and TLS material.
pub const NODE_DIR: &str = "node";
/// Default Ganglion data dir.
pub const COORDINATION_DIR: &str = "coordination";
/// Stroma storage root.
pub const STORAGE_DIR: &str = "storage";
/// Disposable caches.
pub const CACHE_DIR: &str = "cache";
/// Recovery inspection cache under `cache/`.
pub const RECOVERY_INSPECTION_CACHE_DIR: &str = "recovery-inspection";
/// Created by some filesystems at their root, so a mount point used directly
/// as the data dir holds it from the start.
pub const LOST_AND_FOUND: &str = "lost+found";

/// Generated TLS material under `node/`.
pub const GENERATED_TLS_DIR: &str = "tls";
/// Operator material uploaded through first-boot setup, under `node/`. Kept
/// apart from the generated dir so the partial-material check there stays
/// meaningful.
pub const PROVIDED_TLS_DIR: &str = "tls-provided";

/// Marker file inside the coordination data dir.
pub const COORDINATION_MARKER_FILE: &str = "coordination-layout";
/// First value in the coordination marker, identifying the file.
pub const COORDINATION_MARKER_MAGIC: &str = "fibril-coordination";
/// Coordination layout version this build reads and writes.
pub const COORDINATION_LAYOUT_VERSION: u64 = 1;

/// Top-level names of the data dir layout before the node marker existed.
/// Used only to give a precise error when one is found.
const LEGACY_ROOT_ENTRIES: &[&str] = &[
    "setup_complete",
    "config-overlay.toml",
    "cluster.secret",
    "tls",
    "tls-provided",
    "events",
    "messages",
    "snapshots",
    "global",
    "tmp",
    "combined-queue-log.v1",
    "recovery-routes",
    "recovery-installed",
];

#[derive(Debug, thiserror::Error)]
pub enum NodeLayoutError {
    #[error("{action} {path}: {source}")]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("data dir {path} is not a directory")]
    RootNotDirectory { path: PathBuf },
    #[error(
        "data dir {root} uses an older Fibril data layout (found {found}). This version \
         cannot open it and there is no migration. Move the directory aside and start \
         with an empty data dir"
    )]
    OldLayout { root: PathBuf, found: String },
    #[error(
        "data dir {root} is not empty and has no {NODE_MARKER_FILE} marker (found {found}). \
         Point server.data_dir at an empty directory or an existing Fibril node data dir"
    )]
    NotANodeDir { root: PathBuf, found: String },
    #[error(
        "data dir {root} holds entries that are not part of the node layout: {found}. \
         Remove them or move them elsewhere"
    )]
    UnknownEntries { root: PathBuf, found: String },
    #[error("{path} is not a valid Fibril layout marker: {detail}")]
    BadMarker { path: PathBuf, detail: String },
    #[error(
        "data dir {root} has node layout version {found}, this build supports only \
         version {NODE_LAYOUT_VERSION}"
    )]
    UnknownVersion { root: PathBuf, found: u64 },
    #[error(
        "data dir {root} belongs to node `{found}`, but this node is configured as \
         `{expected}`. Set coordination.node_id (or FIBRIL_COORDINATION_NODE_ID) to the \
         node that owns this data dir, or use a different data dir"
    )]
    ForeignNode {
        root: PathBuf,
        found: String,
        expected: String,
    },
    #[error("{path} must be a {expected}")]
    WrongEntryKind {
        path: PathBuf,
        expected: &'static str,
    },
    #[error(
        "data dir {root} is missing {entry}/ while other node data exists. Refusing to \
         start rather than recreate it empty"
    )]
    MissingEntry { root: PathBuf, entry: &'static str },
    #[error(
        "coordination dir {dir} already holds data but has no {COORDINATION_MARKER_FILE} \
         marker (found {found}). It was written by an older layout or another tool. Move \
         it aside or point coordination.ganglion.data_dir at an empty directory"
    )]
    CoordinationUnmarked { dir: PathBuf, found: String },
    #[error(
        "coordination dir {dir} has layout version {found}, this build supports only \
         version {COORDINATION_LAYOUT_VERSION}"
    )]
    CoordinationUnknownVersion { dir: PathBuf, found: u64 },
    #[error(
        "coordination dir {dir} belongs to raft node {found}, but this node is configured \
         with raft id {expected}. Fix coordination.ganglion.raft_node_id or use a \
         different coordination dir"
    )]
    CoordinationForeignNode {
        dir: PathBuf,
        found: u64,
        expected: u64,
    },
}

type Result<T> = std::result::Result<T, NodeLayoutError>;

fn io_err<'a>(action: &'static str, path: &'a Path) -> impl FnOnce(io::Error) -> NodeLayoutError + 'a {
    move |source| NodeLayoutError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

/// Paths of a node's data dir. Building one does no I/O. Use
/// [`NodeLayout::open_or_init`] to validate or create the directory first.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLayout {
    root: PathBuf,
}

impl NodeLayout {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn marker(&self) -> PathBuf {
        self.root.join(NODE_MARKER_FILE)
    }

    pub fn node(&self) -> PathBuf {
        self.root.join(NODE_DIR)
    }

    pub fn storage(&self) -> PathBuf {
        self.root.join(STORAGE_DIR)
    }

    pub fn cache(&self) -> PathBuf {
        self.root.join(CACHE_DIR)
    }

    pub fn recovery_inspection_cache(&self) -> PathBuf {
        self.cache().join(RECOVERY_INSPECTION_CACHE_DIR)
    }

    /// Default Ganglion data dir. `coordination.ganglion.data_dir` may point
    /// elsewhere.
    pub fn coordination(&self) -> PathBuf {
        self.root.join(COORDINATION_DIR)
    }

    pub fn setup_marker(&self) -> PathBuf {
        self.node().join(crate::SETUP_MARKER_FILE)
    }

    pub fn config_overlay(&self) -> PathBuf {
        self.node().join(crate::CONFIG_OVERLAY_FILE)
    }

    pub fn cluster_secret(&self) -> PathBuf {
        self.node().join(crate::CLUSTER_SECRET_FILE)
    }

    pub fn generated_tls(&self) -> PathBuf {
        self.node().join(GENERATED_TLS_DIR)
    }

    pub fn provided_tls(&self) -> PathBuf {
        self.node().join(PROVIDED_TLS_DIR)
    }

    /// Validate the data dir for `node_id`, creating the layout when the
    /// directory is fresh. Refuses older layouts, other nodes' data dirs,
    /// unknown versions and unknown top-level entries.
    pub fn open_or_init(root: impl Into<PathBuf>, node_id: &str) -> Result<Self> {
        let layout = Self::new(root);
        layout.prepare(node_id)?;
        Ok(layout)
    }

    fn prepare(&self, node_id: &str) -> Result<()> {
        let root = &self.root;
        match fs::metadata(root) {
            Ok(meta) if meta.is_dir() => {}
            Ok(_) => return Err(NodeLayoutError::RootNotDirectory { path: root.clone() }),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                create_root_dir(root)?;
            }
            Err(source) => {
                return Err(NodeLayoutError::Io {
                    action: "failed to inspect data dir",
                    path: root.clone(),
                    source,
                });
            }
        }

        let entries = list_entries(root)?;
        let marker_temp = temp_name(NODE_MARKER_FILE);
        let has_marker = entries.iter().any(|name| name == NODE_MARKER_FILE);

        if !has_marker {
            let others: Vec<&String> = entries
                .iter()
                .filter(|name| *name != LOST_AND_FOUND && **name != marker_temp)
                .collect();
            if others.is_empty() {
                return self.init_fresh(node_id);
            }
            let legacy: Vec<&str> = others
                .iter()
                .map(|name| name.as_str())
                .filter(|name| LEGACY_ROOT_ENTRIES.contains(name))
                .collect();
            if !legacy.is_empty() {
                return Err(NodeLayoutError::OldLayout {
                    root: root.clone(),
                    found: legacy.join(", "),
                });
            }
            return Err(NodeLayoutError::NotANodeDir {
                root: root.clone(),
                found: join_names(&others),
            });
        }

        self.check_marker(node_id)?;

        let known = [
            NODE_MARKER_FILE,
            NODE_DIR,
            COORDINATION_DIR,
            STORAGE_DIR,
            CACHE_DIR,
            LOST_AND_FOUND,
        ];
        let unknown: Vec<&String> = entries
            .iter()
            .filter(|name| !known.contains(&name.as_str()) && **name != marker_temp)
            .collect();
        if !unknown.is_empty() {
            return Err(NodeLayoutError::UnknownEntries {
                root: root.clone(),
                found: join_names(&unknown),
            });
        }
        // The marker is written once, so a leftover temp is from an
        // interrupted first write that later completed by other means.
        if entries.contains(&marker_temp) {
            remove_path(&root.join(&marker_temp))?;
            fsync_dir(root).map_err(io_err("failed to sync", root))?;
        }

        expect_kind(&self.marker(), false)?;
        for dir in [self.node(), self.coordination(), self.storage(), self.cache()] {
            if dir.exists() {
                expect_kind(&dir, true)?;
            }
        }

        let node_exists = self.node().exists();
        let storage_exists = self.storage().exists();
        if !node_exists || !storage_exists {
            // A crash between writing the marker and creating the
            // subdirectories leaves only empty directories behind. Finish
            // that initialization. Any other missing directory means data
            // was removed, which is refused.
            if !self.only_empty_dirs_besides_marker(&entries)? {
                let entry = if node_exists { STORAGE_DIR } else { NODE_DIR };
                return Err(NodeLayoutError::MissingEntry {
                    root: root.clone(),
                    entry,
                });
            }
        }
        self.create_subdirs()
    }

    fn only_empty_dirs_besides_marker(&self, entries: &[String]) -> Result<bool> {
        for name in entries {
            if name == NODE_MARKER_FILE || name == LOST_AND_FOUND {
                continue;
            }
            let path = self.root.join(name);
            if !path.is_dir() || !list_entries(&path)?.is_empty() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn init_fresh(&self, node_id: &str) -> Result<()> {
        let marker = encode_marker(&NodeMarker {
            magic: NODE_MARKER_MAGIC,
            layout_version: NODE_LAYOUT_VERSION,
            node_id,
        })?;
        write_file_atomic(&self.marker(), marker.as_bytes(), FileMode::Public)
            .map_err(io_err("failed to write", &self.marker()))?;
        self.create_subdirs()
    }

    fn create_subdirs(&self) -> Result<()> {
        for dir in [self.node(), self.storage(), self.cache()] {
            ensure_dir(&dir, &self.root)?;
        }
        Ok(())
    }

    fn check_marker(&self, node_id: &str) -> Result<()> {
        let path = self.marker();
        let table = read_marker(&path, NODE_MARKER_MAGIC)?;
        let version = marker_u64(&path, &table, "layout_version")?;
        if version != NODE_LAYOUT_VERSION {
            return Err(NodeLayoutError::UnknownVersion {
                root: self.root.clone(),
                found: version,
            });
        }
        let found = match table.get("node_id") {
            Some(toml::Value::String(value)) => value.clone(),
            _ => {
                return Err(NodeLayoutError::BadMarker {
                    path,
                    detail: "node_id is missing or not a string".to_string(),
                });
            }
        };
        if found != node_id {
            return Err(NodeLayoutError::ForeignNode {
                root: self.root.clone(),
                found,
                expected: node_id.to_string(),
            });
        }
        Ok(())
    }
}

/// Validate or create a Ganglion data dir for `raft_node_id`. A missing or
/// empty directory gets a fresh `coordination-layout` marker. An existing
/// marker must carry a known version and the same raft id. A non-empty
/// directory without a marker is refused, since its contents cannot be
/// attributed to this node.
pub fn prepare_coordination_dir(dir: &Path, raft_node_id: u64) -> Result<()> {
    match fs::metadata(dir) {
        Ok(meta) if meta.is_dir() => {}
        Ok(_) => {
            return Err(NodeLayoutError::WrongEntryKind {
                path: dir.to_path_buf(),
                expected: "directory",
            });
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => create_root_dir(dir)?,
        Err(source) => {
            return Err(NodeLayoutError::Io {
                action: "failed to inspect coordination dir",
                path: dir.to_path_buf(),
                source,
            });
        }
    }
    let marker = dir.join(COORDINATION_MARKER_FILE);
    let entries = list_entries(dir)?;
    if !entries.iter().any(|name| name == COORDINATION_MARKER_FILE) {
        let marker_temp = temp_name(COORDINATION_MARKER_FILE);
        let others: Vec<&String> = entries
            .iter()
            .filter(|name| *name != LOST_AND_FOUND && **name != marker_temp)
            .collect();
        if !others.is_empty() {
            return Err(NodeLayoutError::CoordinationUnmarked {
                dir: dir.to_path_buf(),
                found: join_names(&others),
            });
        }
        let contents = encode_marker(&CoordinationMarker {
            magic: COORDINATION_MARKER_MAGIC,
            layout_version: COORDINATION_LAYOUT_VERSION,
            raft_node_id,
        })?;
        return write_file_atomic(&marker, contents.as_bytes(), FileMode::Public)
            .map_err(io_err("failed to write", &marker));
    }
    expect_kind(&marker, false)?;
    let table = read_marker(&marker, COORDINATION_MARKER_MAGIC)?;
    let version = marker_u64(&marker, &table, "layout_version")?;
    if version != COORDINATION_LAYOUT_VERSION {
        return Err(NodeLayoutError::CoordinationUnknownVersion {
            dir: dir.to_path_buf(),
            found: version,
        });
    }
    let found = marker_u64(&marker, &table, "raft_node_id")?;
    if found != raft_node_id {
        return Err(NodeLayoutError::CoordinationForeignNode {
            dir: dir.to_path_buf(),
            found,
            expected: raft_node_id,
        });
    }
    Ok(())
}

/// Permissions for an atomically written file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMode {
    /// Default permissions.
    Public,
    /// Owner read and write only (0600) on unix. Keys and secrets.
    Secret,
}

/// Name of the temporary file used while replacing `name`. Hidden so it
/// stands out from real layout entries.
pub fn temp_name(name: &str) -> String {
    format!(".{name}.tmp")
}

/// Replace `path` with `contents` so a crash leaves either the old file or
/// the new one, never a partial write. The parent directory must exist.
pub fn write_file_atomic(path: &Path, contents: &[u8], mode: FileMode) -> io::Result<()> {
    let parent = parent_dir(path);
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?;
    let temp = parent.join(temp_name(&name.to_string_lossy()));
    // A stale temp from an interrupted write may carry other permissions,
    // and the create mode below applies only to new files.
    match fs::remove_file(&temp) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        if mode == FileMode::Secret {
            options.mode(0o600);
        }
    }
    #[cfg(not(unix))]
    let _ = mode;
    let mut file = options.open(&temp)?;
    file.write_all(contents)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp, path)?;
    fsync_dir(&parent)
}

/// Create `dir` and any missing parents below `owning_root`, syncing each new
/// directory and its parent. `owning_root` must already exist.
pub fn ensure_dir(dir: &Path, owning_root: &Path) -> Result<()> {
    match fs::metadata(dir) {
        Ok(meta) if meta.is_dir() => return Ok(()),
        Ok(_) => {
            return Err(NodeLayoutError::WrongEntryKind {
                path: dir.to_path_buf(),
                expected: "directory",
            });
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(NodeLayoutError::Io {
                action: "failed to inspect",
                path: dir.to_path_buf(),
                source,
            });
        }
    }
    let parent = parent_dir(dir);
    if parent != owning_root && dir != owning_root {
        ensure_dir(&parent, owning_root)?;
    }
    match fs::create_dir(dir) {
        Ok(()) => {}
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => return Ok(()),
        Err(source) => {
            return Err(NodeLayoutError::Io {
                action: "failed to create",
                path: dir.to_path_buf(),
                source,
            });
        }
    }
    fsync_dir(dir).map_err(io_err("failed to sync", dir))?;
    fsync_dir(&parent).map_err(io_err("failed to sync", &parent))
}

/// Create a root directory (data dir or an external coordination dir) and
/// sync its parent once so the new root survives a crash.
fn create_root_dir(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).map_err(io_err("failed to create", dir))?;
    fsync_dir(dir).map_err(io_err("failed to sync", dir))?;
    let parent = parent_dir(dir);
    fsync_dir(&parent).map_err(io_err("failed to sync", &parent))
}

/// Parent of `path`, with `.` for a bare relative name.
pub fn parent_dir(path: &Path) -> PathBuf {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    }
}

fn list_entries(dir: &Path) -> Result<Vec<String>> {
    let mut names = Vec::new();
    for entry in fs::read_dir(dir).map_err(io_err("failed to read", dir))? {
        let entry = entry.map_err(io_err("failed to read", dir))?;
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    Ok(names)
}

fn remove_path(path: &Path) -> Result<()> {
    let result = if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    };
    result.map_err(io_err("failed to remove", path))
}

fn expect_kind(path: &Path, dir: bool) -> Result<()> {
    let meta = fs::metadata(path).map_err(io_err("failed to inspect", path))?;
    if meta.is_dir() != dir {
        return Err(NodeLayoutError::WrongEntryKind {
            path: path.to_path_buf(),
            expected: if dir { "directory" } else { "file" },
        });
    }
    Ok(())
}

fn join_names(names: &[&String]) -> String {
    names
        .iter()
        .map(|name| name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

#[derive(serde::Serialize)]
struct NodeMarker<'a> {
    magic: &'a str,
    layout_version: u64,
    node_id: &'a str,
}

#[derive(serde::Serialize)]
struct CoordinationMarker<'a> {
    magic: &'a str,
    layout_version: u64,
    raft_node_id: u64,
}

fn encode_marker(marker: &impl serde::Serialize) -> Result<String> {
    toml::to_string(marker).map_err(|err| NodeLayoutError::Io {
        action: "failed to encode layout marker",
        path: PathBuf::new(),
        source: io::Error::new(io::ErrorKind::InvalidData, err.to_string()),
    })
}

fn read_marker(path: &Path, magic: &str) -> Result<toml::value::Table> {
    let raw = fs::read_to_string(path).map_err(io_err("failed to read", path))?;
    let table: toml::value::Table =
        toml::from_str(&raw).map_err(|err| NodeLayoutError::BadMarker {
            path: path.to_path_buf(),
            detail: err.to_string(),
        })?;
    match table.get("magic") {
        Some(toml::Value::String(found)) if found == magic => Ok(table),
        _ => Err(NodeLayoutError::BadMarker {
            path: path.to_path_buf(),
            detail: format!("expected magic `{magic}`"),
        }),
    }
}

fn marker_u64(path: &Path, table: &toml::value::Table, key: &str) -> Result<u64> {
    match table.get(key) {
        Some(toml::Value::Integer(value)) if *value >= 0 => Ok(*value as u64),
        _ => Err(NodeLayoutError::BadMarker {
            path: path.to_path_buf(),
            detail: format!("{key} is missing or not a non-negative integer"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_root(tag: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir = std::env::temp_dir().join(format!(
            "fibril-layout-{tag}-{}-{nanos}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn names(dir: &Path) -> Vec<String> {
        list_entries(dir).unwrap()
    }

    #[test]
    fn missing_data_dir_is_initialized() {
        let base = temp_root("missing");
        let root = base.join("data");
        let layout = NodeLayout::open_or_init(&root, "node-a").unwrap();
        assert_eq!(names(&root), ["cache", "fibril-node", "node", "storage"]);
        assert!(layout.node().is_dir() && layout.storage().is_dir() && layout.cache().is_dir());
        // Reopening with the same id succeeds and changes nothing.
        NodeLayout::open_or_init(&root, "node-a").unwrap();
        assert_eq!(names(&root), ["cache", "fibril-node", "node", "storage"]);
        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn empty_and_lost_and_found_only_dirs_are_fresh() {
        let root = temp_root("empty");
        NodeLayout::open_or_init(&root, "n").unwrap();
        assert!(root.join(NODE_MARKER_FILE).is_file());
        let _ = fs::remove_dir_all(&root);

        let root = temp_root("lost-found");
        fs::create_dir(root.join(LOST_AND_FOUND)).unwrap();
        NodeLayout::open_or_init(&root, "n").unwrap();
        assert_eq!(
            names(&root),
            ["cache", "fibril-node", "lost+found", "node", "storage"]
        );
        NodeLayout::open_or_init(&root, "n").unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn interrupted_marker_write_still_counts_as_fresh() {
        let root = temp_root("marker-temp");
        fs::write(root.join(temp_name(NODE_MARKER_FILE)), "partial").unwrap();
        NodeLayout::open_or_init(&root, "n").unwrap();
        assert_eq!(names(&root), ["cache", "fibril-node", "node", "storage"]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn old_layout_is_refused_with_a_clear_error() {
        let root = temp_root("old");
        for dir in ["events", "messages", "global", "tls"] {
            fs::create_dir(root.join(dir)).unwrap();
        }
        fs::write(root.join("setup_complete"), "").unwrap();
        let err = NodeLayout::open_or_init(&root, "n").unwrap_err();
        assert!(matches!(err, NodeLayoutError::OldLayout { .. }), "{err}");
        let text = err.to_string();
        assert!(text.contains("older Fibril data layout"), "{text}");
        assert!(text.contains("events"), "{text}");
        // Nothing was written into the old data dir.
        assert!(!root.join(NODE_MARKER_FILE).exists());
        assert!(!root.join(NODE_DIR).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unmarked_foreign_dir_is_refused() {
        let root = temp_root("foreign-dir");
        fs::write(root.join("notes.txt"), "hello").unwrap();
        let err = NodeLayout::open_or_init(&root, "n").unwrap_err();
        assert!(matches!(err, NodeLayoutError::NotANodeDir { .. }), "{err}");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unknown_top_level_entry_is_refused() {
        let root = temp_root("unknown");
        NodeLayout::open_or_init(&root, "n").unwrap();
        fs::create_dir(root.join("events")).unwrap();
        let err = NodeLayout::open_or_init(&root, "n").unwrap_err();
        assert!(matches!(err, NodeLayoutError::UnknownEntries { .. }), "{err}");
        assert!(err.to_string().contains("events"));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn foreign_node_id_is_refused() {
        let root = temp_root("foreign-node");
        NodeLayout::open_or_init(&root, "node-a").unwrap();
        let err = NodeLayout::open_or_init(&root, "node-b").unwrap_err();
        match &err {
            NodeLayoutError::ForeignNode {
                found, expected, ..
            } => {
                assert_eq!(found, "node-a");
                assert_eq!(expected, "node-b");
            }
            other => panic!("unexpected error: {other}"),
        }
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unknown_version_and_bad_magic_are_refused() {
        let root = temp_root("version");
        NodeLayout::open_or_init(&root, "n").unwrap();
        fs::write(
            root.join(NODE_MARKER_FILE),
            "magic = \"fibril-node\"\nlayout_version = 3\nnode_id = \"n\"\n",
        )
        .unwrap();
        let err = NodeLayout::open_or_init(&root, "n").unwrap_err();
        assert!(
            matches!(err, NodeLayoutError::UnknownVersion { found: 3, .. }),
            "{err}"
        );
        fs::write(root.join(NODE_MARKER_FILE), "something else").unwrap();
        let err = NodeLayout::open_or_init(&root, "n").unwrap_err();
        assert!(matches!(err, NodeLayoutError::BadMarker { .. }), "{err}");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn node_ids_with_quotes_round_trip() {
        let root = temp_root("quoted");
        let id = "we\"ird\\id";
        NodeLayout::open_or_init(&root, id).unwrap();
        NodeLayout::open_or_init(&root, id).unwrap();
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn interrupted_init_is_completed_but_removed_data_is_refused() {
        let root = temp_root("partial-init");
        NodeLayout::open_or_init(&root, "n").unwrap();
        fs::remove_dir(root.join(STORAGE_DIR)).unwrap();
        fs::remove_dir(root.join(CACHE_DIR)).unwrap();
        // Only the marker and an empty node dir: finish the init.
        NodeLayout::open_or_init(&root, "n").unwrap();
        assert!(root.join(STORAGE_DIR).is_dir());

        // Storage gone while node files exist: refuse.
        fs::write(root.join(NODE_DIR).join("cluster.secret"), "s").unwrap();
        fs::remove_dir(root.join(STORAGE_DIR)).unwrap();
        let err = NodeLayout::open_or_init(&root, "n").unwrap_err();
        assert!(
            matches!(err, NodeLayoutError::MissingEntry { entry: STORAGE_DIR, .. }),
            "{err}"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn deleted_cache_is_recreated() {
        let root = temp_root("cache");
        let layout = NodeLayout::open_or_init(&root, "n").unwrap();
        fs::create_dir_all(layout.recovery_inspection_cache()).unwrap();
        fs::write(layout.storage().join("data"), "x").unwrap();
        fs::remove_dir_all(layout.cache()).unwrap();
        NodeLayout::open_or_init(&root, "n").unwrap();
        assert!(layout.cache().is_dir());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn coordination_marker_is_written_and_checked() {
        let base = temp_root("coord");
        let dir = base.join("raft");
        prepare_coordination_dir(&dir, 3).unwrap();
        assert!(dir.join(COORDINATION_MARKER_FILE).is_file());
        fs::write(dir.join("raft-wal.jsonl"), "").unwrap();
        prepare_coordination_dir(&dir, 3).unwrap();

        let err = prepare_coordination_dir(&dir, 4).unwrap_err();
        assert!(
            matches!(
                err,
                NodeLayoutError::CoordinationForeignNode {
                    found: 3,
                    expected: 4,
                    ..
                }
            ),
            "{err}"
        );
        let _ = fs::remove_dir_all(base);
    }

    #[test]
    fn unmarked_coordination_data_is_refused() {
        let dir = temp_root("coord-unmarked");
        fs::write(dir.join("raft-wal.jsonl"), "").unwrap();
        let err = prepare_coordination_dir(&dir, 1).unwrap_err();
        assert!(
            matches!(err, NodeLayoutError::CoordinationUnmarked { .. }),
            "{err}"
        );
        assert!(!dir.join(COORDINATION_MARKER_FILE).exists());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn atomic_write_replaces_and_keeps_secret_mode() {
        let dir = temp_root("atomic");
        let path = dir.join("cluster.secret");
        // A stale temp from an interrupted write does not leak into the result.
        fs::write(dir.join(temp_name("cluster.secret")), "stale").unwrap();
        write_file_atomic(&path, b"one", FileMode::Secret).unwrap();
        write_file_atomic(&path, b"two", FileMode::Secret).unwrap();
        assert_eq!(fs::read_to_string(&path).unwrap(), "two");
        assert_eq!(names(&dir), ["cluster.secret"]);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600);
        }
        let _ = fs::remove_dir_all(dir);
    }
}
