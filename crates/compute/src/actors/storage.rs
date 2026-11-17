//! An awake actor's storage over its [`Writer`] (#115 / S-07, `statefull/v2.md` §5, §8): key/value reads and commits,
//! and SQLite over a VFS whose pages are volume keys.
//!
//! - **Pages.** Page `n` (0-based, [`PAGE`] bytes) is the key `n` as a big-endian `u32`; the file size is [`SIZE_KEY`].
//! - **Misses.** SQLite's VFS calls are synchronous and the volume client is async (user decision 2026-11-17, "page
//!   cache + retry"). A read of a page not in the cache fails the transaction; [`Storage::sql`] rolls it back, fetches
//!   the aligned [`super::volume::CHUNK_PAGES`] chunk around the page and runs the transaction again. The closure must therefore do
//!   nothing but SQL.
//! - **Commits.** Each committed transaction becomes one entry of its written pages (and size), and [`Storage::sql`]
//!   returns only once that entry has its quorum. Every commit returns only after its quorum, so a response that waits
//!   for its storage calls never precedes durability (the output gate). One entry holds at most
//!   [`super::volume::MAX_ENTRY_BYTES`], about 750 pages per transaction.
// ponytail: each storage call awaits its own quorum, one at a time; pipeline commits behind the output gate if
// per-event latency matters (S-10, S-18 measurements).
//! - **Pending pages.** A transaction's pages enter the cache before their entry commits. A commit that fails drops the
//!   database (cache and connection). A commit whose caller is cancelled may still commit later, so it fences the
//!   storage instead: every later call fails and the actor host claims the actor again, recovering from the slots.
// ponytail: the page cache is unbounded while the actor is awake and dropped when it sleeps; evict when a large
// database's working set must fit (S-11 measurements).
//!
//! An actor uses either the key/value calls or SQLite: a page key is 4 bytes, so a customer key could alias it.

use std::collections::BTreeMap;
use std::ffi::CString;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Once, PoisonError};

use bedrock_lib::volume as pb;
use rusqlite::{Connection, OpenFlags, Transaction};
use sqlite_plugin::flags::{AccessFlags, LockLevel, OpenKind, OpenOpts};
use sqlite_plugin::vars;
use sqlite_plugin::vfs::{RegisterOpts, Vfs, VfsHandle, VfsResult};
use tonic::Status;

use super::volume::{Error, Writer};

/// SQLite page size; set on every new database.
pub const PAGE: usize = 4096;
/// The key holding the database file's size in bytes, big-endian `u64`.
pub const SIZE_KEY: &[u8] = b"sqlite/size";
const VFS: &str = "bedrock";

/// An awake actor's storage. Clones share the writer and the database.
#[derive(Clone)]
pub struct Storage(Arc<Inner>);

struct Inner {
    writer: Writer,
    db: Mutex<Option<Db>>,
    /// Set once a commit's outcome is unknown to this storage (its caller was cancelled).
    fenced: AtomicBool,
}

/// Fences the storage when dropped armed: a commit future cancelled before its quorum answered.
struct Barrier<'a>(&'a AtomicBool);

impl Drop for Barrier<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

impl Storage {
    pub(crate) fn new(writer: Writer) -> Self {
        Storage(Arc::new(Inner {
            writer,
            db: Mutex::new(None),
            fenced: AtomicBool::new(false),
        }))
    }

    /// Whether this storage may still be read and written: not fenced, and its writer serving.
    pub(crate) fn serving(&self) -> bool {
        !self.0.fenced.load(Ordering::Relaxed) && self.0.writer.serving()
    }

    fn check(&self) -> Result<(), Error> {
        if self.0.fenced.load(Ordering::Relaxed) {
            return Err(Error::Fenced);
        }
        Ok(())
    }

    /// The writer, once no other clone is left.
    pub(crate) fn into_writer(self) -> Option<Writer> {
        Arc::into_inner(self.0).map(|inner| inner.writer)
    }

    /// `keys` at the commit point or later; a missing key has no value.
    pub async fn get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<pb::Value>, Error> {
        self.check()?;
        self.0.writer.read(keys).await
    }

    /// Commits `changes` as one entry; returns its sequence once two voting slots hold it.
    /// If the caller is cancelled before the answer, the storage fences: the entry may still commit.
    pub async fn commit(&self, changes: Vec<pb::Change>) -> Result<u64, Error> {
        self.check()?;
        let barrier = Barrier(&self.0.fenced);
        let committed = self.0.writer.commit(changes, None, now_ms()).await;
        std::mem::forget(barrier);
        committed
    }

    /// Runs `f` in a SQLite transaction and commits what it wrote as one entry; returns once that entry has its
    /// quorum. `f` may run more than once (see the module docs). An error of `f`'s own rolls the transaction back.
    pub async fn sql<R>(
        &self,
        f: impl Fn(&Transaction<'_>) -> rusqlite::Result<R>,
    ) -> Result<rusqlite::Result<R>, Error> {
        self.check()?;
        if self.lock().is_none() {
            let size = self.get(vec![SIZE_KEY.to_vec()]).await?;
            let size = match size.first().and_then(|v| v.value.as_deref()) {
                None => 0,
                Some(bytes) => u64::from_be_bytes(bytes.try_into().map_err(|_| {
                    Error::Rpc(Status::data_loss("the SQLite size key is not 8 bytes"))
                })?),
            };
            let mut db = self.lock();
            if db.is_none() {
                *db = Some(Db::new(size));
            }
        }
        loop {
            let (outcome, pages) = {
                let mut guard = self.lock();
                let db = guard.as_mut().expect("opened above");
                let result = db.connection().and_then(|c| run(c, &f));
                let mut pages = lock(&db.pages);
                let outcome = match (pages.missing.take(), result) {
                    // Whatever the transaction did, SQLite's state may have used the page it could not read: start
                    // again from a new connection over the synced pages.
                    (Some(page), _) => {
                        pages.discard();
                        drop(pages);
                        db.connection = None;
                        Err(page)
                    }
                    (None, Err(error)) => {
                        pages.discard();
                        Ok((Err(error), Vec::new()))
                    }
                    (None, Ok(result)) => Ok((Ok(result), pages.sync())),
                };
                (outcome, db.pages.clone())
            };
            match outcome {
                Err(page) => {
                    let values = self.0.writer.read_chunk(page).await?;
                    let mut pages = lock(&pages);
                    for value in values {
                        if let (Ok(n), Some(bytes)) =
                            (<[u8; 4]>::try_from(value.key.as_slice()), value.value)
                        {
                            pages.cached.entry(u32::from_be_bytes(n)).or_insert(bytes);
                        }
                    }
                    if !pages.cached.contains_key(&page) {
                        return Err(Error::Rpc(Status::data_loss(format!(
                            "SQLite page {page} lies within the file but has no value"
                        ))));
                    }
                }
                Ok((result, changes)) => {
                    if !changes.is_empty()
                        && let Err(error) = self.commit(changes).await
                    {
                        // The pages are cached but not committed (too large, or the writer stopped): drop the
                        // database, so the next transaction reads the committed state from the slots.
                        *self.lock() = None;
                        return Err(error);
                    }
                    return Ok(result);
                }
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<Db>> {
        self.0.db.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Runs `f` in a transaction and commits it.
fn run<R>(
    connection: &mut Connection,
    f: &impl Fn(&Transaction<'_>) -> rusqlite::Result<R>,
) -> rusqlite::Result<R> {
    let tx = connection.transaction()?;
    let result = f(&tx)?;
    tx.commit()?;
    Ok(result)
}

/// Wall-clock milliseconds for an entry's `committed_at`. madsim simulates the system clock.
#[allow(clippy::disallowed_methods)]
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// One actor's SQLite file and its connection, opened on first use and again after a miss.
struct Db {
    connection: Option<Connection>,
    pages: Arc<Mutex<Pages>>,
    name: String,
}

impl Db {
    fn new(size: u64) -> Self {
        register();
        // Unique in the process, also across simulated nodes and seeds; the name never reaches output.
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!("actor-{}", NEXT.fetch_add(1, Ordering::Relaxed));
        let pages = Arc::new(Mutex::new(Pages {
            size,
            synced_size: size,
            ..Pages::default()
        }));
        lock(&FILES).insert(name.clone(), pages.clone());
        Db {
            connection: None,
            pages,
            name,
        }
    }

    fn connection(&mut self) -> rusqlite::Result<&mut Connection> {
        if self.connection.is_none() {
            let connection = Connection::open_with_flags_and_vfs(
                &self.name,
                OpenFlags::SQLITE_OPEN_READ_WRITE
                    | OpenFlags::SQLITE_OPEN_CREATE
                    | OpenFlags::SQLITE_OPEN_NO_MUTEX,
                VFS,
            )?;
            // Only the main database touches the VFS: the journal and temporary tables stay in memory, and one
            // connection holds the file.
            connection.execute_batch(&format!(
                "PRAGMA page_size = {PAGE}; PRAGMA journal_mode = MEMORY; PRAGMA temp_store = MEMORY;                  PRAGMA locking_mode = EXCLUSIVE;"
            ))?;
            self.connection = Some(connection);
        }
        Ok(self.connection.as_mut().expect("opened above"))
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // The connection closes before its file leaves the VFS.
        self.connection = None;
        lock(&FILES).remove(&self.name);
    }
}

/// The open databases' pages by file name, for the VFS.
static FILES: Mutex<BTreeMap<String, Arc<Mutex<Pages>>>> = Mutex::new(BTreeMap::new());

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

fn register() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let name = CString::new(VFS).expect("no NUL");
        if let Err(code) = sqlite_plugin::vfs::register_static(
            name,
            PagesVfs,
            RegisterOpts {
                make_default: false,
            },
        ) {
            panic!("registering the SQLite VFS failed: {code}");
        }
    });
}

/// A database file: pages read from the volume or written by this holder, and writes since the last sync.
#[derive(Default)]
struct Pages {
    cached: BTreeMap<u32, Vec<u8>>,
    /// Written since the last sync; `None` is a page a truncation removed.
    dirty: BTreeMap<u32, Option<Vec<u8>>>,
    size: u64,
    synced_size: u64,
    /// A page read that missed the cache, for [`Storage::sql`] to fetch.
    missing: Option<u32>,
}

impl Pages {
    /// Drops the writes since the last sync.
    fn discard(&mut self) {
        self.dirty.clear();
        self.size = self.synced_size;
    }

    /// Moves the writes since the last sync into the cache and returns them as changes, with the size if it changed.
    fn sync(&mut self) -> Vec<pb::Change> {
        let mut changes = Vec::with_capacity(self.dirty.len() + 1);
        for (page, bytes) in std::mem::take(&mut self.dirty) {
            match &bytes {
                Some(bytes) => self.cached.insert(page, bytes.clone()),
                None => self.cached.remove(&page),
            };
            changes.push(pb::Change {
                key: page.to_be_bytes().to_vec(),
                value: bytes,
            });
        }
        if !changes.is_empty() || self.size != self.synced_size {
            changes.push(pb::Change {
                key: SIZE_KEY.to_vec(),
                value: Some(self.size.to_be_bytes().to_vec()),
            });
            self.synced_size = self.size;
        }
        changes
    }
}

/// The VFS: every call works on the in-memory [`Pages`] of the file it opened.
struct PagesVfs;

struct File(Arc<Mutex<Pages>>);

impl VfsHandle for File {
    fn readonly(&self) -> bool {
        false
    }

    fn in_memory(&self) -> bool {
        false
    }
}

fn page_of(offset: usize, len: usize) -> Option<(u32, usize)> {
    let page = u32::try_from(offset / PAGE).ok()?;
    let within = offset % PAGE;
    (within + len <= PAGE).then_some((page, within))
}

impl Vfs for PagesVfs {
    type Handle = File;

    fn open(&self, path: Option<&str>, opts: OpenOpts) -> VfsResult<File> {
        if opts.kind() != OpenKind::MainDb {
            return Err(vars::SQLITE_CANTOPEN);
        }
        let pages = path
            .and_then(|path| lock(&FILES).get(path).cloned())
            .ok_or(vars::SQLITE_CANTOPEN)?;
        Ok(File(pages))
    }

    fn delete(&self, _path: &str) -> VfsResult<()> {
        Ok(())
    }

    fn access(&self, path: &str, _flags: AccessFlags) -> VfsResult<bool> {
        Ok(lock(&FILES).contains_key(path))
    }

    fn file_size(&self, file: &mut File) -> VfsResult<usize> {
        usize::try_from(lock(&file.0).size).map_err(|_| vars::SQLITE_IOERR_FSTAT)
    }

    fn truncate(&self, file: &mut File, size: usize) -> VfsResult<()> {
        let mut pages = lock(&file.0);
        let keep = size.div_ceil(PAGE) as u64;
        let end = pages.size.div_ceil(PAGE as u64);
        for page in keep..end {
            pages.dirty.insert(page as u32, None);
        }
        pages.size = size as u64;
        Ok(())
    }

    fn write(&self, file: &mut File, offset: usize, data: &[u8]) -> VfsResult<usize> {
        if !offset.is_multiple_of(PAGE) || data.len() != PAGE {
            return Err(vars::SQLITE_IOERR_WRITE);
        }
        let (page, _) = page_of(offset, data.len()).ok_or(vars::SQLITE_IOERR_WRITE)?;
        let mut pages = lock(&file.0);
        pages.dirty.insert(page, Some(data.to_vec()));
        pages.size = pages.size.max((offset + PAGE) as u64);
        Ok(data.len())
    }

    fn read(&self, file: &mut File, offset: usize, data: &mut [u8]) -> VfsResult<usize> {
        let mut pages = lock(&file.0);
        if offset as u64 >= pages.size {
            return Ok(0);
        }
        let (page, within) = page_of(offset, data.len()).ok_or(vars::SQLITE_IOERR_READ)?;
        let bytes = match pages.dirty.get(&page) {
            Some(Some(bytes)) => bytes,
            Some(None) => return Ok(0),
            None => match pages.cached.get(&page) {
                Some(bytes) => bytes,
                None => {
                    pages.missing = Some(page);
                    return Err(vars::SQLITE_IOERR_READ);
                }
            },
        };
        let n = data.len().min(bytes.len().saturating_sub(within));
        data[..n].copy_from_slice(&bytes[within..within + n]);
        Ok(n)
    }

    fn lock(&self, _file: &mut File, _level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    fn unlock(&self, _file: &mut File, _level: LockLevel) -> VfsResult<()> {
        Ok(())
    }

    fn check_reserved_lock(&self, _file: &mut File) -> VfsResult<bool> {
        Ok(false)
    }

    fn close(&self, _file: File) -> VfsResult<()> {
        Ok(())
    }
}

#[cfg(test)]
#[path = "../../tests/unit/storage.rs"]
mod tests;
