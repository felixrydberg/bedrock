//! Where simulation builds swap components (#25 / B-23). Compiled only under `cfg(madsim)`, and it holds no fault logic:
//! that lives in `tests/sim`. Madsim runs a simulation on one thread and each seed gets a fresh one, so seeds cannot
//! share what a harness installs. Without hooks, nodes get fresh in-memory storage and calls pass through.

use std::cell::RefCell;
use std::path::Path;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use tonic::Status;

use crate::storage::StorageEngine;
use crate::storage::memory::MemoryEngine;
use crate::storage::namespace::NamespaceFactory;
use crate::storage::sim::MemoryFactory;

/// What happens to one call: `Err` fails it unsent. `Ok(Some(status))` lets it run and reports `status` instead of its
/// result, so a call and its lost response are decided together.
pub type RpcFuture = Pin<Box<dyn Future<Output = Result<Option<Status>, Status>> + Send>>;

/// Opens the local engine `name` under a data directory.
pub type EngineOpener = Box<dyn Fn(&Path, &str) -> Arc<dyn StorageEngine>>;

/// What a harness swaps in.
pub struct Hooks {
    pub engine: EngineOpener,
    pub namespaces: Box<dyn Fn() -> Arc<dyn NamespaceFactory>>,
    pub rpc: Box<dyn Fn(&str) -> RpcFuture>,
}

thread_local! {
    static HOOKS: RefCell<Option<Rc<Hooks>>> = const { RefCell::new(None) };
}

/// Installs `hooks` for this thread's simulation; call before creating nodes.
pub fn install(hooks: Hooks) {
    HOOKS.with_borrow_mut(|installed| *installed = Some(Rc::new(hooks)));
}

fn hooks() -> Option<Rc<Hooks>> {
    HOOKS.with_borrow(Clone::clone)
}

/// The engine `name` under `data_dir`.
pub fn engine(data_dir: &Path, name: &str) -> Arc<dyn StorageEngine> {
    match hooks() {
        Some(hooks) => (hooks.engine)(data_dir, name),
        None => Arc::new(MemoryEngine::new(name)),
    }
}

/// The API's namespace store, standing in for S3.
pub fn namespaces() -> Arc<dyn NamespaceFactory> {
    match hooks() {
        Some(hooks) => (hooks.namespaces)(),
        None => Arc::new(MemoryFactory::default()),
    }
}

/// Decides one call to `addr`.
pub fn rpc(addr: &str) -> impl Future<Output = Result<Option<Status>, Status>> + Send + use<> {
    let decided = hooks().map(|hooks| (hooks.rpc)(addr));
    async move {
        match decided {
            Some(decided) => decided.await,
            None => Ok(None),
        }
    }
}

/// Runs `call` to `addr` as [`rpc`] decides.
pub async fn around<T>(
    addr: &str,
    call: impl Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    let lost = rpc(addr).await?;
    let result = call.await;
    lost.map_or(result, Err)
}
