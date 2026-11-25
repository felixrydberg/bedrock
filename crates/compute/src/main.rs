//! Thin entry point: `bedrock_compute::run` holds the host so `bedrock-sim` runs the same code (B-23a).

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), bedrock_lib::node::ConfigError> {
    bedrock_lib::node::init_tracing();
    // `run` adds the KV actor with its directory client; other built-in kinds use this executor.
    let builtin = Arc::new(bedrock_compute::runtime::RustExecutor::new([]));
    bedrock_compute::run(bedrock_lib::node::load()?, builtin).await
}
