//! Thin entry point: `bedrock_compute::run` holds the host so `bedrock-sim` runs the same code (B-23a).

use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), bedrock_lib::node::ConfigError> {
    bedrock_lib::node::init_tracing();
    // No built-in kind has a handler yet; S-11 … S-18 add theirs here.
    let builtin = Arc::new(bedrock_compute::runtime::RustExecutor::new([]));
    bedrock_compute::run(bedrock_lib::node::load()?, builtin).await
}
