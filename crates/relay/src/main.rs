//! Thin entry point: `bedrock_relay::run` holds the node so `bedrock-sim` runs the same code (B-23a).

#[tokio::main]
async fn main() -> Result<(), bedrock_lib::node::ConfigError> {
    bedrock_lib::node::init_tracing();
    bedrock_relay::run(bedrock_lib::node::load()?).await
}
