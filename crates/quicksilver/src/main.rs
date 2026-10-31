//! Thin entry point: `bedrock_quicksilver::run` holds the node so `bedrock-sim` runs the same code (B-23a).

#[tokio::main]
async fn main() -> Result<(), bedrock_lib::node::ConfigError> {
    bedrock_lib::node::init_tracing();
    bedrock_quicksilver::run(bedrock_lib::node::load()?).await
}
