use super::*;
use bedrock_lib::storage::memory::MemoryEngine;
use std::sync::Arc;

#[test]
fn opens_only_cdn() {
    let r = Registry::open(DATASETS, &["cdn".into()], |d| {
        Ok(Arc::new(MemoryEngine::new(d.name)))
    })
    .unwrap();
    assert!(r.get("cdn").unwrap().engine.is_ok());
    for kv in ["quicksilver", "kv", "auth"] {
        assert!(Registry::open(DATASETS, &[kv.into()], |_| unreachable!()).is_err());
    }
}

#[test]
fn cdn_drains_nodes_through_its_ranges_and_refuses_one_namespace() {
    use bedrock_lib::reconciliation::drain::{Error, Outcome, Scope};
    let hooks = bedrock_lib::reconciliation::drain::ranged_with("cdn", |_| Ok(Default::default()));
    assert_eq!(hooks.len(), 1);
    let node = Scope::Node("cdn-1".into());
    assert_eq!(hooks[0].drain(&node, None), Ok(Outcome::NothingToMove));
    assert!(matches!(
        hooks[0].drain(&node, Some("deployments")),
        Err(Error::Unsupported { .. })
    ));
}

/// The artifact store's settings sit beside the service's (#82 / B-34); outside simulation `[s3]` is required.
#[test]
// figment's test error is large; it is only a test failure.
#[allow(clippy::result_large_err)]
fn cdn_config_takes_s3_and_the_cache_size() {
    figment::Jail::expect_with(|jail| {
        jail.create_file(
            "bedrock.toml",
            r#"
            node_id = "cdn-1"
            listen = "0.0.0.0:7200"
            admin_listen = "0.0.0.0:7201"
            data_dir = "data"
            roles = ["l2"]
            datasets = ["cdn"]
            relays = ["relay-1:7300"]
            api = "api:7002"
            s3_endpoint = "http://s3"
            cache_mb = 64
            [location]
            region = "eu"
            dc = "arn1"
            cell = "eu-1a"
            [s3]
            endpoint = "http://s3"
            bucket = "bedrock"
            "#,
        )?;
        jail.set_env("BEDROCK_S3__ACCESS_KEY_ID", "local");
        jail.set_env("BEDROCK_S3__SECRET_ACCESS_KEY", "private-s3-key");
        let config: CdnConfig = node::load().map_err(|e| e.0)?;
        assert_eq!(config.cache_mb, 64);
        assert_eq!(config.s3.as_ref().unwrap().bucket, "bedrock");
        assert!(!format!("{config:?}").contains("private-s3-key"));
        assert_eq!(CdnConfig::from(config.service).cache_mb, 10 * 1024);
        Ok(())
    });
}
