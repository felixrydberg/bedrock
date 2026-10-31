use super::*;
use figment::Jail;

const KV_TOML: &str = r#"
        node_id = "kv-1"
        listen = "0.0.0.0:7100"
        admin_listen = "0.0.0.0:7101"
        data_dir = "data"
        roles = ["l1"]
        datasets = ["quicksilver", "kv"]
        relays = ["relay-1:7300"]
        api = "api:7000"
        s3_endpoint = "http://s3:8333"

        [location]
        region = "eu"
        dc = "eu-1"
        cell = "eu-1a"
    "#;

#[test]
#[allow(clippy::disallowed_methods)] // checks that `load` created the data directory
fn file_then_env_overrides() {
    Jail::expect_with(|jail| {
        jail.create_file("kv.toml", KV_TOML)?;
        jail.set_env("BEDROCK_CONFIG", "kv.toml");
        jail.set_env("BEDROCK_ROLES", "[l1, l2]");
        jail.set_env("BEDROCK_LOCATION__CELL", "eu-1b");
        jail.set_env("BEDROCK_BUDGET__MEMORY_MB", "512");

        let c: ServiceConfig = load().map_err(|e| e.0)?;
        assert_eq!(c.roles, [Role::L1, Role::L2]);
        assert_eq!(c.node.location.cell, "eu-1b");
        assert_eq!(c.node.budget.memory_mb, Some(512));
        assert!(c.node.data_dir.is_dir());
        c.validate().map_err(|e| e.0)?;
        Ok(())
    });
}

#[test]
fn rejects_empty_roles() {
    Jail::expect_with(|jail| {
        jail.create_file("bedrock.toml", KV_TOML)?;
        let mut c: ServiceConfig = load().map_err(|e| e.0)?;
        c.roles.clear();
        assert!(c.validate().is_err());
        Ok(())
    });
}

#[test]
fn relay_tier_and_upstreams() {
    Jail::expect_with(|jail| {
        jail.set_env("BEDROCK_NODE_ID", "relay-1");
        jail.set_env("BEDROCK_LISTEN", "0.0.0.0:7300");
        jail.set_env("BEDROCK_ADMIN_LISTEN", "0.0.0.0:7301");
        jail.set_env("BEDROCK_DATA_DIR", "data");
        jail.set_env("BEDROCK_LOCATION__REGION", "eu");
        jail.set_env("BEDROCK_LOCATION__DC", "eu-1");
        jail.set_env("BEDROCK_LOCATION__CELL", "eu-1a");
        jail.set_env("BEDROCK_TIER", "cell");
        jail.set_env("BEDROCK_UPSTREAMS", "[]");

        let c: RelayConfig = load().map_err(|e| e.0)?;
        assert_eq!(c.tier, Tier::Cell);
        assert!(c.validate().is_err());
        Ok(())
    });
}

#[test]
fn api_requires_write_token_and_redacts_it() {
    let config = Figment::new()
        .merge(Toml::string(KV_TOML))
        .merge(("write_listen", "127.0.0.1:7002"))
        .merge(("s3.endpoint", "http://s3:8333"))
        .merge(("s3.bucket", "bedrock"))
        .merge(("s3.access_key_id", "local"))
        .merge(("s3.secret_access_key", "private-state-secret"));
    let error = config.extract::<ApiConfig>().unwrap_err();
    assert!(error.to_string().contains("write_token"));
    let config: ApiConfig = config
        .merge(("write_token", "private-writer-token"))
        .extract()
        .unwrap();
    assert_eq!(config.write_token, "private-writer-token");
    assert!(!format!("{config:?}").contains("private-writer-token"));
    assert!(!format!("{config:?}").contains("private-state-secret"));
    assert!(config.retention.validate().is_ok());
    assert_eq!(config.retention.event_retention_ms, 86_400_000);
    let mut retention = config.retention;
    retention.event_retention_ms = 0;
    assert!(retention.validate().is_err());
}

#[test]
fn api_write_token_env_is_literal() {
    Jail::expect_with(|jail| {
        jail.create_file(
            "bedrock.toml",
            &format!("write_token = 9274512836401927\n{KV_TOML}"),
        )?;
        jail.set_env("BEDROCK_WRITE_LISTEN", "127.0.0.1:7002");
        jail.set_env("BEDROCK_S3__ENDPOINT", "http://s3:8333");
        jail.set_env("BEDROCK_S3__BUCKET", "bedrock");
        jail.set_env("BEDROCK_S3__ACCESS_KEY_ID", "local");
        jail.set_env("BEDROCK_S3__SECRET_ACCESS_KEY", "local");
        let error = load::<ApiConfig>().unwrap_err();
        assert!(error.to_string().contains("write_token"));
        assert!(!error.to_string().contains("9274512836401927"));
        for token in ["9274512836401927", "false", "\"quoted-token\""] {
            jail.set_env("BEDROCK_WRITE_TOKEN", token);
            let config: ApiConfig = load().map_err(|e| e.0)?;
            assert_eq!(config.write_token, token);
        }
        #[cfg(unix)]
        {
            jail.set_env("bedrock_write_token", "another-token");
            assert!(
                load::<ApiConfig>()
                    .unwrap_err()
                    .to_string()
                    .contains("multiple")
            );
        }
        Ok(())
    });
}

#[test]
fn relay_membership_needs_a_cell_relay_and_its_write_token() {
    let relay = |extra: &str| {
        Figment::new()
            .merge(Toml::string(&format!(
                r#"
                node_id = "relay-1"
                listen = "0.0.0.0:7300"
                admin_listen = "0.0.0.0:7301"
                data_dir = "data"
                tier = "cell"
                upstreams = ["api:7000"]
                replay_peers = ["relay-2:7300", "relay-3:7300"]
                {extra}
                [location]
                region = "eu"
                dc = "eu-1"
                cell = "eu-1a"
                [membership]
                api = "api:7002"
                "#
            )))
            .extract::<RelayConfig>()
            .unwrap()
    };
    let config = relay(
        r#"advertise = "relay-1:7300"
                write_token = "private-writer-token""#,
    );
    config.validate().unwrap();
    assert_eq!(config.membership.as_ref().unwrap().grace_ms, 20_000);
    assert!(!format!("{config:?}").contains("private-writer-token"));
    // A relay is never its own sibling.
    assert!(
        relay(
            r#"advertise = "relay-2:7300"
                write_token = "t""#
        )
        .validate()
        .is_err()
    );
    assert!(relay(r#"advertise = "relay-1:7300""#).validate().is_err());
    let mut dc = relay(
        r#"advertise = "relay-1:7300"
                write_token = "t""#,
    );
    dc.tier = Tier::Dc;
    assert!(dc.validate().is_err());
}
