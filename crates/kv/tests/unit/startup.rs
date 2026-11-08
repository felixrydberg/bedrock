use super::*;
use figment::providers::{Format, Toml};

fn config(extra: &str) -> ServiceConfig {
    figment::Figment::new()
        .merge(Toml::string(&format!(
            r#"
            node_id = "kv-1"
            listen = "0.0.0.0:7100"
            admin_listen = "0.0.0.0:7101"
            data_dir = "data"
            roles = ["l1"]
            relays = ["relay-1:7300"]
            api = "api:7000"
            s3_endpoint = "http://s3:8333"
            {extra}

            [location]
            region = "eu"
            dc = "eu-1"
            cell = "eu-1a"
            "#
        )))
        .extract()
        .unwrap()
}

#[test]
fn hosts_no_datasets() {
    assert!(check_hosts_nothing_pushed(&config("")).is_ok());
    for dataset in ["quicksilver", "auth", "cdn"] {
        let with_dataset = config(&format!("datasets = [\"{dataset}\"]"));
        assert!(check_hosts_nothing_pushed(&with_dataset).is_err());
    }
}

#[test]
fn a_kv_drain_has_nothing_to_move_for_any_scope_and_namespace() {
    use bedrock_lib::reconciliation::drain::Outcome;
    for scope in [Scope::Node("kv-1".into()), Scope::Cell("eu-1a".into())] {
        for namespace in [None, Some("customer")] {
            assert_eq!(Readers.drain(&scope, namespace), Ok(Outcome::NothingToMove));
        }
    }
    assert_eq!(drain_hooks().len(), 1);
}
