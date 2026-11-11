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
            dc = "arn1"
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
fn kv_drains_fail_closed_until_reader_lifecycle_exists() {
    use bedrock_lib::reconciliation::drain::{self, Drain, Scope, State};
    let hooks = drain::ranged_with("kv", |_| unreachable!());
    for scope in [Scope::Node("kv-1".into()), Scope::Cell("eu-1a".into())] {
        for namespace in [None, Some("customer")] {
            let record = Drain {
                scope: scope.clone(),
                datasets: Vec::new(),
                namespace: namespace.map(str::to_owned),
                state: State::Draining,
                rate: 1,
            };
            assert!(matches!(
                drain::validate(&hooks, &record),
                Err(drain::Error::Unsupported { .. })
            ));
        }
    }
}
