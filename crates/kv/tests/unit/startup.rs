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
fn hosts_no_datasets_or_subscriptions() {
    assert!(check_hosts_nothing_pushed(&config("")).is_ok());
    for dataset in ["quicksilver", "auth", "cdn"] {
        let with_dataset = config(&format!("datasets = [\"{dataset}\"]"));
        assert!(check_hosts_nothing_pushed(&with_dataset).is_err());
    }
    let subscribed = config("subscriptions = [{ dataset = \"quicksilver\", namespace = \"n\" }]");
    assert!(check_hosts_nothing_pushed(&subscribed).is_err());
}
