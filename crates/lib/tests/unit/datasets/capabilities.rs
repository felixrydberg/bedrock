use super::*;

#[test]
fn every_ranged_service_has_membership_and_pushed_data() {
    for capability in CAPABILITIES {
        if let Some(service) = capability.ranged_service {
            assert!(SERVICES.contains(&service), "missing service {service}");
            assert!(capability.pushed, "{} is not pushed", capability.dataset);
        }
    }
}
