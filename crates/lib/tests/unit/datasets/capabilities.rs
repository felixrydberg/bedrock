use super::*;

#[test]
fn every_ranged_service_has_membership_and_pushed_data_or_none() {
    for capability in CAPABILITIES {
        if let Some(service) = capability.ranged_service {
            assert!(SERVICES.contains(&service), "missing service {service}");
            // Volume ranges carry no relay data, so no namespace either.
            assert!(
                capability.pushed || capability.namespaces.is_empty(),
                "{} has namespaces but is not pushed",
                capability.dataset
            );
        }
    }
}
