# Node security: mutual TLS, certificates and private networking

Bedrock nodes can talk to each other over mutual TLS. Each process authenticates with its own certificate, and every
server checks who is calling before it answers. Which CA issues the certificates and how the network is kept private
are deployment choices; this guide covers Kubernetes, Docker hosts and bare metal across providers (#44, B-54/B-55).

> **Tested so far:** the binaries' TLS behaviour, by the `bedrock-lib` tests with a generated CA. The OpenBao,
> cert-manager and Nebula setups below have not been run end to end yet; treat them as starting points.

## What the binaries do

- **Off by default.** Without a `[tls]` section, a node speaks plaintext, logs a warning at startup and reports
  `bedrock_transport_tls_enabled 0`.
- **Required once configured.** With `[tls]`, every node-to-node listener and connection uses TLS 1.3 with client
  certificates. Until the three files load, `/ready` stays false; after 60 seconds the process exits. It never falls
  back to plaintext.
- **One mode per cell.** A TLS node rejects plaintext peers and the reverse. Switching an existing cell to TLS is a
  cutover: deploy the certificates, then restart every node in the cell with `[tls]`.
- **Renewal without restart.** The files are re-read every 10 seconds. A renewal applies to new connections; a renewal
  that fails to parse, has expired or names another node is rejected, the previous certificate stays in use and
  `bedrock_transport_tls_reload_failures_total` increments. Servers close connections older than 24 hours so peers
  handshake again. `bedrock_transport_tls_expiry_seconds` shows how long the current certificate has left.
- **Not covered:** the admin endpoint (`/metrics`, `/health`, `/ready`, `/introspect`) stays plain HTTP without
  authentication. Bind `admin_listen` to loopback or a private interface. S3 traffic uses the provider's HTTPS.

```toml
[tls]
cert = "/etc/bedrock/tls/tls.crt"   # leaf first, then any intermediates
key = "/etc/bedrock/tls/tls.key"
ca = "/etc/bedrock/tls/ca.crt"      # the CAs that issue node certificates; may hold the next CA during rotation
```

## Certificate contract

Any CA works if each process's certificate has:

- the URI SAN `bedrock://{service}/{cell}/{node_id}`, where `service` is `quicksilver`, `kv`, `cdn`, `relay` or `api`
  and `cell` and `node_id` match the node's config (letters, digits, `-`, `_`, `.`);
- a DNS or IP SAN for the host in `advertise` (or `listen` when `advertise` is unset), because peers verify the
  address they dial;
- extended key usages `serverAuth` and `clientAuth`.

Startup rejects a certificate that names another service, cell or node, or lacks the advertised host. Processes on one
machine need one certificate each, since the service is part of the identity.

Servers then authorize by the certificate, never by request fields:

| Server | Accepts |
|---|---|
| `bedrock-api` relay port (`listen`) | `relay` |
| `bedrock-relay`, leaf sessions | `quicksilver`, `kv`, `cdn` of the relay's own cell; the announced member must be the caller itself, at one of its SANs |
| `bedrock-relay`, sessions from lower relays and sibling watches | `relay` (sibling watches: the same cell only) |
| `bedrock-api` write port (`write_listen`) | TLS with an optional client certificate; every caller still sends the bearer token |

Clients accept only `relay` and `api` servers. Revocation relies on short certificate lifetimes: when a machine can no
longer renew, its certificates stop working at expiry. For an immediate cut-off, use the network layer (a Nebula
blocklist, a NetworkPolicy or a firewall rule).

## Kubernetes

Use the cluster's certificate tooling and network encryption rather than Nebula.

- **Certificates:** one cert-manager `Certificate` per pod and service, mounted from its Secret. cert-manager's
  `Vault` issuer is expected to work against OpenBao's API, but this has not been verified.

  ```yaml
  apiVersion: cert-manager.io/v1
  kind: Certificate
  metadata: { name: kv-eu-1a-m1 }
  spec:
    secretName: kv-eu-1a-m1-tls
    duration: 168h
    renewBefore: 56h
    privateKey: { algorithm: ECDSA, size: 256, rotationPolicy: Always }
    usages: [server auth, client auth]
    uris: ["bedrock://kv/eu-1a/eu-1a-m1"]
    dnsNames: [eu-1a-m1.kv.bedrock.svc]   # the host in advertise
    issuerRef: { name: bedrock-nodes, kind: ClusterIssuer }
  ```

  Mount the Secret and point `[tls]` at `tls.crt`, `tls.key` and `ca.crt`. Kubernetes replaces mounted Secrets by
  swapping a symlink; Bedrock compares file contents, so renewals are picked up.
- **Network:** encrypt pod traffic with the CNI (for example Cilium or Calico WireGuard) if the cluster spans untrusted
  links, and add NetworkPolicies that open each port only to the callers in the table above.
- **Admin endpoint:** keep `admin_listen` off Services that leave the cluster; scrape it from inside.

## Docker and compose hosts

Run OpenBao Agent next to the Bedrock containers and share a volume. Any other tool that writes the three files works
as well, for example a cron job with the `bao` CLI, or long-lived certificates from a script for small setups.

```hcl
# bao-agent.hcl, one agent per machine (bao agent -config=bao-agent.hcl)
auto_auth {
  method "approle" {
    config = {
      role_id_file_path   = "/etc/bao/role-id"
      secret_id_file_path = "/etc/bao/secret-id"   # delivered once, response-wrapped, at provisioning
    }
  }
}

template {
  destination = "/run/bedrock/kv/.issued"
  # One issue call per template, so the certificate and key match. `secret` re-fetches leased certificates at 85% of
  # their TTL (the roles set generate_lease).
  contents = <<-EOT
  {{ with secret "pki_bedrock/issue/kv" "common_name=eu-1a-m1" "uri_sans=bedrock://kv/eu-1a/eu-1a-m1" "alt_names=eu-1a-m1" "ttl=168h" }}
  {{ .Data.certificate | writeToFile "/run/bedrock/kv/tls.crt" "" "" "0644" }}
  {{ .Data.private_key | writeToFile "/run/bedrock/kv/tls.key" "" "" "0600" }}
  {{ .Data.issuing_ca | writeToFile "/run/bedrock/kv/ca.crt" "" "" "0644" }}
  {{ .Data.serial_number }}{{ end }}
  EOT
}
```

The template uses consul-template's `writeToFile`; check that the agent version supports it. Mount
`/run/bedrock/{service}` read-only into that service's container, with one template per service on the machine. If Bedrock reads the files between two writes, the key does not match the certificate; that reload is
rejected and the next one, 10 seconds later, applies.

On hosts that span networks, add Nebula (below) and bind Bedrock to the overlay address.

## Bare metal across providers

Run OpenBao Agent as a systemd unit as above, and Nebula for a private network between machines that share none.

### Nebula

Nebula gives each machine an overlay address; Bedrock needs no support for it beyond `listen` and `advertise` pointing
at that address (and certificate SANs for it).

- Nebula 1.10 or later with v2 certificates, on a dedicated IPv4 /16, running as a host service; the Bedrock
  containers keep `network_mode: host` and see the `nebula1` interface.
- At least three lighthouses at different providers, listed in every host's `static_host_map`. Hosts with public
  addresses need no Nebula relays.
- Host firewall on the public interface: allow Nebula's UDP port (4242) and SSH from the operator network; drop the rest.
- Groups in each host certificate (`service`, `relay`, `api`, `monitoring`, `ops`) and inbound rules:

  ```yaml
  firewall:
    outbound: [{ port: any, proto: any, host: any }]
    inbound:
      - { port: 7300, proto: tcp, groups: [service] }   # relay listen, from Quicksilver/KV/CDN
      - { port: 7300, proto: tcp, groups: [relay] }     # relay listen, from other relays
      - { port: 7000, proto: tcp, groups: [relay] }     # api listen
      - { port: 7002, proto: tcp, groups: [relay] }     # api write_listen
      - { port: 7002, proto: tcp, groups: [ops] }
      - { port: 7001-7401, proto: tcp, groups: [monitoring] }   # admin_listen ports
  ```

  Adjust the ports to the deployment's config.
- Host keys are generated on the machine (`nebula-cert keygen`) and only the public key is signed
  (`nebula-cert sign -in-pub`). OpenBao has no built-in Nebula engine: keep the Nebula CA key in OpenBao's KV store
  and sign on an operator machine, or evaluate the community `openbao-plugin-secrets-nebula`.
- Removing a machine: add its certificate fingerprint to `pki.blocklist` on every host and set
  `pki.disconnect_invalid: true`, then reload Nebula. Its Bedrock certificates expire within their lifetime because the
  machine's OpenBao login is removed too.

## OpenBao reference setup

One intermediate CA for node certificates, one role per service, and one AppRole per machine. Each machine's identity
entity carries its `cell` and `node_id` as metadata, so a machine can only obtain certificates naming itself.

```sh
bao secrets enable -path=pki_bedrock pki
bao secrets tune -max-lease-ttl=8760h pki_bedrock
# ... generate or import the intermediate CA for pki_bedrock ...

for service in quicksilver kv cdn api; do
  bao write pki_bedrock/roles/$service \
    allowed_uri_sans="bedrock://$service/{{identity.entity.metadata.cell}}/{{identity.entity.metadata.node_id}}" \
    allowed_uri_sans_template=true \
    allow_any_name=true allow_ip_sans=true \
    server_flag=true client_flag=true \
    key_type=ec key_bits=256 ttl=168h max_ttl=168h generate_lease=true
done
# Relays usually have their own node_id; give relay machines a relay_node_id entry.
bao write pki_bedrock/roles/relay \
  allowed_uri_sans="bedrock://relay/{{identity.entity.metadata.cell}}/{{identity.entity.metadata.relay_node_id}}" \
  allowed_uri_sans_template=true allow_any_name=true allow_ip_sans=true \
  server_flag=true client_flag=true key_type=ec key_bits=256 ttl=168h max_ttl=168h generate_lease=true
```

`allow_any_name` leaves DNS names unrestricted; restrict them (`allowed_domains`) when the deployment's host names
follow a pattern. Per machine: an AppRole role, a policy allowing `pki_bedrock/issue/{service}` for the services it
runs, and its entity's metadata (`cell`, `node_id`, and `relay_node_id` on relay machines).

Lifetimes: 7-day node certificates, renewed at 85% of their TTL, tolerate an OpenBao outage of about a day before
renewals fail. Shorter lifetimes shrink the revocation window and the tolerated outage together.
