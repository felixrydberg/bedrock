# Node security: mutual TLS, certificates and private networking

Bedrock nodes can talk to each other over mutual TLS. Each process authenticates with its own certificate, and every
server checks who is calling before it answers. Which CA issues the certificates and how the network is kept private
are deployment choices; this guide covers Kubernetes, Docker hosts and bare metal across providers (#44, B-54/B-55).

> **Tested so far:** the binaries' TLS behaviour, by the `bedrock-lib` tests with a generated CA. The OpenBao,
> cert-manager and Nebula setups below have not been run end to end yet; treat them as starting points.

## What the binaries do

- **Off by default.** Without a `[tls]` section, a node speaks plaintext, logs a warning at startup and reports
  `bedrock_transport_tls_enabled 0`. Production cell relays with `[membership]` require TLS and reject this configuration.
- **Required once configured.** With `[tls]`, every node-to-node listener and connection uses TLS 1.3 with client
  certificates. Until the three files load, `/ready` stays false; after 60 seconds the process exits. It never falls
  back to plaintext.
- **One mode per cell.** A TLS node rejects plaintext peers and the reverse. Switching an existing cell to TLS is a
  cutover: deploy the certificates, then restart every node in the cell with `[tls]`.
- **Renewal without restart.** The files are re-read every 10 seconds. A renewal applies to new connections; a renewal
  that fails to parse, has expired or names another node is rejected, the previous certificate stays in use and
  `bedrock_transport_tls_reload_failures_total` increments. `bedrock_transport_tls_expiry_seconds` shows how long the
  current certificate has left.
- **Existing connections keep their handshake identity.** gRPC servers begin a graceful close of connections older
  than 24 hours, but without a grace limit calls still running, such as relay sessions, are not cut off; the HTTP
  write listener has no age limit. A long-lived session can therefore outlive its peer's certificate. Cut a machine off
  at the network layer when that matters.
- **Not covered:** the admin endpoint (`/metrics`, `/health`, `/ready`, `/introspect`) stays plain HTTP without
  authentication. Bind `admin_listen` to loopback or a private interface. S3 traffic uses the provider's HTTPS.

```toml
[tls]
cert = "/etc/bedrock/tls/tls.crt"   # leaf first, then any intermediates
key = "/etc/bedrock/tls/tls.key"
ca = "/etc/bedrock/tls/ca.crt"      # the shared trust bundle; see CA rotation
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

Cell Raft voters additionally pin each address to a distinct certificate node ID in `[membership.voter_ids]`.
Every voter uses the same complete map, including itself. A host SAN does not grant another process's voter identity,
even when voters share a host or a certificate has multiple host SANs. Cell operator RPCs use the relay's mTLS listener
and additionally require the bearer token.

**CA rotation.** `[tls] ca` is a trust bundle distributed on its own, the same on every node, not the issuing CA
that comes with each leaf certificate. Before any leaf is issued from a new CA, roll out a bundle holding both the old
and the new CA to every node; remove the old CA only once every certificate it issued has expired. Otherwise renewed
nodes and not-yet-renewed nodes reject each other.

Servers then authorize by the certificate, never by request fields:

| Server | Accepts |
|---|---|
| `bedrock-api` relay port (`listen`) | `relay` |
| `bedrock-relay`, leaf sessions | `quicksilver`, `kv`, `cdn` of the relay's own cell; the announced member must be the caller itself, at one of its SANs |
| `bedrock-relay`, sessions from lower relays, leaf sessions without a member, and sibling watches | `relay` (sibling watches: the same cell only) |
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

  Mount the Secret and point `[tls] cert` and `key` at its `tls.crt` and `tls.key`. Kubernetes replaces mounted
  Secrets by swapping a symlink; Bedrock compares file contents, so renewals are picked up. Do not use the Secret's
  `ca.crt` as `[tls] ca`: it is only the CA that issued that pod's certificate, and pods differ during a CA rotation.
  Mount the shared trust bundle instead, for example a trust-manager `Bundle` synced to a ConfigMap in every
  namespace, and follow the CA rotation order above.
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
      role_id_file_path                = "/etc/bao/role-id"
      secret_id_file_path              = "/run/bao/secret-id"   # a wrapping token, written before every start
      # Only accept a token that wrapped this machine's SecretID creation, not any other wrapped response.
      secret_id_response_wrapping_path = "auth/approle/role/eu-1a-m1/secret-id"
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
  {{ .Data.serial_number }}{{ end }}
  EOT
}
```

The template uses consul-template's `writeToFile`; check that the agent version supports it. Mount
`/run/bedrock/{service}` read-only into that service's container, with one template per service on the machine, and
the shared trust bundle as `ca.crt` (see CA rotation). If Bedrock reads the files between two writes, the key does not
match the certificate; that reload is rejected and the next one, 10 seconds later, applies.

**SecretID delivery.** The agent deletes the SecretID file after reading it (`remove_secret_id_file_after_reading`
defaults to true) and keeps the value only in memory, so every agent start needs a fresh one. Before each start, for
example from a systemd `ExecStartPre` or a compose init container, the provisioning service, which alone may create
SecretIDs, runs `bao write -wrap-ttl=60s -f auth/approle/role/eu-1a-m1/secret-id` and the machine writes the
returned wrapping token to `/run/bao/secret-id` on tmpfs. The SecretID itself never reaches disk, and a token wrapped
by any other endpoint is refused. While running, the agent renews its token and logs in again with the cached SecretID
when renewal stops; certificate re-issue uses that token. Give the role a periodic token (`token_period`) so renewal
is the normal path, and a SecretID lifetime and use count (`secret_id_ttl`, `secret_id_num_uses`) that cover
re-logins until the next restart.

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
      - { port: 7001, proto: tcp, groups: [monitoring] }   # api admin_listen
      - { port: 7101, proto: tcp, groups: [monitoring] }   # kv admin_listen
      - { port: 7201, proto: tcp, groups: [monitoring] }   # cdn admin_listen
      - { port: 7301, proto: tcp, groups: [monitoring] }   # relay admin_listen
      - { port: 7401, proto: tcp, groups: [monitoring] }   # quicksilver admin_listen
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
entity carries its `cell`, `node_id` and advertised `host` as metadata, so a machine can only obtain certificates
naming itself and its own host.

```sh
bao secrets enable -path=pki_bedrock pki
bao secrets tune -max-lease-ttl=8760h pki_bedrock
# ... generate or import the intermediate CA for pki_bedrock ...

for service in quicksilver kv cdn api; do
  bao write pki_bedrock/roles/$service \
    allowed_uri_sans="bedrock://$service/{{identity.entity.metadata.cell}}/{{identity.entity.metadata.node_id}}" \
    allowed_uri_sans_template=true \
    allowed_domains="{{identity.entity.metadata.host}}" allowed_domains_template=true \
    allow_bare_domains=true allow_subdomains=false allow_glob_domains=false allow_localhost=false \
    allow_ip_sans=false \
    server_flag=true client_flag=true \
    key_type=ec key_bits=256 ttl=168h max_ttl=168h generate_lease=true
done
# Relays usually have their own node_id and host; give relay machines relay_node_id and relay_host entries.
bao write pki_bedrock/roles/relay \
  allowed_uri_sans="bedrock://relay/{{identity.entity.metadata.cell}}/{{identity.entity.metadata.relay_node_id}}" \
  allowed_uri_sans_template=true \
  allowed_domains="{{identity.entity.metadata.relay_host}}" allowed_domains_template=true \
  allow_bare_domains=true allow_subdomains=false allow_glob_domains=false allow_localhost=false \
  allow_ip_sans=false \
  server_flag=true client_flag=true key_type=ec key_bits=256 ttl=168h max_ttl=168h generate_lease=true
```

The common name and DNS SANs are limited to the machine's own host, and IP SANs are off, so nodes advertise DNS names.
A role cannot bind IP SANs to the requesting machine: `allowed_ip_sans_cidr` takes fixed ranges. Deployments that
advertise IP addresses need either one role per machine with its own address as a /32 in `allowed_ip_sans_cidr`, or
a CEL role (`pki_bedrock/cel/roles/...`) whose program checks the requested addresses; neither is tried here. Per
machine: an AppRole role, a policy allowing `pki_bedrock/issue/{service}` for the services it runs, and its entity's
metadata (`cell`, `node_id`, `host`, plus `relay_node_id` and `relay_host` on relay machines).

Lifetimes: 7-day node certificates, renewed at 85% of their TTL, tolerate an OpenBao outage of about a day before
renewals fail. Shorter lifetimes shrink the revocation window and the tolerated outage together.
