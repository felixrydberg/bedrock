//! Mutual TLS between nodes (B-55, #44), enabled by `NodeConfig::tls`; without it, nodes speak plaintext.
//!
//! Each process presents one certificate. Its URI SAN `bedrock://{service}/{cell}/{node_id}` names the process, and
//! a DNS or IP SAN covers the host in `advertise`. Servers require a client certificate from the CA bundle and hand
//! handlers the verified [`Caller`]; clients accept only `relay` and `api` servers. Which CA issues the certificates is
//! a deployment choice (`docs/deployment/security.md`).
//!
//! The files are re-read every [`RELOAD`]: a renewal applies to new handshakes without a restart, and a renewal that
//! does not load keeps the previous certificate. TLS 1.3 only.
//!
//! madsim does not simulate TLS, so simulation builds compile the adapter out, as with RocksDB: [`Tls`] cannot be
//! constructed there and configuring `tls` fails at startup.

use std::sync::Arc;

use prometheus_client::metrics::gauge::Gauge;

use super::admin::Admin;
use crate::node::{ConfigError, NodeConfig};

#[cfg(not(madsim))]
pub use adapter::Tls;

/// What the local certificate must name.
#[derive(Debug, Clone)]
pub struct Identity {
    pub service: String,
    pub cell: String,
    pub node_id: String,
    /// The host of `advertise`, which peers dial and verify.
    pub host: String,
}

/// Loads `node.tls` for `service` when configured. Until the files load, the node stays unready; it fails after a
/// startup wait instead of falling back to plaintext.
pub async fn start(
    node: &NodeConfig,
    service: &str,
    admin: &Admin,
) -> Result<Option<Arc<Tls>>, ConfigError> {
    let enabled = Gauge::<i64>::default();
    admin.register(
        "transport_tls_enabled",
        "1 when node-to-node traffic uses mutual TLS",
        enabled.clone(),
    );
    let Some(config) = &node.tls else {
        tracing::warn!("tls is not configured; node-to-node traffic is plaintext");
        return Ok(None);
    };
    let advertised = node.advertised();
    let identity = Identity {
        service: service.to_owned(),
        cell: node.location.cell.clone(),
        node_id: node.node_id.clone(),
        host: super::host_of(&advertised).to_owned(),
    };
    let tls = Tls::start(config, identity, admin).await?;
    enabled.set(1);
    Ok(Some(tls))
}

#[cfg(madsim)]
pub use sim::Tls;

#[cfg(madsim)]
mod sim {
    use std::convert::Infallible;
    use std::sync::Arc;

    use tonic::transport::{Channel, Endpoint};

    use super::Identity;
    use crate::node::{ConfigError, TlsConfig};
    use crate::transport::Io;
    use crate::transport::admin::Admin;

    /// Uninhabited: simulation runs plaintext.
    pub struct Tls(Infallible);

    impl Tls {
        pub async fn start(_: &TlsConfig, _: Identity, _: &Admin) -> Result<Arc<Tls>, ConfigError> {
            Err(ConfigError(
                "tls is not simulated; leave it unset under madsim".into(),
            ))
        }
        pub fn channel(&self, _: Endpoint) -> Channel {
            match self.0 {}
        }
        pub async fn dial(&self, _: &str) -> std::io::Result<Box<dyn Io>> {
            match self.0 {}
        }
        pub async fn serve(
            &self,
            _: tonic::transport::server::Router,
            _: std::net::SocketAddr,
        ) -> Result<(), String> {
            match self.0 {}
        }
        pub async fn accept_optional(
            &self,
            _: tokio::net::TcpStream,
        ) -> std::io::Result<Box<dyn Io>> {
            match self.0 {}
        }
    }
}

#[cfg(not(madsim))]
mod adapter {
    use std::future::Future;
    use std::io;
    use std::net::{IpAddr, SocketAddr};
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use hyper_util::rt::TokioIo;
    use prometheus_client::metrics::counter::Counter;
    use prometheus_client::metrics::gauge::Gauge;
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
    use rustls::server::WebPkiClientVerifier;
    use rustls::sign::CertifiedKey;
    use rustls::{ClientConfig, RootCertStore, ServerConfig};
    use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;
    use tokio_rustls::{TlsAcceptor, TlsConnector};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::codegen::http::Uri;
    use tonic::transport::server::Connected;
    use tonic::transport::{Channel, Endpoint};
    use x509_parser::extensions::GeneralName;

    use super::Identity;
    use crate::node::{ConfigError, TlsConfig};
    use crate::transport::admin::Admin;
    use crate::transport::{Caller, Io, parse_identity};

    /// How often the files are checked for renewals.
    pub const RELOAD: Duration = Duration::from_secs(10);
    /// How long startup waits for the files, e.g. while a sidecar issues the first certificate.
    const STARTUP_WAIT: Duration = Duration::from_secs(60);
    const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
    /// Services a node dials: relays and the writer (B-17).
    const SERVERS: &[&str] = &["relay", "api"];

    pub struct Tls {
        config: TlsConfig,
        identity: Identity,
        current: Mutex<Arc<Loaded>>,
        handshake_failures: Counter,
        reload_failures: Counter,
        expiry: Gauge,
    }

    struct Loaded {
        files: [Vec<u8>; 3],
        /// Node-to-node listeners: a client certificate is required.
        server: Arc<ServerConfig>,
        /// The write API: callers without a certificate authenticate with the bearer token.
        server_optional: Arc<ServerConfig>,
        client: Arc<ClientConfig>,
        not_after: i64,
    }

    impl Tls {
        pub async fn start(
            config: &TlsConfig,
            identity: Identity,
            admin: &Admin,
        ) -> Result<Arc<Tls>, ConfigError> {
            let deadline = crate::monotonic_now() + STARTUP_WAIT;
            let loaded = loop {
                match read(config).and_then(|files| load(files, &identity)) {
                    Ok(loaded) => break loaded,
                    Err(error) if crate::monotonic_now() < deadline => {
                        tracing::warn!(%error, "waiting for tls files");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    Err(error) => return Err(ConfigError(format!("tls: {error}"))),
                }
            };
            let tls = Arc::new(Tls {
                config: config.clone(),
                identity,
                current: Mutex::new(Arc::new(loaded)),
                handshake_failures: Counter::default(),
                reload_failures: Counter::default(),
                expiry: Gauge::default(),
            });
            admin.register(
                "transport_tls_handshake_failures",
                "Inbound TLS handshakes that failed or presented no valid node identity",
                tls.handshake_failures.clone(),
            );
            admin.register(
                "transport_tls_reload_failures",
                "Changed TLS files that did not load; the previous certificate stays in use",
                tls.reload_failures.clone(),
            );
            admin.register(
                "transport_tls_expiry_seconds",
                "Seconds until the local certificate expires",
                tls.expiry.clone(),
            );
            tls.update_expiry();
            tokio::spawn(tls.clone().reload());
            Ok(tls)
        }

        async fn reload(self: Arc<Self>) {
            let mut tick = tokio::time::interval(RELOAD);
            loop {
                tick.tick().await;
                self.reload_once();
                self.update_expiry();
            }
        }

        /// Applies changed files; returns whether the certificate changed.
        pub fn reload_once(&self) -> bool {
            let files = match read(&self.config) {
                Ok(files) => files,
                Err(error) => {
                    self.reload_failures.inc();
                    tracing::error!(%error, "tls files unreadable; keeping the current certificate");
                    return false;
                }
            };
            if files == self.current.lock().unwrap().files {
                return false;
            }
            match load(files, &self.identity) {
                Ok(loaded) => {
                    *self.current.lock().unwrap() = Arc::new(loaded);
                    tracing::info!("tls certificate reloaded");
                    true
                }
                Err(error) => {
                    self.reload_failures.inc();
                    tracing::error!(%error, "tls renewal rejected; keeping the current certificate");
                    false
                }
            }
        }

        fn update_expiry(&self) {
            let not_after = self.current.lock().unwrap().not_after;
            self.expiry.set(not_after.saturating_sub(unix_now()));
        }

        fn current(&self) -> Arc<Loaded> {
            self.current.lock().unwrap().clone()
        }

        /// A lazily connecting channel that dials through TLS and accepts only [`SERVERS`].
        pub fn channel(self: &Arc<Self>, endpoint: Endpoint) -> Channel {
            endpoint.connect_with_connector_lazy(Connector(self.clone()))
        }

        pub async fn dial(&self, addr: &str) -> io::Result<Box<dyn Io>> {
            Ok(Box::new(self.connect(addr).await?))
        }

        async fn connect(
            &self,
            addr: &str,
        ) -> io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
            let name = ServerName::try_from(crate::transport::host_of(addr).to_owned())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let tcp = TcpStream::connect(addr).await?;
            tcp.set_nodelay(true)?;
            let connector = TlsConnector::from(self.current().client.clone());
            let stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, connector.connect(name, tcp))
                .await
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::TimedOut, "tls handshake timed out")
                })??;
            let server = peer(stream.get_ref().1.peer_certificates())
                .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
            if !SERVERS.contains(&server.service.as_str()) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    format!(
                        "{addr} is {} {}, not a relay or api",
                        server.service, server.node_id
                    ),
                ));
            }
            Ok(stream)
        }

        /// Serves gRPC on `addr`; only callers with a valid node certificate get through, as a [`Caller`].
        pub async fn serve(
            self: &Arc<Self>,
            router: tonic::transport::server::Router,
            addr: SocketAddr,
        ) -> Result<(), String> {
            let listener = TcpListener::bind(addr).await.map_err(|e| e.to_string())?;
            let (tx, rx) = mpsc::channel(64);
            let accepting = tokio::spawn(self.clone().accept_loop(listener, tx));
            let served = router
                .serve_with_incoming(ReceiverStream::new(rx))
                .await
                .map_err(|e| e.to_string());
            accepting.abort();
            served
        }

        async fn accept_loop(
            self: Arc<Self>,
            listener: TcpListener,
            tx: mpsc::Sender<io::Result<Verified>>,
        ) {
            while !tx.is_closed() {
                let tcp = match listener.accept().await {
                    Ok((tcp, _)) => tcp,
                    Err(error) => {
                        tracing::warn!(%error, "accepting a connection failed");
                        continue;
                    }
                };
                let (tls, tx) = (self.clone(), tx.clone());
                // Handshakes run concurrently, so a slow client does not hold up the listener.
                tokio::spawn(async move {
                    match tls.accept(tcp, true).await {
                        Ok((stream, Some(caller))) => {
                            let _ = tx.send(Ok(Verified { stream, caller })).await;
                        }
                        Ok((_, None)) => unreachable!("client certificates are required"),
                        Err(error) => tracing::debug!(%error, "tls handshake rejected"),
                    }
                });
            }
        }

        /// A write API connection: a client certificate is optional, and the bearer token authenticates.
        pub async fn accept_optional(&self, tcp: TcpStream) -> io::Result<Box<dyn Io>> {
            Ok(Box::new(self.accept(tcp, false).await?.0))
        }

        /// Completes a server handshake. With `require_client`, the caller must present a valid node certificate;
        /// otherwise a certificate is optional and its identity is not checked.
        pub async fn accept(
            &self,
            tcp: TcpStream,
            require_client: bool,
        ) -> io::Result<(tokio_rustls::server::TlsStream<TcpStream>, Option<Caller>)> {
            let current = self.current();
            let config = if require_client {
                current.server.clone()
            } else {
                current.server_optional.clone()
            };
            let result = async {
                let stream =
                    tokio::time::timeout(HANDSHAKE_TIMEOUT, TlsAcceptor::from(config).accept(tcp))
                        .await
                        .map_err(|_| {
                            io::Error::new(io::ErrorKind::TimedOut, "tls handshake timed out")
                        })??;
                if !require_client {
                    return Ok((stream, None));
                }
                let caller = peer(stream.get_ref().1.peer_certificates())
                    .map_err(|e| io::Error::new(io::ErrorKind::PermissionDenied, e))?;
                Ok((stream, Some(caller)))
            }
            .await;
            if result.is_err() {
                self.handshake_failures.inc();
            }
            result
        }
    }

    /// The identity of a verified peer chain.
    fn peer(chain: Option<&[CertificateDer<'_>]>) -> Result<Caller, String> {
        let leaf = chain.and_then(<[_]>::first).ok_or("no peer certificate")?;
        Ok(parse_leaf(leaf)?.0)
    }

    /// A leaf certificate's node identity and expiry (unix seconds).
    fn parse_leaf(leaf: &CertificateDer<'_>) -> Result<(Caller, i64), String> {
        let (_, cert) = x509_parser::parse_x509_certificate(leaf).map_err(|e| e.to_string())?;
        let mut identity = None;
        let mut hosts = Vec::new();
        let names = cert.subject_alternative_name().map_err(|e| e.to_string())?;
        for name in names.iter().flat_map(|san| &san.value.general_names) {
            match name {
                GeneralName::URI(uri) => {
                    if let Some(id) = parse_identity(uri)
                        && identity.replace(id).is_some()
                    {
                        return Err("certificate names more than one bedrock:// identity".into());
                    }
                }
                GeneralName::DNSName(dns) => hosts.push(dns.to_ascii_lowercase()),
                GeneralName::IPAddress(ip) => {
                    let ip = match ip.len() {
                        4 => IpAddr::from(<[u8; 4]>::try_from(*ip).unwrap()),
                        16 => IpAddr::from(<[u8; 16]>::try_from(*ip).unwrap()),
                        _ => continue,
                    };
                    hosts.push(ip.to_string());
                }
                _ => {}
            }
        }
        let (service, cell, node_id) =
            identity.ok_or("certificate has no bedrock://{service}/{cell}/{node_id} URI SAN")?;
        let caller = Caller {
            service: service.to_owned(),
            cell: cell.to_owned(),
            node_id: node_id.to_owned(),
            hosts,
        };
        Ok((caller, cert.validity().not_after.timestamp()))
    }

    // Adapter boundary: certificate files are read outside the simulated runtime; they are small.
    #[allow(clippy::disallowed_methods)]
    fn read(config: &TlsConfig) -> Result<[Vec<u8>; 3], String> {
        let read = |path: &std::path::Path| {
            std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))
        };
        Ok([read(&config.cert)?, read(&config.key)?, read(&config.ca)?])
    }

    fn load(files: [Vec<u8>; 3], identity: &Identity) -> Result<Loaded, String> {
        let [cert, key, ca] = &files;
        let chain = CertificateDer::pem_slice_iter(cert)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| format!("cert: {e}"))?;
        let leaf = chain.first().ok_or("cert: no certificate")?;
        let key = PrivateKeyDer::from_pem_slice(key).map_err(|e| format!("key: {e}"))?;
        let mut roots = RootCertStore::empty();
        for root in CertificateDer::pem_slice_iter(ca) {
            roots
                .add(root.map_err(|e| format!("ca: {e}"))?)
                .map_err(|e| format!("ca: {e}"))?;
        }
        if roots.is_empty() {
            return Err("ca: no certificate".into());
        }
        let (own, not_after) = parse_leaf(leaf).map_err(|e| format!("cert: {e}"))?;
        if not_after <= unix_now() {
            return Err("cert has expired".into());
        }
        if (
            own.service.as_str(),
            own.cell.as_str(),
            own.node_id.as_str(),
        ) != (
            identity.service.as_str(),
            identity.cell.as_str(),
            identity.node_id.as_str(),
        ) {
            return Err(format!(
                "cert names bedrock://{}/{}/{}, but this node is bedrock://{}/{}/{}",
                own.service,
                own.cell,
                own.node_id,
                identity.service,
                identity.cell,
                identity.node_id
            ));
        }
        if !own.has_host(&identity.host) {
            return Err(format!(
                "cert has no SAN for the advertised host {}",
                identity.host
            ));
        }

        let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
        CertifiedKey::from_der(chain.clone(), key.clone_key(), &provider)
            .map_err(|e| format!("key does not match cert: {e}"))?;
        let roots = Arc::new(roots);
        let tls13 = &[&rustls::version::TLS13];
        let server = |verifier| {
            ServerConfig::builder_with_provider(provider.clone())
                .with_protocol_versions(tls13)
                .map_err(|e| e.to_string())?
                .with_client_cert_verifier(verifier)
                .with_single_cert(chain.clone(), key.clone_key())
                .map(Arc::new)
                .map_err(|e| e.to_string())
        };
        let required = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .build()
            .map_err(|e| e.to_string())?;
        let optional = WebPkiClientVerifier::builder_with_provider(roots.clone(), provider.clone())
            .allow_unauthenticated()
            .build()
            .map_err(|e| e.to_string())?;
        let client = ClientConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(tls13)
            .map_err(|e| e.to_string())?
            .with_root_certificates(roots)
            .with_client_auth_cert(chain.clone(), key.clone_key())
            .map_err(|e| e.to_string())?;
        Ok(Loaded {
            server: server(required)?,
            server_optional: server(optional)?,
            client: Arc::new(client),
            not_after,
            files,
        })
    }

    // Adapter boundary: certificate expiry is wall-clock time.
    #[allow(clippy::disallowed_methods)]
    fn unix_now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs() as i64)
    }

    /// Dials for tonic channels.
    #[derive(Clone)]
    struct Connector(Arc<Tls>);

    impl tower_service::Service<Uri> for Connector {
        type Response = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>;
        type Error = io::Error;
        type Future = Pin<Box<dyn Future<Output = io::Result<Self::Response>> + Send>>;

        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, uri: Uri) -> Self::Future {
            let tls = self.0.clone();
            Box::pin(async move {
                let addr = uri
                    .authority()
                    .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no authority"))?
                    .to_string();
                Ok(TokioIo::new(tls.connect(&addr).await?))
            })
        }
    }

    /// An accepted connection and its verified caller, which tonic puts in each request's extensions.
    struct Verified {
        stream: tokio_rustls::server::TlsStream<TcpStream>,
        caller: Caller,
    }

    impl Connected for Verified {
        type ConnectInfo = Caller;
        fn connect_info(&self) -> Caller {
            self.caller.clone()
        }
    }

    impl AsyncRead for Verified {
        fn poll_read(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Verified {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().stream).poll_flush(cx)
        }
        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            Pin::new(&mut self.get_mut().stream).poll_write_vectored(cx, bufs)
        }
        fn is_write_vectored(&self) -> bool {
            self.stream.is_write_vectored()
        }
    }
}

#[cfg(all(test, not(madsim)))]
#[path = "../../tests/unit/transport/tls.rs"]
mod tests;
