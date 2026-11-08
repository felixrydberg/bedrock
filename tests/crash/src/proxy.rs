//! A TCP proxy in front of one listener, so the harness can cut, stall or slow the traffic it carries over real sockets.
//! Every other process dials a node through its proxy, so a fault on it acts on all traffic into that node, while the
//! node's own outbound calls go through the other nodes' proxies: a partition here is one-way.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Open,
    /// Closes every connection and refuses new ones, as a partition or a dead host would.
    Refuse,
    /// Keeps connections open but forwards nothing, as a stalled host or a black-holed route would.
    Stall,
    /// Forwards requests but nothing back, as a lost reply would.
    StallReplies,
    /// Forwards each request, then closes the connection before any reply: the caller cannot know the outcome.
    CutReplies,
    /// Delays every chunk by this many milliseconds.
    Delay(u64),
}

pub struct Proxy {
    pub port: u16,
    mode: watch::Sender<Mode>,
}

impl Proxy {
    /// Listens on a free loopback port and forwards to `target`.
    pub async fn start(target: u16) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (mode, watching) = watch::channel(Mode::Open);
        tokio::spawn(async move {
            while let Ok((client, _)) = listener.accept().await {
                let mode = watching.clone();
                if *mode.borrow() == Mode::Refuse {
                    continue;
                }
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(("127.0.0.1", target)).await else {
                        return;
                    };
                    let (client_read, client_write) = client.into_split();
                    let (server_read, server_write) = server.into_split();
                    // Either direction ending ends both, as a closed socket would.
                    tokio::select! {
                        () = pump(client_read, server_write, mode.clone(), false) => {}
                        () = pump(server_read, client_write, mode, true) => {}
                    }
                });
            }
        });
        Proxy { port, mode }
    }

    pub fn set(&self, mode: Mode) {
        self.mode.send_replace(mode);
    }
}

async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    mut mode: watch::Receiver<Mode>,
    replies: bool,
) {
    let mut buffer = vec![0; 64 * 1024];
    loop {
        let read = tokio::select! {
            read = from.read(&mut buffer) => read,
            changed = mode.changed() => {
                if changed.is_err() || *mode.borrow() == Mode::Refuse {
                    return;
                }
                continue;
            }
        };
        let Ok(n @ 1..) = read else {
            return;
        };
        loop {
            let current = *mode.borrow();
            match current {
                Mode::Open => break,
                Mode::Refuse => return,
                Mode::CutReplies if replies => return,
                Mode::CutReplies => break,
                Mode::Delay(ms) => {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                    break;
                }
                Mode::Stall | Mode::StallReplies => {
                    if current == Mode::StallReplies && !replies {
                        break;
                    }
                    // Hold the data until the fault ends.
                    if mode.changed().await.is_err() {
                        return;
                    }
                }
            }
        }
        if to.write_all(&buffer[..n]).await.is_err() {
            return;
        }
    }
}
