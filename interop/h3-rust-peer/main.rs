//! Loopback HTTP/3 request and WebTransport negotiation probe against zmux-rs.

use std::{
    collections::{HashMap, hash_map::Entry},
    error::Error,
    future::poll_fn,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};

use bytes::{Buf, Bytes, BytesMut};
use http::{Method, Request, Response, StatusCode};
use quinn::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use tokio::sync::{Mutex, oneshot};
use zmux_webtransport::{Url, web_transport_quinn as wt};

type Fail = Box<dyn Error + Send + Sync>;
type Result<T> = std::result::Result<T, Fail>;
type H3Send = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
type PairTable = Arc<Mutex<HashMap<String, PairHalf>>>;

const PAIR_HEADER: &str = "zmux-pipe-pair";

enum PairHalf {
    Down(oneshot::Sender<Bytes>),
    Up(Bytes),
}

fn loopback(port: u16) -> SocketAddr {
    ([127, 0, 0, 1], port).into()
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    let [_, role, mode, port, cert, key] = args.as_slice() else {
        return Err("usage: rust-peer server|client h3|wt PORT CERT KEY".into());
    };
    let port = port.parse()?;
    let certificates =
        CertificateDer::pem_file_iter(cert)?.collect::<std::result::Result<Vec<_>, _>>()?;
    let bound = if role == "server" { 30 } else { 10 };
    tokio::time::timeout(Duration::from_secs(bound), async {
        match (role.as_str(), mode.as_str()) {
            ("server", "h3") => {
                serve_h3(port, certificates, PrivateKeyDer::from_pem_file(key)?).await
            }
            ("client", "h3") => run_h3(port, certificates).await,
            ("server", "wt") => {
                serve_wt(port, certificates, PrivateKeyDer::from_pem_file(key)?).await
            }
            ("client", "wt") => run_wt(port, certificates).await,
            _ => Err("expected server|client h3|wt".into()),
        }
    })
    .await?
}

async fn serve_h3(
    port: u16,
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<()> {
    let tls = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certificates, key)?;
    let config = zmux_h3::server_config(tls, &zmux_h3::Options::default())?;
    let endpoint = quinn::Endpoint::server(config, loopback(port))?;
    println!("READY {}", endpoint.local_addr()?.port());
    let incoming = endpoint.accept().await.ok_or("endpoint closed")?;
    let mut h3 = h3::server::Connection::new(h3_quinn::Connection::new(incoming.await?)).await?;
    let pairs = PairTable::default();
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            next = h3.accept() => {
                let req = match next {
                    Ok(Some(req)) => req,
                    Ok(None) => break,
                    Err(err) if err.is_h3_no_error() => break,
                    // Some peers use QUIC application code zero on ordinary shutdown.
                    Err(h3::error::ConnectionError::Remote(
                        h3::quic::ConnectionErrorIncoming::ApplicationClose { error_code: 0 },
                    )) => break,
                    Err(h3::error::ConnectionError::Remote(
                        h3::quic::ConnectionErrorIncoming::Undefined(err),
                    )) if matches!(
                        err.downcast_ref::<quinn::ConnectionError>(),
                        Some(quinn::ConnectionError::ConnectionClosed(close))
                            if close.error_code == quinn::TransportErrorCode::NO_ERROR
                    ) => break,
                    Err(err) => return Err(err.into()),
                };
                let pairs = pairs.clone();
                tasks.spawn(handle_h3(req, pairs));
            }
            Some(done) = tasks.join_next(), if !tasks.is_empty() => done??,
        }
    }
    while let Some(done) = tasks.join_next().await {
        done??;
    }
    Ok(())
}

async fn handle_h3(
    resolver: h3::server::RequestResolver<h3_quinn::Connection, Bytes>,
    pairs: PairTable,
) -> Result<()> {
    let (req, mut stream) = resolver.resolve_request().await?;
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/health") => {
            stream.send_response(Response::new(())).await?;
            stream.send_data(Bytes::from_static(b"ok")).await?;
        }
        (&Method::POST, "/stream") if !req.headers().contains_key("content-length") => {
            stream.send_response(Response::new(())).await?;
            while let Some(mut data) = stream.recv_data().await? {
                let size = data.remaining();
                stream.send_data(data.copy_to_bytes(size)).await?;
            }
        }
        (&Method::POST, "/exchange") => {
            let length: usize = req
                .headers()
                .get("content-length")
                .ok_or("exchange length missing")?
                .to_str()?
                .parse()?;
            let mut body = BytesMut::new();
            while let Some(data) = stream.recv_data().await? {
                append_body(&mut body, data)?;
            }
            if length != body.len() {
                return Err("exchange length differs from its body".into());
            }
            stream
                .send_response(
                    Response::builder()
                        .header("content-length", length)
                        .body(())?,
                )
                .await?;
            stream.send_data(body.freeze()).await?;
        }
        (&Method::GET, "/split") => {
            let token = pair_key(&req)?;
            let rx = {
                let (tx, rx) = oneshot::channel();
                match pairs.lock().await.entry(token) {
                    Entry::Occupied(pair) => match pair.remove() {
                        PairHalf::Up(data) => tx.send(data).map_err(|_| "split GET canceled")?,
                        PairHalf::Down(_) => return Err("duplicate split GET".into()),
                    },
                    Entry::Vacant(pair) => {
                        pair.insert(PairHalf::Down(tx));
                    }
                }
                rx
            };
            let body = rx.await?;
            stream.send_response(Response::new(())).await?;
            stream.send_data(body).await?;
        }
        (&Method::POST, "/split") => {
            let token = pair_key(&req)?;
            let mut body = BytesMut::new();
            while let Some(data) = stream.recv_data().await? {
                append_body(&mut body, data)?;
            }
            let mut pairs = pairs.lock().await;
            match pairs.entry(token) {
                Entry::Occupied(pair) => match pair.remove() {
                    PairHalf::Down(tx) => {
                        tx.send(body.freeze()).map_err(|_| "split GET canceled")?
                    }
                    PairHalf::Up(_) => return Err("duplicate split POST".into()),
                },
                Entry::Vacant(pair) => {
                    pair.insert(PairHalf::Up(body.freeze()));
                }
            }
            drop(pairs);
            stream.send_response(Response::new(())).await?;
        }
        _ => {
            stream
                .send_response(
                    Response::builder()
                        .status(StatusCode::BAD_REQUEST)
                        .body(())?,
                )
                .await?
        }
    }
    stream.finish().await?;
    Ok(())
}

fn pair_key(req: &Request<()>) -> Result<String> {
    Ok(req
        .headers()
        .get(PAIR_HEADER)
        .ok_or("pairing header missing")?
        .to_str()?
        .to_owned())
}

fn append_body(body: &mut BytesMut, mut data: impl Buf) -> Result<()> {
    let size = data.remaining();
    if size > 4096 - body.len() {
        return Err("peer body too large".into());
    }
    body.extend_from_slice(&data.copy_to_bytes(size));
    Ok(())
}

fn client_config(certificates: Vec<CertificateDer<'static>>) -> Result<quinn::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certificates {
        roots.add(cert)?;
    }
    let tls = rustls::ClientConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth();
    Ok(zmux_h3::client_config(tls, &zmux_h3::Options::default())?)
}

async fn run_h3(port: u16, certificates: Vec<CertificateDer<'static>>) -> Result<()> {
    let mut endpoint = quinn::Endpoint::client(loopback(0))?;
    endpoint.set_default_client_config(client_config(certificates)?);
    let conn = endpoint.connect(loopback(port), "localhost")?.await?;
    let (mut driver, mut requests) =
        h3::client::new(h3_quinn::Connection::new(conn.clone())).await?;
    let driver = tokio::spawn(async move { poll_fn(|cx| driver.poll_close(cx)).await });
    let base = format!("https://localhost:{port}");

    let request = Request::post(format!("{base}/stream")).body(())?;
    let mut stream = requests.send_request(request).await?;
    stream.send_data(Bytes::from_static(b"str")).await?;
    check_status(&mut stream).await?;
    let mut first = BytesMut::new();
    while first.len() < 3 {
        let chunk = stream
            .recv_data()
            .await?
            .ok_or("stream ended before request FIN")?;
        append_body(&mut first, chunk)?;
    }
    if first.as_ref() != b"str" {
        return Err(format!("unexpected first streaming chunk {first:?}").into());
    }
    stream.send_data(Bytes::from_static(b"eam")).await?;
    stream.finish().await?;
    check_body(&mut stream, b"eam").await?;

    let request = Request::post(format!("{base}/exchange"))
        .header("content-length", 8)
        .body(())?;
    let mut exchange = requests.send_request(request).await?;
    exchange.send_data(Bytes::from_static(b"exchange")).await?;
    exchange.finish().await?;
    check_response(exchange, b"exchange").await?;

    // Separate request streams may progress concurrently within the same QUIC connection.
    for (token, up_first) in [
        ("AAAAAAAAAAAAAAAAAAAAAA", false),
        ("AAAAAAAAAAAAAAAAAAAAAQ", true),
    ] {
        let (down, up) = if up_first {
            let up = split_request(&mut requests, Method::POST, &base, token).await?;
            let down = split_request(&mut requests, Method::GET, &base, token).await?;
            (down, up)
        } else {
            let down = split_request(&mut requests, Method::GET, &base, token).await?;
            let up = split_request(&mut requests, Method::POST, &base, token).await?;
            (down, up)
        };
        tokio::try_join!(check_response(down, b"split"), check_response(up, b""))
            .map_err(|err| format!("split pair {token}: {err}"))?;
    }
    let health = Request::get(format!("{base}/health")).body(())?;
    let mut health = requests.send_request(health).await?;
    health.finish().await?;
    check_response(health, b"ok").await?;
    let no_error = quinn::VarInt::from_u64(h3::error::Code::H3_NO_ERROR.value())?;
    conn.close(no_error, b"wire probe done");
    let _ = driver.await?;
    println!("PASS h3 stream exchange split");
    Ok(())
}

type H3ClientStream = h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>;

async fn split_request(
    requests: &mut H3Send,
    method: Method,
    base: &str,
    token: &str,
) -> Result<H3ClientStream> {
    let upload = method == Method::POST;
    let req = Request::builder()
        .method(method)
        .uri(format!("{base}/split"))
        .header(PAIR_HEADER, token)
        .body(())?;
    let mut stream = requests.send_request(req).await?;
    if upload {
        stream.send_data(Bytes::from_static(b"split")).await?;
    }
    stream.finish().await?;
    Ok(stream)
}

async fn check_response(mut stream: H3ClientStream, payload: &[u8]) -> Result<()> {
    check_status(&mut stream).await?;
    check_body(&mut stream, payload).await
}

async fn check_status(stream: &mut H3ClientStream) -> Result<()> {
    let response = stream.recv_response().await?;
    if response.status() != StatusCode::OK {
        return Err(format!("HTTP response {}", response.status()).into());
    }
    Ok(())
}

async fn check_body(stream: &mut H3ClientStream, payload: &[u8]) -> Result<()> {
    let mut body = BytesMut::new();
    while let Some(data) = stream.recv_data().await? {
        append_body(&mut body, data)?;
    }
    if body.as_ref() != payload {
        return Err(format!("response body {body:?} differs from {payload:?}").into());
    }
    Ok(())
}

async fn serve_wt(
    port: u16,
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
) -> Result<()> {
    let tls = rustls::ServerConfig::builder_with_provider(
        rustls::crypto::ring::default_provider().into(),
    )
    .with_safe_default_protocol_versions()?
    .with_no_client_auth()
    .with_single_cert(certificates, key)?;
    let mut config = zmux_h3::server_config(tls, &zmux_h3::Options::default())?;
    config.transport_config(Arc::new(
        zmux_webtransport::Options::default().transport_config(),
    ));
    let endpoint = quinn::Endpoint::server(config, loopback(port))?;
    println!("READY {}", endpoint.local_addr()?.port());
    let conn = endpoint.accept().await.ok_or("endpoint closed")?.await?;
    let request = wt::Request::accept(conn)
        .await
        .map_err(|e| format!("WT negotiation failed: {e:?}"))?;
    let session = request.ok().await?;
    let (send, recv) = session.accept_bi().await?;
    expect_wt_fin(recv, b"client-bidi").await?;
    send_wt_fin(send, b"client-bidi").await?;
    expect_wt_fin(session.accept_uni().await?, b"client-uni").await?;
    send_wt_fin(session.open_uni().await?, b"server-uni").await?;
    let (send, recv) = session.open_bi().await?;
    send_wt_fin(send, b"server-bidi").await?;
    expect_wt_fin(recv, b"server-bidi").await?;
    let close = session.closed().await;
    if !matches!(&close, wt::SessionError::WebTransportError(wt::WebTransportError::Closed(42, reason)) if reason == "done")
    {
        return Err(format!("unexpected WT close {close:?}").into());
    }
    println!("PASS wt server close=42 reason=done");
    Ok(())
}

async fn run_wt(port: u16, certificates: Vec<CertificateDer<'static>>) -> Result<()> {
    let mut config = client_config(certificates)?;
    config.transport_config(Arc::new(
        zmux_webtransport::Options::default().transport_config(),
    ));
    let mut endpoint = quinn::Endpoint::client(loopback(0))?;
    endpoint.set_default_client_config(config);
    let conn = endpoint.connect(loopback(port), "localhost")?.await?;
    let url: Url = format!("https://localhost:{port}/wt").parse()?;
    let session = wt::Session::connect(conn, url)
        .await
        .map_err(|e| format!("WT negotiation failed: {e:?}"))?;
    probe_wt(session).await
}

async fn probe_wt(session: wt::Session) -> Result<()> {
    let (send, recv) = session.open_bi().await?;
    send_wt_fin(send, b"client-bidi").await?;
    expect_wt_fin(recv, b"client-bidi").await?;
    send_wt_fin(session.open_uni().await?, b"client-uni").await?;
    expect_wt_fin(session.accept_uni().await?, b"server-uni").await?;
    let (send, recv) = session.accept_bi().await?;
    expect_wt_fin(recv, b"server-bidi").await?;
    send_wt_fin(send, b"server-bidi").await?;
    session.close(42, b"done");
    let _ = session.closed().await;
    println!("PASS wt client bidi=both uni=both close=42 reason=done");
    Ok(())
}

async fn send_wt_fin(mut send: wt::SendStream, payload: &[u8]) -> Result<()> {
    send.write_all(payload).await?;
    send.finish()?;
    Ok(())
}

async fn expect_wt_fin(mut recv: wt::RecvStream, payload: &[u8]) -> Result<()> {
    let received = recv.read_to_end(4096).await?;
    if received != payload {
        return Err(format!("WT payload {received:?} differs from {payload:?}").into());
    }
    Ok(())
}
