use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::error::PgWireError;
use crate::protocol::backend;
use crate::protocol::frontend;
use crate::protocol::types::{BackendMsg, FrontendMsg};
use crate::scram::ScramClient;

/// Raw PostgreSQL wire connection.
/// Handles TCP I/O, buffered reading, and authentication.
pub struct WireConn {
    stream: TcpStream,
    recv_buf: BytesMut,
    pub pid: i32,
    pub secret: i32,
}

const RECV_BUF_SIZE: usize = 32 * 1024; // 32KB recv buffer

impl WireConn {
    /// Connect to PostgreSQL and perform authentication.
    pub async fn connect(
        addr: &str,
        user: &str,
        password: &str,
        database: &str,
    ) -> Result<Self, PgWireError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        let mut conn = WireConn {
            stream,
            recv_buf: BytesMut::with_capacity(RECV_BUF_SIZE),
            pid: 0,
            secret: 0,
        };

        // Send startup message.
        let mut buf = BytesMut::new();
        frontend::encode_startup(user, database, &mut buf);
        conn.send_raw(&buf).await?;

        // Authentication loop.
        loop {
            let msg = conn.recv_msg().await?;
            match msg {
                BackendMsg::AuthenticationOk => {}
                BackendMsg::AuthenticationCleartextPassword => {
                    let mut buf = BytesMut::new();
                    frontend::encode_password(password.as_bytes(), &mut buf);
                    conn.send_raw(&buf).await?;
                }
                BackendMsg::AuthenticationMd5Password { salt } => {
                    let hash = frontend::md5_password(user, password, &salt);
                    let mut buf = BytesMut::new();
                    frontend::encode_password(&hash, &mut buf);
                    conn.send_raw(&buf).await?;
                }
                BackendMsg::AuthenticationSASL { mechanisms } => {
                    if !mechanisms.iter().any(|m| m == "SCRAM-SHA-256") {
                        return Err(PgWireError::Protocol(
                            format!("No supported SASL mechanism: {:?}", mechanisms),
                        ));
                    }
                    let (scram, client_first) = ScramClient::new(password);
                    let mut buf = BytesMut::new();
                    frontend::encode_message(
                        &FrontendMsg::SASLInitialResponse {
                            mechanism: b"SCRAM-SHA-256",
                            data: &client_first,
                        },
                        &mut buf,
                    );
                    conn.send_raw(&buf).await?;

                    // Wait for server-first.
                    let server_first = loop {
                        match conn.recv_msg().await? {
                            BackendMsg::AuthenticationSASLContinue { data } => break data,
                            BackendMsg::ErrorResponse { fields } => {
                                return Err(PgWireError::Pg(fields));
                            }
                            _ => {}
                        }
                    };

                    let client_final = scram
                        .process_server_first(&server_first)
                        .map_err(PgWireError::Protocol)?;
                    let mut buf = BytesMut::new();
                    frontend::encode_message(
                        &FrontendMsg::SASLResponse(&client_final),
                        &mut buf,
                    );
                    conn.send_raw(&buf).await?;

                    // Wait for server-final + AuthenticationOk.
                    loop {
                        match conn.recv_msg().await? {
                            BackendMsg::AuthenticationSASLFinal { .. } => {}
                            BackendMsg::AuthenticationOk => break,
                            BackendMsg::ErrorResponse { fields } => {
                                return Err(PgWireError::Pg(fields));
                            }
                            _ => {}
                        }
                    }
                }
                BackendMsg::ParameterStatus { .. } => {} // Collect if needed
                BackendMsg::BackendKeyData { pid, secret } => {
                    conn.pid = pid;
                    conn.secret = secret;
                }
                BackendMsg::ReadyForQuery { .. } => break,
                BackendMsg::ErrorResponse { fields } => {
                    return Err(PgWireError::Pg(fields));
                }
                BackendMsg::NoticeResponse { .. } => {}
                other => {
                    tracing::debug!("Startup: ignoring {:?}", other);
                }
            }
        }

        Ok(conn)
    }

    /// Send a raw buffer to the server (one write syscall).
    pub async fn send_raw(&mut self, buf: &[u8]) -> Result<(), PgWireError> {
        self.stream.write_all(buf).await?;
        Ok(())
    }

    /// Read one complete backend message from the connection.
    /// Uses an internal buffer to minimize read() syscalls.
    pub async fn recv_msg(&mut self) -> Result<BackendMsg, PgWireError> {
        loop {
            // Try to parse a message from the buffer.
            if let Some(msg) = backend::parse_message(&mut self.recv_buf)
                .map_err(PgWireError::Protocol)?
            {
                return Ok(msg);
            }

            // Not enough data — read more from the socket.
            let n = self.stream.read_buf(&mut self.recv_buf).await?;
            if n == 0 {
                return Err(PgWireError::ConnectionClosed);
            }
        }
    }

    /// Receive messages until ReadyForQuery, collecting DataRows.
    /// Returns (rows, command_tag).
    pub async fn collect_rows(&mut self) -> Result<(Vec<Vec<Option<Vec<u8>>>>, String), PgWireError> {
        let mut rows = Vec::new();
        let mut tag = String::new();

        loop {
            let msg = self.recv_msg().await?;
            match msg {
                BackendMsg::DataRow { columns } => rows.push(columns),
                BackendMsg::CommandComplete { tag: t } => tag = t,
                BackendMsg::ReadyForQuery { .. } => return Ok((rows, tag)),
                BackendMsg::ParseComplete | BackendMsg::BindComplete | BackendMsg::NoData => {}
                BackendMsg::RowDescription { .. } => {}
                BackendMsg::ErrorResponse { fields } => {
                    // Drain until ReadyForQuery.
                    self.drain_until_ready().await?;
                    return Err(PgWireError::Pg(fields));
                }
                BackendMsg::NoticeResponse { .. } => {}
                BackendMsg::EmptyQueryResponse => {}
                _ => {}
            }
        }
    }

    /// Drain messages until ReadyForQuery (error recovery).
    pub async fn drain_until_ready(&mut self) -> Result<(), PgWireError> {
        loop {
            let msg = self.recv_msg().await?;
            if matches!(msg, BackendMsg::ReadyForQuery { .. }) {
                return Ok(());
            }
        }
    }
}
