//! NBD server implementation.
//!
//! The `NbdServer` handles a single NBD connection, performing the handshake,
//! option negotiation, and transmission phases. It is transport-agnostic and
//! works with any async stream (TCP, Unix, or in-memory).
//!
//! For accepting multiple connections, see `Daemon::listen()` which uses
//! the `Listener` trait.

use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::{debug, warn};

// Protocol types from the nbd crate
use nbd::*;

use super::handler::TransmissionHandler;
use crate::types::BLOCK_SIZE;

/// Maximum length for option data during negotiation.
/// This prevents unbounded allocation from malicious clients.
const OPTION_DATA_MAX_BYTES: usize = 64 * 1024; // 64 KiB

/// Preferred block size for NBD INFO_BLOCK_SIZE advertisement.
/// Matches our internal storage block size for efficient I/O.
const NBD_PREFERRED_BLOCK_SIZE: u32 = BLOCK_SIZE as u32;

/// NBD export configuration.
#[derive(Debug, Clone)]
pub struct NbdExport {
    pub name: String,
    pub size_bytes: u64,
    pub read_only: bool,
}

impl NbdExport {
    fn transmission_flags(&self) -> u16 {
        let mut flags = NBD_FLAG_HAS_FLAGS | NBD_FLAG_SEND_FLUSH | NBD_FLAG_SEND_TRIM;
        if self.read_only {
            flags |= NBD_FLAG_READ_ONLY;
        }
        flags
    }
}

/// NBD server for handling a single connection.
///
/// Uses a `TransmissionHandler` to process I/O operations. The handler can be
/// composed with middleware for recording, metrics, fault injection, etc.
///
/// This server is transport-agnostic and handles one connection at a time.
/// For accepting multiple connections, use `Daemon::listen()` with a `Listener`.
#[derive(Clone)]
pub struct NbdServer {
    handler: Arc<dyn TransmissionHandler>,
}

impl NbdServer {
    /// Create a new NBD server with the given handler.
    pub fn new(handler: Arc<dyn TransmissionHandler>) -> Self {
        Self { handler }
    }

    /// Serve a single NBD connection over any async stream.
    ///
    /// Performs the handshake, option negotiation, and enters transmission mode.
    /// Returns when the client disconnects or an error occurs.
    pub async fn serve<S>(&self, mut stream: S, export: &NbdExport) -> Result<(), NbdError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let no_zeroes = handshake(&mut stream).await?;
        if !negotiate_options(&mut stream, export, no_zeroes).await? {
            return Ok(());
        }
        transmission(&mut stream, &self.handler, export).await
    }
}

async fn handshake<S>(stream: &mut S) -> Result<bool, NbdError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut hello = [0u8; 18];
    hello[0..8].copy_from_slice(&NBD_MAGIC.to_be_bytes());
    hello[8..16].copy_from_slice(&NBD_OPTS_MAGIC.to_be_bytes());
    let flags = NBD_FLAG_FIXED_NEWSTYLE | NBD_FLAG_NO_ZEROES;
    hello[16..18].copy_from_slice(&flags.to_be_bytes());
    stream.write_all(&hello).await?;

    let mut buf = [0u8; 4];
    stream.read_exact(&mut buf).await?;
    let client_flags = u32::from_be_bytes(buf);

    if (client_flags & NBD_FLAG_C_FIXED_NEWSTYLE) == 0 {
        return Err(NbdError::NegotiationFailed {
            reason: "client must use fixed newstyle",
        });
    }

    Ok((client_flags & NBD_FLAG_C_NO_ZEROES) != 0)
}

async fn negotiate_options<S>(
    stream: &mut S,
    export: &NbdExport,
    no_zeroes: bool,
) -> Result<bool, NbdError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let mut header = [0u8; 16];
        stream.read_exact(&mut header).await?;

        let magic = u64::from_be_bytes(header[0..8].try_into().unwrap());
        if magic != NBD_OPTS_MAGIC {
            return Err(NbdError::InvalidMagic {
                expected: NBD_OPTS_MAGIC as u32,
                actual: magic as u32,
            });
        }

        let option = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let length = u32::from_be_bytes(header[12..16].try_into().unwrap()) as usize;

        // Bound allocation to prevent DoS from malicious clients
        if length > OPTION_DATA_MAX_BYTES {
            return Err(NbdError::RequestTooLarge {
                length_bytes: length as u32,
                max_bytes: OPTION_DATA_MAX_BYTES as u32,
            });
        }

        let mut data = vec![0u8; length];
        stream.read_exact(&mut data).await?;

        debug!(option, length, "NBD option");

        match option {
            NBD_OPT_EXPORT_NAME => {
                if String::from_utf8_lossy(&data) != export.name {
                    return Ok(false);
                }
                let mut resp = [0u8; 10];
                resp[0..8].copy_from_slice(&export.size_bytes.to_be_bytes());
                resp[8..10].copy_from_slice(&export.transmission_flags().to_be_bytes());
                stream.write_all(&resp).await?;
                if !no_zeroes {
                    stream.write_all(&[0u8; 124]).await?;
                }
                return Ok(true);
            }
            NBD_OPT_GO | NBD_OPT_INFO => {
                if data.len() < 4 {
                    send_reply(stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                }
                let name_len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
                if data.len() < 4 + name_len {
                    send_reply(stream, option, NBD_REP_ERR_INVALID, &[]).await?;
                    continue;
                }
                if String::from_utf8_lossy(&data[4..4 + name_len]) != export.name {
                    send_reply(stream, option, NBD_REP_ERR_UNKNOWN, &[]).await?;
                    continue;
                }

                // INFO_EXPORT
                let mut info = [0u8; 12];
                info[0..2].copy_from_slice(&NBD_INFO_EXPORT.to_be_bytes());
                info[2..10].copy_from_slice(&export.size_bytes.to_be_bytes());
                info[10..12].copy_from_slice(&export.transmission_flags().to_be_bytes());
                send_reply(stream, option, NBD_REP_INFO, &info).await?;

                // INFO_BLOCK_SIZE: min, preferred, max payload
                let mut block = [0u8; 14];
                block[0..2].copy_from_slice(&NBD_INFO_BLOCK_SIZE.to_be_bytes());
                block[2..6].copy_from_slice(&NBD_MIN_BLOCK_SIZE.to_be_bytes());
                block[6..10].copy_from_slice(&NBD_PREFERRED_BLOCK_SIZE.to_be_bytes());
                block[10..14].copy_from_slice(&NBD_MAX_PAYLOAD_SIZE.to_be_bytes());
                send_reply(stream, option, NBD_REP_INFO, &block).await?;

                send_reply(stream, option, NBD_REP_ACK, &[]).await?;
                if option == NBD_OPT_GO {
                    return Ok(true);
                }
            }
            NBD_OPT_ABORT => {
                send_reply(stream, option, NBD_REP_ACK, &[]).await?;
                return Ok(false);
            }
            NBD_OPT_LIST => {
                let name = export.name.as_bytes();
                let mut list = Vec::with_capacity(4 + name.len());
                list.extend_from_slice(&(name.len() as u32).to_be_bytes());
                list.extend_from_slice(name);
                send_reply(stream, option, NBD_REP_SERVER, &list).await?;
                send_reply(stream, option, NBD_REP_ACK, &[]).await?;
            }
            _ => {
                send_reply(stream, option, NBD_REP_ERR_UNSUP, &[]).await?;
            }
        }
    }
}

async fn send_reply<S>(
    stream: &mut S,
    option: u32,
    reply_type: u32,
    data: &[u8],
) -> Result<(), NbdError>
where
    S: AsyncWrite + Unpin,
{
    let mut header = [0u8; 20];
    header[0..8].copy_from_slice(&NBD_OPTION_REPLY_MAGIC.to_be_bytes());
    header[8..12].copy_from_slice(&option.to_be_bytes());
    header[12..16].copy_from_slice(&reply_type.to_be_bytes());
    header[16..20].copy_from_slice(&(data.len() as u32).to_be_bytes());
    stream.write_all(&header).await?;
    if !data.is_empty() {
        stream.write_all(data).await?;
    }
    Ok(())
}

async fn transmission<S>(
    stream: &mut S,
    handler: &Arc<dyn TransmissionHandler>,
    export: &NbdExport,
) -> Result<(), NbdError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut req_buf = [0u8; NbdRequest::SIZE_BYTES];

    loop {
        match stream.read_exact(&mut req_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e.into()),
        }

        let req = match NbdRequest::from_bytes(&req_buf, export.size_bytes) {
            Ok(req) => req,
            Err(NbdError::RequestTooLarge { .. }) => {
                // Parse just the handle to send error response
                let handle = u64::from_be_bytes(req_buf[8..16].try_into().unwrap());
                stream
                    .write_all(&NbdReply::error(handle, NBD_EOVERFLOW).to_bytes())
                    .await?;
                continue;
            }
            Err(e) => return Err(e),
        };

        match req.command {
            NbdCommand::Read => match handler.read(req.offset, req.length as usize).await {
                Ok(data) => {
                    stream
                        .write_all(&NbdReply::ok(req.handle).to_bytes())
                        .await?;
                    stream.write_all(&data).await?;
                }
                Err(e) => {
                    warn!(error = %e, "read error");
                    stream
                        .write_all(&NbdReply::error(req.handle, NBD_EIO).to_bytes())
                        .await?;
                }
            },
            NbdCommand::Write => {
                let len = req.length as usize;
                let mut data = BytesMut::with_capacity(len);
                data.resize(len, 0);
                stream.read_exact(&mut data).await?;

                if export.read_only {
                    stream
                        .write_all(&NbdReply::error(req.handle, NBD_EPERM).to_bytes())
                        .await?;
                    continue;
                }

                let err = match handler.write(req.offset, data.freeze()).await {
                    Ok(()) => NBD_OK,
                    Err(e) => {
                        warn!(error = %e, "write error");
                        NBD_EIO
                    }
                };
                stream
                    .write_all(
                        &NbdReply {
                            error: err,
                            handle: req.handle,
                        }
                        .to_bytes(),
                    )
                    .await?;
            }
            NbdCommand::Disconnect => return Ok(()),
            NbdCommand::Flush => {
                let err = match handler.flush().await {
                    Ok(()) => NBD_OK,
                    Err(e) => {
                        warn!(error = %e, "flush error");
                        NBD_EIO
                    }
                };
                stream
                    .write_all(
                        &NbdReply {
                            error: err,
                            handle: req.handle,
                        }
                        .to_bytes(),
                    )
                    .await?;
            }
            NbdCommand::Trim => {
                let err = match handler.trim(req.offset, req.length as u64).await {
                    Ok(()) => NBD_OK,
                    Err(e) => {
                        warn!(error = %e, "trim error");
                        NBD_EINVAL
                    }
                };
                stream
                    .write_all(
                        &NbdReply {
                            error: err,
                            handle: req.handle,
                        }
                        .to_bytes(),
                    )
                    .await?;
            }
            NbdCommand::WriteZeroes => {
                if export.read_only {
                    stream
                        .write_all(&NbdReply::error(req.handle, NBD_EPERM).to_bytes())
                        .await?;
                    continue;
                }

                let err = match handler.write_zeroes(req.offset, req.length as u64).await {
                    Ok(()) => NBD_OK,
                    Err(e) => {
                        warn!(error = %e, "write_zeroes error");
                        NBD_EIO
                    }
                };
                stream
                    .write_all(
                        &NbdReply {
                            error: err,
                            handle: req.handle,
                        }
                        .to_bytes(),
                    )
                    .await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nbd::BlockStoreHandler;
    use crate::store::BlockStore;
    use crate::types::BLOCK_SIZE;
    use bytes::Bytes;
    use object_store::memory::InMemory;
    use tokio::sync::RwLock;

    async fn make_handler() -> Arc<BlockStoreHandler> {
        let object_store = Arc::new(InMemory::new());
        let store = BlockStore::open("test", object_store, BLOCK_SIZE as u64 * 10)
            .await
            .unwrap();
        Arc::new(BlockStoreHandler::new(Arc::new(RwLock::new(store))))
    }

    #[tokio::test]
    async fn read_write_through_handler() {
        let handler = make_handler().await;

        let pattern = Bytes::from(vec![0xAB; BLOCK_SIZE]);
        handler.write(0, pattern.clone()).await.unwrap();

        let data = handler.read(0, BLOCK_SIZE).await.unwrap();
        assert_eq!(data, pattern);
    }

    #[tokio::test]
    async fn partial_block_write() {
        let handler = make_handler().await;

        handler
            .write(50, Bytes::from(vec![0xAB; 100]))
            .await
            .unwrap();

        let data = handler.read(0, 200).await.unwrap();
        assert_eq!(&data[..50], &[0x00; 50]);
        assert_eq!(&data[50..150], &[0xAB; 100]);
        assert_eq!(&data[150..200], &[0x00; 50]);
    }
}
