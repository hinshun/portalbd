//! NBD protocol constants and types.
//!
//! Based on https://github.com/NetworkBlockDevice/nbd/blob/master/doc/proto.md
//!
//! Protocol constants are defined for completeness even if not all are currently used.

#![allow(dead_code)]

use std::io;

use thiserror::Error;

// Magic values
pub const NBD_MAGIC: u64 = 0x4e42444d41474943;
pub const NBD_OPTS_MAGIC: u64 = 0x49484156454F5054;
pub const NBD_REQUEST_MAGIC: u32 = 0x25609513;
pub const NBD_SIMPLE_REPLY_MAGIC: u32 = 0x67446698;
pub const NBD_OPTION_REPLY_MAGIC: u64 = 0x0003e889045565a9;

// Handshake flags
pub const NBD_FLAG_FIXED_NEWSTYLE: u16 = 1 << 0;
pub const NBD_FLAG_NO_ZEROES: u16 = 1 << 1;

// Client flags
pub const NBD_FLAG_C_FIXED_NEWSTYLE: u32 = 1 << 0;
pub const NBD_FLAG_C_NO_ZEROES: u32 = 1 << 1;

// Transmission flags
pub const NBD_FLAG_HAS_FLAGS: u16 = 1 << 0;
pub const NBD_FLAG_READ_ONLY: u16 = 1 << 1;
pub const NBD_FLAG_SEND_FLUSH: u16 = 1 << 2;
pub const NBD_FLAG_SEND_FUA: u16 = 1 << 3;
pub const NBD_FLAG_ROTATIONAL: u16 = 1 << 4;
pub const NBD_FLAG_SEND_TRIM: u16 = 1 << 5;
pub const NBD_FLAG_SEND_WRITE_ZEROES: u16 = 1 << 6;

// Option types
pub const NBD_OPT_EXPORT_NAME: u32 = 1;
pub const NBD_OPT_ABORT: u32 = 2;
pub const NBD_OPT_LIST: u32 = 3;
pub const NBD_OPT_INFO: u32 = 6;
pub const NBD_OPT_GO: u32 = 7;

// Option replies
pub const NBD_REP_ACK: u32 = 1;
pub const NBD_REP_SERVER: u32 = 2;
pub const NBD_REP_INFO: u32 = 3;
pub const NBD_REP_ERR_UNSUP: u32 = 0x80000001;
pub const NBD_REP_ERR_POLICY: u32 = 0x80000002;
pub const NBD_REP_ERR_INVALID: u32 = 0x80000003;
pub const NBD_REP_ERR_UNKNOWN: u32 = 0x80000006;

// Info types
pub const NBD_INFO_EXPORT: u16 = 0;
pub const NBD_INFO_NAME: u16 = 1;
pub const NBD_INFO_DESCRIPTION: u16 = 2;
pub const NBD_INFO_BLOCK_SIZE: u16 = 3;

/// Minimum block size (1 byte).
///
/// Most flexible; clients may align to 512 for portability.
pub const NBD_MIN_BLOCK_SIZE: u32 = 1;

/// Maximum payload size per NBD protocol specification (32 MiB).
///
/// This is the default maximum that portable clients expect servers to support.
/// It bounds memory allocation for READ/WRITE buffers.
///
/// Note: Operations without payloads (TRIM, WRITE_ZEROES, CACHE) are not
/// bounded by this limit - they can specify ranges up to the device size.
pub const NBD_MAX_PAYLOAD_SIZE: u32 = 32 * 1024 * 1024; // 32 MiB

// Commands
pub const NBD_CMD_READ: u16 = 0;
pub const NBD_CMD_WRITE: u16 = 1;
pub const NBD_CMD_DISCONNECT: u16 = 2;
pub const NBD_CMD_FLUSH: u16 = 3;
pub const NBD_CMD_TRIM: u16 = 4;
pub const NBD_CMD_WRITE_ZEROES: u16 = 6;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NbdCommand {
    Read,
    Write,
    Disconnect,
    Flush,
    Trim,
    WriteZeroes,
}

impl NbdCommand {
    pub fn from_u16(v: u16) -> Option<Self> {
        match v {
            NBD_CMD_READ => Some(Self::Read),
            NBD_CMD_WRITE => Some(Self::Write),
            NBD_CMD_DISCONNECT => Some(Self::Disconnect),
            NBD_CMD_FLUSH => Some(Self::Flush),
            NBD_CMD_TRIM => Some(Self::Trim),
            NBD_CMD_WRITE_ZEROES => Some(Self::WriteZeroes),
            _ => None,
        }
    }

    pub fn to_u16(self) -> u16 {
        match self {
            Self::Read => NBD_CMD_READ,
            Self::Write => NBD_CMD_WRITE,
            Self::Disconnect => NBD_CMD_DISCONNECT,
            Self::Flush => NBD_CMD_FLUSH,
            Self::Trim => NBD_CMD_TRIM,
            Self::WriteZeroes => NBD_CMD_WRITE_ZEROES,
        }
    }
}

pub const NBD_CMD_FLAG_FUA: u16 = 1 << 0;
pub const NBD_CMD_FLAG_NO_HOLE: u16 = 1 << 1;

// Error codes
pub const NBD_OK: u32 = 0;
pub const NBD_EPERM: u32 = 1;
pub const NBD_EIO: u32 = 5;
pub const NBD_ENOMEM: u32 = 12;
pub const NBD_EINVAL: u32 = 22;
pub const NBD_ENOSPC: u32 = 28;
pub const NBD_EOVERFLOW: u32 = 75;
pub const NBD_ESHUTDOWN: u32 = 108;

/// Maximum length for option reply data during negotiation.
/// This prevents unbounded allocation from malicious servers.
pub const OPTION_REPLY_MAX_BYTES: usize = 64 * 1024; // 64 KiB

/// NBD protocol errors.
#[derive(Debug, Error)]
pub enum NbdError {
    #[error("invalid magic: expected 0x{expected:08x}, got 0x{actual:08x}")]
    InvalidMagic { expected: u32, actual: u32 },

    #[error("protocol negotiation failed: {reason}")]
    NegotiationFailed { reason: &'static str },

    #[error("unsupported command: {command}")]
    UnsupportedCommand { command: u16 },

    #[error("request too large: {length_bytes} bytes (max: {max_bytes})")]
    RequestTooLarge { length_bytes: u32, max_bytes: u32 },

    #[error("unknown export: {name}")]
    UnknownExport { name: String },

    #[error("server error: {code}")]
    ServerError { code: u32 },

    #[error("handle mismatch: expected {expected}, got {actual}")]
    HandleMismatch { expected: u64, actual: u64 },

    #[error("transmission error: {0}")]
    Io(#[from] io::Error),
}

/// NBD request (28 bytes on wire).
#[derive(Debug, Clone, Copy)]
pub struct NbdRequest {
    pub flags: u16,
    pub command: NbdCommand,
    pub handle: u64,
    pub offset: u64,
    /// Request length, validated based on command type:
    /// - Read/Write: bounded by NBD_MAX_PAYLOAD_SIZE (data transfer)
    /// - Trim/WriteZeroes: bounded by device size (no data transfer)
    pub length: u32,
}

impl NbdRequest {
    pub const SIZE_BYTES: usize = 28;

    /// Serialize an NBD request to bytes.
    pub fn to_bytes(&self) -> [u8; Self::SIZE_BYTES] {
        let mut buf = [0u8; Self::SIZE_BYTES];
        buf[0..4].copy_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&self.flags.to_be_bytes());
        buf[6..8].copy_from_slice(&self.command.to_u16().to_be_bytes());
        buf[8..16].copy_from_slice(&self.handle.to_be_bytes());
        buf[16..24].copy_from_slice(&self.offset.to_be_bytes());
        buf[24..28].copy_from_slice(&self.length.to_be_bytes());
        buf
    }

    /// Parse and validate an NBD request.
    ///
    /// Length validation depends on command type per NBD spec:
    /// - Commands that transfer data (Read, Write) are limited to `NBD_MAX_PAYLOAD_SIZE`
    /// - Commands that specify ranges without data transfer (Trim, WriteZeroes) can
    ///   exceed the max payload size, bounded only by device size
    pub fn from_bytes(buf: &[u8; Self::SIZE_BYTES], device_size: u64) -> Result<Self, NbdError> {
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != NBD_REQUEST_MAGIC {
            return Err(NbdError::InvalidMagic {
                expected: NBD_REQUEST_MAGIC,
                actual: magic,
            });
        }

        let flags = u16::from_be_bytes([buf[4], buf[5]]);
        let cmd = u16::from_be_bytes([buf[6], buf[7]]);
        let command =
            NbdCommand::from_u16(cmd).ok_or(NbdError::UnsupportedCommand { command: cmd })?;
        let handle = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        let offset = u64::from_be_bytes(buf[16..24].try_into().unwrap());
        let length = u32::from_be_bytes(buf[24..28].try_into().unwrap());

        // Validate length based on command type per NBD spec.
        // Commands that transfer data are bounded by max payload size.
        // Commands without data transfer (trim, write_zeroes) can exceed this.
        let max_length = match command {
            // Data transfer commands: bounded by max payload (32 MiB)
            NbdCommand::Read | NbdCommand::Write => NBD_MAX_PAYLOAD_SIZE,
            // Range commands without data transfer: bounded by device size
            // Per spec: "the client MAY request an effect length larger than the
            // maximum payload size" for commands like TRIM.
            NbdCommand::Trim | NbdCommand::WriteZeroes => device_size.min(u32::MAX as u64) as u32,
            // No length validation needed
            NbdCommand::Disconnect | NbdCommand::Flush => u32::MAX,
        };

        if length > max_length {
            return Err(NbdError::RequestTooLarge {
                length_bytes: length,
                max_bytes: max_length,
            });
        }

        Ok(Self {
            flags,
            command,
            handle,
            offset,
            length,
        })
    }
}

/// NBD reply (16 bytes on wire).
#[derive(Debug, Clone, Copy)]
pub struct NbdReply {
    pub error: u32,
    pub handle: u64,
}

impl NbdReply {
    pub const SIZE_BYTES: usize = 16;

    pub fn ok(handle: u64) -> Self {
        Self {
            error: NBD_OK,
            handle,
        }
    }

    pub fn error(handle: u64, error: u32) -> Self {
        Self { error, handle }
    }

    pub fn to_bytes(&self) -> [u8; Self::SIZE_BYTES] {
        let mut buf = [0u8; Self::SIZE_BYTES];
        buf[0..4].copy_from_slice(&NBD_SIMPLE_REPLY_MAGIC.to_be_bytes());
        buf[4..8].copy_from_slice(&self.error.to_be_bytes());
        buf[8..16].copy_from_slice(&self.handle.to_be_bytes());
        buf
    }

    pub fn from_bytes(buf: &[u8; Self::SIZE_BYTES]) -> Result<Self, NbdError> {
        let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if magic != NBD_SIMPLE_REPLY_MAGIC {
            return Err(NbdError::InvalidMagic {
                expected: NBD_SIMPLE_REPLY_MAGIC,
                actual: magic,
            });
        }

        let error = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let handle = u64::from_be_bytes(buf[8..16].try_into().unwrap());
        Ok(Self { error, handle })
    }
}

const _: () = {
    assert!(NbdRequest::SIZE_BYTES == 28);
    assert!(NbdReply::SIZE_BYTES == 16);
};

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_DEVICE_SIZE: u64 = 1024 * 1024 * 1024; // 1 GiB

    #[test]
    fn request_roundtrip() {
        let req = NbdRequest {
            flags: 0,
            command: NbdCommand::Read,
            handle: 12345,
            offset: 1024,
            length: 512,
        };
        let buf = req.to_bytes();
        let parsed = NbdRequest::from_bytes(&buf, TEST_DEVICE_SIZE).unwrap();
        assert_eq!(parsed.command, NbdCommand::Read);
        assert_eq!(parsed.handle, 12345);
        assert_eq!(parsed.offset, 1024);
        assert_eq!(parsed.length, 512);
    }

    #[test]
    fn reply_roundtrip() {
        let reply = NbdReply::ok(42);
        let buf = reply.to_bytes();
        let parsed = NbdReply::from_bytes(&buf).unwrap();
        assert_eq!(parsed.error, NBD_OK);
        assert_eq!(parsed.handle, 42);
    }

    #[test]
    fn request_invalid_magic() {
        let mut buf = [0u8; 28];
        buf[0..4].copy_from_slice(&0xDEADBEEFu32.to_be_bytes());
        let result = NbdRequest::from_bytes(&buf, TEST_DEVICE_SIZE);
        assert!(matches!(result, Err(NbdError::InvalidMagic { .. })));
    }

    #[test]
    fn request_unsupported_command() {
        let mut buf = [0u8; 28];
        buf[0..4].copy_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        buf[6..8].copy_from_slice(&99u16.to_be_bytes());
        buf[24..28].copy_from_slice(&512u32.to_be_bytes());
        let result = NbdRequest::from_bytes(&buf, TEST_DEVICE_SIZE);
        assert!(matches!(
            result,
            Err(NbdError::UnsupportedCommand { command: 99 })
        ));
    }

    #[test]
    fn request_read_too_large() {
        let mut buf = [0u8; 28];
        buf[0..4].copy_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        buf[6..8].copy_from_slice(&NBD_CMD_READ.to_be_bytes());
        buf[24..28].copy_from_slice(&(NBD_MAX_PAYLOAD_SIZE + 1).to_be_bytes());
        let result = NbdRequest::from_bytes(&buf, TEST_DEVICE_SIZE);
        assert!(matches!(result, Err(NbdError::RequestTooLarge { .. })));
    }

    #[test]
    fn request_trim_allows_large_length() {
        let mut buf = [0u8; 28];
        buf[0..4].copy_from_slice(&NBD_REQUEST_MAGIC.to_be_bytes());
        buf[6..8].copy_from_slice(&NBD_CMD_TRIM.to_be_bytes());
        buf[24..28].copy_from_slice(&(NBD_MAX_PAYLOAD_SIZE + 1).to_be_bytes());
        let req = NbdRequest::from_bytes(&buf, TEST_DEVICE_SIZE).unwrap();
        assert_eq!(req.command, NbdCommand::Trim);
        assert_eq!(req.length, NBD_MAX_PAYLOAD_SIZE + 1);
    }

    #[test]
    fn all_commands_parse() {
        for (cmd, expected) in [
            (NbdCommand::Read, NBD_CMD_READ),
            (NbdCommand::Write, NBD_CMD_WRITE),
            (NbdCommand::Disconnect, NBD_CMD_DISCONNECT),
            (NbdCommand::Flush, NBD_CMD_FLUSH),
            (NbdCommand::Trim, NBD_CMD_TRIM),
            (NbdCommand::WriteZeroes, NBD_CMD_WRITE_ZEROES),
        ] {
            assert_eq!(cmd.to_u16(), expected);
            assert_eq!(NbdCommand::from_u16(expected), Some(cmd));
        }
    }
}
