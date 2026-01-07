//! Type-safe wrappers for CSI driver domain types.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::Error;

/// An NBD server address in the form "host:port".
///
/// This type ensures the address is properly formatted and provides
/// accessors for the host and port components.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NbdAddress(String);

impl NbdAddress {
    /// Parse an NBD address from a string.
    ///
    /// The address must be in "host:port" format where port is a valid u16.
    pub fn parse(address: impl Into<String>) -> Result<Self, Error> {
        let address = address.into();
        let (host, port_str) = address
            .split_once(':')
            .ok_or_else(|| Error::InvalidAddress {
                address: address.clone(),
            })?;

        if host.is_empty() {
            return Err(Error::InvalidAddress { address });
        }

        // Actually parse the port to validate it's a valid u16
        let _port: u16 = port_str.parse().map_err(|_| Error::InvalidAddress {
            address: address.clone(),
        })?;

        Ok(Self(address))
    }

    /// Create an NBD address from host and port.
    pub fn from_parts(host: &str, port: u16) -> Self {
        Self(format!("{host}:{port}"))
    }

    /// Get the host portion of the address.
    pub fn host(&self) -> &str {
        self.0.split_once(':').map(|(h, _)| h).unwrap_or(&self.0)
    }

    /// Get the port portion of the address as a string.
    pub fn port_str(&self) -> &str {
        self.0.split_once(':').map(|(_, p)| p).unwrap_or("")
    }

    /// Get the full address as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NbdAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nbd_address_parse_valid() {
        let addr = NbdAddress::parse("127.0.0.1:10809").unwrap();
        assert_eq!(addr.host(), "127.0.0.1");
        assert_eq!(addr.port_str(), "10809");
        assert_eq!(addr.as_str(), "127.0.0.1:10809");
    }

    #[test]
    fn nbd_address_parse_missing_port() {
        let result = NbdAddress::parse("invalid-no-port");
        assert!(result.is_err());
    }

    #[test]
    fn nbd_address_parse_invalid_port() {
        // Port is not a valid number
        let result = NbdAddress::parse("127.0.0.1:notaport");
        assert!(result.is_err());
    }

    #[test]
    fn nbd_address_parse_port_overflow() {
        // Port exceeds u16::MAX
        let result = NbdAddress::parse("127.0.0.1:99999");
        assert!(result.is_err());
    }

    #[test]
    fn nbd_address_parse_empty_host() {
        let result = NbdAddress::parse(":10809");
        assert!(result.is_err());
    }
}
