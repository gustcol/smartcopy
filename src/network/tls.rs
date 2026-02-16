//! Self-signed TLS certificate generation
//!
//! Generates ephemeral or cached self-signed certificates for agent/server
//! communication. Uses rcgen for certificate generation and outputs PEM format.
//! Certificates include SANs for localhost and any custom hostnames.

use crate::error::{Result, SmartCopyError};
use std::path::{Path, PathBuf};

/// Default certificate validity period in days.
const DEFAULT_VALIDITY_DAYS: u32 = 365;

/// Default certificate cache directory name.
const CERT_DIR: &str = "certs";

/// Generated certificate and key pair in PEM format.
#[derive(Debug, Clone)]
pub struct CertKeyPair {
    /// PEM-encoded certificate
    pub cert_pem: String,
    /// PEM-encoded private key
    pub key_pem: String,
}

/// Generate a self-signed certificate with SANs for localhost and optional
/// additional hostnames.
///
/// The certificate uses ECDSA P-256 by default (fast and compact).
pub fn generate_self_signed_cert(
    additional_sans: &[String],
) -> Result<CertKeyPair> {
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new())
        .map_err(|e| SmartCopyError::ConfigError(format!("Certificate params error: {}", e)))?;

    // Add Subject Alternative Names
    let mut sans = vec![
        rcgen::SanType::DnsName("localhost".try_into().map_err(|e| {
            SmartCopyError::ConfigError(format!("Invalid SAN: {}", e))
        })?),
        rcgen::SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
        rcgen::SanType::IpAddress(std::net::IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)),
    ];

    for host in additional_sans {
        // Try to parse as IP address first
        if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            sans.push(rcgen::SanType::IpAddress(ip));
        } else {
            sans.push(rcgen::SanType::DnsName(host.clone().try_into().map_err(
                |e| SmartCopyError::ConfigError(format!("Invalid SAN '{}': {}", host, e)),
            )?));
        }
    }

    params.subject_alt_names = sans;

    // Set validity
    let now = rcgen::date_time_ymd(2024, 1, 1);
    let later = rcgen::date_time_ymd(2030, 12, 31);
    params.not_before = now;
    params.not_after = later;

    // Set distinguished name
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "SmartCopy Self-Signed");
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "SmartCopy");

    let key_pair = rcgen::KeyPair::generate().map_err(|e| {
        SmartCopyError::ConfigError(format!("Key generation error: {}", e))
    })?;

    let cert = params.self_signed(&key_pair).map_err(|e| {
        SmartCopyError::ConfigError(format!("Certificate signing error: {}", e))
    })?;

    Ok(CertKeyPair {
        cert_pem: cert.pem(),
        key_pem: key_pair.serialize_pem(),
    })
}

/// Get or generate a cached certificate.
///
/// If certificates exist in the given base directory under `certs/`,
/// they are loaded and reused. Otherwise, new certificates are generated
/// and cached.
pub fn get_or_generate_cert(
    base_dir: &Path,
    additional_sans: &[String],
) -> Result<CertKeyPair> {
    let cert_dir = base_dir.join(CERT_DIR);
    let cert_path = cert_dir.join("smartcopy.crt");
    let key_path = cert_dir.join("smartcopy.key");

    // Check if cached certificates exist
    if cert_path.exists() && key_path.exists() {
        let cert_pem = std::fs::read_to_string(&cert_path)
            .map_err(|e| SmartCopyError::io(&cert_path, e))?;
        let key_pem = std::fs::read_to_string(&key_path)
            .map_err(|e| SmartCopyError::io(&key_path, e))?;

        tracing::info!("Using cached TLS certificate from {:?}", cert_dir);

        return Ok(CertKeyPair { cert_pem, key_pem });
    }

    // Generate new certificate
    tracing::info!("Generating new self-signed TLS certificate");
    let pair = generate_self_signed_cert(additional_sans)?;

    // Cache the certificate
    std::fs::create_dir_all(&cert_dir)
        .map_err(|e| SmartCopyError::io(&cert_dir, e))?;

    std::fs::write(&cert_path, &pair.cert_pem)
        .map_err(|e| SmartCopyError::io(&cert_path, e))?;

    std::fs::write(&key_path, &pair.key_pem)
        .map_err(|e| SmartCopyError::io(&key_path, e))?;

    // Restrict key file permissions on Unix
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        let _ = std::fs::set_permissions(&key_path, perms);
    }

    Ok(pair)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_self_signed_cert() {
        let pair = generate_self_signed_cert(&[]).unwrap();
        assert!(pair.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(pair.key_pem.contains("BEGIN PRIVATE KEY"));
    }

    #[test]
    fn test_generate_with_custom_sans() {
        let sans = vec!["myhost.local".to_string(), "192.168.1.100".to_string()];
        let pair = generate_self_signed_cert(&sans).unwrap();
        assert!(pair.cert_pem.contains("BEGIN CERTIFICATE"));
    }

    #[test]
    fn test_cert_caching() {
        let tmp = tempfile::tempdir().unwrap();
        let sans = vec![];

        // First call should generate
        let pair1 = get_or_generate_cert(tmp.path(), &sans).unwrap();

        // Second call should load from cache
        let pair2 = get_or_generate_cert(tmp.path(), &sans).unwrap();

        assert_eq!(pair1.cert_pem, pair2.cert_pem);
        assert_eq!(pair1.key_pem, pair2.key_pem);
    }
}
