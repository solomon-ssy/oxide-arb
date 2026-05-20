//! L2 HMAC credential derivation and management.

use zeroize::Zeroize;

/// L2 HMAC API credentials for authenticated CLOB access.
#[derive(Clone)]
pub struct L2Credentials {
    pub api_key: String,
    pub api_secret: String,
    pub passphrase: String,
}

impl Drop for L2Credentials {
    fn drop(&mut self) {
        self.api_key.zeroize();
        self.api_secret.zeroize();
        self.passphrase.zeroize();
    }
}

impl std::fmt::Debug for L2Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("L2Credentials")
            .field("api_key", &"[REDACTED]")
            .field("api_secret", &"[REDACTED]")
            .field("passphrase", &"[REDACTED]")
            .finish()
    }
}
