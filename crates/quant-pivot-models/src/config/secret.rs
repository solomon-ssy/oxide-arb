//! In-memory deploy secret value with non-leaking formatting semantics.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{env, fmt, fs, ops::Deref, path::Path};

use quant_pivot_error::config::ConfigError;
use serde::{Deserialize, Deserializer};
use zeroize::Zeroizing;

/// Maximum accepted systemd credential size. Credentials in this service are
/// compact tokens/keys; rejecting larger files avoids accidental binary or
/// certificate-bundle ingestion through a text credential field.
const MAX_CREDENTIAL_BYTES: u64 = 64 * 1024;

/// Reference to a credential provisioned by systemd `LoadCredential=`.
///
/// Only the credential name is deserialized from deploy TOML. Plaintext is
/// read from `$CREDENTIALS_DIRECTORY/<name>` at process bootstrap and is never
/// serialized back into configuration snapshots.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SystemdCredentialRef {
    pub name: String,
    #[serde(skip)]
    value: SecretText,
}

impl SystemdCredentialRef {
    /// Resolve the referenced credential once during process bootstrap.
    ///
    /// An empty name represents an intentionally unconfigured optional
    /// credential. A non-empty reference always fails closed when the systemd
    /// credential directory, file shape, size, or value is invalid.
    pub fn resolve(&mut self, field: &str) -> Result<(), ConfigError> {
        let name = self.name.trim();
        if name.is_empty() {
            self.value = SecretText::default();
            return Ok(());
        }
        if !self.value.is_empty() {
            return Ok(());
        }
        if !valid_credential_name(name) {
            return Err(ConfigError::InvalidValue {
                field: field.to_owned(),
                reason: "credential name must contain only ASCII letters, digits, '.', '_', or '-'"
                    .to_owned(),
            });
        }
        let directory =
            env::var_os("CREDENTIALS_DIRECTORY").ok_or_else(|| ConfigError::MissingField {
                section: "process environment".to_owned(),
                field: "CREDENTIALS_DIRECTORY".to_owned(),
            })?;
        self.resolve_from_directory(field, Path::new(&directory))
    }

    fn resolve_from_directory(&mut self, field: &str, directory: &Path) -> Result<(), ConfigError> {
        let name = self.name.trim();
        let path = directory.join(name);
        let metadata = fs::symlink_metadata(&path).map_err(|error| ConfigError::InvalidValue {
            field: field.to_owned(),
            reason: format!("cannot stat systemd credential {name}: {error}"),
        })?;
        if metadata.file_type().is_symlink()
            || !metadata.is_file()
            || metadata.len() > MAX_CREDENTIAL_BYTES
        {
            return Err(ConfigError::InvalidValue {
                field: field.to_owned(),
                reason: format!(
                    "systemd credential {name} must be a non-symlink regular file no larger than {MAX_CREDENTIAL_BYTES} bytes"
                ),
            });
        }
        #[cfg(unix)]
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(ConfigError::InvalidValue {
                field: field.to_owned(),
                reason: format!(
                    "systemd credential {name} must not grant group or other permissions"
                ),
            });
        }
        let value = fs::read_to_string(&path).map_err(|error| ConfigError::InvalidValue {
            field: field.to_owned(),
            reason: format!("cannot read systemd credential {name}: {error}"),
        })?;
        let value = value.trim_end_matches(['\r', '\n']);
        if value.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: field.to_owned(),
                reason: format!("systemd credential {name} is empty"),
            });
        }
        self.value = SecretText::from(value);
        Ok(())
    }

    /// Return a cloned optional value for adapters that own their credential.
    pub fn resolve_optional(&self, field: &str) -> Result<Option<SecretText>, ConfigError> {
        if self.name.trim().is_empty() {
            return Ok(None);
        }
        if self.value.is_empty() {
            return Err(ConfigError::InvalidValue {
                field: field.to_owned(),
                reason: "credential reference was not resolved during process bootstrap".to_owned(),
            });
        }
        Ok(Some(self.value.clone()))
    }

    /// Whether a non-empty credential reference is configured.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        !self.name.trim().is_empty()
    }

    /// Access the already-resolved value at the owning adapter boundary.
    #[must_use]
    pub const fn secret(&self) -> &SecretText {
        &self.value
    }

    /// Construct an already-resolved value for programmatic test fixtures.
    /// This is not a deserialization compatibility path: deploy files only
    /// accept `{ name = "..." }` references.
    #[must_use]
    pub fn from_resolved(value: impl Into<String>) -> Self {
        let value = value.into();
        if value.is_empty() {
            return Self::default();
        }
        Self {
            name: "programmatic-fixture".to_owned(),
            value: SecretText::from(value),
        }
    }
}

impl Deref for SystemdCredentialRef {
    type Target = SecretText;

    fn deref(&self) -> &Self::Target {
        self.secret()
    }
}

impl From<String> for SystemdCredentialRef {
    fn from(value: String) -> Self {
        Self::from_resolved(value)
    }
}

impl From<&str> for SystemdCredentialRef {
    fn from(value: &str) -> Self {
        Self::from_resolved(value)
    }
}

fn valid_credential_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Secret configuration text.
///
/// The plaintext is zeroized on drop and is deliberately neither `Display`
/// nor `Serialize`. `Debug` exposes only whether a value is configured.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct SecretText(Zeroizing<String>);

impl SecretText {
    #[must_use]
    pub fn expose_secret(&self) -> &str {
        self.0.as_str()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(if self.is_empty() {
            "<secret:unset>"
        } else {
            "<secret:redacted>"
        })
    }
}

impl<'de> Deserialize<'de> for SecretText {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::from)
    }
}

impl From<String> for SecretText {
    fn from(value: String) -> Self {
        Self(Zeroizing::new(value))
    }
}

impl From<&str> for SecretText {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::{env, fs};

    use uuid::Uuid;

    use super::{SecretText, SystemdCredentialRef, valid_credential_name};

    #[test]
    fn debug_never_exposes_plaintext() {
        let secret = SecretText::from("correct horse battery staple");
        let debug = format!("{secret:?}");
        assert!(!debug.contains("correct horse"));
        assert_eq!(debug, "<secret:redacted>");
    }

    #[test]
    fn credential_names_cannot_escape_the_systemd_directory() {
        assert!(valid_credential_name("quant-pivot.telegram-token"));
        assert!(!valid_credential_name("../token"));
        assert!(!valid_credential_name("nested/token"));
        assert!(!valid_credential_name(""));
    }

    #[test]
    fn credential_is_resolved_once_from_a_restricted_regular_file() {
        let directory = env::temp_dir().join(format!("qp-credential-{}", Uuid::now_v7()));
        fs::create_dir(&directory).expect("create credential directory");
        let path = directory.join("jwt-signing-key");
        fs::write(&path, "credential-value\n").expect("write credential");
        #[cfg(unix)]
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).expect("restrict credential");

        let mut reference = SystemdCredentialRef {
            name: "jwt-signing-key".to_owned(),
            value: SecretText::default(),
        };
        reference
            .resolve_from_directory("web.jwt.signing_key", &directory)
            .expect("resolve credential");
        assert_eq!(reference.secret().expose_secret(), "credential-value");

        fs::remove_dir_all(directory).expect("remove credential directory");
    }

    #[cfg(unix)]
    #[test]
    fn world_readable_credential_is_rejected() {
        let directory = env::temp_dir().join(format!("qp-credential-{}", Uuid::now_v7()));
        fs::create_dir(&directory).expect("create credential directory");
        let path = directory.join("unsafe-key");
        fs::write(&path, "credential-value").expect("write credential");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("set unsafe permissions");

        let mut reference = SystemdCredentialRef {
            name: "unsafe-key".to_owned(),
            value: SecretText::default(),
        };
        let error = reference
            .resolve_from_directory("test.credential", &directory)
            .expect_err("world-readable credential must fail closed");
        assert!(error.to_string().contains("group or other permissions"));

        fs::remove_dir_all(directory).expect("remove credential directory");
    }
}
