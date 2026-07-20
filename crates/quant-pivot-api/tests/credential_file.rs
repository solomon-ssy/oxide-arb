//! Permission-checked credential-file loader for ignored live tests.

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::{env, fs, path::PathBuf};

const MAX_TEST_CREDENTIAL_BYTES: u64 = 64 * 1024;
pub const PRIVATE_KEY_FILE_ENV: &str = "QUANT_PIVOT_TEST_PRIVATE_KEY_FILE";

pub fn required_private_key() -> Result<String, String> {
    let path = env::var_os(PRIVATE_KEY_FILE_ENV)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{PRIVATE_KEY_FILE_ENV} is required"))?;
    let metadata = fs::symlink_metadata(&path)
        .map_err(|error| format!("cannot stat credential file: {error}"))?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() > MAX_TEST_CREDENTIAL_BYTES
    {
        return Err(format!(
            "{PRIVATE_KEY_FILE_ENV} must reference a non-symlink regular file no larger than {MAX_TEST_CREDENTIAL_BYTES} bytes"
        ));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(format!(
            "{PRIVATE_KEY_FILE_ENV} must reference a file with no group or other permissions"
        ));
    }
    let value = fs::read_to_string(&path)
        .map_err(|error| format!("cannot read credential file: {error}"))?;
    let value = value.trim_end_matches(['\r', '\n']);
    if value.is_empty() {
        return Err(format!("{PRIVATE_KEY_FILE_ENV} references an empty file"));
    }
    Ok(value.to_owned())
}
