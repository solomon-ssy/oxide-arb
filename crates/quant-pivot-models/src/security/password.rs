//! argon2id password hashing primitives.
//!
//! These are the single source of truth for credential hashing: the RBAC seed
//! hashes the bootstrap admin password at migration time, and the web login
//! path verifies submitted passwords against the stored PHC string — both via
//! the functions below, so the parameters can never diverge.
//!
//! # Security properties
//!
//! - argon2id with the crate's recommended default parameters.
//! - A fresh cryptographically-random salt per hash ([`SaltString::generate`]).
//! - Output is a self-describing PHC string (`$argon2id$v=19$m=...,t=...,p=...$salt$hash`)
//!   stored verbatim in `user.password_hash`.
//! - [`verify_password`] is **fail-closed**: any malformed stored hash, parse
//!   error, or mismatch yields `false`. It never panics and never short-circuits
//!   to `true`.

use argon2::{
    Argon2,
    password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
};
use quant_pivot_error::security::PasswordError;
use rand_core_06::OsRng;

/// Hash a plaintext password with argon2id and return its PHC string.
///
/// A new random salt is generated for every call, so identical passwords
/// produce distinct hashes. The returned string is safe to persist directly.
///
/// # Errors
///
/// Returns [`PasswordError::HashFailed`] if the underlying argon2id routine
/// fails (e.g. an internal parameter/memory error).
pub fn hash_password(plaintext: &str) -> Result<String, PasswordError> {
    let salt = SaltString::generate(&mut OsRng);
    let hasher = Argon2::default();
    let hash = hasher
        .hash_password(plaintext.as_bytes(), &salt)
        .map_err(|error| PasswordError::HashFailed(error.to_string()))?;
    Ok(hash.to_string())
}

/// Verify a plaintext password against a stored argon2id PHC string.
///
/// Returns `true` only when `phc` is a well-formed argon2 hash and `plaintext`
/// matches it. All failure modes (malformed hash, parse error, mismatch) return
/// `false`. This function never panics.
#[must_use]
pub fn verify_password(plaintext: &str, phc: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(phc) else {
        return false;
    };
    Argon2::default()
        .verify_password(plaintext.as_bytes(), &parsed)
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::{hash_password, verify_password};

    #[test]
    fn hash_verify_round_trips() {
        let phc = hash_password("correct horse battery staple").expect("hash");
        assert!(verify_password("correct horse battery staple", &phc));
    }

    #[test]
    fn verify_rejects_wrong_password() {
        let phc = hash_password("s3cret").expect("hash");
        assert!(!verify_password("wrong", &phc));
    }

    #[test]
    fn each_hash_uses_salt() {
        let a = hash_password("same").expect("hash");
        let b = hash_password("same").expect("hash");
        assert_ne!(
            a, b,
            "identical plaintext must yield distinct salted hashes"
        );
        assert!(verify_password("same", &a));
        assert!(verify_password("same", &b));
    }

    #[test]
    fn verify_rejects_garbage() {
        assert!(!verify_password("anything", ""));
        assert!(!verify_password("anything", "not-a-phc-string"));
        assert!(!verify_password("anything", "$argon2id$broken"));
    }
}
