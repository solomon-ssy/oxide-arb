//! Keystore unit tests.

use oxide_arb_api::keystore::Keystore;
use oxide_arb_models::config::{KeySource, KeysConfig};

#[test]
fn keystore_loads_valid_hex_key() {
    let config = KeysConfig {
        source: KeySource::Env,
        private_key: Some(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
        ),
        polymarket_api_key: Some("test-key".into()),
        polymarket_api_secret: Some("test-secret".into()),
        polymarket_passphrase: Some("test-pass".into()),
        keystore_path: None,
    };

    let keystore = Keystore::from_config(&config);
    assert!(keystore.is_ok());
    let ks = keystore.unwrap();
    assert!(ks.credentials().is_some());
    // This is the known address for the hardhat test key #0
    assert_eq!(
        ks.address_string(),
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    );
}

#[test]
fn keystore_rejects_invalid_hex() {
    let config = KeysConfig {
        source: KeySource::Env,
        private_key: Some("not-valid-hex".into()),
        polymarket_api_key: None,
        polymarket_api_secret: None,
        polymarket_passphrase: None,
        keystore_path: None,
    };

    let result = Keystore::from_config(&config);
    assert!(result.is_err());
}

#[test]
fn keystore_rejects_wrong_length() {
    let config = KeysConfig {
        source: KeySource::Env,
        private_key: Some("0xdeadbeef".into()),
        polymarket_api_key: None,
        polymarket_api_secret: None,
        polymarket_passphrase: None,
        keystore_path: None,
    };

    let result = Keystore::from_config(&config);
    assert!(result.is_err());
}

#[test]
fn keystore_fails_without_private_key() {
    let config = KeysConfig::default();
    let result = Keystore::from_config(&config);
    assert!(result.is_err());
}

#[test]
fn keystore_no_credentials_when_partial() {
    let config = KeysConfig {
        source: KeySource::Env,
        private_key: Some(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
        ),
        polymarket_api_key: Some("only-key".into()),
        polymarket_api_secret: None,
        polymarket_passphrase: None,
        keystore_path: None,
    };

    let ks = Keystore::from_config(&config).unwrap();
    assert!(ks.credentials().is_none());
}
