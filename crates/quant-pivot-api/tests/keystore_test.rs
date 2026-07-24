//! Keystore unit tests.

use quant_pivot_api::keystore::Keystore;
use quant_pivot_models::config::KeysConfig;

#[test]
fn keystore_loads_valid_key() {
    let config = KeysConfig {
        private_key: Some(
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80".into(),
        ),
    };

    let keystore = Keystore::from_config(&config);
    assert!(keystore.is_ok());
    let ks = keystore.unwrap();
    assert_eq!(
        ks.address_string(),
        "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
    );
}

#[test]
fn keystore_rejects_invalid_hex() {
    let config = KeysConfig {
        private_key: Some("not-valid-hex".into()),
    };

    let result = Keystore::from_config(&config);
    assert!(result.is_err());
}

#[test]
fn keystore_rejects_wrong_length() {
    let config = KeysConfig {
        private_key: Some("0xdeadbeef".into()),
    };

    let result = Keystore::from_config(&config);
    assert!(result.is_err());
}

#[test]
fn keystore_fails_without_key() {
    let config = KeysConfig::default();
    let result = Keystore::from_config(&config);
    assert!(result.is_err());
}
