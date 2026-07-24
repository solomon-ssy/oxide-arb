//! Unit tests for cache layer components (no external deps required).

use std::time::Duration;

use quant_pivot_models::types::MarketId;
use quant_pivot_storage::cache::{CacheBackend, CacheKey, MokaBackend};

#[tokio::test]
async fn moka_set_get_roundtrip() {
    let backend = MokaBackend::new(100);
    backend
        .set("key1", b"value1", Duration::from_mins(1))
        .await
        .unwrap();
    let val = backend.get("key1").await.unwrap();
    assert_eq!(val, Some(b"value1".to_vec()));
}

#[tokio::test]
async fn moka_missing_returns_none() {
    let backend = MokaBackend::new(100);
    let val = backend.get("nonexistent").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn moka_delete_removes_entry() {
    let backend = MokaBackend::new(100);
    backend
        .set("key1", b"value1", Duration::from_mins(1))
        .await
        .unwrap();
    backend.delete("key1").await.unwrap();
    let val = backend.get("key1").await.unwrap();
    assert_eq!(val, None);
}

#[tokio::test]
async fn moka_exists_check() {
    let backend = MokaBackend::new(100);
    assert!(!backend.exists("key1").await.unwrap());
    backend
        .set("key1", b"value1", Duration::from_mins(1))
        .await
        .unwrap();
    assert!(backend.exists("key1").await.unwrap());
}

#[tokio::test]
async fn moka_mget_returns_order() {
    let backend = MokaBackend::new(100);
    backend
        .set("a", b"1", Duration::from_mins(1))
        .await
        .unwrap();
    backend
        .set("c", b"3", Duration::from_mins(1))
        .await
        .unwrap();

    let results = backend.mget(&["a", "b", "c"]).await.unwrap();
    assert_eq!(results[0], Some(b"1".to_vec()));
    assert_eq!(results[1], None);
    assert_eq!(results[2], Some(b"3".to_vec()));
}

#[tokio::test]
async fn moka_mset_bulk_write() {
    let backend = MokaBackend::new(100);
    backend
        .mset(&[("x", b"10"), ("y", b"20")], Duration::from_mins(1))
        .await
        .unwrap();
    assert_eq!(backend.get("x").await.unwrap(), Some(b"10".to_vec()));
    assert_eq!(backend.get("y").await.unwrap(), Some(b"20".to_vec()));
}

#[tokio::test]
async fn cache_key_format_ttl() {
    let key = CacheKey::MarketInfo {
        market_id: MarketId::new("0xabc"),
    };
    assert_eq!(key.as_str(), "mkt:0xabc");
    assert_eq!(key.domain(), "market");
    assert_eq!(key.ttl(), Duration::from_mins(5));
}

#[tokio::test]
async fn moka_per_entry_expiry() {
    let backend = MokaBackend::new(100);
    backend
        .set("ephemeral", b"gone", Duration::from_millis(1))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let val = backend.get("ephemeral").await.unwrap();
    assert_eq!(val, None, "Entry with 1ms TTL should have expired");
}

#[derive(bitcode::Encode, bitcode::Decode, Debug, PartialEq, Eq, Clone)]
struct CachedMarketStub {
    market_id: String,
    question: String,
    tick_size: String,
}

#[test]
fn bitcode_roundtrip_market_stub() {
    let value = CachedMarketStub {
        market_id: "0xabc123".into(),
        question: "Will it rain?".into(),
        tick_size: "0.01".into(),
    };
    let bytes = bitcode::encode(&value);
    let decoded: CachedMarketStub = bitcode::decode(&bytes).expect("decode");
    assert_eq!(decoded, value);
}

#[test]
fn bitcode_roundtrip_calibration_payload() {
    #[derive(bitcode::Encode, bitcode::Decode, Debug, PartialEq, Eq)]
    struct CalibrationCache {
        category: String,
        price_zone: String,
        duration_bucket: String,
        posterior_mean: Option<String>,
    }

    let value = CalibrationCache {
        category: "sports".into(),
        price_zone: "Z99".into(),
        duration_bucket: "short".into(),
        posterior_mean: Some("0.9823".into()),
    };
    let decoded: CalibrationCache = bitcode::decode(&bitcode::encode(&value)).expect("roundtrip");
    assert_eq!(decoded, value);
}
