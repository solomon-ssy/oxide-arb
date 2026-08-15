//! Read-only proof that the configured exchange-history provider pair agrees.

use anyhow::{Context, Result, bail};
use quant_pivot_api::exchange::history_client::{
    ExchangeHistoryAttestor, ExchangeHistoryExtractor, chunks_agree,
};
use quant_pivot_models::config::{DeployConfig, DeployConfigLoadRequest};

const PINNED_EXCHANGE_EVENT_BLOCK: u64 = 92_033_110;

pub async fn run(request: &DeployConfigLoadRequest) -> Result<()> {
    let deploy = DeployConfig::load(request).context("load exchange-history deploy config")?;
    let config = &deploy.market_data.finalized_exchange_history;
    if !config.enabled {
        bail!("finalized exchange history is disabled");
    }

    let extractor = ExchangeHistoryExtractor::connect(config)
        .context("build primary exchange-history extractor")?;
    let attestor = ExchangeHistoryAttestor::connect(config)
        .context("build independent exchange-history attestor")?;
    let archive = attestor
        .probe_archive()
        .await
        .context("attest archive depth, finalized head, block lookup, logs, and bytecode")?;
    let to_block = archive
        .finalized_head
        .number
        .checked_sub(config.model_confirmation_blocks)
        .context("finalized head is below the model confirmation policy")?;
    let probe_span = config.min_blocks_per_chunk;
    let from_block = to_block.saturating_sub(probe_span.saturating_sub(1));

    let (extracted, attested) = tokio::try_join!(
        extractor.fetch_chunk(from_block, to_block),
        attestor.fetch_chunk(from_block, to_block),
    )
    .context("read the same finalized chunk from both providers")?;
    if !chunks_agree(&extracted, &attested) {
        bail!("provider count, digest, boundary hash, or rollback proof disagrees");
    }
    if !attestor
        .verify_continuity(&extracted.continuity_proof)
        .await
        .context("attest the HyperSync continuity proof")?
    {
        bail!("HyperSync continuity proof disagrees with the independent attestor");
    }
    let first_log_block = extracted
        .logs
        .first()
        .map(|log| log.block_number)
        .context("agreed finalized probe range contains no exchange event")?;
    let (pinned_extracted, pinned_attested) = tokio::try_join!(
        extractor.fetch_chunk(PINNED_EXCHANGE_EVENT_BLOCK, PINNED_EXCHANGE_EVENT_BLOCK),
        attestor.fetch_chunk(PINNED_EXCHANGE_EVENT_BLOCK, PINNED_EXCHANGE_EVENT_BLOCK),
    )
    .context("read the pinned exchange-event block from both providers")?;
    if !chunks_agree(&pinned_extracted, &pinned_attested)
        || !attestor
            .verify_continuity(&pinned_extracted.continuity_proof)
            .await
            .context("attest pinned-block continuity")?
    {
        bail!("providers disagree on the pinned exchange-event block");
    }
    if pinned_extracted.logs.is_empty() {
        bail!("pinned exchange-event block contains no canonical log");
    }

    println!(
        "smoke exchange-history passed for blocks {from_block}..={to_block} with {} canonical logs, first log at {first_log_block}, pinned block {PINNED_EXCHANGE_EVENT_BLOCK} with {} logs, and {} bytecode attestations",
        extracted.logs.len(),
        pinned_extracted.logs.len(),
        archive.contract_code_hashes.len(),
    );
    Ok(())
}
