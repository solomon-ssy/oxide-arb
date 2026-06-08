//! Bucket-risk dimension grouping and parent-bucket shrink for insufficient samples.

use oxide_arb_models::{
    domain::control_factor::{BucketRiskDimensions, FactorDimensions},
    enums::control_factor::ControlFactorType,
};

use crate::{evidence::training::TrainingExampleArtifact, factor::stats::parent_bucket_required};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketBuildGroup {
    pub dimensions: BucketRiskDimensions,
    pub sample_count: u64,
    pub parent_shrunk: bool,
}

pub fn bucket_build_groups(
    training: &TrainingExampleArtifact,
    min_samples: u64,
) -> Vec<BucketBuildGroup> {
    let mut raw_counts = Vec::<(BucketRiskDimensions, u64)>::new();
    for example in training
        .examples
        .iter()
        .filter(|example| example.factor_type == ControlFactorType::BucketRisk)
    {
        if let FactorDimensions::BucketRisk(dimensions) = &example.entity_key {
            if let Some((_, count)) = raw_counts
                .iter_mut()
                .find(|(bucket, _)| bucket == dimensions)
            {
                *count += 1;
            } else {
                raw_counts.push((dimensions.clone(), 1));
            }
        }
    }

    let mut merged = Vec::<(BucketRiskDimensions, u64, bool)>::new();
    for (dimensions, count) in raw_counts {
        let shrunk = parent_bucket_required(count, min_samples);
        let target = if shrunk {
            shrink_bucket_dimensions(&dimensions)
        } else {
            dimensions
        };
        if let Some((_, total, was_shrunk)) =
            merged.iter_mut().find(|(bucket, _, _)| bucket == &target)
        {
            *total += count;
            *was_shrunk |= shrunk;
        } else {
            merged.push((target, count, shrunk));
        }
    }

    let mut groups = merged
        .into_iter()
        .map(
            |(dimensions, sample_count, parent_shrunk)| BucketBuildGroup {
                dimensions,
                sample_count,
                parent_shrunk,
            },
        )
        .collect::<Vec<_>>();
    groups.sort_by_key(|group| group.dimensions.category);
    groups
}

#[must_use]
pub const fn shrink_bucket_dimensions(dimensions: &BucketRiskDimensions) -> BucketRiskDimensions {
    BucketRiskDimensions {
        category: dimensions.category,
        price_zone: dimensions.price_zone,
        duration_bucket: dimensions.duration_bucket,
        hours_to_settlement_bucket: None,
        neg_risk: None,
        fee_profile: None,
    }
}

#[cfg(test)]
mod tests {
    use oxide_arb_models::{
        domain::{
            control_factor::{
                BucketRiskDimensions, FactorDimensions, FeeProfileBucket, TimeToSettlementBucket,
            },
            evidence::{EvidenceSourceRefs, FactorFeatureVector, FactorTrainingExample},
        },
        enums::{
            calibration::{DurationBucket, PriceZone},
            common::MarketCategory,
            control_factor::ControlFactorType,
        },
        types::{MarketId, OpportunityId},
    };

    use super::{bucket_build_groups, shrink_bucket_dimensions};
    use crate::evidence::training::{TrainingExampleArtifact, TrainingExampleReport};

    #[test]
    fn parent_bucket_shrink_merges_insufficient_child_samples() {
        let training = TrainingExampleArtifact {
            report: TrainingExampleReport {
                dataset_hash: "d".into(),
                feature_schema_hash: "f".into(),
                label_schema_hash: "l".into(),
                entity_count: 1,
                example_count: 1,
                label_count: 0,
                factor_types: vec![ControlFactorType::BucketRisk],
                query_fingerprints: Vec::new(),
            },
            examples: vec![example_with_hours_bucket()],
        };
        let groups = bucket_build_groups(&training, 100);
        assert_eq!(groups.len(), 1);
        assert!(groups[0].parent_shrunk);
        assert!(groups[0].dimensions.hours_to_settlement_bucket.is_none());
    }

    #[test]
    fn shrink_clears_fine_grained_bucket_fields() {
        let dimensions = BucketRiskDimensions {
            category: MarketCategory::Politics,
            price_zone: PriceZone::Z95,
            duration_bucket: DurationBucket::Short,
            hours_to_settlement_bucket: Some(TimeToSettlementBucket::UnderOneHour),
            neg_risk: Some(true),
            fee_profile: Some(FeeProfileBucket::Low),
        };
        let shrunk = shrink_bucket_dimensions(&dimensions);
        assert!(shrunk.hours_to_settlement_bucket.is_none());
        assert!(shrunk.neg_risk.is_none());
        assert!(shrunk.fee_profile.is_none());
    }

    fn example_with_hours_bucket() -> FactorTrainingExample {
        FactorTrainingExample {
            opportunity_id: OpportunityId::new(oxide_arb_test_support::seeded_uuid("opp")),
            market_id: MarketId::new("market"),
            factor_type: ControlFactorType::BucketRisk,
            entity_key: FactorDimensions::BucketRisk(BucketRiskDimensions {
                category: MarketCategory::Politics,
                price_zone: PriceZone::Z95,
                duration_bucket: DurationBucket::Short,
                hours_to_settlement_bucket: Some(TimeToSettlementBucket::UnderOneHour),
                neg_risk: None,
                fee_profile: None,
            }),
            event_time: chrono::Utc::now(),
            features: FactorFeatureVector {
                schema_version: 1,
                entries: Vec::new(),
            },
            label: None,
            outcome_available_at: None,
            source_refs: EvidenceSourceRefs {
                query_refs: Vec::new(),
                artifact_refs: Vec::new(),
            },
        }
    }
}
