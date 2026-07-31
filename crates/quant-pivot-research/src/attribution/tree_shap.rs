use std::collections::BTreeSet;

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{hashing::CanonicalDigest, types::ContentHash};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::artifacts::PredictionContribution;

const MAX_TREE_SHAP_RESIDUAL: Decimal = Decimal::from_parts(1, 0, 0, false, 12);
const MAX_TREE_PREDICTION_RESIDUAL: Decimal = Decimal::from_parts(1, 0, 0, false, 10);
const TREE_ENSEMBLE_HASH_DOMAIN: &str = "quant-pivot/tree-ensemble-spec";
const TREE_ENSEMBLE_HASH_VERSION: u32 = 1;

fn invalid(detail: impl Into<String>) -> QuantError {
    ResearchError::InvalidModelArtifact {
        detail: detail.into(),
    }
    .into()
}

fn methodology(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}

/// Branch used when the model input is missing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingBranch {
    Left,
    Right,
}

/// Flat, content-addressable decision-tree node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum TreeNode {
    Split {
        feature_index: usize,
        threshold: Decimal,
        missing_branch: MissingBranch,
        left_child: usize,
        right_child: usize,
        cover: Decimal,
    },
    Leaf {
        value: Decimal,
        cover: Decimal,
    },
}

impl TreeNode {
    const fn cover(&self) -> Decimal {
        match self {
            Self::Split { cover, .. } | Self::Leaf { cover, .. } => *cover,
        }
    }
}

/// One exact tree. Node zero is the root and every other node has one parent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionTreeSpec {
    pub nodes: Vec<TreeNode>,
}

/// Portable GBDT representation required by the explanation quality gate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TreeEnsembleSpec {
    pub serialized_model_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub background_distribution_hash: ContentHash,
    pub feature_names: Vec<String>,
    pub base_value: Decimal,
    pub trees: Vec<DecisionTreeSpec>,
}

impl TreeEnsembleSpec {
    pub fn content_hash(&self) -> QuantResult<ContentHash> {
        self.validate_shape()?;
        CanonicalDigest::content_hash_typed(
            TREE_ENSEMBLE_HASH_DOMAIN,
            TREE_ENSEMBLE_HASH_VERSION,
            self,
        )
        .map_err(Into::into)
    }

    pub fn predict(&self, input: &TreeEnsembleInput) -> QuantResult<Decimal> {
        validate_ensemble(self, input)?;
        self.trees
            .iter()
            .try_fold(self.base_value, |prediction, tree| {
                Ok(prediction + predict_tree(tree, input, 0)?)
            })
    }

    fn validate_shape(&self) -> QuantResult<()> {
        if self.feature_names.is_empty() || self.trees.is_empty() {
            return Err(invalid(
                "TreeSHAP requires non-empty trees and exactly aligned model inputs",
            ));
        }
        let names = self
            .feature_names
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if names.len() != self.feature_names.len()
            || self.feature_names.iter().any(|name| name.trim().is_empty())
        {
            return Err(invalid(
                "TreeSHAP feature names must be non-empty and unique",
            ));
        }
        for tree in &self.trees {
            validate_tree(tree, self.feature_names.len())?;
        }
        Ok(())
    }
}

/// One transformed input in exact feature-contract order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeEnsembleInput {
    pub values: Vec<Option<Decimal>>,
}

pub struct TreeShapExplanation {
    pub baseline_output: Decimal,
    pub predicted_output: Decimal,
    pub contributions: Vec<PredictionContribution>,
    pub efficiency_residual: Decimal,
}

/// Portable ensemble plus the training-time cross-verification report bound
/// into a classical model payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TreeShapModelContract {
    pub ensemble: TreeEnsembleSpec,
    pub ensemble_hash: ContentHash,
    pub verified_case_count: u64,
    pub max_efficiency_residual: Decimal,
    pub max_prediction_residual: Decimal,
}

impl TreeShapModelContract {
    pub fn verify(
        ensemble: TreeEnsembleSpec,
        inputs: &[TreeEnsembleInput],
        reference_predictions: &[Decimal],
    ) -> QuantResult<Self> {
        if inputs.is_empty() || inputs.len() != reference_predictions.len() {
            return Err(methodology(
                "TreeSHAP verification requires aligned non-empty inputs and predictions",
            ));
        }
        let ensemble_hash = ensemble.content_hash()?;
        let mut max_efficiency_residual = Decimal::ZERO;
        let mut max_prediction_residual = Decimal::ZERO;
        for (input, reference_prediction) in inputs.iter().zip(reference_predictions) {
            let explanation = TreeShapExplainer::explain(&ensemble, input)?;
            max_efficiency_residual =
                max_efficiency_residual.max(explanation.efficiency_residual.abs());
            max_prediction_residual = max_prediction_residual
                .max((explanation.predicted_output - *reference_prediction).abs());
        }
        let contract = Self {
            ensemble,
            ensemble_hash,
            verified_case_count: u64::try_from(inputs.len()).map_err(|error| {
                methodology(format!(
                    "TreeSHAP verification case count overflow: {error}"
                ))
            })?,
            max_efficiency_residual,
            max_prediction_residual,
        };
        contract.validate()?;
        Ok(contract)
    }

    pub fn validate(&self) -> QuantResult<()> {
        if self.verified_case_count == 0
            || self.ensemble.content_hash()? != self.ensemble_hash
            || self.max_efficiency_residual.is_sign_negative()
            || self.max_efficiency_residual > MAX_TREE_SHAP_RESIDUAL
            || self.max_prediction_residual.is_sign_negative()
            || self.max_prediction_residual > MAX_TREE_PREDICTION_RESIDUAL
        {
            return Err(methodology(
                "TreeSHAP model contract is incomplete or exceeds verification tolerances",
            ));
        }
        Ok(())
    }
}

/// Exact polynomial-time `TreeSHAP` for the portable tree representation.
pub struct TreeShapExplainer;

impl TreeShapExplainer {
    pub fn explain(
        spec: &TreeEnsembleSpec,
        input: &TreeEnsembleInput,
    ) -> QuantResult<TreeShapExplanation> {
        validate_ensemble(spec, input)?;
        let mut phi = vec![Decimal::ZERO; spec.feature_names.len()];
        let mut baseline_output = spec.base_value;
        let mut predicted_output = spec.base_value;

        for tree in &spec.trees {
            baseline_output += expected_value(tree, 0)?;
            predicted_output += predict_tree(tree, input, 0)?;
            recurse_tree(
                tree,
                input,
                0,
                Vec::new(),
                PathEdge {
                    zero_fraction: Decimal::ONE,
                    one_fraction: Decimal::ONE,
                    feature_index: usize::MAX,
                },
                &mut phi,
            )?;
        }

        let mut contributions = spec
            .feature_names
            .iter()
            .cloned()
            .zip(input.values.iter().copied())
            .zip(phi)
            .map(
                |((input_name, input_value), contribution)| PredictionContribution {
                    input_name,
                    input_value,
                    contribution,
                },
            )
            .collect::<Vec<_>>();
        contributions.sort_by(|left, right| left.input_name.cmp(&right.input_name));
        let efficiency_residual = predicted_output
            - baseline_output
            - contributions
                .iter()
                .map(|term| term.contribution)
                .sum::<Decimal>();
        if efficiency_residual.abs() > MAX_TREE_SHAP_RESIDUAL {
            return Err(methodology(format!(
                "exact TreeSHAP efficiency residual {efficiency_residual} exceeds tolerance"
            )));
        }
        Ok(TreeShapExplanation {
            baseline_output,
            predicted_output,
            contributions,
            efficiency_residual,
        })
    }
}

#[derive(Clone, Copy)]
struct PathEdge {
    zero_fraction: Decimal,
    one_fraction: Decimal,
    feature_index: usize,
}

#[derive(Clone, Copy)]
struct PathElement {
    zero_fraction: Decimal,
    one_fraction: Decimal,
    feature_index: usize,
    permutation_weight: Decimal,
}

fn recurse_tree(
    tree: &DecisionTreeSpec,
    input: &TreeEnsembleInput,
    node_index: usize,
    mut path: Vec<PathElement>,
    incoming: PathEdge,
    phi: &mut [Decimal],
) -> QuantResult<()> {
    extend_path(&mut path, incoming)?;
    match tree
        .nodes
        .get(node_index)
        .ok_or_else(|| invalid(format!("tree node {node_index} does not exist")))?
    {
        TreeNode::Leaf { value, .. } => {
            for path_index in 1..path.len() {
                let element = path[path_index];
                let weight = unwound_path_sum(&path, path_index)?;
                phi[element.feature_index] +=
                    weight * (element.one_fraction - element.zero_fraction) * *value;
            }
            Ok(())
        }
        TreeNode::Split {
            feature_index,
            threshold,
            missing_branch,
            left_child,
            right_child,
            cover,
        } => {
            let goes_left = input.values[*feature_index]
                .map_or(*missing_branch == MissingBranch::Left, |value| {
                    value <= *threshold
                });
            let hot_child = if goes_left { *left_child } else { *right_child };
            let cold_child = if hot_child == *left_child {
                *right_child
            } else {
                *left_child
            };
            let hot_zero = child_fraction(tree, hot_child, *cover)?;
            let cold_zero = child_fraction(tree, cold_child, *cover)?;
            let mut incoming_zero = Decimal::ONE;
            let mut incoming_one = Decimal::ONE;
            if let Some(path_index) = path
                .iter()
                .position(|element| element.feature_index == *feature_index)
            {
                incoming_zero = path[path_index].zero_fraction;
                incoming_one = path[path_index].one_fraction;
                unwind_path(&mut path, path_index)?;
            }
            recurse_tree(
                tree,
                input,
                hot_child,
                path.clone(),
                PathEdge {
                    zero_fraction: hot_zero * incoming_zero,
                    one_fraction: incoming_one,
                    feature_index: *feature_index,
                },
                phi,
            )?;
            recurse_tree(
                tree,
                input,
                cold_child,
                path,
                PathEdge {
                    zero_fraction: cold_zero * incoming_zero,
                    one_fraction: Decimal::ZERO,
                    feature_index: *feature_index,
                },
                phi,
            )
        }
    }
}

fn extend_path(path: &mut Vec<PathElement>, incoming: PathEdge) -> QuantResult<()> {
    let depth = path.len();
    path.push(PathElement {
        zero_fraction: incoming.zero_fraction,
        one_fraction: incoming.one_fraction,
        feature_index: incoming.feature_index,
        permutation_weight: if depth == 0 {
            Decimal::ONE
        } else {
            Decimal::ZERO
        },
    });
    if depth == 0 {
        return Ok(());
    }
    let denominator = Decimal::from(
        u64::try_from(depth + 1)
            .map_err(|error| methodology(format!("TreeSHAP depth overflow: {error}")))?,
    );
    for index in (0..depth).rev() {
        let index_weight = path[index].permutation_weight;
        let selected_count = Decimal::from(
            u64::try_from(index + 1)
                .map_err(|error| methodology(format!("TreeSHAP index overflow: {error}")))?,
        );
        let unselected_count = Decimal::from(
            u64::try_from(depth - index)
                .map_err(|error| methodology(format!("TreeSHAP index overflow: {error}")))?,
        );
        path[index + 1].permutation_weight +=
            incoming.one_fraction * index_weight * selected_count / denominator;
        path[index].permutation_weight =
            incoming.zero_fraction * index_weight * unselected_count / denominator;
    }
    Ok(())
}

fn unwind_path(path: &mut Vec<PathElement>, path_index: usize) -> QuantResult<()> {
    let depth = path
        .len()
        .checked_sub(1)
        .ok_or_else(|| methodology("cannot unwind an empty TreeSHAP decision path"))?;
    if path_index > depth {
        return Err(methodology("TreeSHAP unwind index is outside the path"));
    }
    let one_fraction = path[path_index].one_fraction;
    let zero_fraction = path[path_index].zero_fraction;
    let denominator = Decimal::from(
        u64::try_from(depth + 1)
            .map_err(|error| methodology(format!("TreeSHAP depth overflow: {error}")))?,
    );
    let mut next_one_portion = path[depth].permutation_weight;
    for index in (0..depth).rev() {
        let selected_count = Decimal::from(
            u64::try_from(index + 1)
                .map_err(|error| methodology(format!("TreeSHAP index overflow: {error}")))?,
        );
        let unselected_count = Decimal::from(
            u64::try_from(depth - index)
                .map_err(|error| methodology(format!("TreeSHAP index overflow: {error}")))?,
        );
        if one_fraction.is_zero() {
            if zero_fraction.is_zero() {
                return Err(methodology(
                    "TreeSHAP path has zero one- and zero-fractions",
                ));
            }
            path[index].permutation_weight =
                path[index].permutation_weight * denominator / (zero_fraction * unselected_count);
        } else {
            let previous = path[index].permutation_weight;
            path[index].permutation_weight =
                next_one_portion * denominator / (selected_count * one_fraction);
            next_one_portion = previous
                - path[index].permutation_weight * zero_fraction * unselected_count / denominator;
        }
    }
    for index in path_index..depth {
        path[index].feature_index = path[index + 1].feature_index;
        path[index].zero_fraction = path[index + 1].zero_fraction;
        path[index].one_fraction = path[index + 1].one_fraction;
    }
    path.pop();
    Ok(())
}

fn unwound_path_sum(path: &[PathElement], path_index: usize) -> QuantResult<Decimal> {
    let depth = path
        .len()
        .checked_sub(1)
        .ok_or_else(|| methodology("cannot sum an empty TreeSHAP decision path"))?;
    let element = path
        .get(path_index)
        .ok_or_else(|| methodology("TreeSHAP sum index is outside the path"))?;
    let denominator = Decimal::from(
        u64::try_from(depth + 1)
            .map_err(|error| methodology(format!("TreeSHAP depth overflow: {error}")))?,
    );
    let mut next_one_portion = path[depth].permutation_weight;
    let mut total = Decimal::ZERO;
    for index in (0..depth).rev() {
        let selected_count = Decimal::from(
            u64::try_from(index + 1)
                .map_err(|error| methodology(format!("TreeSHAP index overflow: {error}")))?,
        );
        let unselected_count = Decimal::from(
            u64::try_from(depth - index)
                .map_err(|error| methodology(format!("TreeSHAP index overflow: {error}")))?,
        );
        if element.one_fraction.is_zero() {
            if element.zero_fraction.is_zero() {
                return Err(methodology(
                    "TreeSHAP path has zero one- and zero-fractions",
                ));
            }
            total += path[index].permutation_weight * denominator
                / (element.zero_fraction * unselected_count);
        } else {
            let portion = next_one_portion * denominator / (selected_count * element.one_fraction);
            total += portion;
            next_one_portion = path[index].permutation_weight
                - portion * element.zero_fraction * unselected_count / denominator;
        }
    }
    Ok(total)
}

fn validate_ensemble(spec: &TreeEnsembleSpec, input: &TreeEnsembleInput) -> QuantResult<()> {
    spec.validate_shape()?;
    if input.values.len() != spec.feature_names.len() {
        return Err(invalid(
            "TreeSHAP requires non-empty trees and exactly aligned model inputs",
        ));
    }
    Ok(())
}

fn validate_tree(tree: &DecisionTreeSpec, feature_count: usize) -> QuantResult<()> {
    if tree.nodes.is_empty() {
        return Err(invalid("TreeSHAP tree has no root"));
    }
    let mut state = vec![0_u8; tree.nodes.len()];
    let mut parents = vec![0_u32; tree.nodes.len()];
    validate_node(tree, 0, feature_count, &mut state, &mut parents)?;
    if state.iter().any(|value| *value != 2)
        || parents[0] != 0
        || parents.iter().skip(1).any(|count| *count != 1)
    {
        return Err(invalid(
            "TreeSHAP tree must be connected with one parent per non-root node",
        ));
    }
    Ok(())
}

fn validate_node(
    tree: &DecisionTreeSpec,
    node_index: usize,
    feature_count: usize,
    state: &mut [u8],
    parents: &mut [u32],
) -> QuantResult<()> {
    let node = tree
        .nodes
        .get(node_index)
        .ok_or_else(|| invalid(format!("TreeSHAP child node {node_index} is missing")))?;
    if state[node_index] == 1 {
        return Err(invalid("TreeSHAP tree contains a cycle"));
    }
    if state[node_index] == 2 {
        return Ok(());
    }
    if node.cover() <= Decimal::ZERO {
        return Err(invalid("TreeSHAP node cover must be positive"));
    }
    state[node_index] = 1;
    if let TreeNode::Split {
        feature_index,
        left_child,
        right_child,
        cover,
        ..
    } = node
    {
        if *feature_index >= feature_count
            || *left_child == *right_child
            || *left_child == node_index
            || *right_child == node_index
        {
            return Err(invalid("TreeSHAP split binding is invalid"));
        }
        for child in [*left_child, *right_child] {
            let parent_count = parents
                .get_mut(child)
                .ok_or_else(|| invalid(format!("TreeSHAP child node {child} is missing")))?;
            *parent_count = parent_count
                .checked_add(1)
                .ok_or_else(|| invalid("TreeSHAP parent count overflow"))?;
            validate_node(tree, child, feature_count, state, parents)?;
        }
        let child_cover = tree.nodes[*left_child].cover() + tree.nodes[*right_child].cover();
        if child_cover != *cover {
            return Err(invalid(
                "TreeSHAP parent cover must equal its two child covers",
            ));
        }
    }
    state[node_index] = 2;
    Ok(())
}

fn child_fraction(
    tree: &DecisionTreeSpec,
    child_index: usize,
    parent_cover: Decimal,
) -> QuantResult<Decimal> {
    if parent_cover <= Decimal::ZERO {
        return Err(invalid("TreeSHAP parent cover must be positive"));
    }
    Ok(tree.nodes[child_index].cover() / parent_cover)
}

fn expected_value(tree: &DecisionTreeSpec, node_index: usize) -> QuantResult<Decimal> {
    match &tree.nodes[node_index] {
        TreeNode::Leaf { value, .. } => Ok(*value),
        TreeNode::Split {
            left_child,
            right_child,
            cover,
            ..
        } => {
            let left = expected_value(tree, *left_child)?;
            let right = expected_value(tree, *right_child)?;
            Ok(
                (left * tree.nodes[*left_child].cover() + right * tree.nodes[*right_child].cover())
                    / *cover,
            )
        }
    }
}

fn predict_tree(
    tree: &DecisionTreeSpec,
    input: &TreeEnsembleInput,
    node_index: usize,
) -> QuantResult<Decimal> {
    match &tree.nodes[node_index] {
        TreeNode::Leaf { value, .. } => Ok(*value),
        TreeNode::Split {
            feature_index,
            threshold,
            missing_branch,
            left_child,
            right_child,
            ..
        } => {
            let goes_left = input.values[*feature_index]
                .map_or(*missing_branch == MissingBranch::Left, |value| {
                    value <= *threshold
                });
            let child = if goes_left { *left_child } else { *right_child };
            predict_tree(tree, input, child)
        }
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{hashing::CanonicalDigest, types::ContentHash};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        DecisionTreeSpec, MissingBranch, TreeEnsembleInput, TreeEnsembleSpec, TreeNode,
        TreeShapExplainer, TreeShapModelContract,
    };

    fn hash(seed: &str) -> ContentHash {
        CanonicalDigest::content_hash_json(&seed).expect("fixture hash")
    }

    impl TreeEnsembleSpec {
        fn single_split_fixture() -> Self {
            Self {
                serialized_model_hash: hash("model"),
                input_contract_hash: hash("input"),
                background_distribution_hash: hash("background"),
                feature_names: vec!["temperature".to_owned()],
                base_value: dec!(0.1),
                trees: vec![DecisionTreeSpec {
                    nodes: vec![
                        TreeNode::Split {
                            feature_index: 0,
                            threshold: dec!(20),
                            missing_branch: MissingBranch::Left,
                            left_child: 1,
                            right_child: 2,
                            cover: dec!(10),
                        },
                        TreeNode::Leaf {
                            value: dec!(-0.2),
                            cover: dec!(4),
                        },
                        TreeNode::Leaf {
                            value: dec!(0.3),
                            cover: dec!(6),
                        },
                    ],
                }],
            }
        }
    }

    #[test]
    fn single_split_efficiency_exact() {
        let spec = TreeEnsembleSpec::single_split_fixture();
        let explanation = TreeShapExplainer::explain(
            &spec,
            &TreeEnsembleInput {
                values: vec![Some(dec!(25))],
            },
        )
        .expect("exact TreeSHAP");
        assert_eq!(explanation.baseline_output, dec!(0.2));
        assert_eq!(explanation.predicted_output, dec!(0.4));
        assert_eq!(explanation.contributions[0].contribution, dec!(0.2));
        assert_eq!(explanation.efficiency_residual, Decimal::ZERO);
    }

    #[test]
    fn verification_binds_predictions() {
        let spec = TreeEnsembleSpec::single_split_fixture();
        let inputs = vec![
            TreeEnsembleInput {
                values: vec![Some(dec!(10))],
            },
            TreeEnsembleInput {
                values: vec![Some(dec!(25))],
            },
        ];
        let reference_predictions = inputs
            .iter()
            .map(|input| spec.predict(input).expect("portable prediction"))
            .collect::<Vec<_>>();
        let contract = TreeShapModelContract::verify(spec, &inputs, &reference_predictions)
            .expect("verified TreeSHAP contract");
        assert_eq!(contract.verified_case_count, 2);
        assert_eq!(contract.max_efficiency_residual, Decimal::ZERO);
        assert_eq!(contract.max_prediction_residual, Decimal::ZERO);

        let mut tampered = contract.clone();
        tampered.ensemble.trees[0].nodes[1] = TreeNode::Leaf {
            value: dec!(-0.25),
            cover: dec!(4),
        };
        assert!(tampered.validate().is_err());

        let wrong_predictions = vec![Decimal::ONE; inputs.len()];
        assert!(
            TreeShapModelContract::verify(contract.ensemble, &inputs, &wrong_predictions).is_err()
        );
    }

    #[test]
    fn repeated_split_preserves_efficiency() {
        let spec = TreeEnsembleSpec {
            serialized_model_hash: hash("model"),
            input_contract_hash: hash("input"),
            background_distribution_hash: hash("background"),
            feature_names: vec!["x".to_owned()],
            base_value: Decimal::ZERO,
            trees: vec![DecisionTreeSpec {
                nodes: vec![
                    TreeNode::Split {
                        feature_index: 0,
                        threshold: dec!(0),
                        missing_branch: MissingBranch::Left,
                        left_child: 1,
                        right_child: 2,
                        cover: dec!(10),
                    },
                    TreeNode::Leaf {
                        value: dec!(-1),
                        cover: dec!(4),
                    },
                    TreeNode::Split {
                        feature_index: 0,
                        threshold: dec!(1),
                        missing_branch: MissingBranch::Left,
                        left_child: 3,
                        right_child: 4,
                        cover: dec!(6),
                    },
                    TreeNode::Leaf {
                        value: dec!(0.5),
                        cover: dec!(2),
                    },
                    TreeNode::Leaf {
                        value: dec!(2),
                        cover: dec!(4),
                    },
                ],
            }],
        };
        let explanation = TreeShapExplainer::explain(
            &spec,
            &TreeEnsembleInput {
                values: vec![Some(dec!(2))],
            },
        )
        .expect("repeated-feature TreeSHAP");
        assert_eq!(explanation.predicted_output, dec!(2));
        assert!(explanation.efficiency_residual.abs() <= super::MAX_TREE_SHAP_RESIDUAL);
    }
}
