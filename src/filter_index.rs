use std::collections::HashMap;

use dicom_object::InMemDicomObject;
use regex::Regex;

use crate::filter::get_field_string;
use crate::recipe::{
    Condition, CoordinateRegion, FilterLabel, FilterType, LogicalOp, Predicate, Recipe,
};

// ---------------------------------------------------------------------------
// Compiled types
// ---------------------------------------------------------------------------

/// A pre-compiled version of a regex or substring pattern.
enum CompiledPattern {
    Regex(Regex),
    Substring(String),
}

/// A condition with a pre-compiled pattern for Contains/NotContains predicates.
struct CompiledCondition {
    operator: LogicalOp,
    predicate: Predicate,
    /// Pre-compiled pattern for Contains/NotContains; `None` for other predicates.
    compiled_pattern: Option<CompiledPattern>,
}

/// A filter label with pre-compiled conditions and dispatch metadata.
struct CompiledLabel {
    name: String,
    conditions: Vec<CompiledCondition>,
    /// Indices of conditions that are satisfied by the dispatch tree lookup,
    /// so they can be skipped during evaluation.
    skip_indices: Vec<usize>,
    coordinates: Vec<CoordinateRegion>,
}

// ---------------------------------------------------------------------------
// Dispatch tree
// ---------------------------------------------------------------------------

/// Labels bucketed by manufacturer within a single modality bucket.
struct ManufacturerBucket {
    by_manufacturer: HashMap<String, Vec<CompiledLabel>>,
    any_manufacturer: Vec<CompiledLabel>,
}

/// Two-level dispatch tree: Modality → Manufacturer → labels.
struct ModalityDispatchTree {
    modality_buckets: HashMap<String, ManufacturerBucket>,
    any_modality: ManufacturerBucket,
}

// ---------------------------------------------------------------------------
// FilterIndex
// ---------------------------------------------------------------------------

/// Pre-compiled, indexed filter evaluation engine.
///
/// Builds once from a `Recipe`, then evaluates per-file with:
/// - Pre-compiled regexes (no per-file compilation)
/// - Short-circuit condition evaluation
/// - Modality/Manufacturer dispatch to avoid evaluating irrelevant labels
pub struct FilterIndex {
    blacklist_labels: Vec<CompiledLabel>,
    whitelist_labels: Vec<CompiledLabel>,
    graylist_tree: ModalityDispatchTree,
}

impl FilterIndex {
    /// Build a `FilterIndex` from a parsed recipe.
    pub fn new(recipe: &Recipe) -> Self {
        let mut blacklist_labels = Vec::new();
        let mut whitelist_labels = Vec::new();
        let mut graylist_labels = Vec::new();

        for section in &recipe.filters {
            for label in &section.labels {
                match section.filter_type {
                    FilterType::Blacklist => {
                        blacklist_labels.push(compile_label(label, &[]));
                    }
                    FilterType::Whitelist => {
                        whitelist_labels.push(compile_label(label, &[]));
                    }
                    FilterType::Graylist => {
                        graylist_labels.push(label);
                    }
                }
            }
        }

        let graylist_tree = build_dispatch_tree(graylist_labels);

        FilterIndex {
            blacklist_labels,
            whitelist_labels,
            graylist_tree,
        }
    }

    /// Return the name of the first matching blacklist label, or `None`.
    ///
    /// Also rejects files that don't match any whitelist label (if whitelist
    /// sections exist).
    pub fn blacklist_reason(&self, obj: &InMemDicomObject) -> Option<&str> {
        // Check explicit blacklist
        for label in &self.blacklist_labels {
            if evaluate_compiled_conditions(&label.conditions, &label.skip_indices, obj) {
                return Some(&label.name);
            }
        }

        // Check whitelist: file must match at least one whitelist label
        if !self.whitelist_labels.is_empty() {
            let matches_any = self.whitelist_labels.iter().any(|label| {
                evaluate_compiled_conditions(&label.conditions, &label.skip_indices, obj)
            });
            if !matches_any {
                return Some("whitelist_rejected");
            }
        }

        None
    }

    /// Collect all coordinate regions from matching graylist labels.
    pub fn get_graylist_regions(&self, obj: &InMemDicomObject) -> Vec<CoordinateRegion> {
        let candidates = dispatch_candidates(&self.graylist_tree, obj);
        let mut regions = Vec::new();
        for label in candidates {
            if evaluate_compiled_conditions(&label.conditions, &label.skip_indices, obj) {
                regions.extend(label.coordinates.iter().cloned());
            }
        }
        regions
    }
}

// ---------------------------------------------------------------------------
// Compilation helpers
// ---------------------------------------------------------------------------

fn compile_pattern(value: &str) -> CompiledPattern {
    let pattern = format!("(?i){}", value);
    match Regex::new(&pattern) {
        Ok(re) => CompiledPattern::Regex(re),
        Err(_) => CompiledPattern::Substring(value.to_lowercase()),
    }
}

fn compile_condition(condition: &Condition) -> CompiledCondition {
    let compiled_pattern = match &condition.predicate {
        Predicate::Contains { value, .. } | Predicate::NotContains { value, .. } => {
            Some(compile_pattern(value))
        }
        _ => None,
    };
    CompiledCondition {
        operator: condition.operator,
        predicate: condition.predicate.clone(),
        compiled_pattern,
    }
}

fn compile_label(label: &FilterLabel, skip_indices: &[usize]) -> CompiledLabel {
    CompiledLabel {
        name: label.name.clone(),
        conditions: label.conditions.iter().map(compile_condition).collect(),
        skip_indices: skip_indices.to_vec(),
        coordinates: label.coordinates.clone(),
    }
}

// ---------------------------------------------------------------------------
// Dispatch tree construction
// ---------------------------------------------------------------------------

/// Dispatch key extraction result for a single label.
struct DispatchKeys {
    /// Lowercased modality values (from contains/equals Modality).
    modality_keys: Vec<String>,
    /// Index of the modality condition within the label's conditions.
    modality_condition_index: Option<usize>,
    /// Lowercased manufacturer values (from contains/equals Manufacturer).
    manufacturer_keys: Vec<String>,
    /// Index of the manufacturer condition within the label's conditions.
    manufacturer_condition_index: Option<usize>,
}

fn extract_dispatch_keys(label: &FilterLabel) -> DispatchKeys {
    let mut modality_keys = Vec::new();
    let mut modality_condition_index = None;
    let mut manufacturer_keys = Vec::new();
    let mut manufacturer_condition_index = None;

    for (i, condition) in label.conditions.iter().enumerate() {
        // Only extract from AND-chained conditions (First or And).
        // OR conditions would require placing the label in the union of buckets
        // but also in the catch-all, making dispatch useless.
        if condition.operator == LogicalOp::Or {
            continue;
        }

        match &condition.predicate {
            Predicate::Contains { field, value } | Predicate::Equals { field, value } => {
                let field_lower = field.to_lowercase();
                if field_lower == "modality" && modality_condition_index.is_none() {
                    modality_condition_index = Some(i);
                    // Split on pipe for alternation (e.g. "CT|MR")
                    for part in value.split('|') {
                        let trimmed = part.trim().to_lowercase();
                        if !trimmed.is_empty() {
                            modality_keys.push(trimmed);
                        }
                    }
                } else if field_lower == "manufacturer" && manufacturer_condition_index.is_none() {
                    manufacturer_condition_index = Some(i);
                    for part in value.split('|') {
                        let trimmed = part.trim().to_lowercase();
                        if !trimmed.is_empty() {
                            manufacturer_keys.push(trimmed);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    DispatchKeys {
        modality_keys,
        modality_condition_index,
        manufacturer_keys,
        manufacturer_condition_index,
    }
}

/// Intermediate tuple: (manufacturer_keys, manufacturer_condition_index, compiled_label).
type LabelEntry = (Vec<String>, Option<usize>, CompiledLabel);

fn build_dispatch_tree(labels: Vec<&FilterLabel>) -> ModalityDispatchTree {
    let mut modality_buckets: HashMap<String, Vec<LabelEntry>> = HashMap::new();
    let mut any_modality_labels: Vec<LabelEntry> = Vec::new();

    for label in labels {
        let keys = extract_dispatch_keys(label);

        // Build skip_indices from the dispatch keys
        let mut skip_indices = Vec::new();
        if let Some(idx) = keys.modality_condition_index {
            skip_indices.push(idx);
        }
        if let Some(idx) = keys.manufacturer_condition_index {
            skip_indices.push(idx);
        }

        let compiled = compile_label(label, &skip_indices);

        if keys.modality_keys.is_empty() {
            // No modality condition → goes into any_modality
            any_modality_labels.push((
                keys.manufacturer_keys,
                keys.manufacturer_condition_index,
                compiled,
            ));
        } else {
            // Place into each modality bucket
            for mod_key in &keys.modality_keys {
                modality_buckets.entry(mod_key.clone()).or_default().push((
                    keys.manufacturer_keys.clone(),
                    keys.manufacturer_condition_index,
                    compile_label(label, &skip_indices),
                ));
            }
        }
    }

    // Now build ManufacturerBuckets for each modality
    let modality_buckets = modality_buckets
        .into_iter()
        .map(|(mod_key, entries)| {
            let bucket = build_manufacturer_bucket(entries);
            (mod_key, bucket)
        })
        .collect();

    let any_modality = build_manufacturer_bucket(any_modality_labels);

    ModalityDispatchTree {
        modality_buckets,
        any_modality,
    }
}

fn build_manufacturer_bucket(entries: Vec<LabelEntry>) -> ManufacturerBucket {
    let mut by_manufacturer: HashMap<String, Vec<CompiledLabel>> = HashMap::new();
    let mut any_manufacturer = Vec::new();

    for (mfr_keys, _mfr_idx, compiled) in entries {
        if mfr_keys.is_empty() {
            any_manufacturer.push(compiled);
        } else {
            for mfr_key in &mfr_keys {
                by_manufacturer
                    .entry(mfr_key.clone())
                    .or_default()
                    .push(compile_label_from_compiled(&compiled));
            }
            // We consumed the original compiled label into the first bucket above;
            // the compile_label_from_compiled creates clones for subsequent buckets.
            // But we don't need the original anymore since it went into by_manufacturer.
        }
    }

    ManufacturerBucket {
        by_manufacturer,
        any_manufacturer,
    }
}

/// Re-compile a CompiledLabel (used when placing a label into multiple dispatch buckets).
fn compile_label_from_compiled(label: &CompiledLabel) -> CompiledLabel {
    CompiledLabel {
        name: label.name.clone(),
        conditions: label
            .conditions
            .iter()
            .map(|c| {
                let compiled_pattern = match &c.predicate {
                    Predicate::Contains { value, .. } | Predicate::NotContains { value, .. } => {
                        Some(compile_pattern(value))
                    }
                    _ => None,
                };
                CompiledCondition {
                    operator: c.operator,
                    predicate: c.predicate.clone(),
                    compiled_pattern,
                }
            })
            .collect(),
        skip_indices: label.skip_indices.clone(),
        coordinates: label.coordinates.clone(),
    }
}

// ---------------------------------------------------------------------------
// Dispatch at evaluation time
// ---------------------------------------------------------------------------

/// Collect candidate labels from the dispatch tree for a given DICOM object.
fn dispatch_candidates<'a>(
    tree: &'a ModalityDispatchTree,
    obj: &InMemDicomObject,
) -> Vec<&'a CompiledLabel> {
    let mut candidates: Vec<&CompiledLabel> = Vec::new();

    let modality_val = get_field_string(obj, "Modality")
        .unwrap_or_default()
        .to_lowercase();
    let manufacturer_val = get_field_string(obj, "Manufacturer")
        .unwrap_or_default()
        .to_lowercase();

    // Collect matching modality buckets
    let mut matched_buckets: Vec<&ManufacturerBucket> = Vec::new();
    for (key, bucket) in &tree.modality_buckets {
        if modality_val.contains(key.as_str()) {
            matched_buckets.push(bucket);
        }
    }
    // Always include any_modality
    matched_buckets.push(&tree.any_modality);

    // Within each matched modality bucket, collect manufacturer matches
    for bucket in matched_buckets {
        for (key, labels) in &bucket.by_manufacturer {
            if manufacturer_val.contains(key.as_str()) {
                candidates.extend(labels.iter());
            }
        }
        candidates.extend(bucket.any_manufacturer.iter());
    }

    candidates
}

// ---------------------------------------------------------------------------
// Compiled condition evaluation
// ---------------------------------------------------------------------------

fn evaluate_predicate_compiled(condition: &CompiledCondition, obj: &InMemDicomObject) -> bool {
    match &condition.predicate {
        Predicate::Contains { field, value } => {
            let Some(field_val) = get_field_string(obj, field) else {
                return false;
            };
            match &condition.compiled_pattern {
                Some(CompiledPattern::Regex(re)) => re.is_match(&field_val),
                Some(CompiledPattern::Substring(lower)) => {
                    field_val.to_lowercase().contains(lower.as_str())
                }
                None => field_val.to_lowercase().contains(&value.to_lowercase()),
            }
        }
        Predicate::NotContains { field, value } => {
            let Some(field_val) = get_field_string(obj, field) else {
                return true;
            };
            match &condition.compiled_pattern {
                Some(CompiledPattern::Regex(re)) => !re.is_match(&field_val),
                Some(CompiledPattern::Substring(lower)) => {
                    !field_val.to_lowercase().contains(lower.as_str())
                }
                None => !field_val.to_lowercase().contains(&value.to_lowercase()),
            }
        }
        // For non-regex predicates, delegate to the original evaluator logic inline
        Predicate::Equals { field, value } => {
            let Some(field_val) = get_field_string(obj, field) else {
                return false;
            };
            field_val.to_lowercase() == value.to_lowercase()
        }
        Predicate::NotEquals { field, value } => {
            let Some(field_val) = get_field_string(obj, field) else {
                return true;
            };
            field_val.to_lowercase() != value.to_lowercase()
        }
        Predicate::StartsWith { field, value } => {
            let Some(field_val) = get_field_string(obj, field) else {
                return false;
            };
            field_val.to_lowercase().starts_with(&value.to_lowercase())
        }
        Predicate::NotStartsWith { field, value } => {
            let Some(field_val) = get_field_string(obj, field) else {
                return true;
            };
            !field_val.to_lowercase().starts_with(&value.to_lowercase())
        }
        Predicate::Missing { field } => obj.element_by_name(field).is_err(),
        Predicate::Empty { field } => match obj.element_by_name(field) {
            Ok(elem) => match elem.value() {
                dicom_core::value::Value::Primitive(prim) => match prim {
                    dicom_core::value::PrimitiveValue::Empty => true,
                    _ => elem.value().to_str().map(|s| s.is_empty()).unwrap_or(true),
                },
                _ => false,
            },
            Err(_) => false,
        },
        Predicate::Present { field } => obj.element_by_name(field).is_ok(),
    }
}

fn evaluate_compiled_conditions(
    conditions: &[CompiledCondition],
    skip_indices: &[usize],
    obj: &InMemDicomObject,
) -> bool {
    let mut result = false;
    for (i, condition) in conditions.iter().enumerate() {
        // Short-circuit
        match condition.operator {
            LogicalOp::And if !result => continue,
            LogicalOp::Or if result => continue,
            _ => {}
        }

        // Skip conditions satisfied by dispatch
        if skip_indices.contains(&i) {
            // The dispatch tree already verified this condition matches.
            // Treat as true for the logical combination.
            result = match condition.operator {
                LogicalOp::First | LogicalOp::Or => true,
                LogicalOp::And => result,
            };
            continue;
        }

        let pred_result = evaluate_predicate_compiled(condition, obj);
        result = match condition.operator {
            LogicalOp::First => pred_result,
            LogicalOp::And => result && pred_result,
            LogicalOp::Or => result || pred_result,
        };
    }
    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipe::*;
    use crate::test_helpers::*;
    use dicom_core::VR;
    use dicom_dictionary_std::tags;

    // -----------------------------------------------------------------------
    // Equivalence: FilterIndex vs filter:: functions
    // -----------------------------------------------------------------------

    fn make_recipe_with_graylist_and_blacklist() -> Recipe {
        Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![
                FilterSection {
                    filter_type: FilterType::Blacklist,
                    labels: vec![
                        FilterLabel {
                            name: "Reject SR".into(),
                            conditions: vec![Condition {
                                operator: LogicalOp::First,
                                predicate: Predicate::Equals {
                                    field: "Modality".into(),
                                    value: "SR".into(),
                                },
                            }],
                            coordinates: vec![],
                        },
                        FilterLabel {
                            name: "Reject Missing Modality".into(),
                            conditions: vec![Condition {
                                operator: LogicalOp::First,
                                predicate: Predicate::Missing {
                                    field: "Modality".into(),
                                },
                            }],
                            coordinates: vec![],
                        },
                    ],
                },
                FilterSection {
                    filter_type: FilterType::Graylist,
                    labels: vec![
                        FilterLabel {
                            name: "GE CT".into(),
                            conditions: vec![
                                Condition {
                                    operator: LogicalOp::First,
                                    predicate: Predicate::Contains {
                                        field: "Modality".into(),
                                        value: "CT".into(),
                                    },
                                },
                                Condition {
                                    operator: LogicalOp::And,
                                    predicate: Predicate::Contains {
                                        field: "Manufacturer".into(),
                                        value: "GE".into(),
                                    },
                                },
                            ],
                            coordinates: vec![CoordinateRegion {
                                xmin: 0,
                                ymin: 0,
                                xmax: 512,
                                ymax: 100,
                                keep: false,
                            }],
                        },
                        FilterLabel {
                            name: "Siemens CT".into(),
                            conditions: vec![
                                Condition {
                                    operator: LogicalOp::First,
                                    predicate: Predicate::Contains {
                                        field: "Modality".into(),
                                        value: "CT".into(),
                                    },
                                },
                                Condition {
                                    operator: LogicalOp::And,
                                    predicate: Predicate::Contains {
                                        field: "Manufacturer".into(),
                                        value: "SIEMENS".into(),
                                    },
                                },
                            ],
                            coordinates: vec![CoordinateRegion {
                                xmin: 0,
                                ymin: 0,
                                xmax: 512,
                                ymax: 50,
                                keep: false,
                            }],
                        },
                        FilterLabel {
                            name: "Any US".into(),
                            conditions: vec![Condition {
                                operator: LogicalOp::First,
                                predicate: Predicate::Equals {
                                    field: "Modality".into(),
                                    value: "US".into(),
                                },
                            }],
                            coordinates: vec![CoordinateRegion {
                                xmin: 0,
                                ymin: 0,
                                xmax: 640,
                                ymax: 60,
                                keep: false,
                            }],
                        },
                    ],
                },
            ],
        }
    }

    #[test]
    fn equivalence_blacklist_matching() {
        let recipe = make_recipe_with_graylist_and_blacklist();
        let index = FilterIndex::new(&recipe);

        // SR → blacklisted
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "SR");
        assert_eq!(
            crate::filter::blacklist_reason(&recipe, &obj).map(|s| s.to_string()),
            index.blacklist_reason(&obj).map(|s| s.to_string()),
        );

        // CT → not blacklisted
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");
        assert_eq!(
            crate::filter::blacklist_reason(&recipe, &obj).map(|s| s.to_string()),
            index.blacklist_reason(&obj).map(|s| s.to_string()),
        );

        // Missing modality → blacklisted
        let obj = create_test_obj();
        assert_eq!(
            crate::filter::blacklist_reason(&recipe, &obj).map(|s| s.to_string()),
            index.blacklist_reason(&obj).map(|s| s.to_string()),
        );
    }

    #[test]
    fn equivalence_graylist_ge_ct() {
        let recipe = make_recipe_with_graylist_and_blacklist();
        let index = FilterIndex::new(&recipe);

        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL SYSTEMS");

        let expected = crate::filter::get_graylist_regions(&recipe, &obj);
        let actual = index.get_graylist_regions(&obj);
        assert_eq!(expected.len(), actual.len());
        for (e, a) in expected.iter().zip(actual.iter()) {
            assert_eq!(e, a);
        }
    }

    #[test]
    fn equivalence_graylist_siemens_ct() {
        let recipe = make_recipe_with_graylist_and_blacklist();
        let index = FilterIndex::new(&recipe);

        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "SIEMENS");

        let expected = crate::filter::get_graylist_regions(&recipe, &obj);
        let actual = index.get_graylist_regions(&obj);
        assert_eq!(expected.len(), actual.len());
        for (e, a) in expected.iter().zip(actual.iter()) {
            assert_eq!(e, a);
        }
    }

    #[test]
    fn equivalence_graylist_us() {
        let recipe = make_recipe_with_graylist_and_blacklist();
        let index = FilterIndex::new(&recipe);

        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "US");

        let expected = crate::filter::get_graylist_regions(&recipe, &obj);
        let actual = index.get_graylist_regions(&obj);
        assert_eq!(expected.len(), actual.len());
    }

    #[test]
    fn equivalence_graylist_no_match() {
        let recipe = make_recipe_with_graylist_and_blacklist();
        let index = FilterIndex::new(&recipe);

        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "MR");
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "PHILIPS");

        let expected = crate::filter::get_graylist_regions(&recipe, &obj);
        let actual = index.get_graylist_regions(&obj);
        assert_eq!(expected.len(), actual.len());
        assert!(actual.is_empty());
    }

    // -----------------------------------------------------------------------
    // Dispatch key extraction edge cases
    // -----------------------------------------------------------------------

    #[test]
    fn dispatch_extracts_pipe_separated_modality() {
        let label = FilterLabel {
            name: "Multi-modality".into(),
            conditions: vec![Condition {
                operator: LogicalOp::First,
                predicate: Predicate::Contains {
                    field: "Modality".into(),
                    value: "CT|MR".into(),
                },
            }],
            coordinates: vec![],
        };
        let keys = extract_dispatch_keys(&label);
        assert_eq!(keys.modality_keys, vec!["ct", "mr"]);
        assert_eq!(keys.modality_condition_index, Some(0));
    }

    #[test]
    fn dispatch_extracts_manufacturer_not_first() {
        let label = FilterLabel {
            name: "Mfr second".into(),
            conditions: vec![
                Condition {
                    operator: LogicalOp::First,
                    predicate: Predicate::Contains {
                        field: "Manufacturer".into(),
                        value: "GE".into(),
                    },
                },
                Condition {
                    operator: LogicalOp::And,
                    predicate: Predicate::Contains {
                        field: "Modality".into(),
                        value: "CT".into(),
                    },
                },
            ],
            coordinates: vec![],
        };
        let keys = extract_dispatch_keys(&label);
        assert_eq!(keys.modality_keys, vec!["ct"]);
        assert_eq!(keys.modality_condition_index, Some(1));
        assert_eq!(keys.manufacturer_keys, vec!["ge"]);
        assert_eq!(keys.manufacturer_condition_index, Some(0));
    }

    #[test]
    fn dispatch_no_modality_goes_to_any() {
        let label = FilterLabel {
            name: "No modality".into(),
            conditions: vec![Condition {
                operator: LogicalOp::First,
                predicate: Predicate::Contains {
                    field: "Manufacturer".into(),
                    value: "ADAC".into(),
                },
            }],
            coordinates: vec![],
        };
        let keys = extract_dispatch_keys(&label);
        assert!(keys.modality_keys.is_empty());
        assert!(keys.modality_condition_index.is_none());
    }

    #[test]
    fn dispatch_or_condition_not_extracted() {
        let label = FilterLabel {
            name: "OR modality".into(),
            conditions: vec![
                Condition {
                    operator: LogicalOp::First,
                    predicate: Predicate::Contains {
                        field: "Manufacturer".into(),
                        value: "GE".into(),
                    },
                },
                Condition {
                    operator: LogicalOp::Or,
                    predicate: Predicate::Contains {
                        field: "Modality".into(),
                        value: "CT".into(),
                    },
                },
            ],
            coordinates: vec![],
        };
        let keys = extract_dispatch_keys(&label);
        // Modality is under OR, so it should NOT be extracted as a dispatch key
        assert!(keys.modality_keys.is_empty());
    }

    // -----------------------------------------------------------------------
    // Short-circuit behavior
    // -----------------------------------------------------------------------

    #[test]
    fn short_circuit_false_and_skips_evaluation() {
        // false AND <anything> = false; the second condition shouldn't matter
        let conditions = vec![
            CompiledCondition {
                operator: LogicalOp::First,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "NEVER_MATCHES".into(),
                },
                compiled_pattern: None,
            },
            CompiledCondition {
                operator: LogicalOp::And,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "CT".into(),
                },
                compiled_pattern: None,
            },
        ];
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");
        assert!(!evaluate_compiled_conditions(&conditions, &[], &obj));
    }

    #[test]
    fn short_circuit_true_or_skips_evaluation() {
        // true OR <anything> = true
        let conditions = vec![
            CompiledCondition {
                operator: LogicalOp::First,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "CT".into(),
                },
                compiled_pattern: None,
            },
            CompiledCondition {
                operator: LogicalOp::Or,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "NEVER_MATCHES".into(),
                },
                compiled_pattern: None,
            },
        ];
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");
        assert!(evaluate_compiled_conditions(&conditions, &[], &obj));
    }

    // -----------------------------------------------------------------------
    // Compiled regex vs substring fallback
    // -----------------------------------------------------------------------

    #[test]
    fn compiled_regex_matches() {
        let mut obj = create_test_obj();
        put_str(
            &mut obj,
            tags::MANUFACTURER_MODEL_NAME,
            VR::LO,
            "LightSpeed VCT",
        );

        let condition = compile_condition(&Condition {
            operator: LogicalOp::First,
            predicate: Predicate::Contains {
                field: "ManufacturerModelName".into(),
                value: "Light.*VCT".into(),
            },
        });
        assert!(evaluate_predicate_compiled(&condition, &obj));
    }

    #[test]
    fn compiled_notcontains_works() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "SIEMENS");

        let condition = compile_condition(&Condition {
            operator: LogicalOp::First,
            predicate: Predicate::NotContains {
                field: "Manufacturer".into(),
                value: "GE".into(),
            },
        });
        assert!(evaluate_predicate_compiled(&condition, &obj));
    }

    // -----------------------------------------------------------------------
    // Labels with no conditions
    // -----------------------------------------------------------------------

    #[test]
    fn label_with_no_conditions_does_not_match() {
        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Graylist,
                labels: vec![FilterLabel {
                    name: "Empty".into(),
                    conditions: vec![],
                    coordinates: vec![CoordinateRegion {
                        xmin: 0,
                        ymin: 0,
                        xmax: 100,
                        ymax: 100,
                        keep: false,
                    }],
                }],
            }],
        };
        let index = FilterIndex::new(&recipe);
        let obj = create_test_obj();
        let regions = index.get_graylist_regions(&obj);
        // No conditions → result starts false, stays false
        assert!(regions.is_empty());
    }
}
