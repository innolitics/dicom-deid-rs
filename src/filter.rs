use crate::recipe::{Condition, CoordinateRegion, FilterLabel, FilterType, LogicalOp, Predicate, Recipe};
use dicom_object::InMemDicomObject;

/// Evaluate a single filter predicate against a DICOM object.
///
/// Field names in predicates are resolved to DICOM tags by keyword lookup.
pub fn evaluate_predicate(predicate: &Predicate, obj: &InMemDicomObject) -> bool {
    todo!()
}

/// Evaluate a list of conditions with logical operators against a DICOM object.
///
/// Conditions are evaluated left-to-right: each AND/OR operator combines the
/// running result with the current condition's result.
pub fn evaluate_conditions(conditions: &[Condition], obj: &InMemDicomObject) -> bool {
    todo!()
}

/// Check whether a filter label's conditions match the given DICOM object.
pub fn matches_label(label: &FilterLabel, obj: &InMemDicomObject) -> bool {
    evaluate_conditions(&label.conditions, obj)
}

/// Check if a DICOM object is blacklisted by any blacklist filter in the recipe.
///
/// Returns `true` if the object matches any label within any blacklist filter
/// section, meaning it should be excluded from output.
pub fn is_blacklisted(recipe: &Recipe, obj: &InMemDicomObject) -> bool {
    todo!()
}

/// Collect all coordinate regions from graylist filters whose conditions match
/// the given DICOM object.
pub fn get_graylist_regions(
    recipe: &Recipe,
    obj: &InMemDicomObject,
) -> Vec<CoordinateRegion> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipe::*;
    use crate::test_helpers::*;
    use dicom_core::VR;
    use dicom_dictionary_std::tags;

    // -----------------------------------------------------------------------
    // Predicate evaluation (r-2-6)
    // -----------------------------------------------------------------------

    /// Requirement r-2-6-1
    #[test]
    fn r2_6_1_contains_matches_substring() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL SYSTEMS");

        let pred = Predicate::Contains {
            field: "Manufacturer".into(),
            value: "GE".into(),
        };
        assert!(evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-6-1
    #[test]
    fn r2_6_1_contains_no_match() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL SYSTEMS");

        let pred = Predicate::Contains {
            field: "Manufacturer".into(),
            value: "SIEMENS".into(),
        };
        assert!(!evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-6-1
    #[test]
    fn r2_6_1_contains_matches_regex() {
        let mut obj = create_test_obj();
        put_str(
            &mut obj,
            tags::MANUFACTURER_MODEL_NAME,
            VR::LO,
            "LightSpeed VCT",
        );

        let pred = Predicate::Contains {
            field: "ManufacturerModelName".into(),
            value: "Light.*VCT".into(),
        };
        assert!(evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-6-2
    #[test]
    fn r2_6_2_notcontains_rejects_substring() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL SYSTEMS");

        let pred = Predicate::NotContains {
            field: "Manufacturer".into(),
            value: "GE".into(),
        };
        assert!(
            !evaluate_predicate(&pred, &obj),
            "notcontains should be false when substring is present"
        );
    }

    /// Requirement r-2-6-2
    #[test]
    fn r2_6_2_notcontains_accepts_absent_substring() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL SYSTEMS");

        let pred = Predicate::NotContains {
            field: "Manufacturer".into(),
            value: "SIEMENS".into(),
        };
        assert!(evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-6-3
    #[test]
    fn r2_6_3_equals_case_insensitive_match() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let pred = Predicate::Equals {
            field: "Modality".into(),
            value: "ct".into(),
        };
        assert!(
            evaluate_predicate(&pred, &obj),
            "equals should be case-insensitive"
        );
    }

    /// Requirement r-2-6-3
    #[test]
    fn r2_6_3_equals_no_match() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let pred = Predicate::Equals {
            field: "Modality".into(),
            value: "MR".into(),
        };
        assert!(!evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-6-4
    #[test]
    fn r2_6_4_notequals_different_value() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let pred = Predicate::NotEquals {
            field: "Modality".into(),
            value: "MR".into(),
        };
        assert!(evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-6-4
    #[test]
    fn r2_6_4_notequals_same_value_case_insensitive() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let pred = Predicate::NotEquals {
            field: "Modality".into(),
            value: "ct".into(),
        };
        assert!(
            !evaluate_predicate(&pred, &obj),
            "notequals should be case-insensitive"
        );
    }

    /// Requirement r-2-6-5
    #[test]
    fn r2_6_5_missing_field_not_present() {
        let obj = create_test_obj();

        let pred = Predicate::Missing {
            field: "Manufacturer".into(),
        };
        assert!(
            evaluate_predicate(&pred, &obj),
            "missing should return true when field is absent"
        );
    }

    /// Requirement r-2-6-5
    #[test]
    fn r2_6_5_missing_field_is_present() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE");

        let pred = Predicate::Missing {
            field: "Manufacturer".into(),
        };
        assert!(
            !evaluate_predicate(&pred, &obj),
            "missing should return false when field is present"
        );
    }

    /// Requirement r-2-6-6
    #[test]
    fn r2_6_6_empty_field_present_and_empty() {
        let mut obj = create_test_obj();
        put_empty(&mut obj, tags::MANUFACTURER, VR::LO);

        let pred = Predicate::Empty {
            field: "Manufacturer".into(),
        };
        assert!(
            evaluate_predicate(&pred, &obj),
            "empty should return true when field is present but empty"
        );
    }

    /// Requirement r-2-6-6
    #[test]
    fn r2_6_6_empty_field_present_nonempty() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE");

        let pred = Predicate::Empty {
            field: "Manufacturer".into(),
        };
        assert!(
            !evaluate_predicate(&pred, &obj),
            "empty should return false when field has a value"
        );
    }

    /// Requirement r-2-6-6
    #[test]
    fn r2_6_6_empty_field_missing_entirely() {
        let obj = create_test_obj();

        let pred = Predicate::Empty {
            field: "Manufacturer".into(),
        };
        assert!(
            !evaluate_predicate(&pred, &obj),
            "empty should return false when field is not present at all"
        );
    }

    /// Requirement r-2-6-7
    #[test]
    fn r2_6_7_present_field_exists() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE");

        let pred = Predicate::Present {
            field: "Manufacturer".into(),
        };
        assert!(evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-6-7
    #[test]
    fn r2_6_7_present_field_absent() {
        let obj = create_test_obj();

        let pred = Predicate::Present {
            field: "Manufacturer".into(),
        };
        assert!(!evaluate_predicate(&pred, &obj));
    }

    // -----------------------------------------------------------------------
    // Logical operators (r-2-7)
    // -----------------------------------------------------------------------

    /// Requirement r-2-7-1
    #[test]
    fn r2_7_1_and_both_true() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_SEX, VR::CS, "M");
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let conditions = vec![
            Condition {
                operator: LogicalOp::First,
                predicate: Predicate::Equals {
                    field: "PatientSex".into(),
                    value: "M".into(),
                },
            },
            Condition {
                operator: LogicalOp::And,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "CT".into(),
                },
            },
        ];
        assert!(evaluate_conditions(&conditions, &obj));
    }

    /// Requirement r-2-7-1
    #[test]
    fn r2_7_1_and_second_false() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_SEX, VR::CS, "M");
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let conditions = vec![
            Condition {
                operator: LogicalOp::First,
                predicate: Predicate::Equals {
                    field: "PatientSex".into(),
                    value: "M".into(),
                },
            },
            Condition {
                operator: LogicalOp::And,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "MR".into(),
                },
            },
        ];
        assert!(!evaluate_conditions(&conditions, &obj));
    }

    /// Requirement r-2-7-2
    #[test]
    fn r2_7_2_or_first_true() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let conditions = vec![
            Condition {
                operator: LogicalOp::First,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "CT".into(),
                },
            },
            Condition {
                operator: LogicalOp::Or,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "MR".into(),
                },
            },
        ];
        assert!(evaluate_conditions(&conditions, &obj));
    }

    /// Requirement r-2-7-2
    #[test]
    fn r2_7_2_or_both_false() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "US");

        let conditions = vec![
            Condition {
                operator: LogicalOp::First,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "CT".into(),
                },
            },
            Condition {
                operator: LogicalOp::Or,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "MR".into(),
                },
            },
        ];
        assert!(!evaluate_conditions(&conditions, &obj));
    }

    /// Requirement r-2-7-3
    #[test]
    fn r2_7_3_mixed_and_or_operators() {
        // Evaluates left-to-right:
        //   (PatientSex==M AND Modality==CT) OR Manufacturer contains GE
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_SEX, VR::CS, "F");
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL");

        let conditions = vec![
            Condition {
                operator: LogicalOp::First,
                predicate: Predicate::Equals {
                    field: "PatientSex".into(),
                    value: "M".into(),
                },
            },
            Condition {
                operator: LogicalOp::And,
                predicate: Predicate::Equals {
                    field: "Modality".into(),
                    value: "CT".into(),
                },
            },
            Condition {
                operator: LogicalOp::Or,
                predicate: Predicate::Contains {
                    field: "Manufacturer".into(),
                    value: "GE".into(),
                },
            },
        ];
        // (F==M && CT==CT) || GE in "GE MEDICAL" => (false && true) || true => true
        assert!(evaluate_conditions(&conditions, &obj));
    }

    /// Requirement r-2-7-4
    #[test]
    fn r2_7_4_pipe_alternatives_match_first() {
        let mut obj = create_test_obj();
        put_str(
            &mut obj,
            tags::MANUFACTURER_MODEL_NAME,
            VR::LO,
            "A400 Scanner",
        );

        let pred = Predicate::Contains {
            field: "ManufacturerModelName".into(),
            value: "A400|A500|A600".into(),
        };
        assert!(
            evaluate_predicate(&pred, &obj),
            "pipe-separated value should match as regex alternation"
        );
    }

    /// Requirement r-2-7-4
    #[test]
    fn r2_7_4_pipe_alternatives_match_second() {
        let mut obj = create_test_obj();
        put_str(
            &mut obj,
            tags::MANUFACTURER_MODEL_NAME,
            VR::LO,
            "A500 Premium",
        );

        let pred = Predicate::Contains {
            field: "ManufacturerModelName".into(),
            value: "A400|A500|A600".into(),
        };
        assert!(evaluate_predicate(&pred, &obj));
    }

    /// Requirement r-2-7-4
    #[test]
    fn r2_7_4_pipe_alternatives_no_match() {
        let mut obj = create_test_obj();
        put_str(
            &mut obj,
            tags::MANUFACTURER_MODEL_NAME,
            VR::LO,
            "B700 Scanner",
        );

        let pred = Predicate::Contains {
            field: "ManufacturerModelName".into(),
            value: "A400|A500|A600".into(),
        };
        assert!(!evaluate_predicate(&pred, &obj));
    }

    // -----------------------------------------------------------------------
    // Blacklist / file filtering (r-5)
    // -----------------------------------------------------------------------

    /// Requirement r-5-1
    #[test]
    fn r5_1_blacklist_excludes_matching_file() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "SR");

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Blacklist,
                labels: vec![FilterLabel {
                    name: "Reject Structured Reports".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Equals {
                            field: "Modality".into(),
                            value: "SR".into(),
                        },
                    }],
                    coordinates: vec![],
                }],
            }],
        };

        assert!(
            is_blacklisted(&recipe, &obj),
            "SR modality should be blacklisted"
        );
    }

    /// Requirement r-5-1
    #[test]
    fn r5_1_blacklist_does_not_exclude_non_matching() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Blacklist,
                labels: vec![FilterLabel {
                    name: "Reject Structured Reports".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Equals {
                            field: "Modality".into(),
                            value: "SR".into(),
                        },
                    }],
                    coordinates: vec![],
                }],
            }],
        };

        assert!(
            !is_blacklisted(&recipe, &obj),
            "CT modality should not be blacklisted"
        );
    }

    /// Requirement r-5-1
    #[test]
    fn r5_1_graylist_does_not_exclude_file() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MODALITY, VR::CS, "SR");

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Graylist, // graylist, not blacklist
                labels: vec![FilterLabel {
                    name: "Graylist SR".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Equals {
                            field: "Modality".into(),
                            value: "SR".into(),
                        },
                    }],
                    coordinates: vec![],
                }],
            }],
        };

        assert!(
            !is_blacklisted(&recipe, &obj),
            "graylist filters should not cause blacklist exclusion"
        );
    }

    // -----------------------------------------------------------------------
    // Graylist region collection (r-2-10-1)
    // -----------------------------------------------------------------------

    /// Requirement r-2-10-1
    #[test]
    fn r2_10_1_graylist_returns_regions_on_match() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL SYSTEMS");

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Graylist,
                labels: vec![FilterLabel {
                    name: "GE CT".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Contains {
                            field: "Manufacturer".into(),
                            value: "GE".into(),
                        },
                    }],
                    coordinates: vec![CoordinateRegion {
                        xmin: 0,
                        ymin: 0,
                        xmax: 512,
                        ymax: 100,
                        keep: false,
                    }],
                }],
            }],
        };

        let regions = get_graylist_regions(&recipe, &obj);
        assert_eq!(regions.len(), 1);
        assert_eq!(regions[0].xmax, 512);
        assert_eq!(regions[0].ymax, 100);
    }

    /// Requirement r-2-10-1
    #[test]
    fn r2_10_1_graylist_no_regions_on_mismatch() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "SIEMENS");

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Graylist,
                labels: vec![FilterLabel {
                    name: "GE CT".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Contains {
                            field: "Manufacturer".into(),
                            value: "GE".into(),
                        },
                    }],
                    coordinates: vec![CoordinateRegion {
                        xmin: 0,
                        ymin: 0,
                        xmax: 512,
                        ymax: 100,
                        keep: false,
                    }],
                }],
            }],
        };

        let regions = get_graylist_regions(&recipe, &obj);
        assert!(
            regions.is_empty(),
            "non-matching filters should yield no regions"
        );
    }
}
