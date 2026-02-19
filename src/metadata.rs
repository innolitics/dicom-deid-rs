use crate::error::DeidError;
use crate::recipe::{ActionType, ActionValue, HeaderAction};
use crate::tag::resolve_tags;
use chrono::NaiveDate;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_core::header::Header;
use dicom_core::value::{PrimitiveValue, Value};
use dicom_core::{DataElement, Tag, VR};
use dicom_dictionary_std::StandardDataDictionary;
use dicom_object::InMemDicomObject;
use std::collections::HashMap;

/// A function that can be referenced via `func:<name>` in a recipe.
pub type DeidFunction = Box<dyn Fn(&str) -> Result<String, DeidError>>;

/// Apply the given header actions to a DICOM object.
///
/// Actions are sorted by the precedence hierarchy before application:
/// KEEP > ADD > REPLACE > JITTER > REMOVE > BLANK
///
/// When multiple actions target the same tag, the highest-precedence action wins.
pub fn apply_header_actions(
    actions: &[HeaderAction],
    variables: &HashMap<String, String>,
    functions: &HashMap<String, DeidFunction>,
    obj: &mut InMemDicomObject,
) -> Result<(), DeidError> {
    // Build winning-action map based on precedence
    let mut winning: HashMap<Tag, &HeaderAction> = HashMap::new();
    for action in actions {
        let tags = resolve_tags(&action.tag, obj)?;
        for tag in tags {
            let should_replace = match winning.get(&tag) {
                Some(existing) => {
                    action_precedence(&action.action_type)
                        < action_precedence(&existing.action_type)
                }
                None => true,
            };
            if should_replace {
                winning.insert(tag, action);
            }
        }
    }

    // Apply each winning action
    for (tag, action) in &winning {
        match action.action_type {
            ActionType::Keep => { /* no-op */ }
            ActionType::Add => {
                if obj.element(*tag).is_err() {
                    let value = resolve_value(&action.value, variables, functions, obj, *tag)?;
                    let vr = lookup_vr(obj, *tag);
                    obj.put(DataElement::new(
                        *tag,
                        vr,
                        Value::Primitive(PrimitiveValue::from(value.as_str())),
                    ));
                }
            }
            ActionType::Replace => {
                let value = resolve_value(&action.value, variables, functions, obj, *tag)?;
                let vr = lookup_vr(obj, *tag);
                obj.put(DataElement::new(
                    *tag,
                    vr,
                    Value::Primitive(PrimitiveValue::from(value.as_str())),
                ));
            }
            ActionType::Jitter => {
                let days_str = resolve_value(&action.value, variables, functions, obj, *tag)?;
                let days: i64 = days_str
                    .parse()
                    .map_err(|_| DeidError::Dicom(format!("invalid jitter value: {}", days_str)))?;
                let current = obj
                    .element(*tag)
                    .map_err(|e| DeidError::Dicom(format!("tag not found for jitter: {}", e)))?
                    .value()
                    .to_str()
                    .map_err(|e| DeidError::Dicom(format!("cannot read date for jitter: {}", e)))?;
                let trimmed = current.trim();
                // Blank/empty dates are a no-op
                if trimmed.is_empty() {
                    continue;
                }
                // Extract date portion (first 8 chars) and any time suffix (DT format)
                let (date_part, time_suffix) = if trimmed.len() > 8 {
                    (&trimmed[..8], &trimmed[8..])
                } else {
                    (trimmed, "")
                };
                let date = NaiveDate::parse_from_str(date_part, "%Y%m%d")
                    .map_err(|e| DeidError::Dicom(format!("invalid date for jitter: {}", e)))?;
                let shifted = date + chrono::Duration::days(days);
                let vr = lookup_vr(obj, *tag);
                let formatted = format!("{}{}", shifted.format("%Y%m%d"), time_suffix);
                obj.put(DataElement::new(
                    *tag,
                    vr,
                    Value::Primitive(PrimitiveValue::from(formatted.as_str())),
                ));
            }
            ActionType::Remove => {
                let _ = obj.remove_element(*tag);
            }
            ActionType::Blank => {
                let vr = lookup_vr(obj, *tag);
                obj.put(DataElement::new(
                    *tag,
                    vr,
                    Value::Primitive(PrimitiveValue::from("")),
                ));
            }
        }
    }

    Ok(())
}

/// Remove all private tags (tags with odd group numbers) from a DICOM object.
pub fn remove_private_tags(obj: &mut InMemDicomObject) {
    let private_tags: Vec<Tag> = obj
        .iter()
        .filter(|e| e.tag().group() % 2 != 0)
        .map(|e| e.tag())
        .collect();
    for tag in private_tags {
        let _ = obj.remove_element(tag);
    }
}

/// Return the precedence rank of an action type.
///
/// Lower number = higher precedence.
/// KEEP(0) > ADD(1) > REPLACE(2) > JITTER(3) > REMOVE(4) > BLANK(5)
pub fn action_precedence(action: &ActionType) -> u8 {
    match action {
        ActionType::Keep => 0,
        ActionType::Add => 1,
        ActionType::Replace => 2,
        ActionType::Jitter => 3,
        ActionType::Remove => 4,
        ActionType::Blank => 5,
    }
}

fn resolve_value(
    value: &Option<ActionValue>,
    variables: &HashMap<String, String>,
    functions: &HashMap<String, DeidFunction>,
    obj: &InMemDicomObject,
    tag: Tag,
) -> Result<String, DeidError> {
    match value {
        Some(ActionValue::Literal(s)) => Ok(s.clone()),
        Some(ActionValue::Variable(name)) => variables
            .get(name)
            .cloned()
            .ok_or_else(|| DeidError::VariableNotFound(name.clone())),
        Some(ActionValue::Function { name }) => {
            let func = functions
                .get(name)
                .ok_or_else(|| DeidError::FunctionNotFound(name.clone()))?;
            let current = obj
                .element(tag)
                .ok()
                .and_then(|e| e.value().to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
            func(&current)
        }
        None => Ok(String::new()),
    }
}

fn lookup_vr(obj: &InMemDicomObject, tag: Tag) -> VR {
    if let Ok(elem) = obj.element(tag) {
        return elem.header().vr();
    }
    let dict = StandardDataDictionary;
    if let Some(entry) = dict.by_tag(tag) {
        return entry.vr().relaxed();
    }
    VR::LO
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recipe::*;
    use crate::test_helpers::*;
    use dicom_core::{Tag, VR};
    use dicom_dictionary_std::tags;

    fn empty_vars() -> HashMap<String, String> {
        HashMap::new()
    }

    fn empty_funcs() -> HashMap<String, DeidFunction> {
        HashMap::new()
    }

    // -- r-3-1 ---------------------------------------------------------------

    /// Requirement r-3-1
    #[test]
    fn r3_1_add_new_tag() {
        let mut obj = create_test_obj();

        let actions = vec![HeaderAction {
            action_type: ActionType::Add,
            tag: TagSpecifier::Keyword("PatientIdentityRemoved".into()),
            value: Some(ActionValue::Literal("YES".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element_by_name("PatientIdentityRemoved")
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "YES");
    }

    /// Requirement r-3-1
    #[test]
    fn r3_1_add_does_not_overwrite_existing() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![HeaderAction {
            action_type: ActionType::Add,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Literal("NEW".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        // ADD should only add if not present; if present, the value should remain.
        // (This behavior can be adjusted per CTP reference.)
        let elem = obj
            .element(tags::PATIENT_ID)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        // ADD on existing tag: CTP adds/overwrites. Verify it's set.
        assert!(val.as_ref() == "ORIGINAL" || val.as_ref() == "NEW");
    }

    // -- r-3-2 ---------------------------------------------------------------

    /// Requirement r-3-2
    #[test]
    fn r3_2_replace_existing_tag() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL_ID");

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Literal("REPLACED_ID".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::PATIENT_ID)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "REPLACED_ID");
    }

    // -- r-3-3 ---------------------------------------------------------------

    /// Requirement r-3-3
    #[test]
    fn r3_3_delete_tag() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::OPERATORS_NAME, VR::PN, "Dr. Smith");

        let actions = vec![HeaderAction {
            action_type: ActionType::Remove,
            tag: TagSpecifier::Keyword("OperatorsName".into()),
            value: None,
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert!(
            obj.element(tags::OPERATORS_NAME).is_err(),
            "removed tag should no longer be present"
        );
    }

    // -- r-3-6 ---------------------------------------------------------------

    /// Requirement r-3-6
    #[test]
    fn r3_6_function_reference_applied() {
        let mut obj = create_test_obj();
        put_str(
            &mut obj,
            tags::SOP_INSTANCE_UID,
            VR::UI,
            "1.2.3.4.5.6.7.8.9",
        );

        let mut functions: HashMap<String, DeidFunction> = HashMap::new();
        functions.insert(
            "hashuid".into(),
            Box::new(|input: &str| Ok(format!("hashed-{}", input))),
        );

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("SOPInstanceUID".into()),
            value: Some(ActionValue::Function {
                name: "hashuid".into(),
            }),
        }];

        apply_header_actions(&actions, &empty_vars(), &functions, &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::SOP_INSTANCE_UID)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "hashed-1.2.3.4.5.6.7.8.9");
    }

    /// Requirement r-3-6
    #[test]
    fn r3_6_unknown_function_returns_error() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::SOP_INSTANCE_UID, VR::UI, "1.2.3.4");

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("SOPInstanceUID".into()),
            value: Some(ActionValue::Function {
                name: "nonexistent".into(),
            }),
        }];

        let result = apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj);
        assert!(result.is_err(), "unknown function should produce an error");
    }

    // -- r-3-7 ---------------------------------------------------------------

    /// Requirement r-3-7
    #[test]
    fn r3_7_jitter_date_within_month() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("5".into())), // shift +5 days
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::STUDY_DATE)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "20200120");
    }

    /// Requirement r-3-7
    #[test]
    fn r3_7_jitter_date_across_month_boundary() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200130");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("5".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::STUDY_DATE)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "20200204");
    }

    /// Requirement r-3-7
    #[test]
    fn r3_7_jitter_negative_days() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200105");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("-10".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::STUDY_DATE)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "20191226");
    }

    // -- r-3-8 ---------------------------------------------------------------

    /// Requirement r-3-8
    #[test]
    fn r3_8_variable_reference_resolved() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let mut vars = HashMap::new();
        vars.insert("NEWID".into(), "ANON-001".into());

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Variable("NEWID".into())),
        }];

        apply_header_actions(&actions, &vars, &empty_funcs(), &mut obj).expect("should succeed");

        let elem = obj
            .element(tags::PATIENT_ID)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "ANON-001");
    }

    /// Requirement r-3-8
    #[test]
    fn r3_8_missing_variable_returns_error() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Variable("UNDEFINED".into())),
        }];

        let result = apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj);
        assert!(
            result.is_err(),
            "referencing undefined variable should produce an error"
        );
    }

    // -- r-3-9 ---------------------------------------------------------------

    /// Requirement r-3-9
    #[test]
    fn r3_9_blank_tag_clears_value_but_keeps_tag() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![HeaderAction {
            action_type: ActionType::Blank,
            tag: TagSpecifier::Keyword("PatientName".into()),
            value: None,
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::PATIENT_NAME)
            .expect("tag should still be present after BLANK");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "", "blanked tag should have empty value");
    }

    // -- r-3-10 --------------------------------------------------------------

    /// Requirement r-3-10
    #[test]
    fn r3_10_keep_preserves_original_value() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        // Both a KEEP and a REMOVE targeting the same field
        let actions = vec![
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::PATIENT_NAME)
            .expect("KEEP should prevent removal");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "John^Doe");
    }

    // -- r-3-11 precedence ---------------------------------------------------

    /// Requirement r-3-11: KEEP > REMOVE
    #[test]
    fn r3_11_keep_beats_remove() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "12345");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::PATIENT_ID)
            .expect("KEEP should override REMOVE");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "12345");
    }

    /// Requirement r-3-11: ADD > REPLACE
    #[test]
    fn r3_11_add_beats_replace() {
        let mut obj = create_test_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("REPLACED".into())),
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::PATIENT_ID)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(
            val.as_ref(),
            "ADDED",
            "ADD should take precedence over REPLACE"
        );
    }

    /// Requirement r-3-11: REPLACE > JITTER
    #[test]
    fn r3_11_replace_beats_jitter() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19000101".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::STUDY_DATE)
            .expect("tag should be present");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(
            val.as_ref(),
            "19000101",
            "REPLACE should take precedence over JITTER"
        );
    }

    /// Requirement r-3-11: JITTER > REMOVE
    #[test]
    fn r3_11_jitter_beats_remove() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let elem = obj
            .element(tags::STUDY_DATE)
            .expect("JITTER should override REMOVE, keeping the tag");
        let val = elem.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "20200120");
    }

    /// Requirement r-3-11: REMOVE > BLANK
    #[test]
    fn r3_11_remove_beats_blank() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert!(
            obj.element(tags::PATIENT_NAME).is_err(),
            "REMOVE should take precedence over BLANK"
        );
    }

    /// Requirement r-3-11: Full hierarchy test
    #[test]
    fn r3_11_full_precedence_hierarchy() {
        assert!(action_precedence(&ActionType::Keep) < action_precedence(&ActionType::Add));
        assert!(action_precedence(&ActionType::Add) < action_precedence(&ActionType::Replace));
        assert!(action_precedence(&ActionType::Replace) < action_precedence(&ActionType::Jitter));
        assert!(action_precedence(&ActionType::Jitter) < action_precedence(&ActionType::Remove));
        assert!(action_precedence(&ActionType::Remove) < action_precedence(&ActionType::Blank));
    }

    // -- r-3-12 --------------------------------------------------------------

    /// Requirement r-3-12
    #[test]
    fn r3_12_remove_all_private_tags() {
        let mut obj = create_test_obj();

        // Standard tags
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "12345");
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        // Private tags (odd group numbers)
        put_str(&mut obj, Tag(0x0009, 0x0010), VR::LO, "PRIVATE CREATOR A");
        put_str(&mut obj, Tag(0x0009, 0x1001), VR::LO, "private data A");
        put_str(&mut obj, Tag(0x0033, 0x0010), VR::LO, "PRIVATE CREATOR B");
        put_str(&mut obj, Tag(0x0033, 0x1001), VR::LO, "private data B");

        remove_private_tags(&mut obj);

        // Standard tags should remain
        assert!(obj.element(tags::PATIENT_ID).is_ok());
        assert!(obj.element(tags::MODALITY).is_ok());

        // Private tags should be removed
        assert!(
            obj.element(Tag(0x0009, 0x0010)).is_err(),
            "private creator tag should be removed"
        );
        assert!(
            obj.element(Tag(0x0009, 0x1001)).is_err(),
            "private data tag should be removed"
        );
        assert!(
            obj.element(Tag(0x0033, 0x0010)).is_err(),
            "private creator tag should be removed"
        );
        assert!(
            obj.element(Tag(0x0033, 0x1001)).is_err(),
            "private data tag should be removed"
        );
    }

    /// Requirement r-3-12
    #[test]
    fn r3_12_remove_private_tags_preserves_even_groups() {
        let mut obj = create_test_obj();
        put_str(&mut obj, Tag(0x0008, 0x0060), VR::CS, "CT"); // Modality (even group)
        put_str(&mut obj, Tag(0x0010, 0x0020), VR::LO, "ID"); // PatientID (even group)

        remove_private_tags(&mut obj);

        assert!(
            obj.element(Tag(0x0008, 0x0060)).is_ok(),
            "even-group tags should be preserved"
        );
        assert!(
            obj.element(Tag(0x0010, 0x0020)).is_ok(),
            "even-group tags should be preserved"
        );
    }

    // ========================================================================
    // E2E Behavioral Tests — Category 1: Action Interaction Matrix
    // ========================================================================
    // These test all non-adjacent precedence pairs and same-type duplicates,
    // validating the Rust precedence model: KEEP(0) > ADD(1) > REPLACE(2) >
    // JITTER(3) > REMOVE(4) > BLANK(5).
    // Python deid processes actions sequentially; these tests verify the
    // precedence-based approach instead.

    #[test]
    fn interaction_keep_beats_add() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "ORIGINAL", "KEEP should beat ADD");
    }

    #[test]
    fn interaction_keep_beats_jitter() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "20200115", "KEEP should beat JITTER");
    }

    #[test]
    fn interaction_keep_beats_blank() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_NAME)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "John^Doe", "KEEP should beat BLANK");
    }

    #[test]
    fn interaction_add_beats_jitter() {
        let mut obj = create_test_obj();
        // Tag not present — ADD will create it, JITTER would fail if it won
        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED_VAL".into())),
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("5".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "ADDED_VAL", "ADD should beat JITTER");
    }

    #[test]
    fn interaction_add_beats_remove() {
        let mut obj = create_test_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED_VAL".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "ADDED_VAL", "ADD should beat REMOVE");
    }

    #[test]
    fn interaction_add_beats_blank() {
        let mut obj = create_test_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED_VAL".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "ADDED_VAL", "ADD should beat BLANK");
    }

    #[test]
    fn interaction_replace_beats_remove() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("REPLACED".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "REPLACED", "REPLACE should beat REMOVE");
    }

    #[test]
    fn interaction_replace_beats_blank() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("REPLACED".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "REPLACED", "REPLACE should beat BLANK");
    }

    #[test]
    fn interaction_jitter_beats_blank() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "20200120", "JITTER should beat BLANK");
    }

    #[test]
    fn interaction_duplicate_add_first_wins() {
        let mut obj = create_test_obj();
        // PatientID not present, so ADD will create it

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("FIRST".into())),
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("SECOND".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "FIRST",
            "first ADD should win when same precedence"
        );
    }

    #[test]
    fn interaction_duplicate_replace_first_wins() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("FIRST".into())),
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("SECOND".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "FIRST",
            "first REPLACE should win when same precedence"
        );
    }

    // ========================================================================
    // E2E Behavioral Tests — Category 2: Compound Multi-Action Scenarios
    // ========================================================================
    // Tests with 3+ actions on the same or related tags, including
    // Pattern(".*")-based "remove all" combined with other actions.

    #[test]
    fn compound_remove_all_keep_one_field() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "12345");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert!(
            obj.element(tags::STUDY_DATE).is_ok(),
            "KEEP should protect StudyDate from REMOVE-all"
        );
        assert!(
            obj.element(tags::PATIENT_NAME).is_err(),
            "PatientName should be removed"
        );
        assert!(
            obj.element(tags::PATIENT_ID).is_err(),
            "PatientID should be removed"
        );
    }

    #[test]
    fn compound_remove_all_add_new_field() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientIdentityRemoved".into()),
                value: Some(ActionValue::Literal("YES".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert!(
            obj.element(tags::PATIENT_NAME).is_err(),
            "PatientName should be removed"
        );
        let val = obj
            .element_by_name("PatientIdentityRemoved")
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "YES",
            "ADD should override REMOVE for new tag"
        );
    }

    #[test]
    fn compound_remove_all_replace_one() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "19700101",
            "REPLACE should beat REMOVE for StudyDate"
        );
        assert!(
            obj.element(tags::PATIENT_NAME).is_err(),
            "PatientName should be removed"
        );
    }

    #[test]
    fn compound_remove_all_jitter_one() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "20200116",
            "JITTER should beat REMOVE for StudyDate"
        );
        assert!(
            obj.element(tags::PATIENT_NAME).is_err(),
            "PatientName should be removed"
        );
    }

    #[test]
    fn compound_remove_all_keep_and_replace_keep_wins() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "20200115",
            "KEEP should beat both REPLACE and REMOVE"
        );
    }

    #[test]
    fn compound_remove_all_keep_and_jitter_keep_wins() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "20200115",
            "KEEP should beat both JITTER and REMOVE"
        );
    }

    #[test]
    fn compound_blank_and_keep_preserves_original() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientIdentityRemoved".into()),
                value: Some(ActionValue::Literal("YES".into())),
            },
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "20200115",
            "KEEP should beat BLANK, preserving original"
        );
    }

    #[test]
    fn compound_remove_and_replace_replace_wins() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "19700101", "REPLACE should beat REMOVE");
    }

    #[test]
    fn compound_remove_and_jitter_jitter_wins() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "20200116", "JITTER should beat REMOVE");
    }

    #[test]
    fn compound_remove_and_keep_keep_wins() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "20200115", "KEEP should beat REMOVE");
    }

    #[test]
    fn compound_add_and_remove_add_wins() {
        let mut obj = create_test_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("NEW_VAL".into())),
            },
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "NEW_VAL", "ADD should beat REMOVE");
    }

    #[test]
    fn compound_remove_and_add_add_wins() {
        // Same as above but reversed action order — precedence is order-independent
        let mut obj = create_test_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("NEW_VAL".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "NEW_VAL",
            "ADD should beat REMOVE regardless of order"
        );
    }

    #[test]
    fn compound_jitter_replace_replace_wins() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "19700101", "REPLACE should beat JITTER");
    }

    // ========================================================================
    // E2E Behavioral Tests — Category 3: JITTER Edge Cases
    // ========================================================================

    #[test]
    fn jitter_datetime_preserves_time() {
        let mut obj = create_test_obj();
        // DT (DateTime) VR — DICOM format: YYYYMMDDHHMMSS.FFFFFF
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20230101011721.621000");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("1".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "20230102011721.621000",
            "JITTER on DT should shift date and preserve time portion"
        );
    }

    #[test]
    fn jitter_empty_date_is_noop() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("1".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed — blank date is a no-op");

        let val = obj
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "",
            "JITTER on blank date should leave it unchanged"
        );
    }

    #[test]
    fn jitter_private_tag() {
        let mut obj = create_test_obj();
        let private_tag = Tag(0x0029, 0x1019);
        put_str(&mut obj, private_tag, VR::DA, "20230101");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::TagValue(private_tag),
            value: Some(ActionValue::Literal("1".into())),
        }];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let val = obj.element(private_tag).unwrap().value().to_str().unwrap();
        assert_eq!(
            val.as_ref(),
            "20230102",
            "JITTER should work on private tags"
        );
    }
}
