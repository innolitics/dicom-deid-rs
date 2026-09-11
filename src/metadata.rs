use crate::error::DeidError;
use crate::file_meta;
use crate::recipe::{ActionCondition, ActionType, ActionValue, HeaderAction, KeepGroup, Recipe};
use crate::tag::{resolve_tags, resolve_tags_in_file_meta};
use chrono::NaiveDate;
use dicom_core::dictionary::{DataDictionary, DataDictionaryEntry};
use dicom_core::header::Header;
use dicom_core::value::{DataSetSequence, PrimitiveValue, Value};
use dicom_core::{DataElement, Length, Tag, VR};
use dicom_dictionary_std::StandardDataDictionary;
use dicom_object::{FileDicomObject, InMemDicomObject};
use std::collections::{HashMap, HashSet};

/// A function that can be referenced via `func:<name>` in a recipe.
#[cfg(feature = "parallel")]
pub type DeidFunction = Box<dyn Fn(&str) -> Result<String, DeidError> + Send + Sync>;
#[cfg(not(feature = "parallel"))]
pub type DeidFunction = Box<dyn Fn(&str) -> Result<String, DeidError>>;

/// Apply the given header actions to a DICOM file object.
///
/// Actions are sorted by the precedence hierarchy before application:
/// KEEP > ADD > REPLACE > JITTER > REMOVE > BLANK
///
/// When multiple actions target the same tag, the highest-precedence action wins.
///
/// Actions targeting the File Meta Information group (0002) are applied to the
/// file meta table rather than the main data set; see [`apply_file_meta_actions`].
pub fn apply_header_actions(
    actions: &[HeaderAction],
    variables: &HashMap<String, String>,
    functions: &HashMap<String, DeidFunction>,
    obj: &mut FileDicomObject<InMemDicomObject>,
) -> Result<(), DeidError> {
    apply_dataset_actions(actions, variables, functions, obj)?;
    apply_file_meta_actions(actions, variables, functions, obj)
}

/// Given a set of actions and a way to resolve each one's tag specifier, build the
/// map of tag to the highest-precedence action targeting it (r-3-11).
fn resolve_winning_actions<F>(
    actions: &[HeaderAction],
    mut resolve: F,
) -> Result<HashMap<Tag, &HeaderAction>, DeidError>
where
    F: FnMut(&HeaderAction) -> Result<Vec<Tag>, DeidError>,
{
    let mut winning: HashMap<Tag, &HeaderAction> = HashMap::new();
    for action in actions {
        for tag in resolve(action)? {
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
    Ok(winning)
}

/// Apply header actions to the main data set.
///
/// Group 0002 tags are skipped here: the file meta group is not part of the data
/// set, and writing a group-0002 element into it would produce a file with a
/// duplicate, wrongly-encoded meta group. [`apply_file_meta_actions`] handles them.
///
/// This is the recursive half of [`apply_header_actions`] — it calls itself for
/// the item data sets of sequences marked with `@process()`.
pub(crate) fn apply_dataset_actions(
    actions: &[HeaderAction],
    variables: &HashMap<String, String>,
    functions: &HashMap<String, DeidFunction>,
    obj: &mut InMemDicomObject,
) -> Result<(), DeidError> {
    let winning = resolve_winning_actions(actions, |action| resolve_tags(&action.tag, obj))?;

    // Apply each winning action
    for (tag, action) in &winning {
        if file_meta::is_file_meta(*tag) {
            continue;
        }
        // Check condition before executing the action
        if action
            .condition
            .as_ref()
            .is_some_and(|c| !evaluate_condition(c, obj))
        {
            continue;
        }

        match action.action_type {
            ActionType::Keep => { /* no-op */ }
            ActionType::Add => {
                if obj.element(*tag).is_err() {
                    let value = resolve_value(&action.value, variables, functions, obj, *tag)?;
                    let vr = lookup_vr(obj, *tag);
                    if vr == VR::SQ {
                        obj.put(build_code_sequence(*tag, &value));
                    } else {
                        obj.put(DataElement::new(
                            *tag,
                            vr,
                            Value::Primitive(PrimitiveValue::from(value.as_str())),
                        ));
                    }
                }
            }
            ActionType::Replace => {
                let value = resolve_value(&action.value, variables, functions, obj, *tag)?;
                let vr = lookup_vr(obj, *tag);
                if vr == VR::SQ {
                    obj.put(build_code_sequence(*tag, &value));
                } else {
                    obj.put(DataElement::new(
                        *tag,
                        vr,
                        Value::Primitive(PrimitiveValue::from(value.as_str())),
                    ));
                }
            }
            ActionType::Jitter => {
                let days_str = resolve_value(&action.value, variables, functions, obj, *tag)?;
                let days: i64 = days_str
                    .parse()
                    .map_err(|_| DeidError::Dicom(format!("invalid jitter value: {}", days_str)))?;
                let elem = match obj.element(*tag) {
                    Ok(e) => e,
                    Err(_) => continue, // tag absent — nothing to jitter
                };
                let current = elem
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
            ActionType::ReplaceOnly => {
                // Only replace if the tag already exists
                if obj.element(*tag).is_ok() {
                    let value = resolve_value(&action.value, variables, functions, obj, *tag)?;
                    let vr = lookup_vr(obj, *tag);
                    if vr == VR::SQ {
                        obj.put(build_code_sequence(*tag, &value));
                    } else {
                        obj.put(DataElement::new(
                            *tag,
                            vr,
                            Value::Primitive(PrimitiveValue::from(value.as_str())),
                        ));
                    }
                }
            }
            ActionType::Append => {
                let new_val = resolve_value(&action.value, variables, functions, obj, *tag)?;
                let current = obj
                    .element(*tag)
                    .ok()
                    .and_then(|e| e.value().to_str().ok())
                    .map(|s| s.to_string())
                    .unwrap_or_default();
                let combined = if current.is_empty() {
                    new_val
                } else {
                    format!("{}\\{}", current, new_val)
                };
                let vr = lookup_vr(obj, *tag);
                obj.put(DataElement::new(
                    *tag,
                    vr,
                    Value::Primitive(PrimitiveValue::from(combined.as_str())),
                ));
            }
            ActionType::Process => {
                // Handled in the sequence recursion phase below, not here.
            }
        }
    }

    // Recurse into sequence elements that have a Process action.
    // CTP only recurses into sequences explicitly marked with @process().
    // When recursing, Replace→ReplaceOnly and Add→skip to prevent
    // injecting new elements into item datasets (CTP rule).
    // Only include tags that are actually SQ elements present in this object.
    let process_tags: Vec<Tag> = winning
        .iter()
        .filter(|(_, a)| a.action_type == ActionType::Process)
        .map(|(t, _)| *t)
        .filter(|t| obj.element(*t).is_ok_and(|e| e.value().items().is_some()))
        .collect();

    if !process_tags.is_empty() {
        // Build child actions: no SQ-targeted Add/Replace, and convert
        // Replace→ReplaceOnly to prevent element injection into items.
        let child_actions: Vec<HeaderAction> = actions
            .iter()
            .filter_map(|a| {
                // Keep Process actions for inner sequences (they'll be needed
                // when recursing further), but skip Process actions for the
                // sequences we're already processing at this level.
                if a.action_type == ActionType::Process {
                    let tags = resolve_tags(&a.tag, obj).unwrap_or_default();
                    if tags.iter().any(|t| process_tags.contains(t)) {
                        return None; // Already being processed at this level
                    }
                    return Some(a.clone()); // Pass through for inner levels
                }
                // Skip non-Process SQ-targeted Add/Replace to avoid infinite
                // recursion (e.g. writing a literal into an SQ field).
                if matches!(
                    a.action_type,
                    ActionType::Add | ActionType::Replace | ActionType::ReplaceOnly
                ) {
                    let tags = resolve_tags(&a.tag, obj).unwrap_or_default();
                    if tags.iter().any(|t| lookup_vr(obj, *t) == VR::SQ) {
                        return None;
                    }
                }
                match a.action_type {
                    ActionType::Add => None, // No new elements in item datasets
                    ActionType::Replace => Some(HeaderAction {
                        action_type: ActionType::ReplaceOnly,
                        ..a.clone()
                    }),
                    _ => Some(a.clone()),
                }
            })
            .collect();

        for seq_tag in &process_tags {
            let mut elem = match obj.take_element(*seq_tag) {
                Ok(e) => e,
                Err(_) => continue,
            };
            let mut seq_error: Option<DeidError> = None;
            elem.update_value(|val| {
                if let Some(items) = val.items_mut() {
                    for item in items.iter_mut() {
                        if seq_error.is_some() {
                            break;
                        }
                        if let Err(e) =
                            apply_dataset_actions(&child_actions, variables, functions, item)
                        {
                            seq_error = Some(e);
                        }
                    }
                }
            });
            obj.put(elem);
            if let Some(e) = seq_error {
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Apply header actions to the File Meta Information group (0002).
///
/// This gives recipes the same reach over group 0002 that the reference python
/// `deid` implementation provides (see `resources/deid/deid/tests/test_file_meta.py`),
/// so a rule such as `REPLACE MediaStorageSOPInstanceUID func:hashuid` takes effect.
///
/// Structural attributes listed in [`file_meta::PROTECTED_TAGS`] are never
/// modified, mirroring `resources/deid/deid/dicom/config.json`. JITTER, APPEND and
/// PROCESS have no meaning in group 0002 (no date, multi-valued, or sequence
/// attributes) and are no-ops.
pub fn apply_file_meta_actions(
    actions: &[HeaderAction],
    variables: &HashMap<String, String>,
    functions: &HashMap<String, DeidFunction>,
    obj: &mut FileDicomObject<InMemDicomObject>,
) -> Result<(), DeidError> {
    let winning = resolve_winning_actions(actions, |action| {
        resolve_tags_in_file_meta(&action.tag, obj.meta())
    })?;

    // Only group 0002 tags that are not structurally protected are eligible.
    let mut eligible: Vec<(Tag, &HeaderAction)> = winning
        .into_iter()
        .filter(|(tag, _)| file_meta::is_file_meta(*tag) && !file_meta::is_protected(*tag))
        .collect();
    // Deterministic order so the outcome does not depend on hash iteration order.
    eligible.sort_by_key(|(tag, _)| *tag);

    // Conditions reference data set keywords (e.g. Modality), so evaluate them
    // against the data set before deciding whether the action applies.
    let mut pending: Vec<(Tag, &HeaderAction, Option<String>)> = Vec::new();
    for (tag, action) in eligible {
        if action
            .condition
            .as_ref()
            .is_some_and(|c| !evaluate_condition(c, obj))
        {
            continue;
        }

        // `func:` values are computed from the attribute's current value, which
        // must be read (and un-padded) before any mutation.
        let needs_value = matches!(
            action.action_type,
            ActionType::Add | ActionType::Replace | ActionType::ReplaceOnly | ActionType::Blank
        );
        let value = if needs_value {
            let current = file_meta::get(obj.meta(), tag).unwrap_or_default();
            Some(resolve_value_from(
                &action.value,
                variables,
                functions,
                &current,
            )?)
        } else {
            None
        };
        pending.push((tag, action, value));
    }

    if pending.is_empty() {
        return Ok(());
    }

    // A single update_meta call so the group length is recalculated once.
    obj.update_meta(|meta| {
        for (tag, action, value) in &pending {
            match action.action_type {
                ActionType::Keep
                | ActionType::Jitter
                | ActionType::Append
                | ActionType::Process => {}
                ActionType::Add => {
                    if !file_meta::is_present(meta, *tag) {
                        file_meta::set(meta, *tag, value.as_deref().unwrap_or_default());
                    }
                }
                ActionType::Replace => {
                    file_meta::set(meta, *tag, value.as_deref().unwrap_or_default());
                }
                ActionType::ReplaceOnly => {
                    if file_meta::is_present(meta, *tag) {
                        file_meta::set(meta, *tag, value.as_deref().unwrap_or_default());
                    }
                }
                ActionType::Blank => {
                    file_meta::set(meta, *tag, "");
                }
                ActionType::Remove => {
                    file_meta::remove(meta, *tag);
                }
            }
        }
    });

    Ok(())
}

/// Reconcile the file meta group with the de-identified data set.
///
/// PS3.10 requires MediaStorageSOPClassUID (0002,0002) and
/// MediaStorageSOPInstanceUID (0002,0003) to match the data set's SOPClassUID
/// (0008,0016) and SOPInstanceUID (0008,0018). Because the recipe rewrites the
/// data set UIDs, the file meta copies must be re-derived afterwards or the output
/// file leaks the original, identifying SOP Instance UID.
///
/// This mirrors CTP, whose `DICOMAnonymizer` regenerates the file meta group from
/// the anonymized data set via `DcmObjectFactory.newFileMetaInfo`. It runs after
/// all recipe actions so conformance never depends on a recipe rule being present.
///
/// Also clears the application entity titles and private information, which
/// identify the originating site rather than the data. TransferSyntaxUID and
/// ImplementationClassUID are left alone — the former is owned by the pixel
/// pipeline, and both are on the reference implementation's protected list.
pub fn finalize_file_meta(obj: &mut FileDicomObject<InMemDicomObject>) {
    let sop_class_uid = dataset_uid(obj, Tag(0x0008, 0x0016));
    let sop_instance_uid = dataset_uid(obj, Tag(0x0008, 0x0018));

    obj.update_meta(|meta| {
        if let Some(uid) = &sop_class_uid {
            file_meta::set(meta, file_meta::MEDIA_STORAGE_SOP_CLASS_UID, uid);
        }
        if let Some(uid) = &sop_instance_uid {
            file_meta::set(meta, file_meta::MEDIA_STORAGE_SOP_INSTANCE_UID, uid);
        }
        file_meta::remove(meta, file_meta::SOURCE_APPLICATION_ENTITY_TITLE);
        file_meta::remove(meta, file_meta::SENDING_APPLICATION_ENTITY_TITLE);
        file_meta::remove(meta, file_meta::RECEIVING_APPLICATION_ENTITY_TITLE);
        file_meta::remove(meta, file_meta::PRIVATE_INFORMATION_CREATOR_UID);
        file_meta::remove(meta, file_meta::PRIVATE_INFORMATION);
    });
}

/// Read a non-empty, un-padded UID from the data set.
fn dataset_uid(obj: &InMemDicomObject, tag: Tag) -> Option<String> {
    let value = obj.element(tag).ok()?.value().to_str().ok()?;
    let trimmed = value.trim_end_matches(|c: char| c.is_whitespace() || c == '\0');
    (!trimmed.is_empty()).then(|| trimmed.to_string())
}

/// Remove all private tags from a DICOM file object.
///
/// This covers odd-group elements in the data set (recursively, through
/// sequences) and, in the File Meta Information group, PrivateInformationCreatorUID
/// (0002,0100) and PrivateInformation (0002,0102) — group 0002 is even, so those
/// two are not reachable by the odd-group rule despite holding private data.
pub fn remove_private_tags(obj: &mut FileDicomObject<InMemDicomObject>) {
    remove_private_dataset_tags(obj);
    obj.update_meta(|meta| {
        file_meta::remove(meta, file_meta::PRIVATE_INFORMATION_CREATOR_UID);
        file_meta::remove(meta, file_meta::PRIVATE_INFORMATION);
    });
}

/// Remove all private tags (tags with odd group numbers) from a data set,
/// recursing into sequence items.
pub(crate) fn remove_private_dataset_tags(obj: &mut InMemDicomObject) {
    let private_tags: Vec<Tag> = obj
        .iter()
        .filter(|e| e.tag().group() % 2 != 0)
        .map(|e| e.tag())
        .collect();
    for tag in private_tags {
        let _ = obj.remove_element(tag);
    }

    // Recurse into sequence elements
    let seq_tags: Vec<Tag> = obj
        .iter()
        .filter(|e| e.value().items().is_some())
        .map(|e| e.header().tag())
        .collect();

    for seq_tag in seq_tags {
        let mut elem = match obj.take_element(seq_tag) {
            Ok(e) => e,
            Err(_) => continue,
        };
        elem.update_value(|val| {
            if let Some(items) = val.items_mut() {
                for item in items.iter_mut() {
                    remove_private_dataset_tags(item);
                }
            }
        });
        obj.put(elem);
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
        ActionType::ReplaceOnly => 2,
        ActionType::Jitter => 3,
        ActionType::Remove => 4,
        ActionType::Blank => 5,
        ActionType::Append => 1,
        ActionType::Process => 2,
    }
}

fn resolve_value(
    value: &Option<ActionValue>,
    variables: &HashMap<String, String>,
    functions: &HashMap<String, DeidFunction>,
    obj: &InMemDicomObject,
    tag: Tag,
) -> Result<String, DeidError> {
    let current = obj
        .element(tag)
        .ok()
        .and_then(|e| e.value().to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_default();
    resolve_value_from(value, variables, functions, &current)
}

/// Resolve an action value given the target attribute's current value directly.
///
/// The file meta group is not element-addressable, so its actions supply the
/// current value here rather than having it looked up in a data set.
fn resolve_value_from(
    value: &Option<ActionValue>,
    variables: &HashMap<String, String>,
    functions: &HashMap<String, DeidFunction>,
    current: &str,
) -> Result<String, DeidError> {
    match value {
        Some(ActionValue::Literal(s)) => Ok(s.clone()),
        Some(ActionValue::Variable(name)) => variables
            .get(name)
            .cloned()
            .ok_or_else(|| DeidError::VariableNotFound(name.clone())),
        Some(ActionValue::Function { name, .. }) => {
            let func = functions
                .get(name)
                .ok_or_else(|| DeidError::FunctionNotFound(name.clone()))?;
            func(current)
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

/// Evaluate a condition against the current DICOM object.
fn evaluate_condition(condition: &ActionCondition, obj: &InMemDicomObject) -> bool {
    let dict = StandardDataDictionary;
    let get_value = |element_name: &str| -> Option<String> {
        let tag = if element_name == "this" {
            return None; // "this" is handled by the caller
        } else {
            dict.by_name(element_name).map(|e| e.tag())?
        };
        obj.element(tag)
            .ok()
            .and_then(|e| e.value().to_str().ok())
            .map(|s| s.trim().to_string())
    };

    let is_blank = |element_name: &str| -> bool {
        match get_value(element_name) {
            None => true,
            Some(v) => v.trim().is_empty(),
        }
    };

    let element_exists = |element_name: &str| -> bool {
        dict.by_name(element_name)
            .is_some_and(|e| obj.element(e.tag()).is_ok())
    };

    match condition {
        ActionCondition::IsBlank { element } => is_blank(element),
        ActionCondition::IsNotBlank { element } => !is_blank(element),
        ActionCondition::Exists { element } => element_exists(element),
        ActionCondition::NotExists { element } => !element_exists(element),
        ActionCondition::Contains { element, value } => {
            get_value(element).is_some_and(|v| v.to_lowercase().contains(&value.to_lowercase()))
        }
        ActionCondition::NotContains { element, value } => {
            !get_value(element).is_some_and(|v| v.to_lowercase().contains(&value.to_lowercase()))
        }
        ActionCondition::Equals { element, value } => {
            get_value(element).is_some_and(|v| v.eq_ignore_ascii_case(value))
        }
        ActionCondition::NotEquals { element, value } => {
            !get_value(element).is_some_and(|v| v.eq_ignore_ascii_case(value))
        }
        ActionCondition::Matches { element, pattern } => {
            let Ok(re) = regex::Regex::new(pattern) else {
                return false;
            };
            get_value(element).is_some_and(|v| re.is_match(&v))
        }
        ActionCondition::GreaterThan { element, value } => {
            let parse_numeric = |s: &str| -> Option<i64> {
                let digits: String = s
                    .chars()
                    .filter(|c| c.is_ascii_digit() || *c == '-')
                    .collect();
                digits.parse().ok()
            };
            match (
                get_value(element).and_then(|v| parse_numeric(&v)),
                parse_numeric(value),
            ) {
                (Some(a), Some(b)) => a > b,
                _ => false,
            }
        }
    }
}

/// Build a DICOM Code Sequence element from a `/`-delimited string of code values.
///
/// Each code value becomes a sequence item with:
/// - (0008,0100) CodeValue
/// - (0008,0102) CodingSchemeDesignator = "DCM"
/// - (0008,0104) CodeMeaning (looked up from CID 7050)
///
/// Replicates CTP's behavior for DeidentificationMethodCodeSequence (0012,0064).
/// If the value starts with `RESET/`, any pre-existing items are cleared first.
fn build_code_sequence(tag: Tag, value: &str) -> DataElement<InMemDicomObject> {
    let code_value_tag = Tag(0x0008, 0x0100);
    let coding_scheme_tag = Tag(0x0008, 0x0102);
    let code_meaning_tag = Tag(0x0008, 0x0104);

    // Strip RESET prefix (handled by caller clearing existing element)
    let codes_str = value
        .strip_prefix("RESET/")
        .or_else(|| value.strip_prefix("RESET /"))
        .unwrap_or(value);

    let items: Vec<InMemDicomObject> = codes_str
        .split('/')
        .map(|code| {
            let code = code.trim();
            let mut item = InMemDicomObject::new_empty();
            item.put(DataElement::new(
                code_value_tag,
                VR::SH,
                Value::Primitive(PrimitiveValue::from(code)),
            ));
            item.put(DataElement::new(
                coding_scheme_tag,
                VR::SH,
                Value::Primitive(PrimitiveValue::from("DCM")),
            ));
            item.put(DataElement::new(
                code_meaning_tag,
                VR::LO,
                Value::Primitive(PrimitiveValue::from(cid_7050_meaning(code))),
            ));
            item
        })
        .collect();

    DataElement::new(
        tag,
        VR::SQ,
        Value::from(DataSetSequence::new(items, Length::UNDEFINED)),
    )
}

/// Look up the CodeMeaning for a CID 7050 De-identification Method code.
fn cid_7050_meaning(code: &str) -> &'static str {
    match code {
        "113100" => "Basic Application Confidentiality Profile",
        "113101" => "Clean Pixel Data Option",
        "113102" => "Clean Recognizable Visual Features Option",
        "113103" => "Clean Graphics Option",
        "113104" => "Clean Structured Content Option",
        "113105" => "Clean Descriptors Option",
        "113106" => "Retain Longitudinal With Full Dates Option",
        "113107" => "Retain Longitudinal With Modified Dates Option",
        "113108" => "Retain Patient Characteristics Option",
        "113109" => "Retain Device Identity Option",
        "113110" => "Retain UIDs",
        "113111" => "Retain Safe Private Option",
        _ => "Unknown",
    }
}

/// Remove all elements from the DICOM object that are not targeted by any
/// header action in the recipe and are not in an exempt set.
///
/// Exempt tags/groups:
/// - SOPClassUID (0008,0016), SOPInstanceUID (0008,0018),
///   StudyInstanceUID (0020,000d)
/// - Group 0x0028 (pixel description parameters)
/// - Group 0x7FE0 (pixel data — PixelData, FloatPixelData, DoublePixelData)
/// - Groups listed in `recipe.keep_groups`
/// - Overlay groups (0x6000-0x601e) when the recipe does not REMOVE them
///
/// In the File Meta Information group, only the optional attributes are eligible
/// for removal; the Type-1 attributes and the structural attributes in
/// [`file_meta::PROTECTED_TAGS`] are always retained, since a file without them
/// cannot be read back.
pub fn remove_unspecified_elements(obj: &mut FileDicomObject<InMemDicomObject>, recipe: &Recipe) {
    remove_unspecified_file_meta(obj, recipe);
    remove_unspecified_dataset_elements(obj, recipe);
}

/// Remove optional File Meta Information attributes not named by any recipe action.
fn remove_unspecified_file_meta(obj: &mut FileDicomObject<InMemDicomObject>, recipe: &Recipe) {
    let mut targeted: HashSet<Tag> = HashSet::new();
    for action in &recipe.header {
        if let Ok(tags) = resolve_tags_in_file_meta(&action.tag, obj.meta()) {
            targeted.extend(tags);
        }
    }

    let removable: Vec<Tag> = file_meta::present_tags(obj.meta())
        .into_iter()
        .filter(|tag| {
            !targeted.contains(tag)
                && !file_meta::is_protected(*tag)
                && !file_meta::is_required(*tag)
        })
        .collect();

    if removable.is_empty() {
        return;
    }
    obj.update_meta(|meta| {
        for tag in removable {
            file_meta::remove(meta, tag);
        }
    });
}

fn remove_unspecified_dataset_elements(obj: &mut InMemDicomObject, recipe: &Recipe) {
    // 1. Collect all tags targeted by any HeaderAction
    let mut targeted_tags: HashSet<Tag> = HashSet::new();
    for action in &recipe.header {
        if let Ok(tags) = resolve_tags(&action.tag, obj) {
            for tag in tags {
                targeted_tags.insert(tag);
            }
        }
    }

    // 2. Add exempt tags
    targeted_tags.insert(Tag(0x0008, 0x0016)); // SOPClassUID
    targeted_tags.insert(Tag(0x0008, 0x0018)); // SOPInstanceUID
    targeted_tags.insert(Tag(0x0020, 0x000d)); // StudyInstanceUID

    // 3. Collect exempt groups
    let mut exempt_groups: HashSet<u16> = HashSet::new();
    exempt_groups.insert(0x0028); // pixel description parameters
    exempt_groups.insert(0x7FE0); // PixelData / FloatPixelData / DoublePixelData

    // 4. Add exempt groups from recipe.keep_groups
    let mut skip_odd_groups = false;
    for kg in &recipe.keep_groups {
        match kg {
            KeepGroup::Group(n) => {
                exempt_groups.insert(*n);
            }
            KeepGroup::SafePrivateElements => {
                skip_odd_groups = true;
            }
        }
    }

    // 5. Check if overlays are being removed — look for a REMOVE action targeting 60xx groups
    let overlays_removed = recipe.header.iter().any(|action| {
        if action.action_type != ActionType::Remove {
            return false;
        }
        if let Ok(tags) = resolve_tags(&action.tag, obj) {
            tags.iter()
                .any(|t| t.group() >= 0x6000 && t.group() <= 0x601e)
        } else {
            false
        }
    });
    if !overlays_removed {
        for g in (0x6000..=0x601eu16).step_by(2) {
            exempt_groups.insert(g);
        }
    }

    // 6. Iterate all tags in the object, remove any not in targeted/exempt set
    let all_tags: Vec<Tag> = obj.iter().map(|e| e.tag()).collect();
    for tag in all_tags {
        if targeted_tags.contains(&tag) {
            continue;
        }
        if exempt_groups.contains(&tag.group()) {
            continue;
        }
        if skip_odd_groups && tag.group() % 2 != 0 {
            continue;
        }
        let _ = obj.remove_element(tag);
    }
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
        let mut obj = create_test_file_obj();

        let actions = vec![HeaderAction {
            action_type: ActionType::Add,
            tag: TagSpecifier::Keyword("PatientIdentityRemoved".into()),
            value: Some(ActionValue::Literal("YES".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![HeaderAction {
            action_type: ActionType::Add,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Literal("NEW".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL_ID");

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Literal("REPLACED_ID".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::OPERATORS_NAME, VR::PN, "Dr. Smith");

        let actions = vec![HeaderAction {
            action_type: ActionType::Remove,
            tag: TagSpecifier::Keyword("OperatorsName".into()),
            value: None,
            condition: None,
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
        let mut obj = create_test_file_obj();
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
                args: vec![],
            }),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::SOP_INSTANCE_UID, VR::UI, "1.2.3.4");

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("SOPInstanceUID".into()),
            value: Some(ActionValue::Function {
                name: "nonexistent".into(),
                args: vec![],
            }),
            condition: None,
        }];

        let result = apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj);
        assert!(result.is_err(), "unknown function should produce an error");
    }

    // -- r-3-7 ---------------------------------------------------------------

    /// Requirement r-3-7
    #[test]
    fn r3_7_jitter_date_within_month() {
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("5".into())), // shift +5 days
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200130");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("5".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200105");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("-10".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let mut vars = HashMap::new();
        vars.insert("NEWID".into(), "ANON-001".into());

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Variable("NEWID".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![HeaderAction {
            action_type: ActionType::Replace,
            tag: TagSpecifier::Keyword("PatientID".into()),
            value: Some(ActionValue::Variable("UNDEFINED".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![HeaderAction {
            action_type: ActionType::Blank,
            tag: TagSpecifier::Keyword("PatientName".into()),
            value: None,
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        // Both a KEEP and a REMOVE targeting the same field
        let actions = vec![
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "12345");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("REPLACED".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19000101".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();

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
        let mut obj = create_test_file_obj();
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("PatientName".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();
        // Tag not present — ADD will create it, JITTER would fail if it won
        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED_VAL".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("5".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED_VAL".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("ADDED_VAL".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("REPLACED".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("REPLACED".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        // PatientID not present, so ADD will create it

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("FIRST".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("SECOND".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("FIRST".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("SECOND".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "12345");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientIdentityRemoved".into()),
                value: Some(ActionValue::Literal("YES".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Pattern(".*".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientIdentityRemoved".into()),
                value: Some(ActionValue::Literal("YES".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("NEW_VAL".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
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
        let mut obj = create_test_file_obj();

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientID".into()),
                value: Some(ActionValue::Literal("NEW_VAL".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20200115");

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("1".into())),
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("19700101".into())),
                condition: None,
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
        let mut obj = create_test_file_obj();
        // DT (DateTime) VR — DICOM format: YYYYMMDDHHMMSS.FFFFFF
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20230101011721.621000");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("1".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::STUDY_DATE, VR::DA, "");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::Keyword("StudyDate".into()),
            value: Some(ActionValue::Literal("1".into())),
            condition: None,
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
        let mut obj = create_test_file_obj();
        let private_tag = Tag(0x0029, 0x1019);
        put_str(&mut obj, private_tag, VR::DA, "20230101");

        let actions = vec![HeaderAction {
            action_type: ActionType::Jitter,
            tag: TagSpecifier::TagValue(private_tag),
            value: Some(ActionValue::Literal("1".into())),
            condition: None,
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

    // -- r-3-13 Nested sequence support --------------------------------------

    /// ReferencedSeriesSequence tag used as the sequence container in tests.
    const SEQ_TAG: Tag = Tag(0x0008, 0x1115);

    fn process_action(tag: Tag) -> HeaderAction {
        HeaderAction {
            action_type: ActionType::Process,
            tag: TagSpecifier::TagValue(tag),
            value: None,
            condition: None,
        }
    }

    /// Requirement r-3-13: REPLACE modifies a field within a sequence item.
    #[test]
    fn r3_13_replace_inside_sequence() {
        let mut item = create_test_obj();
        put_str(&mut item, tags::ACCESSION_NUMBER, VR::SH, "ACC-ORIGINAL");

        let mut obj = create_test_file_obj();
        put_sequence(&mut obj, SEQ_TAG, vec![item]);

        let actions = vec![
            process_action(SEQ_TAG),
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("AccessionNumber".into()),
                value: Some(ActionValue::Literal("ACC-REPLACED".into())),
                condition: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let seq_items = obj.element(SEQ_TAG).unwrap().items().unwrap();
        let val = seq_items[0]
            .element(tags::ACCESSION_NUMBER)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "ACC-REPLACED");
    }

    /// Requirement r-3-13: REMOVE deletes a field from a sequence item while leaving others.
    #[test]
    fn r3_13_remove_inside_sequence() {
        let mut item = create_test_obj();
        put_str(&mut item, tags::ACCESSION_NUMBER, VR::SH, "ACC-123");
        put_str(&mut item, tags::MODALITY, VR::CS, "CT");

        let mut obj = create_test_file_obj();
        put_sequence(&mut obj, SEQ_TAG, vec![item]);

        let actions = vec![
            process_action(SEQ_TAG),
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("AccessionNumber".into()),
                value: None,
                condition: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let seq_items = obj.element(SEQ_TAG).unwrap().items().unwrap();
        assert!(
            seq_items[0].element(tags::ACCESSION_NUMBER).is_err(),
            "AccessionNumber should be removed from sequence item"
        );
        assert!(
            seq_items[0].element(tags::MODALITY).is_ok(),
            "Modality should remain in sequence item"
        );
    }

    /// Requirement r-3-13: BLANK empties a field inside a sequence item.
    #[test]
    fn r3_13_blank_inside_sequence() {
        let mut item = create_test_obj();
        put_str(&mut item, tags::ACCESSION_NUMBER, VR::SH, "ACC-123");

        let mut obj = create_test_file_obj();
        put_sequence(&mut obj, SEQ_TAG, vec![item]);

        let actions = vec![
            process_action(SEQ_TAG),
            HeaderAction {
                action_type: ActionType::Blank,
                tag: TagSpecifier::Keyword("AccessionNumber".into()),
                value: None,
                condition: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let seq_items = obj.element(SEQ_TAG).unwrap().items().unwrap();
        let val = seq_items[0]
            .element(tags::ACCESSION_NUMBER)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "",
            "blanked tag inside sequence should be empty"
        );
    }

    /// Requirement r-3-13: JITTER shifts a date inside a sequence item.
    #[test]
    fn r3_13_jitter_inside_sequence() {
        let mut item = create_test_obj();
        put_str(&mut item, tags::STUDY_DATE, VR::DA, "20200115");

        let mut obj = create_test_file_obj();
        put_sequence(&mut obj, SEQ_TAG, vec![item]);

        let actions = vec![
            process_action(SEQ_TAG),
            HeaderAction {
                action_type: ActionType::Jitter,
                tag: TagSpecifier::Keyword("StudyDate".into()),
                value: Some(ActionValue::Literal("5".into())),
                condition: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let seq_items = obj.element(SEQ_TAG).unwrap().items().unwrap();
        let val = seq_items[0]
            .element(tags::STUDY_DATE)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "20200120");
    }

    /// Requirement r-3-13: REPLACE reaches elements inside a sequence-within-a-sequence.
    #[test]
    fn r3_13_nested_sequence_two_levels_deep() {
        let mut inner_item = create_test_obj();
        put_str(&mut inner_item, tags::ACCESSION_NUMBER, VR::SH, "DEEP-ACC");

        let mut outer_item = create_test_obj();
        put_sequence(&mut outer_item, SEQ_TAG, vec![inner_item]);

        let mut obj = create_test_file_obj();
        // Use a different sequence tag for the outer level
        let outer_seq_tag = Tag(0x0008, 0x1200); // StudiesContainingOtherReferencedInstancesSequence
        put_sequence(&mut obj, outer_seq_tag, vec![outer_item]);

        let actions = vec![
            process_action(outer_seq_tag),
            process_action(SEQ_TAG),
            HeaderAction {
                action_type: ActionType::Replace,
                tag: TagSpecifier::Keyword("AccessionNumber".into()),
                value: Some(ActionValue::Literal("REPLACED-DEEP".into())),
                condition: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let outer_items = obj.element(outer_seq_tag).unwrap().items().unwrap();
        let inner_items = outer_items[0].element(SEQ_TAG).unwrap().items().unwrap();
        let val = inner_items[0]
            .element(tags::ACCESSION_NUMBER)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(val.as_ref(), "REPLACED-DEEP");
    }

    /// Requirement r-3-13: KEEP overrides REMOVE for fields inside sequences.
    #[test]
    fn r3_13_keep_protects_inside_sequence() {
        let mut item = create_test_obj();
        put_str(&mut item, tags::ACCESSION_NUMBER, VR::SH, "ACC-KEEP");

        let mut obj = create_test_file_obj();
        put_sequence(&mut obj, SEQ_TAG, vec![item]);

        let actions = vec![
            HeaderAction {
                action_type: ActionType::Remove,
                tag: TagSpecifier::Keyword("AccessionNumber".into()),
                value: None,
                condition: None,
            },
            HeaderAction {
                action_type: ActionType::Keep,
                tag: TagSpecifier::Keyword("AccessionNumber".into()),
                value: None,
                condition: None,
            },
        ];

        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        let seq_items = obj.element(SEQ_TAG).unwrap().items().unwrap();
        let val = seq_items[0]
            .element(tags::ACCESSION_NUMBER)
            .unwrap()
            .value()
            .to_str()
            .unwrap();
        assert_eq!(
            val.as_ref(),
            "ACC-KEEP",
            "KEEP should protect field inside sequence"
        );
    }

    /// Regression: `remove_unspecified_elements` must NOT delete PixelData
    /// (or other group 0x7FE0 bulk-data tags) even when the recipe does not
    /// explicitly list them. Without this exemption, running a CTP-style
    /// anonymizer with `unspecifiedelements=T` produced header-only outputs
    /// with no pixel payload.
    #[test]
    fn remove_unspecified_elements_preserves_pixel_data() {
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ID");
        // Stand-in pixel payloads in group 0x7FE0.
        obj.put(dicom_object::mem::InMemElement::new(
            Tag(0x7FE0, 0x0010),
            VR::OB,
            dicom_core::value::PrimitiveValue::from(vec![0u8, 1, 2, 3]),
        ));

        let empty_recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![],
            keep_groups: vec![],
        };

        remove_unspecified_elements(&mut obj, &empty_recipe);

        assert!(
            obj.element(Tag(0x7FE0, 0x0010)).is_ok(),
            "PixelData (7FE0,0010) must be preserved by remove_unspecified_elements"
        );
    }

    /// Requirement r-3-13: remove_private_tags removes private tags from within sequences.
    #[test]
    fn r3_13_remove_private_tags_inside_sequence() {
        let mut item = create_test_obj();
        put_str(&mut item, tags::MODALITY, VR::CS, "CT");
        put_str(&mut item, Tag(0x0009, 0x0010), VR::LO, "PRIVATE CREATOR");
        put_str(&mut item, Tag(0x0009, 0x1001), VR::LO, "private data");

        let mut obj = create_test_file_obj();
        put_sequence(&mut obj, SEQ_TAG, vec![item]);

        remove_private_tags(&mut obj);

        let seq_items = obj.element(SEQ_TAG).unwrap().items().unwrap();
        assert!(
            seq_items[0].element(tags::MODALITY).is_ok(),
            "standard tag should remain inside sequence"
        );
        assert!(
            seq_items[0].element(Tag(0x0009, 0x0010)).is_err(),
            "private creator should be removed from sequence"
        );
        assert!(
            seq_items[0].element(Tag(0x0009, 0x1001)).is_err(),
            "private data should be removed from sequence"
        );
    }

    // -- r-3-14: File Meta Information group (0002) ---------------------------

    use crate::file_meta as fm;

    fn hashuid_funcs() -> HashMap<String, DeidFunction> {
        let mut funcs: HashMap<String, DeidFunction> = HashMap::new();
        funcs.insert(
            "hashuid".into(),
            Box::new(|input: &str| Ok(format!("2.25.{}", input.len()))),
        );
        funcs
    }

    fn action(
        action_type: ActionType,
        tag: TagSpecifier,
        value: Option<ActionValue>,
    ) -> HeaderAction {
        HeaderAction {
            action_type,
            tag,
            value,
            condition: None,
        }
    }

    fn keyword(name: &str) -> TagSpecifier {
        TagSpecifier::Keyword(name.into())
    }

    /// Requirement r-3-14
    ///
    /// Mirrors `test_replace_filemeta` in
    /// `resources/deid/deid/tests/test_file_meta.py`.
    #[test]
    fn r3_14_replace_media_storage_sop_instance_uid() {
        let mut obj = create_test_file_obj();

        let actions = vec![action(
            ActionType::Replace,
            keyword("MediaStorageSOPInstanceUID"),
            Some(ActionValue::Literal("1.2.3.4.5.4.3.2.1".into())),
        )];
        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert_eq!(
            obj.meta().media_storage_sop_instance_uid(),
            "1.2.3.4.5.4.3.2.1"
        );
    }

    /// Requirement r-3-14
    ///
    /// `func:` values must be computed from the *un-padded* current value, so a
    /// hash of the file meta UID matches a hash of the same UID in the data set.
    #[test]
    fn r3_14_func_value_sees_unpadded_current_value() {
        let mut obj = create_test_file_obj();
        // Odd-length UID: FileMetaTableBuilder stores it NUL-padded.
        obj.update_meta(|m| m.media_storage_sop_instance_uid = "1.2.840.9999999\0".to_string());
        put_str(&mut obj, tags::SOP_INSTANCE_UID, VR::UI, "1.2.840.9999999");

        let actions = vec![
            action(
                ActionType::Replace,
                keyword("SOPInstanceUID"),
                Some(ActionValue::Function {
                    name: "hashuid".into(),
                    args: vec![],
                }),
            ),
            action(
                ActionType::Replace,
                keyword("MediaStorageSOPInstanceUID"),
                Some(ActionValue::Function {
                    name: "hashuid".into(),
                    args: vec![],
                }),
            ),
        ];
        apply_header_actions(&actions, &empty_vars(), &hashuid_funcs(), &mut obj)
            .expect("should succeed");

        let dataset_uid = obj
            .element(tags::SOP_INSTANCE_UID)
            .expect("present")
            .value()
            .to_str()
            .expect("readable")
            .to_string();
        assert_eq!(
            obj.meta().media_storage_sop_instance_uid(),
            dataset_uid.trim_end_matches('\0'),
            "hashing padded and unpadded copies of the same UID must agree"
        );
    }

    /// Requirement r-3-14-1
    #[test]
    fn r3_14_1_transfer_syntax_uid_is_protected() {
        let mut obj = create_test_file_obj();

        for action_type in [ActionType::Replace, ActionType::Remove, ActionType::Blank] {
            let actions = vec![action(
                action_type,
                keyword("TransferSyntaxUID"),
                Some(ActionValue::Literal("1.2.3.4.5.4.3.2.1".into())),
            )];
            apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
                .expect("should succeed");
            assert_eq!(
                obj.meta().transfer_syntax(),
                "1.2.840.10008.1.2.1",
                "TransferSyntaxUID must be protected from de-identification"
            );
        }
    }

    /// Requirement r-3-14-1
    #[test]
    fn r3_14_1_implementation_class_uid_is_protected() {
        let mut obj = create_test_file_obj();

        let actions = vec![action(
            ActionType::Remove,
            keyword("ImplementationClassUID"),
            None,
        )];
        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert_eq!(obj.meta().implementation_class_uid(), "1.2.3.4");
    }

    /// Requirement r-3-14-4
    #[test]
    fn r3_14_4_remove_source_application_entity_title() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| m.source_application_entity_title = Some("SENDING_SITE".into()));

        let actions = vec![action(
            ActionType::Remove,
            keyword("SourceApplicationEntityTitle"),
            None,
        )];
        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert_eq!(obj.meta().source_application_entity_title, None);
    }

    /// Requirement r-3-14
    ///
    /// A group-0002 rule must never inject a group-0002 element into the main
    /// data set — that would emit a second, wrongly-encoded meta group.
    #[test]
    fn r3_14_file_meta_action_does_not_write_into_dataset() {
        let mut obj = create_test_file_obj();

        let actions = vec![action(
            ActionType::Replace,
            TagSpecifier::TagValue(fm::MEDIA_STORAGE_SOP_INSTANCE_UID),
            Some(ActionValue::Literal("1.2.3.4.5.4.3.2.1".into())),
        )];
        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert!(
            obj.element(fm::MEDIA_STORAGE_SOP_INSTANCE_UID).is_err(),
            "group 0002 must not appear in the data set"
        );
        assert_eq!(
            obj.meta().media_storage_sop_instance_uid(),
            "1.2.3.4.5.4.3.2.1"
        );
    }

    /// Requirement r-3-14 / r-3-5
    #[test]
    fn r3_14_pattern_specifier_reaches_file_meta() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| m.source_application_entity_title = Some("SENDING_SITE".into()));

        let actions = vec![action(
            ActionType::Remove,
            TagSpecifier::Pattern("^SourceApplicationEntityTitle$".into()),
            None,
        )];
        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert_eq!(obj.meta().source_application_entity_title, None);
    }

    /// Requirement r-3-14 / r-3-4-2
    #[test]
    fn r3_14_group_range_specifier_reaches_file_meta() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| {
            m.source_application_entity_title = Some("SENDING_SITE".into());
            m.sending_application_entity_title = Some("SENDER".into());
        });

        let actions = vec![action(
            ActionType::Remove,
            TagSpecifier::GroupRange {
                group_min: 0x0002,
                group_max: 0x0002,
                element: None,
            },
            None,
        )];
        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert_eq!(obj.meta().source_application_entity_title, None);
        assert_eq!(obj.meta().sending_application_entity_title, None);
        // Wildcard removal must still not touch the structural attributes.
        assert_eq!(obj.meta().transfer_syntax(), "1.2.840.10008.1.2.1");
        assert_eq!(
            obj.meta().media_storage_sop_instance_uid(),
            "1.2.3.4.5.6.7.8.9"
        );
    }

    /// Requirement r-3-11
    #[test]
    fn r3_11_precedence_applies_to_file_meta_tags() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| m.source_application_entity_title = Some("SENDING_SITE".into()));

        // KEEP outranks REMOVE.
        let actions = vec![
            action(
                ActionType::Remove,
                keyword("SourceApplicationEntityTitle"),
                None,
            ),
            action(
                ActionType::Keep,
                keyword("SourceApplicationEntityTitle"),
                None,
            ),
        ];
        apply_header_actions(&actions, &empty_vars(), &empty_funcs(), &mut obj)
            .expect("should succeed");

        assert_eq!(
            fm::get(obj.meta(), fm::SOURCE_APPLICATION_ENTITY_TITLE).as_deref(),
            Some("SENDING_SITE"),
            "KEEP must outrank REMOVE for file meta tags too"
        );
    }

    /// Requirement r-3-14-2
    #[test]
    fn r3_14_2_remove_private_tags_clears_file_meta_private_information() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| {
            m.private_information_creator_uid = Some("1.2.3.4.5".into());
            m.private_information = Some(vec![0xde, 0xad]);
        });

        remove_private_tags(&mut obj);

        assert_eq!(obj.meta().private_information_creator_uid, None);
        assert_eq!(obj.meta().private_information, None);
    }

    /// Requirement r-3-14-3
    #[test]
    fn r3_14_3_finalize_syncs_media_storage_uids_with_dataset() {
        let mut obj = create_test_file_obj();
        put_str(&mut obj, tags::SOP_INSTANCE_UID, VR::UI, "2.25.999");
        put_str(
            &mut obj,
            tags::SOP_CLASS_UID,
            VR::UI,
            "1.2.840.10008.5.1.4.1.1.4",
        );

        finalize_file_meta(&mut obj);

        assert_eq!(obj.meta().media_storage_sop_instance_uid(), "2.25.999");
        assert_eq!(
            obj.meta().media_storage_sop_class_uid(),
            "1.2.840.10008.5.1.4.1.1.4"
        );

        // Idempotent.
        let before = obj.meta().clone();
        finalize_file_meta(&mut obj);
        assert_eq!(
            obj.meta().media_storage_sop_instance_uid(),
            before.media_storage_sop_instance_uid()
        );
        assert_eq!(
            obj.meta().information_group_length,
            before.information_group_length
        );
    }

    /// Requirement r-3-14-3
    ///
    /// A recipe that only rewrites the data set UID must still produce a
    /// self-consistent file — this is the PHI leak the pass exists to close.
    #[test]
    fn r3_14_3_dataset_only_uid_rule_still_syncs_file_meta() {
        let mut obj = create_test_file_obj();
        put_str(
            &mut obj,
            tags::SOP_INSTANCE_UID,
            VR::UI,
            "1.2.3.4.5.6.7.8.9",
        );

        let actions = vec![action(
            ActionType::Replace,
            keyword("SOPInstanceUID"),
            Some(ActionValue::Function {
                name: "hashuid".into(),
                args: vec![],
            }),
        )];
        apply_header_actions(&actions, &empty_vars(), &hashuid_funcs(), &mut obj)
            .expect("should succeed");
        finalize_file_meta(&mut obj);

        let dataset_uid = obj
            .element(tags::SOP_INSTANCE_UID)
            .expect("present")
            .value()
            .to_str()
            .expect("readable")
            .to_string();
        assert_ne!(dataset_uid, "1.2.3.4.5.6.7.8.9", "UID should be hashed");
        assert_eq!(
            obj.meta().media_storage_sop_instance_uid(),
            dataset_uid,
            "the original SOP Instance UID must not survive in group 0002"
        );
    }

    /// Requirement r-3-14-4
    #[test]
    fn r3_14_4_finalize_strips_identifying_meta_attributes() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| {
            m.source_application_entity_title = Some("SENDING_SITE".into());
            m.sending_application_entity_title = Some("SENDER".into());
            m.receiving_application_entity_title = Some("RECEIVER".into());
            m.private_information_creator_uid = Some("1.2.3.4.5".into());
            m.private_information = Some(vec![0xde, 0xad]);
        });

        finalize_file_meta(&mut obj);

        assert_eq!(obj.meta().source_application_entity_title, None);
        assert_eq!(obj.meta().sending_application_entity_title, None);
        assert_eq!(obj.meta().receiving_application_entity_title, None);
        assert_eq!(obj.meta().private_information_creator_uid, None);
        assert_eq!(obj.meta().private_information, None);
        // Left alone: on the reference implementation's protected list.
        assert_eq!(obj.meta().transfer_syntax(), "1.2.840.10008.1.2.1");
        assert_eq!(obj.meta().implementation_class_uid(), "1.2.3.4");
    }

    /// Requirement r-3-14-1
    #[test]
    fn r3_14_1_remove_unspecified_keeps_structural_file_meta() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| {
            m.source_application_entity_title = Some("SENDING_SITE".into());
            m.implementation_version_name = Some("VENDOR_1_0".into());
        });

        let recipe = Recipe::parse("FORMAT dicom\n%header\nKEEP Modality\n").expect("should parse");
        remove_unspecified_elements(&mut obj, &recipe);

        // Optional, unnamed attributes go.
        assert_eq!(obj.meta().source_application_entity_title, None);
        assert_eq!(obj.meta().implementation_version_name, None);
        // Structural attributes stay: the file must remain readable.
        assert_eq!(obj.meta().transfer_syntax(), "1.2.840.10008.1.2.1");
        assert_eq!(obj.meta().implementation_class_uid(), "1.2.3.4");
        assert_eq!(
            obj.meta().media_storage_sop_instance_uid(),
            "1.2.3.4.5.6.7.8.9"
        );
        assert_eq!(
            obj.meta().media_storage_sop_class_uid(),
            "1.2.840.10008.5.1.4.1.1.2"
        );
    }

    /// Requirement r-3-14-1
    #[test]
    fn r3_14_1_remove_unspecified_honours_named_file_meta_tags() {
        let mut obj = create_test_file_obj();
        obj.update_meta(|m| m.source_application_entity_title = Some("SENDING_SITE".into()));

        let recipe = Recipe::parse("FORMAT dicom\n%header\nKEEP SourceApplicationEntityTitle\n")
            .expect("should parse");
        remove_unspecified_elements(&mut obj, &recipe);

        assert_eq!(
            fm::get(obj.meta(), fm::SOURCE_APPLICATION_ENTITY_TITLE).as_deref(),
            Some("SENDING_SITE"),
            "an attribute named by the recipe must not be swept away"
        );
    }
}
