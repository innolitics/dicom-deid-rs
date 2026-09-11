//! Tag-addressable access to the DICOM File Meta Information group (0002).
//!
//! `dicom_object::meta::FileMetaTable` models the file meta group as a struct of
//! named fields rather than an element map, so the de-identification code cannot
//! reach it the way it reaches the main data set (`obj.element(tag)` /
//! `obj.put(...)`). This module provides the missing bridge: read, write, and
//! remove group-0002 attributes by `Tag`.
//!
//! `FileMetaTable` does implement `dicom_core::ops::ApplyOp`, but it is not used
//! here: it fails rather than no-ops when asked to remove a Type-1 attribute, and
//! it has no arm for `PrivateInformation` (0002,0102).

use dicom_core::{Tag, VR};
use dicom_object::meta::FileMetaTable;

/// The DICOM File Meta Information group number.
pub const GROUP: u16 = 0x0002;

pub const FILE_META_INFORMATION_GROUP_LENGTH: Tag = Tag(0x0002, 0x0000);
pub const FILE_META_INFORMATION_VERSION: Tag = Tag(0x0002, 0x0001);
pub const MEDIA_STORAGE_SOP_CLASS_UID: Tag = Tag(0x0002, 0x0002);
pub const MEDIA_STORAGE_SOP_INSTANCE_UID: Tag = Tag(0x0002, 0x0003);
pub const TRANSFER_SYNTAX_UID: Tag = Tag(0x0002, 0x0010);
pub const IMPLEMENTATION_CLASS_UID: Tag = Tag(0x0002, 0x0012);
pub const IMPLEMENTATION_VERSION_NAME: Tag = Tag(0x0002, 0x0013);
pub const SOURCE_APPLICATION_ENTITY_TITLE: Tag = Tag(0x0002, 0x0016);
pub const SENDING_APPLICATION_ENTITY_TITLE: Tag = Tag(0x0002, 0x0017);
pub const RECEIVING_APPLICATION_ENTITY_TITLE: Tag = Tag(0x0002, 0x0018);
pub const PRIVATE_INFORMATION_CREATOR_UID: Tag = Tag(0x0002, 0x0100);
pub const PRIVATE_INFORMATION: Tag = Tag(0x0002, 0x0102);

/// Group 0002 tags that de-identification must never modify.
///
/// Mirrors the skip list used by the reference python `deid` implementation
/// (`resources/deid/deid/dicom/config.json`). `TransferSyntaxUID` is additionally
/// owned by the pixel pipeline, which rewrites it after decompression
/// (`crate::pixel::decompress_pixel_data`).
pub const PROTECTED_TAGS: &[Tag] = &[
    FILE_META_INFORMATION_GROUP_LENGTH,
    FILE_META_INFORMATION_VERSION,
    TRANSFER_SYNTAX_UID,
    IMPLEMENTATION_CLASS_UID,
];

/// Group 0002 attributes that are Type 1 (required, non-empty) and therefore
/// cannot be removed from a conformant file.
pub const REQUIRED_TAGS: &[Tag] = &[
    MEDIA_STORAGE_SOP_CLASS_UID,
    MEDIA_STORAGE_SOP_INSTANCE_UID,
    TRANSFER_SYNTAX_UID,
    IMPLEMENTATION_CLASS_UID,
];

/// Every group-0002 attribute representable by `FileMetaTable`, in ascending
/// tag order.
const ALL_TAGS: &[Tag] = &[
    FILE_META_INFORMATION_GROUP_LENGTH,
    FILE_META_INFORMATION_VERSION,
    MEDIA_STORAGE_SOP_CLASS_UID,
    MEDIA_STORAGE_SOP_INSTANCE_UID,
    TRANSFER_SYNTAX_UID,
    IMPLEMENTATION_CLASS_UID,
    IMPLEMENTATION_VERSION_NAME,
    SOURCE_APPLICATION_ENTITY_TITLE,
    SENDING_APPLICATION_ENTITY_TITLE,
    RECEIVING_APPLICATION_ENTITY_TITLE,
    PRIVATE_INFORMATION_CREATOR_UID,
    PRIVATE_INFORMATION,
];

/// Whether `tag` belongs to the File Meta Information group.
pub fn is_file_meta(tag: Tag) -> bool {
    tag.group() == GROUP
}

/// Whether `tag` is protected from de-identification actions.
pub fn is_protected(tag: Tag) -> bool {
    PROTECTED_TAGS.contains(&tag)
}

/// Whether `tag` is a Type-1 file meta attribute that cannot be removed.
pub fn is_required(tag: Tag) -> bool {
    REQUIRED_TAGS.contains(&tag)
}

/// The value representation of a group-0002 attribute, or `None` if the tag is
/// not one `FileMetaTable` can represent.
pub fn vr_for(tag: Tag) -> Option<VR> {
    Some(match tag {
        FILE_META_INFORMATION_GROUP_LENGTH => VR::UL,
        FILE_META_INFORMATION_VERSION | PRIVATE_INFORMATION => VR::OB,
        MEDIA_STORAGE_SOP_CLASS_UID
        | MEDIA_STORAGE_SOP_INSTANCE_UID
        | TRANSFER_SYNTAX_UID
        | IMPLEMENTATION_CLASS_UID
        | PRIVATE_INFORMATION_CREATOR_UID => VR::UI,
        IMPLEMENTATION_VERSION_NAME => VR::SH,
        SOURCE_APPLICATION_ENTITY_TITLE
        | SENDING_APPLICATION_ENTITY_TITLE
        | RECEIVING_APPLICATION_ENTITY_TITLE => VR::AE,
        _ => return None,
    })
}

/// Trim the trailing NUL / whitespace padding DICOM uses to pad string values to
/// an even byte length.
fn trim_padding(s: &str) -> &str {
    s.trim_end_matches(|c: char| c.is_whitespace() || c == '\0')
}

/// Pad a value to an even byte length using the padding character for its VR,
/// matching what `FileMetaTableBuilder` stores (`ui_padded` / `txt_padded` in
/// dicom-object). UI values pad with NUL, text values with a space.
fn pad_for(tag: Tag, value: &str) -> String {
    let mut out = value.to_string();
    if !out.len().is_multiple_of(2) {
        out.push(if vr_for(tag) == Some(VR::UI) {
            '\0'
        } else {
            ' '
        });
    }
    out
}

/// The group-0002 tags actually present in `meta`, in ascending tag order.
///
/// This is the file meta analogue of `obj.iter().map(|e| e.tag())`, and is what
/// pattern and group-range tag specifiers match against.
pub fn present_tags(meta: &FileMetaTable) -> Vec<Tag> {
    ALL_TAGS
        .iter()
        .copied()
        .filter(|tag| is_present(meta, *tag))
        .collect()
}

/// Whether `meta` carries a value for `tag`.
///
/// The Type-1 attributes and the group length are always present. `PrivateInformation`
/// is binary, so it has no string value but can still be present and removable.
pub fn is_present(meta: &FileMetaTable, tag: Tag) -> bool {
    match tag {
        FILE_META_INFORMATION_GROUP_LENGTH | FILE_META_INFORMATION_VERSION => true,
        MEDIA_STORAGE_SOP_CLASS_UID
        | MEDIA_STORAGE_SOP_INSTANCE_UID
        | TRANSFER_SYNTAX_UID
        | IMPLEMENTATION_CLASS_UID => true,
        IMPLEMENTATION_VERSION_NAME => meta.implementation_version_name.is_some(),
        SOURCE_APPLICATION_ENTITY_TITLE => meta.source_application_entity_title.is_some(),
        SENDING_APPLICATION_ENTITY_TITLE => meta.sending_application_entity_title.is_some(),
        RECEIVING_APPLICATION_ENTITY_TITLE => meta.receiving_application_entity_title.is_some(),
        PRIVATE_INFORMATION_CREATOR_UID => meta.private_information_creator_uid.is_some(),
        PRIVATE_INFORMATION => meta.private_information.is_some(),
        _ => false,
    }
}

/// The current string value of a group-0002 attribute, with DICOM padding
/// trimmed.
///
/// Trimming matters: an input file may pad `(0002,0003)` with a trailing NUL to
/// reach an even length, and hashing the untrimmed value would yield a different
/// UID than hashing the same UID read from the data set.
///
/// Returns `None` for absent attributes and for the two binary attributes
/// (`FileMetaInformationVersion`, `PrivateInformation`), which have no string
/// representation.
pub fn get(meta: &FileMetaTable, tag: Tag) -> Option<String> {
    let value = match tag {
        FILE_META_INFORMATION_GROUP_LENGTH => {
            return Some(meta.information_group_length.to_string());
        }
        MEDIA_STORAGE_SOP_CLASS_UID => meta.media_storage_sop_class_uid.as_str(),
        MEDIA_STORAGE_SOP_INSTANCE_UID => meta.media_storage_sop_instance_uid.as_str(),
        TRANSFER_SYNTAX_UID => meta.transfer_syntax.as_str(),
        IMPLEMENTATION_CLASS_UID => meta.implementation_class_uid.as_str(),
        IMPLEMENTATION_VERSION_NAME => meta.implementation_version_name.as_deref()?,
        SOURCE_APPLICATION_ENTITY_TITLE => meta.source_application_entity_title.as_deref()?,
        SENDING_APPLICATION_ENTITY_TITLE => meta.sending_application_entity_title.as_deref()?,
        RECEIVING_APPLICATION_ENTITY_TITLE => meta.receiving_application_entity_title.as_deref()?,
        PRIVATE_INFORMATION_CREATOR_UID => meta.private_information_creator_uid.as_deref()?,
        _ => return None,
    };
    Some(trim_padding(value).to_string())
}

/// Set the value of a group-0002 attribute, padded to an even byte length the
/// same way `FileMetaTableBuilder` does.
///
/// Returns `false` if `tag` is not a string-valued attribute that `FileMetaTable`
/// can represent. The caller is responsible for recalculating the group length
/// (use `FileDicomObject::update_meta`, which does so automatically).
pub fn set(meta: &mut FileMetaTable, tag: Tag, value: &str) -> bool {
    if vr_for(tag).is_none() || matches!(tag, FILE_META_INFORMATION_VERSION | PRIVATE_INFORMATION) {
        return false;
    }
    let padded = pad_for(tag, value);
    match tag {
        MEDIA_STORAGE_SOP_CLASS_UID => meta.media_storage_sop_class_uid = padded,
        MEDIA_STORAGE_SOP_INSTANCE_UID => meta.media_storage_sop_instance_uid = padded,
        TRANSFER_SYNTAX_UID => meta.transfer_syntax = padded,
        IMPLEMENTATION_CLASS_UID => meta.implementation_class_uid = padded,
        IMPLEMENTATION_VERSION_NAME => meta.implementation_version_name = Some(padded),
        SOURCE_APPLICATION_ENTITY_TITLE => meta.source_application_entity_title = Some(padded),
        SENDING_APPLICATION_ENTITY_TITLE => meta.sending_application_entity_title = Some(padded),
        RECEIVING_APPLICATION_ENTITY_TITLE => {
            meta.receiving_application_entity_title = Some(padded)
        }
        PRIVATE_INFORMATION_CREATOR_UID => meta.private_information_creator_uid = Some(padded),
        _ => return false,
    }
    true
}

/// Remove a group-0002 attribute.
///
/// Type-1 attributes (`REQUIRED_TAGS`) and the group length are structural and are
/// left untouched, returning `false`. Optional attributes are cleared to `None`.
pub fn remove(meta: &mut FileMetaTable, tag: Tag) -> bool {
    match tag {
        IMPLEMENTATION_VERSION_NAME => meta.implementation_version_name = None,
        SOURCE_APPLICATION_ENTITY_TITLE => meta.source_application_entity_title = None,
        SENDING_APPLICATION_ENTITY_TITLE => meta.sending_application_entity_title = None,
        RECEIVING_APPLICATION_ENTITY_TITLE => meta.receiving_application_entity_title = None,
        PRIVATE_INFORMATION_CREATOR_UID => meta.private_information_creator_uid = None,
        PRIVATE_INFORMATION => meta.private_information = None,
        _ => return false,
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use dicom_object::meta::FileMetaTableBuilder;

    fn meta() -> FileMetaTable {
        FileMetaTableBuilder::new()
            .transfer_syntax("1.2.840.10008.1.2.1")
            .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.2")
            .media_storage_sop_instance_uid("1.2.3.4.5.6.7.8.9")
            .implementation_class_uid("1.2.3.4")
            .implementation_version_name("DEID_TEST")
            .source_application_entity_title("SENDING_SITE")
            .build()
            .expect("valid file meta")
    }

    #[test]
    fn is_file_meta_matches_group_0002_only() {
        assert!(is_file_meta(MEDIA_STORAGE_SOP_INSTANCE_UID));
        assert!(!is_file_meta(Tag(0x0008, 0x0018)));
    }

    #[test]
    fn protected_tags_match_reference_skip_list() {
        assert!(is_protected(TRANSFER_SYNTAX_UID));
        assert!(is_protected(IMPLEMENTATION_CLASS_UID));
        assert!(is_protected(FILE_META_INFORMATION_GROUP_LENGTH));
        assert!(is_protected(FILE_META_INFORMATION_VERSION));
        assert!(!is_protected(MEDIA_STORAGE_SOP_INSTANCE_UID));
        assert!(!is_protected(SOURCE_APPLICATION_ENTITY_TITLE));
    }

    #[test]
    fn get_returns_current_values() {
        let m = meta();
        assert_eq!(
            get(&m, MEDIA_STORAGE_SOP_INSTANCE_UID).as_deref(),
            Some("1.2.3.4.5.6.7.8.9")
        );
        assert_eq!(
            get(&m, SOURCE_APPLICATION_ENTITY_TITLE).as_deref(),
            Some("SENDING_SITE")
        );
    }

    #[test]
    fn get_trims_dicom_padding() {
        let mut m = meta();
        m.media_storage_sop_instance_uid = "1.2.3.4.5.6.7.8.9\0".to_string();
        assert_eq!(
            get(&m, MEDIA_STORAGE_SOP_INSTANCE_UID).as_deref(),
            Some("1.2.3.4.5.6.7.8.9"),
            "trailing NUL padding must be trimmed so hashes match the data set"
        );
    }

    #[test]
    fn get_returns_none_for_absent_and_binary_attributes() {
        let m = meta();
        assert_eq!(get(&m, SENDING_APPLICATION_ENTITY_TITLE), None);
        assert_eq!(get(&m, PRIVATE_INFORMATION), None);
        assert_eq!(get(&m, FILE_META_INFORMATION_VERSION), None);
    }

    #[test]
    fn set_updates_required_and_optional_attributes() {
        let mut m = meta();
        assert!(set(&mut m, MEDIA_STORAGE_SOP_INSTANCE_UID, "2.25.1"));
        assert_eq!(
            get(&m, MEDIA_STORAGE_SOP_INSTANCE_UID).as_deref(),
            Some("2.25.1")
        );

        assert!(set(&mut m, SENDING_APPLICATION_ENTITY_TITLE, "AE1"));
        assert_eq!(
            get(&m, SENDING_APPLICATION_ENTITY_TITLE).as_deref(),
            Some("AE1")
        );
    }

    #[test]
    fn set_pads_odd_length_values_to_even_length() {
        let mut m = meta();
        // 7 characters — must be NUL-padded, as FileMetaTableBuilder does for UI.
        assert!(set(&mut m, MEDIA_STORAGE_SOP_INSTANCE_UID, "2.25.12"));
        assert_eq!(m.media_storage_sop_instance_uid, "2.25.12\0");
        assert_eq!(
            get(&m, MEDIA_STORAGE_SOP_INSTANCE_UID).as_deref(),
            Some("2.25.12"),
            "padding must round-trip away through get"
        );

        // Text VRs pad with a space instead.
        assert!(set(&mut m, SENDING_APPLICATION_ENTITY_TITLE, "AE1"));
        assert_eq!(m.sending_application_entity_title.as_deref(), Some("AE1 "));
    }

    #[test]
    fn set_refuses_binary_and_unknown_attributes() {
        let mut m = meta();
        assert!(!set(&mut m, PRIVATE_INFORMATION, "x"));
        assert!(!set(&mut m, Tag(0x0002, 0x0099), "x"));
    }

    #[test]
    fn remove_clears_optional_attributes() {
        let mut m = meta();
        assert!(remove(&mut m, SOURCE_APPLICATION_ENTITY_TITLE));
        assert_eq!(m.source_application_entity_title, None);
    }

    #[test]
    fn remove_leaves_type_1_attributes_intact() {
        let mut m = meta();
        for tag in REQUIRED_TAGS {
            assert!(!remove(&mut m, *tag), "{tag} must not be removable");
        }
        assert_eq!(
            get(&m, MEDIA_STORAGE_SOP_INSTANCE_UID).as_deref(),
            Some("1.2.3.4.5.6.7.8.9")
        );
        assert_eq!(
            get(&m, TRANSFER_SYNTAX_UID).as_deref(),
            Some("1.2.840.10008.1.2.1")
        );
    }

    #[test]
    fn present_tags_reports_only_populated_attributes() {
        let tags = present_tags(&meta());
        assert!(tags.contains(&MEDIA_STORAGE_SOP_INSTANCE_UID));
        assert!(tags.contains(&SOURCE_APPLICATION_ENTITY_TITLE));
        assert!(!tags.contains(&SENDING_APPLICATION_ENTITY_TITLE));
        assert!(!tags.contains(&PRIVATE_INFORMATION));

        let mut sorted = tags.clone();
        sorted.sort();
        assert_eq!(tags, sorted, "present_tags must be in ascending tag order");
    }

    #[test]
    fn vr_for_covers_every_representable_attribute() {
        for tag in ALL_TAGS {
            assert!(vr_for(*tag).is_some(), "missing VR for {tag}");
        }
        assert_eq!(vr_for(Tag(0x0002, 0x0099)), None);
    }
}
