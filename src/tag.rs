use crate::error::DeidError;
use crate::recipe::TagSpecifier;
use dicom_core::Tag;
use dicom_object::InMemDicomObject;

/// Parse a tag string in parenthesized format "(GGGG,EEEE)" into a `Tag`.
pub fn parse_parenthesized_tag(s: &str) -> Result<Tag, DeidError> {
    todo!()
}

/// Parse a tag string in bare hex format "GGGGEEEE" into a `Tag`.
pub fn parse_bare_hex_tag(s: &str) -> Result<Tag, DeidError> {
    todo!()
}

/// Resolve a `TagSpecifier` into one or more concrete `Tag` values.
///
/// For pattern-based specifiers, the object is inspected to find all matching
/// tags. For keyword and direct tag specifiers, the result is a single tag.
pub fn resolve_tags(
    specifier: &TagSpecifier,
    obj: &InMemDicomObject,
) -> Result<Vec<Tag>, DeidError> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use dicom_core::{Tag, VR};
    use dicom_dictionary_std::tags;

    // -- r-3-4-1 -------------------------------------------------------------

    /// Requirement r-3-4-1
    #[test]
    fn r3_4_1_resolve_keyword_patient_id() {
        let obj = create_test_obj();
        let spec = TagSpecifier::Keyword("PatientID".into());
        let tags = resolve_tags(&spec, &obj).expect("should resolve");
        assert_eq!(tags, vec![tags::PATIENT_ID]);
    }

    /// Requirement r-3-4-1
    #[test]
    fn r3_4_1_resolve_keyword_patient_name() {
        let obj = create_test_obj();
        let spec = TagSpecifier::Keyword("PatientName".into());
        let tags = resolve_tags(&spec, &obj).expect("should resolve");
        assert_eq!(tags, vec![tags::PATIENT_NAME]);
    }

    // -- r-3-4-2 -------------------------------------------------------------

    /// Requirement r-3-4-2
    #[test]
    fn r3_4_2_parse_parenthesized_tag() {
        let tag = parse_parenthesized_tag("(0010,0020)").expect("should parse");
        assert_eq!(tag, Tag(0x0010, 0x0020));
    }

    /// Requirement r-3-4-2
    #[test]
    fn r3_4_2_parse_parenthesized_tag_uppercase() {
        let tag = parse_parenthesized_tag("(0010,0020)").expect("should parse");
        assert_eq!(tag, Tag(0x0010, 0x0020));
    }

    /// Requirement r-3-4-2
    #[test]
    fn r3_4_2_parse_bare_hex_tag() {
        let tag = parse_bare_hex_tag("00100020").expect("should parse");
        assert_eq!(tag, Tag(0x0010, 0x0020));
    }

    /// Requirement r-3-4-2
    #[test]
    fn r3_4_2_resolve_tag_value_specifier() {
        let obj = create_test_obj();
        let spec = TagSpecifier::TagValue(Tag(0x0010, 0x0020));
        let tags = resolve_tags(&spec, &obj).expect("should resolve");
        assert_eq!(tags, vec![Tag(0x0010, 0x0020)]);
    }

    // -- r-3-4-3 -------------------------------------------------------------

    /// Requirement r-3-4-3
    #[test]
    fn r3_4_3_resolve_private_tag() {
        let mut obj = create_test_obj();
        // Register a private creator block
        put_str(&mut obj, Tag(0x0009, 0x0010), VR::LO, "MY PRIVATE CREATOR");
        put_str(&mut obj, Tag(0x0009, 0x1001), VR::LO, "private value");

        let spec = TagSpecifier::PrivateTag {
            group: 0x0009,
            creator: "MY PRIVATE CREATOR".into(),
            element_offset: 0x01,
        };
        let tags = resolve_tags(&spec, &obj).expect("should resolve");
        // Should resolve to Tag(0x0009, 0x1001) since creator is at slot 0x10
        assert_eq!(tags, vec![Tag(0x0009, 0x1001)]);
    }

    // -- r-3-5 ---------------------------------------------------------------

    /// Requirement r-3-5
    #[test]
    fn r3_5_pattern_matches_multiple_tags() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "12345");
        put_str(&mut obj, tags::PATIENT_SEX, VR::CS, "M");
        put_str(&mut obj, tags::MODALITY, VR::CS, "CT");

        // Pattern matching tags whose keyword starts with "Patient"
        let spec = TagSpecifier::Pattern("Patient.*".into());
        let matched = resolve_tags(&spec, &obj).expect("should resolve");
        assert!(matched.contains(&tags::PATIENT_NAME));
        assert!(matched.contains(&tags::PATIENT_ID));
        assert!(matched.contains(&tags::PATIENT_SEX));
        assert!(
            !matched.contains(&tags::MODALITY),
            "Modality should not match Patient.* pattern"
        );
    }

    /// Requirement r-3-5
    #[test]
    fn r3_5_pattern_matches_by_tag_value() {
        let mut obj = create_test_obj();
        put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
        put_str(&mut obj, tags::PATIENT_ID, VR::LO, "12345");

        // Pattern matching tags by group 0010
        let spec = TagSpecifier::Pattern("(0010,.*)".into());
        let matched = resolve_tags(&spec, &obj).expect("should resolve");
        assert!(matched.contains(&tags::PATIENT_NAME));
        assert!(matched.contains(&tags::PATIENT_ID));
    }
}
