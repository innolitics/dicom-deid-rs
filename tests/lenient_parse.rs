//! Files whose meta group violates PS3.10 but that viewers (and the upload
//! page's JavaScript parser) accept must still be readable by the pipeline.

use std::io::Cursor;

use dicom_deid_rs::parse::read_file;
use dicom_dictionary_std::tags;

const EXPLICIT_VR_LE: &str = "1.2.840.10008.1.2.1";
const CT_SOP_CLASS: &str = "1.2.840.10008.5.1.4.1.1.2";
const SOP_INSTANCE: &str = "1.2.3.4.5.6.7.8.9";

fn padded_uid(uid: &str) -> Vec<u8> {
    let mut bytes = uid.as_bytes().to_vec();
    if bytes.len() % 2 == 1 {
        bytes.push(0);
    }
    bytes
}

fn explicit(group: u16, element: u16, vr: &[u8; 2], value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&group.to_le_bytes());
    out.extend_from_slice(&element.to_le_bytes());
    out.extend_from_slice(vr);
    if matches!(vr, b"OB" | b"OW" | b"UN" | b"SQ" | b"UT") {
        out.extend_from_slice(&[0, 0]);
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    } else {
        out.extend_from_slice(&(value.len() as u16).to_le_bytes());
    }
    out.extend_from_slice(value);
    out
}

fn implicit(group: u16, element: u16, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&group.to_le_bytes());
    out.extend_from_slice(&element.to_le_bytes());
    out.extend_from_slice(&(value.len() as u32).to_le_bytes());
    out.extend_from_slice(value);
    out
}

struct MetaOptions {
    version: Option<Vec<u8>>,
    media_storage_uids: bool,
    implicit_vr: bool,
}

impl Default for MetaOptions {
    fn default() -> Self {
        Self {
            version: Some(vec![0, 1]),
            media_storage_uids: true,
            implicit_vr: false,
        }
    }
}

fn meta_body(options: &MetaOptions) -> Vec<u8> {
    let element = |el: u16, vr: &[u8; 2], value: &[u8]| {
        if options.implicit_vr {
            implicit(0x0002, el, value)
        } else {
            explicit(0x0002, el, vr, value)
        }
    };
    let mut body = Vec::new();
    if let Some(version) = &options.version {
        body.extend(element(0x0001, b"OB", version));
    }
    if options.media_storage_uids {
        body.extend(element(0x0002, b"UI", &padded_uid(CT_SOP_CLASS)));
        body.extend(element(0x0003, b"UI", &padded_uid(SOP_INSTANCE)));
    }
    body.extend(element(0x0010, b"UI", &padded_uid(EXPLICIT_VR_LE)));
    body.extend(element(0x0012, b"UI", &padded_uid("1.2.3")));
    body
}

fn data_set() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(explicit(0x0008, 0x0016, b"UI", &padded_uid(CT_SOP_CLASS)));
    out.extend(explicit(0x0008, 0x0018, b"UI", &padded_uid(SOP_INSTANCE)));
    out.extend(explicit(0x0008, 0x0060, b"CS", b"CT"));
    out.extend(explicit(0x0010, 0x0020, b"LO", b"PID1"));
    out.extend(explicit(0x0020, 0x000D, b"UI", &padded_uid("1.2.3.4.5")));
    out.extend(explicit(0x0020, 0x000E, b"UI", &padded_uid("1.2.3.4.5.6")));
    out.extend(explicit(0x0028, 0x0010, b"US", &2u16.to_le_bytes()));
    out.extend(explicit(0x0028, 0x0011, b"US", &2u16.to_le_bytes()));
    out.extend(explicit(0x0028, 0x0100, b"US", &16u16.to_le_bytes()));
    out.extend(explicit(0x7FE0, 0x0010, b"OW", &[0u8; 8]));
    out
}

enum GroupLength {
    Correct,
    Missing,
    Value(u32),
}

fn dicom_file(options: MetaOptions, group_length: GroupLength, preamble: bool) -> Vec<u8> {
    let body = meta_body(&options);
    let mut file = Vec::new();
    if preamble {
        file.extend_from_slice(&[0u8; 128]);
    }
    file.extend_from_slice(b"DICM");
    match group_length {
        GroupLength::Correct => file.extend(explicit(
            0x0002,
            0x0000,
            b"UL",
            &(body.len() as u32).to_le_bytes(),
        )),
        GroupLength::Missing => {}
        GroupLength::Value(v) => file.extend(explicit(0x0002, 0x0000, b"UL", &v.to_le_bytes())),
    }
    file.extend(body);
    file.extend(data_set());
    file
}

fn assert_parsed_correctly(bytes: &[u8]) {
    let obj = read_file(bytes).expect("lenient reader should accept the file");
    assert_eq!(obj.meta().transfer_syntax(), EXPLICIT_VR_LE);
    assert_eq!(obj.meta().media_storage_sop_class_uid(), CT_SOP_CLASS);
    assert_eq!(obj.meta().media_storage_sop_instance_uid(), SOP_INSTANCE);
    assert_eq!(
        obj.element(tags::PATIENT_ID)
            .unwrap()
            .value()
            .to_str()
            .unwrap(),
        "PID1"
    );
    assert_eq!(
        obj.element(tags::PIXEL_DATA)
            .unwrap()
            .value()
            .to_bytes()
            .unwrap()
            .len(),
        8
    );

    // The rewritten file must satisfy the strict reader, so downstream tools
    // never see the original defect.
    let mut rewritten = Vec::new();
    obj.write_all(&mut rewritten).expect("write");
    let reread = dicom_object::from_reader(Cursor::new(&rewritten)).expect("strict reread");
    assert_eq!(reread.meta().media_storage_sop_instance_uid(), SOP_INSTANCE);
}

#[test]
fn strict_reader_still_handles_conforming_files() {
    let bytes = dicom_file(MetaOptions::default(), GroupLength::Correct, true);
    assert!(dicom_object::from_reader(Cursor::new(&bytes)).is_ok());
    assert_parsed_correctly(&bytes);
}

#[test]
fn missing_group_length_is_tolerated() {
    let bytes = dicom_file(MetaOptions::default(), GroupLength::Missing, true);
    assert!(dicom_object::from_reader(Cursor::new(&bytes)).is_err());
    assert_parsed_correctly(&bytes);
}

#[test]
fn zero_group_length_is_tolerated() {
    let bytes = dicom_file(MetaOptions::default(), GroupLength::Value(0), true);
    assert!(dicom_object::from_reader(Cursor::new(&bytes)).is_err());
    assert_parsed_correctly(&bytes);
}

#[test]
fn too_small_group_length_is_tolerated() {
    let half = meta_body(&MetaOptions::default()).len() as u32 / 2;
    let bytes = dicom_file(MetaOptions::default(), GroupLength::Value(half), true);
    assert!(dicom_object::from_reader(Cursor::new(&bytes)).is_err());
    assert_parsed_correctly(&bytes);
}

#[test]
fn too_large_group_length_is_tolerated() {
    let too_large = meta_body(&MetaOptions::default()).len() as u32 + 40;
    let bytes = dicom_file(MetaOptions::default(), GroupLength::Value(too_large), true);
    assert_parsed_correctly(&bytes);
}

#[test]
fn nonstandard_version_length_is_tolerated() {
    let options = MetaOptions {
        version: Some(vec![0, 1, 0, 0]),
        ..MetaOptions::default()
    };
    let bytes = dicom_file(options, GroupLength::Correct, true);
    assert!(dicom_object::from_reader(Cursor::new(&bytes)).is_err());
    assert_parsed_correctly(&bytes);
}

#[test]
fn implicit_vr_meta_group_is_tolerated() {
    let options = MetaOptions {
        implicit_vr: true,
        ..MetaOptions::default()
    };
    let bytes = dicom_file(options, GroupLength::Correct, true);
    assert!(dicom_object::from_reader(Cursor::new(&bytes)).is_err());
    assert_parsed_correctly(&bytes);
}

#[test]
fn media_storage_uids_are_backfilled_from_data_set() {
    let options = MetaOptions {
        media_storage_uids: false,
        ..MetaOptions::default()
    };
    let bytes = dicom_file(options, GroupLength::Missing, true);
    assert_parsed_correctly(&bytes);
}

#[test]
fn magic_code_without_preamble_is_tolerated() {
    let bytes = dicom_file(MetaOptions::default(), GroupLength::Missing, false);
    assert_parsed_correctly(&bytes);
}

#[test]
fn error_names_both_strict_cause_and_lenient_cause() {
    let error = read_file(b"this is not DICOM at all").expect_err("garbage must fail");
    let message = error.to_string();
    assert!(message.contains("Failed to parse DICOM"), "{message}");
    assert!(
        message.contains("lenient fallback also failed"),
        "{message}"
    );
    assert!(message.contains("DICM magic code not found"), "{message}");
}

#[test]
fn strict_error_chain_is_preserved_in_message() {
    let mut bytes = dicom_file(MetaOptions::default(), GroupLength::Missing, true);
    // Corrupt the transfer syntax so both readers fail
    let ts_position = bytes
        .windows(EXPLICIT_VR_LE.len())
        .position(|w| w == EXPLICIT_VR_LE.as_bytes())
        .unwrap();
    bytes[ts_position..ts_position + 3].copy_from_slice(b"9.9");
    let message = read_file(&bytes)
        .expect_err("unknown transfer syntax")
        .to_string();
    assert!(
        message.contains("Unexpected data element tagged (0002,0001)"),
        "{message}"
    );
    assert!(
        message.contains("transfer syntax 9.9.840.10008.1.2.1"),
        "{message}"
    );
}
