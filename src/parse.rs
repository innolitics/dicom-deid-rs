//! Tolerant parsing of DICOM Part 10 files.
//!
//! `dicom_object::from_reader` rejects files whose file meta group breaks the
//! rules in PS3.10 (missing or wrong group length, non-standard version element
//! length, implicit VR meta elements). Such files are common in the wild and
//! are accepted by most viewers, including the JavaScript parser used at upload
//! time. [`read_file`] first tries the strict reader and, if that fails, walks
//! the meta group leniently, rebuilds a standards-conforming meta table, and
//! reads the data set with the declared transfer syntax.

use std::error::Error;
use std::io::Cursor;

use dicom_dictionary_std::tags;
use dicom_encoding::Codec;
use dicom_object::meta::FileMetaTableBuilder;
use dicom_object::{FileDicomObject, InMemDicomObject};
use dicom_transfer_syntax_registry::{TransferSyntaxIndex, TransferSyntaxRegistry};

use crate::error::DeidError;

const DICM_MAGIC: &[u8; 4] = b"DICM";
const PREAMBLE_LEN: usize = 128;
const UNDEFINED_LENGTH: u32 = 0xFFFF_FFFF;
const META_GROUP: u16 = 0x0002;

/// Read a DICOM file from memory, falling back to a lenient meta group parse
/// when the strict reader rejects the file.
pub fn read_file(bytes: &[u8]) -> Result<FileDicomObject<InMemDicomObject>, DeidError> {
    let strict_error = match dicom_object::from_reader(Cursor::new(bytes)) {
        Ok(obj) => return Ok(obj),
        Err(e) => e,
    };
    read_file_lenient(bytes).map_err(|lenient_error| {
        DeidError::Dicom(format!(
            "Failed to parse DICOM: {}; lenient fallback also failed: {}",
            error_chain(&strict_error),
            lenient_error
        ))
    })
}

/// Format an error together with its chain of sources, innermost last.
pub fn error_chain(error: &dyn Error) -> String {
    let mut parts = vec![error.to_string()];
    let mut source = error.source();
    while let Some(inner) = source {
        parts.push(inner.to_string());
        source = inner.source();
    }
    parts.join(" <- ")
}

#[derive(Default)]
struct LenientMeta {
    transfer_syntax: Option<String>,
    media_storage_sop_class_uid: Option<String>,
    media_storage_sop_instance_uid: Option<String>,
    implementation_class_uid: Option<String>,
    implementation_version_name: Option<String>,
    data_set_offset: usize,
}

struct ElementHeader {
    group: u16,
    element: u16,
    value_offset: usize,
    length: u32,
}

fn read_file_lenient(bytes: &[u8]) -> Result<FileDicomObject<InMemDicomObject>, DeidError> {
    let meta = read_meta_lenient(bytes)?;

    let ts_uid = meta
        .transfer_syntax
        .clone()
        .ok_or_else(|| DeidError::Dicom("file meta group has no TransferSyntaxUID".into()))?;
    let ts = TransferSyntaxRegistry
        .get(&ts_uid)
        .ok_or_else(|| DeidError::UnsupportedFormat(format!("transfer syntax {ts_uid}")))?;
    if matches!(ts.codec(), Codec::Dataset(_)) {
        return Err(DeidError::UnsupportedFormat(format!(
            "transfer syntax {ts_uid} requires a data set codec"
        )));
    }

    let data_set =
        InMemDicomObject::read_dataset_with_ts(Cursor::new(&bytes[meta.data_set_offset..]), ts)
            .map_err(|e| {
                DeidError::Dicom(format!("could not read data set: {}", error_chain(&e)))
            })?;

    let string_tag = |tag| {
        data_set
            .element(tag)
            .ok()
            .and_then(|e| e.value().to_str().ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };
    let mut builder = FileMetaTableBuilder::new()
        .transfer_syntax(ts_uid)
        .media_storage_sop_class_uid(
            meta.media_storage_sop_class_uid
                .or_else(|| string_tag(tags::SOP_CLASS_UID))
                .unwrap_or_default(),
        )
        .media_storage_sop_instance_uid(
            meta.media_storage_sop_instance_uid
                .or_else(|| string_tag(tags::SOP_INSTANCE_UID))
                .unwrap_or_default(),
        );
    if let Some(uid) = meta.implementation_class_uid {
        builder = builder.implementation_class_uid(uid);
    }
    if let Some(name) = meta.implementation_version_name {
        builder = builder.implementation_version_name(name);
    }
    let meta_table = builder
        .build()
        .map_err(|e| DeidError::Dicom(format!("could not rebuild file meta group: {e}")))?;

    let mut file = FileDicomObject::new_empty_with_meta(meta_table);
    for element in data_set {
        file.put(element);
    }
    Ok(file)
}

/// Walk every group 0002 element after the `DICM` magic code without trusting
/// the group length, keeping only the attributes needed to rebuild the meta table.
fn read_meta_lenient(bytes: &[u8]) -> Result<LenientMeta, DeidError> {
    let mut position = find_meta_start(bytes)
        .ok_or_else(|| DeidError::Dicom("DICM magic code not found".into()))?;
    let mut meta = LenientMeta::default();

    while let Some(header) = read_element_header(bytes, position) {
        if header.group != META_GROUP {
            break;
        }
        if header.length == UNDEFINED_LENGTH {
            return Err(DeidError::Dicom(format!(
                "meta element (0002,{:04X}) has undefined length",
                header.element
            )));
        }
        let value_end = header.value_offset + header.length as usize;
        if value_end > bytes.len() {
            return Err(DeidError::Dicom(format!(
                "meta element (0002,{:04X}) extends past end of file",
                header.element
            )));
        }
        let value = &bytes[header.value_offset..value_end];
        match header.element {
            0x0002 => meta.media_storage_sop_class_uid = Some(decode_string(value)),
            0x0003 => meta.media_storage_sop_instance_uid = Some(decode_string(value)),
            0x0010 => meta.transfer_syntax = Some(decode_string(value)),
            0x0012 => meta.implementation_class_uid = Some(decode_string(value)),
            0x0013 => meta.implementation_version_name = Some(decode_string(value)),
            _ => {}
        }
        position = value_end;
    }

    meta.data_set_offset = position;
    Ok(meta)
}

fn find_meta_start(bytes: &[u8]) -> Option<usize> {
    let after_preamble = PREAMBLE_LEN + DICM_MAGIC.len();
    if bytes.len() >= after_preamble && &bytes[PREAMBLE_LEN..after_preamble] == DICM_MAGIC {
        return Some(after_preamble);
    }
    if bytes.len() >= DICM_MAGIC.len() && &bytes[..DICM_MAGIC.len()] == DICM_MAGIC {
        return Some(DICM_MAGIC.len());
    }
    None
}

/// Decode a little endian element header, accepting both explicit and
/// implicit VR encodings. Returns `None` when there are not enough bytes left.
fn read_element_header(bytes: &[u8], position: usize) -> Option<ElementHeader> {
    let at = |offset: usize, len: usize| bytes.get(position + offset..position + offset + len);
    let group = u16::from_le_bytes(at(0, 2)?.try_into().ok()?);
    let element = u16::from_le_bytes(at(2, 2)?.try_into().ok()?);
    let vr = at(4, 2)?;

    if !vr.iter().all(u8::is_ascii_uppercase) {
        let length = u32::from_le_bytes(at(4, 4)?.try_into().ok()?);
        return Some(ElementHeader {
            group,
            element,
            value_offset: position + 8,
            length,
        });
    }

    let has_long_length = matches!(
        vr,
        b"OB"
            | b"OD"
            | b"OF"
            | b"OL"
            | b"OV"
            | b"OW"
            | b"SQ"
            | b"SV"
            | b"UC"
            | b"UN"
            | b"UR"
            | b"UT"
            | b"UV"
    );
    if has_long_length {
        let length = u32::from_le_bytes(at(8, 4)?.try_into().ok()?);
        Some(ElementHeader {
            group,
            element,
            value_offset: position + 12,
            length,
        })
    } else {
        let length = u16::from_le_bytes(at(6, 2)?.try_into().ok()?) as u32;
        Some(ElementHeader {
            group,
            element,
            value_offset: position + 8,
            length,
        })
    }
}

fn decode_string(value: &[u8]) -> String {
    String::from_utf8_lossy(value)
        .trim_end_matches(|c: char| c == '\0' || c.is_whitespace())
        .to_string()
}
