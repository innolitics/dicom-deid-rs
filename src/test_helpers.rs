use dicom_core::value::{DataSetSequence, PrimitiveValue, Value};
use dicom_core::{DataElement, Length, Tag, VR};
use dicom_object::meta::FileMetaTableBuilder;
use dicom_object::{FileDicomObject, InMemDicomObject};

/// Create an empty in-memory DICOM object for testing.
pub fn create_test_obj() -> InMemDicomObject {
    InMemDicomObject::new_empty()
}

/// Insert a string-valued element into a DICOM object.
pub fn put_str(obj: &mut InMemDicomObject, tag: Tag, vr: VR, value: &str) {
    obj.put(DataElement::new(
        tag,
        vr,
        Value::Primitive(PrimitiveValue::from(value)),
    ));
}

/// Insert an element with an empty value into a DICOM object.
pub fn put_empty(obj: &mut InMemDicomObject, tag: Tag, vr: VR) {
    obj.put(DataElement::new(
        tag,
        vr,
        Value::Primitive(PrimitiveValue::Empty),
    ));
}

/// Insert a sequence element into a DICOM object.
pub fn put_sequence(obj: &mut InMemDicomObject, tag: Tag, items: Vec<InMemDicomObject>) {
    obj.put(DataElement::new(
        tag,
        VR::SQ,
        Value::from(DataSetSequence::new(items, Length::UNDEFINED)),
    ));
}

/// Insert a u16-valued element into a DICOM object (e.g. for Rows/Columns).
pub fn put_u16(obj: &mut InMemDicomObject, tag: Tag, vr: VR, value: u16) {
    obj.put(DataElement::new(
        tag,
        vr,
        Value::Primitive(PrimitiveValue::from(value)),
    ));
}

/// Create a `FileDicomObject` with Explicit VR Little Endian transfer syntax
/// for testing decompression and file-level operations.
pub fn create_test_file_obj() -> FileDicomObject<InMemDicomObject> {
    FileDicomObject::new_empty_with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax("1.2.840.10008.1.2.1")
            .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.2")
            .media_storage_sop_instance_uid("1.2.3.4.5.6.7.8.9")
            .implementation_class_uid("1.2.3.4")
            .build()
            .expect("valid file meta"),
    )
}
