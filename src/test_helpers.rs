use dicom_core::value::{PrimitiveValue, Value};
use dicom_core::{DataElement, Tag, VR};
use dicom_object::InMemDicomObject;

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

/// Insert a u16-valued element into a DICOM object (e.g. for Rows/Columns).
pub fn put_u16(obj: &mut InMemDicomObject, tag: Tag, vr: VR, value: u16) {
    obj.put(DataElement::new(
        tag,
        vr,
        Value::Primitive(PrimitiveValue::from(value)),
    ));
}
