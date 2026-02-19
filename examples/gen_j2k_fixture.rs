/// Helper binary to generate the JPEG 2000 Lossless test fixture.
///
/// Run with: cargo run --example gen_j2k_fixture --features jpeg2000
///
/// This creates tests/fixtures/j2k_lossless_8x8.dcm — an 8x8 monochrome
/// DICOM image compressed with JPEG 2000 Lossless (transfer syntax
/// 1.2.840.10008.1.2.4.90).
use dicom_core::value::{PrimitiveValue, Value};
use dicom_core::{DataElement, VR};
use dicom_dictionary_std::tags;
use dicom_object::FileDicomObject;
use dicom_object::meta::FileMetaTableBuilder;

/// JPEG 2000 Lossless codestream for an 8x8 all-white (255) monochrome image.
/// Generated with Pillow (OpenJPEG 2.5.4, irreversible=False).
#[rustfmt::skip]
const J2K_DATA: [u8; 136] = [
    0xff, 0x4f, 0xff, 0x51, 0x00, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x08,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x08,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x07, 0x01, 0x01, 0xff, 0x52, 0x00,
    0x0c, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x04, 0x04, 0x00, 0x01, 0xff, 0x5c, 0x00, 0x0d, 0x40,
    0x40, 0x48, 0x48, 0x50, 0x48, 0x48, 0x50, 0x48, 0x48, 0x50, 0xff, 0x64, 0x00, 0x25, 0x00, 0x01,
    0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x64, 0x20, 0x62, 0x79, 0x20, 0x4f, 0x70, 0x65, 0x6e, 0x4a,
    0x50, 0x45, 0x47, 0x20, 0x76, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e, 0x20, 0x32, 0x2e, 0x35, 0x2e,
    0x34, 0xff, 0x90, 0x00, 0x0a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x01, 0xff, 0x93, 0xcf,
    0xb4, 0x04, 0x00, 0x80, 0x80, 0x80, 0xff, 0xd9,
];

fn main() {
    // JPEG 2000 Lossless transfer syntax
    let ts = "1.2.840.10008.1.2.4.90";

    let meta = FileMetaTableBuilder::new()
        .transfer_syntax(ts)
        .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.2") // CT Image Storage
        .media_storage_sop_instance_uid("1.2.3.4.5.6.7.8.99")
        .implementation_class_uid("1.2.3.4")
        .build()
        .expect("valid file meta");

    let mut obj = FileDicomObject::new_empty_with_meta(meta);

    // Image dimensions and pixel format
    obj.put(DataElement::new(
        tags::ROWS,
        VR::US,
        Value::Primitive(PrimitiveValue::from(8u16)),
    ));
    obj.put(DataElement::new(
        tags::COLUMNS,
        VR::US,
        Value::Primitive(PrimitiveValue::from(8u16)),
    ));
    obj.put(DataElement::new(
        tags::BITS_ALLOCATED,
        VR::US,
        Value::Primitive(PrimitiveValue::from(8u16)),
    ));
    obj.put(DataElement::new(
        tags::BITS_STORED,
        VR::US,
        Value::Primitive(PrimitiveValue::from(8u16)),
    ));
    obj.put(DataElement::new(
        tags::HIGH_BIT,
        VR::US,
        Value::Primitive(PrimitiveValue::from(7u16)),
    ));
    obj.put(DataElement::new(
        tags::PIXEL_REPRESENTATION,
        VR::US,
        Value::Primitive(PrimitiveValue::from(0u16)),
    ));
    obj.put(DataElement::new(
        tags::SAMPLES_PER_PIXEL,
        VR::US,
        Value::Primitive(PrimitiveValue::from(1u16)),
    ));
    obj.put(DataElement::new(
        tags::PHOTOMETRIC_INTERPRETATION,
        VR::CS,
        Value::Primitive(PrimitiveValue::from("MONOCHROME2")),
    ));

    // Encapsulated pixel data (JPEG 2000 codestream as single fragment)
    let pixel_data = DataElement::new(
        tags::PIXEL_DATA,
        VR::OB,
        Value::PixelSequence(dicom_core::value::PixelFragmentSequence::new(
            vec![0u32],              // offset table: single frame at offset 0
            vec![J2K_DATA.to_vec()], // single fragment
        )),
    );
    obj.put(pixel_data);

    let path = "tests/fixtures/j2k_lossless_8x8.dcm";
    obj.write_to_file(path).expect("should write DICOM file");
    println!("Created {path}");
}
