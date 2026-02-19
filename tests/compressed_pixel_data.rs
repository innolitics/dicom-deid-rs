use dicom_core::value::Value;
use dicom_dictionary_std::tags;
use dicom_object::open_file;

use dicom_deid_rs::pixel::{apply_pixel_mask, decompress_pixel_data};
use dicom_deid_rs::recipe::CoordinateRegion;

/// Requirement r-4-7: decompress RLE Lossless compressed pixel data
#[test]
fn r4_7_decompress_rle_lossless() {
    let mut obj =
        open_file("tests/fixtures/rle_lossless_8x8.dcm").expect("should open RLE fixture");

    // Before decompression, pixel data should be encapsulated
    let elem = obj.element(tags::PIXEL_DATA).unwrap();
    assert!(
        matches!(elem.value(), Value::PixelSequence { .. }),
        "fixture should have encapsulated pixel data"
    );

    decompress_pixel_data(&mut obj).expect("decompression should succeed");

    // After decompression, pixel data should be primitive
    let elem = obj.element(tags::PIXEL_DATA).unwrap();
    assert!(
        matches!(elem.value(), Value::Primitive(_)),
        "decompressed pixel data should be primitive"
    );

    // Transfer syntax should be Explicit VR Little Endian (r-4-8)
    assert_eq!(
        obj.meta().transfer_syntax(),
        "1.2.840.10008.1.2.1",
        "transfer syntax should be updated to Explicit VR Little Endian"
    );

    // Pixel data should have the right size for an 8x8 image
    let bytes = elem.value().to_bytes().expect("should read pixel bytes");
    assert!(
        bytes.len() >= 64,
        "8x8 image should have at least 64 bytes, got {}",
        bytes.len()
    );
    // The vast majority of pixels should be 255 (white)
    let white_count = bytes[..64].iter().filter(|&&b| b == 255).count();
    assert!(
        white_count >= 63,
        "at least 63 of 64 pixels should be 255 (white), got {}/64",
        white_count
    );
}

/// Requirement r-4-7 + r-4-8: decompress then apply pixel mask
#[test]
fn r4_7_decompress_and_mask_rle_lossless() {
    let mut obj =
        open_file("tests/fixtures/rle_lossless_8x8.dcm").expect("should open RLE fixture");

    decompress_pixel_data(&mut obj).expect("decompression should succeed");

    // Mask the top-left 4x4 region
    let regions = vec![CoordinateRegion {
        xmin: 0,
        ymin: 0,
        xmax: 4,
        ymax: 4,
        keep: false,
    }];

    apply_pixel_mask(&mut obj, &regions).expect("masking should succeed");

    let elem = obj.element(tags::PIXEL_DATA).unwrap();
    let bytes = elem.value().to_bytes().expect("should read pixel bytes");

    // Top-left 4x4 should be zeroed
    for row in 0..4 {
        for col in 0..4 {
            assert_eq!(
                bytes[row * 8 + col],
                0,
                "masked pixel ({},{}) should be 0",
                col,
                row
            );
        }
    }

    // Bottom-right should remain 255
    for row in 4..8 {
        for col in 4..8 {
            assert_eq!(
                bytes[row * 8 + col],
                255,
                "unmasked pixel ({},{}) should be 255",
                col,
                row
            );
        }
    }
}

/// Without decompression, apply_pixel_mask should return CompressedPixelData error
#[test]
fn r4_7_mask_without_decompress_returns_error() {
    let mut obj =
        open_file("tests/fixtures/rle_lossless_8x8.dcm").expect("should open RLE fixture");

    let regions = vec![CoordinateRegion {
        xmin: 0,
        ymin: 0,
        xmax: 4,
        ymax: 4,
        keep: false,
    }];

    // apply_pixel_mask auto-derefs FileDicomObject to InMemDicomObject
    let result = apply_pixel_mask(&mut obj, &regions);
    assert!(result.is_err(), "should fail on compressed data");
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("decompress_pixel_data()"),
        "error should mention decompress_pixel_data(): {}",
        err_msg
    );
}
