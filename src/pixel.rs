use crate::error::DeidError;
use crate::recipe::CoordinateRegion;
use dicom_core::DataElement;
use dicom_core::value::{PrimitiveValue, Value};
use dicom_dictionary_std::tags;
use dicom_object::{FileDicomObject, InMemDicomObject};
use dicom_pixeldata::PixelDecoder;

/// Read a u16 value from a DICOM tag, returning an error if missing or invalid.
fn read_u16_tag(obj: &InMemDicomObject, tag: dicom_core::Tag) -> Result<u16, DeidError> {
    let elem = obj
        .element(tag)
        .map_err(|_| DeidError::Dicom(format!("missing required tag {:?}", tag)))?;
    elem.value()
        .to_int::<u16>()
        .map_err(|_| DeidError::Dicom(format!("cannot read tag {:?} as u16", tag)))
}

/// Decompress encapsulated (compressed) pixel data in-place.
///
/// If the pixel data is already uncompressed or absent, this is a no-op.
/// After decompression the transfer syntax is updated to Explicit VR Little
/// Endian (1.2.840.10008.1.2.1).
pub fn decompress_pixel_data(obj: &mut FileDicomObject<InMemDicomObject>) -> Result<(), DeidError> {
    // No pixel data → nothing to do
    let elem = match obj.element(tags::PIXEL_DATA) {
        Ok(e) => e,
        Err(_) => return Ok(()),
    };

    // Already uncompressed → no-op
    if !matches!(elem.value(), Value::PixelSequence { .. }) {
        return Ok(());
    }

    let vr = elem.header().vr();

    // Decode using dicom-pixeldata's PixelDecoder trait
    let decoded = obj
        .decode_pixel_data()
        .map_err(|e| DeidError::Dicom(format!("pixel decompression failed: {}", e)))?;
    let raw_bytes = decoded.data().to_vec();

    // Replace PixelData with uncompressed bytes
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        vr,
        Value::Primitive(PrimitiveValue::U8(raw_bytes.into())),
    ));

    // Update transfer syntax to Explicit VR Little Endian
    obj.update_meta(|meta| {
        meta.transfer_syntax = "1.2.840.10008.1.2.1".to_string();
    });

    Ok(())
}

/// Apply pixel masking to a DICOM object.
///
/// Mask regions are filled with black pixels. Keep regions are excluded from
/// masking (inverse mask). When both mask and keep regions are provided, the
/// effective mask is: mask_regions - keep_regions.
pub fn apply_pixel_mask(
    obj: &mut InMemDicomObject,
    regions: &[CoordinateRegion],
) -> Result<(), DeidError> {
    if regions.is_empty() {
        return Ok(());
    }

    let rows = read_u16_tag(obj, tags::ROWS)? as usize;
    let cols = read_u16_tag(obj, tags::COLUMNS)? as usize;
    let bits_allocated = read_u16_tag(obj, tags::BITS_ALLOCATED)? as usize;
    let samples_per_pixel = read_u16_tag(obj, tags::SAMPLES_PER_PIXEL).unwrap_or(1) as usize;
    let bytes_per_pixel = (bits_allocated / 8) * samples_per_pixel;

    let elem = obj
        .element(tags::PIXEL_DATA)
        .map_err(|_| DeidError::Dicom("missing PixelData element".into()))?;
    let vr = elem.header().vr();

    let mut pixel_data = match elem.value() {
        Value::Primitive(_) => elem
            .value()
            .to_bytes()
            .map_err(|e| DeidError::Dicom(format!("cannot read pixel data: {}", e)))?
            .to_vec(),
        Value::PixelSequence { .. } => {
            return Err(DeidError::CompressedPixelData(
                "encapsulated pixel data detected; call decompress_pixel_data() first".into(),
            ));
        }
        _ => {
            return Err(DeidError::Dicom("unexpected pixel data value type".into()));
        }
    };

    // Build per-pixel mask: true = keep, false = zero out
    let total_pixels = rows * cols;
    let mut mask = vec![true; total_pixels];

    // First pass: apply mask regions (keep=false)
    for region in regions.iter().filter(|r| !r.keep) {
        let ymin = (region.ymin as usize).min(rows);
        let ymax = (region.ymax as usize).min(rows);
        let xmin = (region.xmin as usize).min(cols);
        let xmax = (region.xmax as usize).min(cols);
        for row in ymin..ymax {
            for col in xmin..xmax {
                mask[row * cols + col] = false;
            }
        }
    }

    // Second pass: carve out keep regions (keep=true overrides mask)
    for region in regions.iter().filter(|r| r.keep) {
        let ymin = (region.ymin as usize).min(rows);
        let ymax = (region.ymax as usize).min(rows);
        let xmin = (region.xmin as usize).min(cols);
        let xmax = (region.xmax as usize).min(cols);
        for row in ymin..ymax {
            for col in xmin..xmax {
                mask[row * cols + col] = true;
            }
        }
    }

    // Apply mask to pixel bytes
    for (i, keep) in mask.iter().enumerate() {
        if !keep {
            let offset = i * bytes_per_pixel;
            let end = (offset + bytes_per_pixel).min(pixel_data.len());
            for byte in &mut pixel_data[offset..end] {
                *byte = 0;
            }
        }
    }

    // Write back
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        vr,
        Value::Primitive(PrimitiveValue::U8(pixel_data.into())),
    ));

    Ok(())
}

/// Convert CTP coordinates (x, y, width, height) to standard coordinates
/// (xmin, ymin, xmax, ymax).
pub fn ctp_to_standard(x: u32, y: u32, width: u32, height: u32) -> CoordinateRegion {
    CoordinateRegion {
        xmin: x,
        ymin: y,
        xmax: x + width,
        ymax: y + height,
        keep: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter;
    use crate::recipe::*;
    use crate::test_helpers::*;
    use dicom_core::DataElement;
    use dicom_core::value::{PrimitiveValue, Value};
    use dicom_core::{Tag, VR};
    use dicom_dictionary_std::tags;

    /// Helper to create a minimal DICOM object with pixel data.
    ///
    /// Creates a 4x4 grayscale image (16 pixels, 1 byte each) filled with
    /// value 255 (white).
    fn create_pixel_obj(rows: u16, cols: u16) -> InMemDicomObject {
        let mut obj = create_test_obj();
        put_u16(&mut obj, tags::ROWS, VR::US, rows);
        put_u16(&mut obj, tags::COLUMNS, VR::US, cols);
        put_str(
            &mut obj,
            tags::PHOTOMETRIC_INTERPRETATION,
            VR::CS,
            "MONOCHROME2",
        );
        put_u16(&mut obj, tags::BITS_ALLOCATED, VR::US, 8);
        put_u16(&mut obj, tags::BITS_STORED, VR::US, 8);
        put_u16(&mut obj, tags::HIGH_BIT, VR::US, 7);
        put_u16(&mut obj, tags::PIXEL_REPRESENTATION, VR::US, 0);
        put_u16(&mut obj, tags::SAMPLES_PER_PIXEL, VR::US, 1);

        // Create pixel data: all white (255)
        let pixel_count = (rows as usize) * (cols as usize);
        let pixels = vec![255u8; pixel_count];
        obj.put(DataElement::new(
            tags::PIXEL_DATA,
            VR::OW,
            Value::Primitive(PrimitiveValue::U8(pixels.into())),
        ));

        obj
    }

    // -- r-4-1 ---------------------------------------------------------------

    /// Requirement r-4-1
    #[test]
    fn r4_1_apply_pixel_mask_blacks_out_region() {
        let mut obj = create_pixel_obj(4, 4);

        let regions = vec![CoordinateRegion {
            xmin: 0,
            ymin: 0,
            xmax: 2,
            ymax: 2,
            keep: false,
        }];

        apply_pixel_mask(&mut obj, &regions).expect("should succeed");

        let elem = obj
            .element(tags::PIXEL_DATA)
            .expect("pixel data should exist");
        let bytes = elem.value().to_bytes().expect("should read pixel bytes");

        // The top-left 2x2 region (rows 0-1, cols 0-1) should be black (0)
        // Row 0: pixels [0,1] should be 0, pixels [2,3] should be 255
        assert_eq!(bytes[0], 0, "masked pixel (0,0) should be black");
        assert_eq!(bytes[1], 0, "masked pixel (1,0) should be black");
        assert_eq!(bytes[2], 255, "unmasked pixel (2,0) should remain white");
        assert_eq!(bytes[3], 255, "unmasked pixel (3,0) should remain white");
        // Row 1: pixels [0,1] should be 0
        assert_eq!(bytes[4], 0, "masked pixel (0,1) should be black");
        assert_eq!(bytes[5], 0, "masked pixel (1,1) should be black");
        // Row 2: all should remain 255
        assert_eq!(bytes[8], 255, "unmasked pixel (0,2) should remain white");
    }

    // -- r-4-3 ---------------------------------------------------------------

    /// Requirement r-4-3
    #[test]
    fn r4_3_raw_coordinate_format() {
        let region = CoordinateRegion {
            xmin: 10,
            ymin: 20,
            xmax: 300,
            ymax: 400,
            keep: false,
        };
        assert_eq!(region.xmin, 10);
        assert_eq!(region.ymin, 20);
        assert_eq!(region.xmax, 300);
        assert_eq!(region.ymax, 400);
    }

    /// Requirement r-4-3
    #[test]
    fn r4_3_ctp_coordinate_conversion() {
        let region = ctp_to_standard(10, 20, 100, 50);
        assert_eq!(region.xmin, 10);
        assert_eq!(region.ymin, 20);
        assert_eq!(region.xmax, 110); // 10 + 100
        assert_eq!(region.ymax, 70); // 20 + 50
        assert!(!region.keep);
    }

    // -- r-4-4 ---------------------------------------------------------------

    /// Requirement r-4-4
    #[test]
    fn r4_4_keep_regions_excluded_from_mask() {
        let mut obj = create_pixel_obj(4, 4);

        let regions = vec![
            // Mask entire top half
            CoordinateRegion {
                xmin: 0,
                ymin: 0,
                xmax: 4,
                ymax: 2,
                keep: false,
            },
            // But keep a 2x2 region within it
            CoordinateRegion {
                xmin: 0,
                ymin: 0,
                xmax: 2,
                ymax: 2,
                keep: true,
            },
        ];

        apply_pixel_mask(&mut obj, &regions).expect("should succeed");

        let elem = obj
            .element(tags::PIXEL_DATA)
            .expect("pixel data should exist");
        let bytes = elem.value().to_bytes().expect("should read pixel bytes");

        // Keep region (0,0)-(2,2) should remain 255
        assert_eq!(bytes[0], 255, "kept pixel (0,0) should remain white");
        assert_eq!(bytes[1], 255, "kept pixel (1,0) should remain white");
        // Masked region outside keep: (2,0)-(4,2) should be 0
        assert_eq!(bytes[2], 0, "masked pixel (2,0) should be black");
        assert_eq!(bytes[3], 0, "masked pixel (3,0) should be black");
    }

    // -- r-4-5 ---------------------------------------------------------------

    /// Requirement r-4-5
    #[test]
    fn r4_5_multiple_coordinate_regions() {
        let mut obj = create_pixel_obj(8, 8);

        let regions = vec![
            CoordinateRegion {
                xmin: 0,
                ymin: 0,
                xmax: 4,
                ymax: 2,
                keep: false,
            },
            CoordinateRegion {
                xmin: 0,
                ymin: 6,
                xmax: 4,
                ymax: 8,
                keep: false,
            },
        ];

        apply_pixel_mask(&mut obj, &regions).expect("should succeed");

        let elem = obj
            .element(tags::PIXEL_DATA)
            .expect("pixel data should exist");
        let bytes = elem.value().to_bytes().expect("should read pixel bytes");

        // First region: row 0, col 0 should be masked
        assert_eq!(bytes[0], 0, "first region should be masked");
        // Middle rows should be untouched
        let mid_row = 3 * 8; // row 3
        assert_eq!(bytes[mid_row], 255, "middle rows should be unmasked");
        // Second region: row 6, col 0 should be masked
        let bottom_row = 6 * 8;
        assert_eq!(bytes[bottom_row], 0, "second region should be masked");
    }

    // -- r-4-6 ---------------------------------------------------------------

    /// Requirement r-4-6
    #[test]
    fn r4_6_regions_only_applied_when_filter_matches() {
        let mut obj = create_pixel_obj(4, 4);
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "SIEMENS");

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Graylist,
                labels: vec![FilterLabel {
                    name: "GE Only".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Contains {
                            field: "Manufacturer".into(),
                            value: "GE".into(),
                        },
                    }],
                    coordinates: vec![CoordinateRegion {
                        xmin: 0,
                        ymin: 0,
                        xmax: 4,
                        ymax: 4,
                        keep: false,
                    }],
                }],
            }],
        };

        // Manufacturer is SIEMENS, so GE filter should NOT match
        let regions = filter::get_graylist_regions(&recipe, &obj);
        assert!(
            regions.is_empty(),
            "regions should not be returned when filter conditions don't match"
        );
    }

    /// Requirement r-4-6
    #[test]
    fn r4_6_regions_applied_when_filter_matches() {
        let mut obj = create_pixel_obj(4, 4);
        put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL SYSTEMS");

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![],
            filters: vec![FilterSection {
                filter_type: FilterType::Graylist,
                labels: vec![FilterLabel {
                    name: "GE Mask".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Contains {
                            field: "Manufacturer".into(),
                            value: "GE".into(),
                        },
                    }],
                    coordinates: vec![CoordinateRegion {
                        xmin: 0,
                        ymin: 0,
                        xmax: 4,
                        ymax: 2,
                        keep: false,
                    }],
                }],
            }],
        };

        let regions = filter::get_graylist_regions(&recipe, &obj);
        assert_eq!(
            regions.len(),
            1,
            "matching filter should return its regions"
        );
    }

    // -- r-4-7 ---------------------------------------------------------------

    /// Requirement r-4-7: decompress is a no-op for uncompressed pixel data
    #[test]
    fn r4_7_decompress_noop_for_uncompressed() {
        let mut file_obj = create_test_file_obj();
        put_u16(&mut file_obj, tags::ROWS, VR::US, 4);
        put_u16(&mut file_obj, tags::COLUMNS, VR::US, 4);
        put_str(
            &mut file_obj,
            tags::PHOTOMETRIC_INTERPRETATION,
            VR::CS,
            "MONOCHROME2",
        );
        put_u16(&mut file_obj, tags::BITS_ALLOCATED, VR::US, 8);
        put_u16(&mut file_obj, tags::BITS_STORED, VR::US, 8);
        put_u16(&mut file_obj, tags::HIGH_BIT, VR::US, 7);
        put_u16(&mut file_obj, tags::PIXEL_REPRESENTATION, VR::US, 0);
        put_u16(&mut file_obj, tags::SAMPLES_PER_PIXEL, VR::US, 1);

        let pixels = vec![128u8; 16];
        file_obj.put(DataElement::new(
            tags::PIXEL_DATA,
            VR::OW,
            Value::Primitive(PrimitiveValue::U8(pixels.clone().into())),
        ));

        decompress_pixel_data(&mut file_obj).expect("should succeed");

        // Transfer syntax should be unchanged
        assert_eq!(file_obj.meta().transfer_syntax(), "1.2.840.10008.1.2.1");

        // Pixel data should be unchanged
        let bytes = file_obj
            .element(tags::PIXEL_DATA)
            .unwrap()
            .value()
            .to_bytes()
            .unwrap();
        assert_eq!(bytes.as_ref(), &pixels[..]);
    }

    /// Requirement r-4-7: decompress is a no-op when no pixel data present
    #[test]
    fn r4_7_decompress_noop_for_no_pixel_data() {
        let mut file_obj = create_test_file_obj();
        decompress_pixel_data(&mut file_obj).expect("should succeed with no pixel data");
    }
}
