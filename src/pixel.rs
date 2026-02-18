use crate::error::DeidError;
use crate::recipe::CoordinateRegion;
use dicom_object::InMemDicomObject;

/// Apply pixel masking to a DICOM object.
///
/// Mask regions are filled with black pixels. Keep regions are excluded from
/// masking (inverse mask). When both mask and keep regions are provided, the
/// effective mask is: mask_regions - keep_regions.
pub fn apply_pixel_mask(
    obj: &mut InMemDicomObject,
    regions: &[CoordinateRegion],
) -> Result<(), DeidError> {
    todo!()
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
}
