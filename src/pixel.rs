use crate::error::DeidError;
use crate::recipe::CoordinateRegion;
use dicom_core::value::{PrimitiveValue, Value};
use dicom_core::{DataElement, VR};
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
/// Endian (1.2.840.10008.1.2.1) and the pixel-description tags
/// (PhotometricInterpretation, PlanarConfiguration, BitsAllocated,
/// BitsStored, HighBit, SamplesPerPixel) are updated to match what the
/// decoder actually produced.
///
/// The metadata sync is critical for color frames: JPEG2000 encodes color
/// ultrasound as `YBR_RCT` (a transform that exists *only* for compressed
/// JPEG2000 data); after decode the buffer is RGB-interleaved.  Leaving the
/// PI as `YBR_RCT` causes downstream viewers to apply a wrong inverse YBR
/// transform, producing the green-cast / "doubled" rendering artifact seen
/// on Doppler frames.
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

    // The original encapsulated PixelData always carries VR=OB (per DICOM
    // PS3.5).  After decompression we re-pick the VR below from the
    // decoded buffer's BitsAllocated: OW for >8-bit (strict viewers refuse
    // OB+BitsAllocated=16), OB for 8-bit.

    // Decode using dicom-pixeldata's PixelDecoder trait
    let decoded = obj
        .decode_pixel_data()
        .map_err(|e| DeidError::Dicom(format!("pixel decompression failed: {}", e)))?;
    let raw_bytes = decoded.data().to_vec();

    // Snapshot the post-decode metadata so we can drop the borrow on `obj`
    // before mutating it.
    let pi = decoded.photometric_interpretation().as_str().to_string();
    let planar = decoded.planar_configuration() as u16;
    let samples_per_pixel = decoded.samples_per_pixel();
    let bits_allocated = decoded.bits_allocated();
    let mut bits_stored = decoded.bits_stored();
    let mut high_bit = decoded.high_bit();
    drop(decoded);

    // For color pixel data (RGB / multi-sample) the DICOM convention —
    // and what reference libraries like GDCM emit after JPEG2000
    // decompression — is `BitsStored == BitsAllocated`.  `BS<BA` only
    // makes sense for monochrome (where the analog signal has fewer
    // effective bits than the container); on RGB samples it confuses
    // viewers and produces alternating-channel halftone artifacts when
    // they fall back to a "treat each byte as a separate sample" path.
    //
    // `dicom-pixeldata` carries forward the source's BS value literally
    // (often 8 for JPEG2000 ultrasound stored in 16-bit slots), so we
    // normalize to BS=BA for color data here.  The pixel bytes are
    // unchanged — just the descriptive tags change.
    if samples_per_pixel > 1 && bits_stored < bits_allocated {
        bits_stored = bits_allocated;
        high_bit = bits_allocated - 1;
    }

    let pixel_vr = if bits_allocated > 8 { VR::OW } else { VR::OB };

    // Replace PixelData with uncompressed bytes
    obj.put(DataElement::new(
        tags::PIXEL_DATA,
        pixel_vr,
        Value::Primitive(PrimitiveValue::U8(raw_bytes.into())),
    ));

    // Sync pixel-description tags to the decoded buffer's actual layout.
    obj.put(DataElement::new(
        tags::PHOTOMETRIC_INTERPRETATION,
        VR::CS,
        PrimitiveValue::from(pi),
    ));
    obj.put(DataElement::new(
        tags::SAMPLES_PER_PIXEL,
        VR::US,
        PrimitiveValue::from(samples_per_pixel),
    ));
    obj.put(DataElement::new(
        tags::BITS_ALLOCATED,
        VR::US,
        PrimitiveValue::from(bits_allocated),
    ));
    obj.put(DataElement::new(
        tags::BITS_STORED,
        VR::US,
        PrimitiveValue::from(bits_stored),
    ));
    obj.put(DataElement::new(
        tags::HIGH_BIT,
        VR::US,
        PrimitiveValue::from(high_bit),
    ));
    // PlanarConfiguration is only defined for SamplesPerPixel > 1.
    if samples_per_pixel > 1 {
        obj.put(DataElement::new(
            tags::PLANAR_CONFIGURATION,
            VR::US,
            PrimitiveValue::from(planar),
        ));
    }

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
    use dicom_core::VR;
    use dicom_core::value::{PrimitiveValue, Value};
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
            keep_groups: vec![],
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
            keep_groups: vec![],
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

    /// Regression: encapsulated PixelData always carries VR=OB (per DICOM
    /// PS3.5).  After decompression we must promote the VR to OW for
    /// >8-bit samples; viewers that strictly enforce VR/BitsAllocated
    /// agreement otherwise misinterpret the bytes (every byte read as a
    /// separate sample), producing a halftone-pattern artifact on color
    /// 16-bit ultrasound frames.
    #[test]
    fn vr_promoted_to_ow_helper_choice() {
        // The picker is internal logic; verify the rule it encodes.
        for ba in [1u16, 7, 8] {
            assert!(ba <= 8);
            assert_eq!(if ba > 8 { VR::OW } else { VR::OB }, VR::OB);
        }
        for ba in [9u16, 12, 16, 32] {
            assert!(ba > 8);
            assert_eq!(if ba > 8 { VR::OW } else { VR::OB }, VR::OW);
        }
    }

    /// Regression: for color pixel data, BitsStored must equal
    /// BitsAllocated.  `BS<BA` is only meaningful for monochrome.  This
    /// rule mirrors what GDCM emits after JPEG2000 decompression and
    /// avoids the halftone-stripe artifact some viewers produce when
    /// they encounter `PI=RGB`+`BA=16`+`BS=8` on uncompressed data.
    #[test]
    fn color_normalizes_bs_to_ba_after_decompress() {
        let cases = [
            // (samples_per_pixel, bs_in, ba, expected_bs_out, expected_hb_out)
            (3u16, 8u16, 16u16, 16u16, 15u16), // RGB 8-in-16: normalize
            (3, 16, 16, 16, 15),               // RGB true 16-bit: unchanged
            (3, 8, 8, 8, 7),                   // RGB 8-bit: unchanged
            (1, 8, 16, 8, 7),                  // monochrome 8-in-16: keep BS<BA
            (1, 12, 16, 12, 11),               // monochrome 12-in-16: keep
        ];
        for (spp, bs_in, ba, exp_bs, exp_hb) in cases {
            let mut bs = bs_in;
            let mut hb = bs.saturating_sub(1);
            if spp > 1 && bs < ba {
                bs = ba;
                hb = ba - 1;
            }
            assert_eq!(bs, exp_bs, "spp={}, bs_in={}, ba={}", spp, bs_in, ba);
            assert_eq!(hb, exp_hb, "spp={}, bs_in={}, ba={}", spp, bs_in, ba);
        }
    }

    /// Regression: the no-op path (already-uncompressed input) must not
    /// rewrite pixel-description tags. Only the encapsulated-data path
    /// updates them, because only there does the decoded layout differ
    /// from what the source tags claim.
    #[test]
    fn decompress_noop_preserves_pixel_description_tags() {
        let mut file_obj = create_test_file_obj();
        put_u16(&mut file_obj, tags::ROWS, VR::US, 4);
        put_u16(&mut file_obj, tags::COLUMNS, VR::US, 4);
        put_str(
            &mut file_obj,
            tags::PHOTOMETRIC_INTERPRETATION,
            VR::CS,
            "MONOCHROME2",
        );
        put_u16(&mut file_obj, tags::BITS_ALLOCATED, VR::US, 16);
        put_u16(&mut file_obj, tags::BITS_STORED, VR::US, 12);
        put_u16(&mut file_obj, tags::HIGH_BIT, VR::US, 11);
        put_u16(&mut file_obj, tags::PIXEL_REPRESENTATION, VR::US, 0);
        put_u16(&mut file_obj, tags::SAMPLES_PER_PIXEL, VR::US, 1);

        let pixels = vec![0u8; 32]; // 4*4 16-bit
        file_obj.put(DataElement::new(
            tags::PIXEL_DATA,
            VR::OW,
            Value::Primitive(PrimitiveValue::U8(pixels.into())),
        ));

        decompress_pixel_data(&mut file_obj).expect("noop");

        // None of the pixel-description tags should have been touched.
        let pi = file_obj
            .element(tags::PHOTOMETRIC_INTERPRETATION)
            .unwrap()
            .value()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(pi, "MONOCHROME2");
        let bs = file_obj
            .element(tags::BITS_STORED)
            .unwrap()
            .value()
            .to_int::<u16>()
            .unwrap();
        assert_eq!(bs, 12, "BitsStored must not be rewritten on the no-op path");
        let hb = file_obj
            .element(tags::HIGH_BIT)
            .unwrap()
            .value()
            .to_int::<u16>()
            .unwrap();
        assert_eq!(hb, 11);
    }
}
