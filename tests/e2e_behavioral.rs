//! End-to-end behavioral tests ported from the Python deid reference implementation.
//!
//! Category 4: Tag format integration — recipe text parsed end-to-end.
//! Category 5: Pipeline-level E2E — blacklist, multi-file, graylist pixel masking.

use dicom_core::value::{PrimitiveValue, Value};
use dicom_core::{DataElement, Tag, VR};
use dicom_dictionary_std::tags;
use dicom_object::meta::FileMetaTableBuilder;
use dicom_object::{FileDicomObject, InMemDicomObject, open_file};
use std::collections::HashMap;
use std::fs;
use tempfile::TempDir;

use dicom_deid_rs::metadata::{DeidFunction, apply_header_actions};
use dicom_deid_rs::pipeline::{DeidConfig, DeidPipeline};
use dicom_deid_rs::recipe::Recipe;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn put_str(obj: &mut InMemDicomObject, tag: Tag, vr: VR, value: &str) {
    obj.put(DataElement::new(
        tag,
        vr,
        Value::Primitive(PrimitiveValue::from(value)),
    ));
}

fn create_test_file_obj() -> FileDicomObject<InMemDicomObject> {
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

/// Set the tags required for CTP-style output path generation.
fn put_path_tags(
    obj: &mut FileDicomObject<InMemDicomObject>,
    patient_id: &str,
    study_date: &str,
    series_number: &str,
    sop_uid: &str,
) {
    put_str(obj, tags::PATIENT_ID, VR::LO, patient_id);
    put_str(obj, tags::STUDY_DATE, VR::DA, study_date);
    put_str(obj, tags::SERIES_NUMBER, VR::IS, series_number);
    put_str(obj, tags::SOP_INSTANCE_UID, VR::UI, sop_uid);
}

fn empty_vars() -> HashMap<String, String> {
    HashMap::new()
}

fn empty_funcs() -> HashMap<String, DeidFunction> {
    HashMap::new()
}

// ============================================================================
// Category 4: Tag Format Integration
// ============================================================================
// Full recipe-text-to-output tests verifying different tag format syntax.

#[test]
fn tag_format_remove_via_keyword() {
    let mut obj = create_test_file_obj();
    put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

    let recipe_text = "FORMAT dicom\n%header\nREMOVE PatientName\n";
    let recipe = Recipe::parse(recipe_text).expect("should parse");

    apply_header_actions(&recipe.header, &empty_vars(), &empty_funcs(), &mut obj)
        .expect("should succeed");

    assert!(
        obj.element(tags::PATIENT_NAME).is_err(),
        "PatientName should be removed via keyword"
    );
}

#[test]
fn tag_format_remove_via_hex() {
    let mut obj = create_test_file_obj();
    put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

    let recipe_text = "FORMAT dicom\n%header\nREMOVE 00100010\n";
    let recipe = Recipe::parse(recipe_text).expect("should parse");

    apply_header_actions(&recipe.header, &empty_vars(), &empty_funcs(), &mut obj)
        .expect("should succeed");

    assert!(
        obj.element(tags::PATIENT_NAME).is_err(),
        "PatientName should be removed via bare hex"
    );
}

#[test]
fn tag_format_remove_via_dicom_format() {
    let mut obj = create_test_file_obj();
    put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");

    let recipe_text = "FORMAT dicom\n%header\nREMOVE (0010,0010)\n";
    let recipe = Recipe::parse(recipe_text).expect("should parse");

    apply_header_actions(&recipe.header, &empty_vars(), &empty_funcs(), &mut obj)
        .expect("should succeed");

    assert!(
        obj.element(tags::PATIENT_NAME).is_err(),
        "PatientName should be removed via (GGGG,EEEE) format"
    );
}

#[test]
fn tag_format_replace_via_keyword_and_hex() {
    let mut obj = create_test_file_obj();
    put_str(&mut obj, tags::PATIENT_ID, VR::LO, "ORIGINAL_ID");
    // Private tag at (0019,0010)
    put_str(&mut obj, Tag(0x0019, 0x0010), VR::LO, "OLD_PRIVATE");

    let recipe_text = "FORMAT dicom\n%header\nREPLACE PatientID ANON\nREPLACE 00190010 NEW\n";
    let recipe = Recipe::parse(recipe_text).expect("should parse");

    apply_header_actions(&recipe.header, &empty_vars(), &empty_funcs(), &mut obj)
        .expect("should succeed");

    let pid = obj
        .element(tags::PATIENT_ID)
        .unwrap()
        .value()
        .to_str()
        .unwrap();
    assert_eq!(pid.as_ref(), "ANON", "keyword REPLACE should work");

    let priv_val = obj
        .element(Tag(0x0019, 0x0010))
        .unwrap()
        .value()
        .to_str()
        .unwrap();
    assert_eq!(priv_val.as_ref(), "NEW", "hex REPLACE should work");
}

#[test]
fn tag_format_add_via_hex_private_tag() {
    let mut obj = create_test_file_obj();

    let recipe_text = "FORMAT dicom\n%header\nADD 11112221 SIMPSON\n";
    let recipe = Recipe::parse(recipe_text).expect("should parse");

    apply_header_actions(&recipe.header, &empty_vars(), &empty_funcs(), &mut obj)
        .expect("should succeed");

    let val = obj
        .element(Tag(0x1111, 0x2221))
        .unwrap()
        .value()
        .to_str()
        .unwrap();
    assert_eq!(
        val.as_ref(),
        "SIMPSON",
        "ADD via hex should create private tag with value"
    );
}

// ============================================================================
// Category 5: Pipeline-Level E2E
// ============================================================================

#[test]
fn pipeline_blacklist_excludes_file() {
    let tmp = TempDir::new().expect("should create temp dir");
    let input_dir = tmp.path().join("input");
    let output_dir = tmp.path().join("output");
    fs::create_dir_all(&input_dir).expect("create input dir");

    // CT file — should pass through
    let mut ct_file = create_test_file_obj();
    put_str(&mut ct_file, tags::MODALITY, VR::CS, "CT");
    put_str(&mut ct_file, tags::PATIENT_NAME, VR::PN, "John^Doe");
    put_path_tags(&mut ct_file, "PID001", "20250101", "1", "1.2.3.4.5.6.7.8.9");
    ct_file
        .write_to_file(input_dir.join("ct.dcm"))
        .expect("write CT file");

    // SR file — should be blacklisted
    let mut sr_file = FileDicomObject::new_empty_with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax("1.2.840.10008.1.2.1")
            .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.88.11")
            .media_storage_sop_instance_uid("1.2.3.4.5.6.7.8.10")
            .implementation_class_uid("1.2.3.4")
            .build()
            .expect("valid file meta"),
    );
    put_str(&mut sr_file, tags::MODALITY, VR::CS, "SR");
    put_str(&mut sr_file, tags::PATIENT_NAME, VR::PN, "Jane^Doe");
    put_path_tags(
        &mut sr_file,
        "PID002",
        "20250101",
        "1",
        "1.2.3.4.5.6.7.8.10",
    );
    sr_file
        .write_to_file(input_dir.join("sr.dcm"))
        .expect("write SR file");

    // Recipe with blacklist for SR
    let recipe_path = tmp.path().join("recipe.txt");
    fs::write(
        &recipe_path,
        "\
FORMAT dicom

%filter blacklist

LABEL Reject SR
equals Modality SR

%header

REPLACE PatientName ANON
",
    )
    .expect("write recipe");

    let config = DeidConfig {
        input_dir: input_dir.clone(),
        output_dir: output_dir.clone(),
        recipe_path,
        variables: HashMap::new(),
        functions: HashMap::new(),
        remove_private_tags: true,
        remove_unspecified_elements: false,
        quarantine_dir: None,
    };

    let pipeline = DeidPipeline::new(config).expect("should create pipeline");
    let report = pipeline.run().expect("should run pipeline");

    assert_eq!(report.files_processed, 1, "only CT should be processed");
    assert_eq!(report.files_blacklisted, 1, "SR should be blacklisted");

    // CT file should exist in output at CTP-style path
    let ct_output = output_dir
        .join("DATE-20250101--PID-PID001")
        .join("SER-00001")
        .join("1.2.3.4.5.6.7.8.9.dcm");
    assert!(ct_output.exists(), "CT output file should exist");

    // SR file should NOT exist in output (blacklisted)
    assert!(
        !output_dir.join("DATE-20250101--PID-PID002").exists(),
        "SR output directory should not exist"
    );

    // Verify CT was de-identified
    let result = open_file(&ct_output).expect("should open CT output");
    let name = result
        .element_by_name("PatientName")
        .expect("should have PatientName");
    let val = name.value().to_str().expect("should read value");
    assert_eq!(val.as_ref(), "ANON");
}

#[test]
fn pipeline_multiple_files_nested_dirs() {
    let tmp = TempDir::new().expect("should create temp dir");
    let input_dir = tmp.path().join("input");
    let output_dir = tmp.path().join("output");

    // Create nested directory structure
    let sub1 = input_dir.join("sub1");
    let sub2 = input_dir.join("sub1").join("sub2");
    fs::create_dir_all(&sub2).expect("create nested dirs");

    // Create 3 DICOM files in different locations (simulating storescp output)
    for (dir, name, uid_suffix, series_num) in [
        (input_dir.as_path(), "root.dcm", "1", "1"),
        (sub1.as_path(), "level1.dcm", "2", "2"),
        (sub2.as_path(), "level2.dcm", "3", "3"),
    ] {
        let mut file_obj = FileDicomObject::new_empty_with_meta(
            FileMetaTableBuilder::new()
                .transfer_syntax("1.2.840.10008.1.2.1")
                .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.2")
                .media_storage_sop_instance_uid(format!("1.2.3.4.5.6.7.8.{}", uid_suffix))
                .implementation_class_uid("1.2.3.4")
                .build()
                .expect("valid file meta"),
        );
        put_str(&mut file_obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
        put_str(&mut file_obj, tags::MODALITY, VR::CS, "CT");
        put_path_tags(
            &mut file_obj,
            "PID001",
            "20250101",
            series_num,
            &format!("1.2.3.4.5.6.7.8.{}", uid_suffix),
        );
        file_obj
            .write_to_file(dir.join(name))
            .expect("write DICOM file");
    }

    let recipe_path = tmp.path().join("recipe.txt");
    fs::write(
        &recipe_path,
        "FORMAT dicom\n%header\nREPLACE PatientName ANON\n",
    )
    .expect("write recipe");

    let config = DeidConfig {
        input_dir: input_dir.clone(),
        output_dir: output_dir.clone(),
        recipe_path,
        variables: HashMap::new(),
        functions: HashMap::new(),
        remove_private_tags: true,
        remove_unspecified_elements: false,
        quarantine_dir: None,
    };

    let pipeline = DeidPipeline::new(config).expect("should create pipeline");
    let report = pipeline.run().expect("should run pipeline");

    assert_eq!(report.files_processed, 3, "all 3 files should be processed");

    // Verify output uses CTP-style directory structure
    let study_dir = output_dir.join("DATE-20250101--PID-PID001");
    assert!(study_dir.exists(), "study directory should exist");
    assert!(
        study_dir
            .join("SER-00001")
            .join("1.2.3.4.5.6.7.8.1.dcm")
            .exists(),
        "series 1 file should exist"
    );
    assert!(
        study_dir
            .join("SER-00002")
            .join("1.2.3.4.5.6.7.8.2.dcm")
            .exists(),
        "series 2 file should exist"
    );
    assert!(
        study_dir
            .join("SER-00003")
            .join("1.2.3.4.5.6.7.8.3.dcm")
            .exists(),
        "series 3 file should exist"
    );

    // Verify all files were de-identified
    for i in 1..=3 {
        let path = study_dir
            .join(format!("SER-{:05}", i))
            .join(format!("1.2.3.4.5.6.7.8.{}.dcm", i));
        let result = open_file(&path).expect("should open output");
        let name = result
            .element_by_name("PatientName")
            .expect("should have PatientName");
        let val = name.value().to_str().expect("should read value");
        assert_eq!(
            val.as_ref(),
            "ANON",
            "PatientName should be replaced in {}",
            path.display()
        );
    }
}

#[test]
fn pipeline_graylist_pixel_masking() {
    let tmp = TempDir::new().expect("should create temp dir");
    let input_dir = tmp.path().join("input");
    let output_dir = tmp.path().join("output");
    fs::create_dir_all(&input_dir).expect("create input dir");

    // Create a DICOM file with pixel data
    let mut file_obj = create_test_file_obj();
    put_str(&mut file_obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL");
    put_str(&mut file_obj, tags::MODALITY, VR::CS, "CT");
    put_str(&mut file_obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
    put_path_tags(
        &mut file_obj,
        "PID001",
        "20250101",
        "1",
        "1.2.3.4.5.6.7.8.9",
    );

    // Set pixel data attributes: 4x4 monochrome image, 8-bit
    file_obj.put(DataElement::new(
        tags::ROWS,
        VR::US,
        Value::Primitive(PrimitiveValue::from(4u16)),
    ));
    file_obj.put(DataElement::new(
        tags::COLUMNS,
        VR::US,
        Value::Primitive(PrimitiveValue::from(4u16)),
    ));
    file_obj.put(DataElement::new(
        tags::BITS_ALLOCATED,
        VR::US,
        Value::Primitive(PrimitiveValue::from(8u16)),
    ));
    file_obj.put(DataElement::new(
        tags::BITS_STORED,
        VR::US,
        Value::Primitive(PrimitiveValue::from(8u16)),
    ));
    file_obj.put(DataElement::new(
        tags::HIGH_BIT,
        VR::US,
        Value::Primitive(PrimitiveValue::from(7u16)),
    ));
    file_obj.put(DataElement::new(
        tags::PIXEL_REPRESENTATION,
        VR::US,
        Value::Primitive(PrimitiveValue::from(0u16)),
    ));
    file_obj.put(DataElement::new(
        tags::SAMPLES_PER_PIXEL,
        VR::US,
        Value::Primitive(PrimitiveValue::from(1u16)),
    ));
    put_str(
        &mut file_obj,
        tags::PHOTOMETRIC_INTERPRETATION,
        VR::CS,
        "MONOCHROME2",
    );

    // 4x4 = 16 bytes of pixel data, all 0xFF
    let pixel_data: Vec<u8> = vec![0xFF; 16];
    file_obj.put(DataElement::new(
        tags::PIXEL_DATA,
        VR::OW,
        Value::Primitive(PrimitiveValue::from(pixel_data)),
    ));

    let dcm_path = input_dir.join("ge_ct.dcm");
    file_obj.write_to_file(&dcm_path).expect("write DICOM file");

    // Recipe with graylist for GE that masks the top 2 rows (0,0 to 4,2)
    let recipe_path = tmp.path().join("recipe.txt");
    fs::write(
        &recipe_path,
        "\
FORMAT dicom

%filter graylist

LABEL GE CT
contains Manufacturer GE
coordinates 0,0,4,2

%header

REPLACE PatientName ANON
",
    )
    .expect("write recipe");

    let config = DeidConfig {
        input_dir: input_dir.clone(),
        output_dir: output_dir.clone(),
        recipe_path,
        variables: HashMap::new(),
        functions: HashMap::new(),
        remove_private_tags: true,
        remove_unspecified_elements: false,
        quarantine_dir: None,
    };

    let pipeline = DeidPipeline::new(config).expect("should create pipeline");
    let report = pipeline.run().expect("should run pipeline");

    assert_eq!(report.files_processed, 1);

    let output_file = output_dir
        .join("DATE-20250101--PID-PID001")
        .join("SER-00001")
        .join("1.2.3.4.5.6.7.8.9.dcm");
    assert!(output_file.exists(), "output file should exist");

    // Read back and check pixel data has masked region
    let result = open_file(&output_file).expect("should open output");

    let pixel_elem = result
        .element(tags::PIXEL_DATA)
        .expect("should have pixels");
    let pixel_bytes = pixel_elem
        .value()
        .to_bytes()
        .expect("should read pixel bytes");

    // Top 2 rows (8 bytes) should be masked (0x00), bottom 2 rows should remain 0xFF
    for (i, byte) in pixel_bytes.iter().enumerate() {
        if i < 8 {
            assert_eq!(*byte, 0x00, "pixel byte {} in masked region should be 0", i);
        } else {
            assert_eq!(
                *byte, 0xFF,
                "pixel byte {} outside masked region should be unchanged",
                i
            );
        }
    }

    // Also verify metadata was de-identified
    let name = result
        .element_by_name("PatientName")
        .expect("should have PatientName");
    let val = name.value().to_str().expect("should read value");
    assert_eq!(val.as_ref(), "ANON");
}

// ============================================================================
// Category 6: Test Recipe File Parsing & Application
// ============================================================================
// Verifies that resources/test_recipe.txt parses and that its header actions
// apply correctly to a representative DICOM object.

#[test]
fn test_recipe_parses_and_applies() {
    let recipe_text =
        fs::read_to_string("resources/test_recipe.txt").expect("should read test recipe file");
    let recipe = Recipe::parse(&recipe_text).expect("test recipe should parse");

    // Verify structure
    assert_eq!(recipe.format, "dicom");
    assert_eq!(
        recipe.filters.len(),
        2,
        "should have graylist and blacklist"
    );

    let graylist = &recipe.filters[0];
    assert_eq!(graylist.labels.len(), 8, "graylist should have 8 labels");

    let blacklist = &recipe.filters[1];
    assert_eq!(blacklist.labels.len(), 3, "blacklist should have 3 labels");

    assert!(
        !recipe.header.is_empty(),
        "header should have at least one action"
    );

    // Build a representative DICOM object
    let mut obj = create_test_file_obj();
    put_str(&mut obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
    put_str(&mut obj, tags::PATIENT_ID, VR::LO, "MRN-12345");
    put_str(&mut obj, tags::PATIENT_SEX, VR::CS, "M");
    put_str(&mut obj, tags::PATIENT_BIRTH_DATE, VR::DA, "19800101");
    put_str(&mut obj, tags::STUDY_DATE, VR::DA, "20230615");
    put_str(&mut obj, tags::SERIES_DATE, VR::DA, "20230615");
    put_str(&mut obj, tags::STUDY_DESCRIPTION, VR::LO, "CT ABDOMEN");
    put_str(&mut obj, tags::INSTITUTION_NAME, VR::LO, "General Hospital");
    put_str(
        &mut obj,
        tags::INSTITUTION_ADDRESS,
        VR::ST,
        "123 Medical Dr",
    );
    put_str(&mut obj, tags::STATION_NAME, VR::SH, "CT-SCANNER-01");
    put_str(&mut obj, tags::MANUFACTURER, VR::LO, "GE MEDICAL");
    put_str(&mut obj, tags::MODALITY, VR::CS, "CT");
    put_str(
        &mut obj,
        tags::REFERRING_PHYSICIAN_NAME,
        VR::PN,
        "Dr. Smith",
    );
    put_str(
        &mut obj,
        tags::PERFORMING_PHYSICIAN_NAME,
        VR::PN,
        "Dr. Jones",
    );
    put_str(&mut obj, tags::OPERATORS_NAME, VR::PN, "Tech One");
    put_str(&mut obj, tags::IMAGE_COMMENTS, VR::LT, "Some comment");
    put_str(&mut obj, tags::PATIENT_ADDRESS, VR::LO, "456 Patient St");
    put_str(&mut obj, tags::OCCUPATION, VR::SH, "Engineer");

    // Provide required variables and a stub hashuid function
    let mut vars = HashMap::new();
    vars.insert("PATIENT_ID".into(), "ANON-001".into());
    vars.insert("PATIENT_NAME".into(), "ANONYMOUS".into());
    vars.insert("DATEINC".into(), "5".into());

    let mut funcs: HashMap<String, DeidFunction> = HashMap::new();
    funcs.insert(
        "hashuid".into(),
        Box::new(|input: &str| Ok(format!("hashed-{}", input))),
    );
    funcs.insert(
        "hash_accession".into(),
        Box::new(|input: &str| Ok(format!("acc-{}", input))),
    );

    apply_header_actions(&recipe.header, &vars, &funcs, &mut obj)
        .expect("applying test recipe header actions should succeed");

    // --- Verify KEEP actions preserved tags ----------------------------------
    let modality = obj
        .element(tags::MODALITY)
        .expect("Modality should be kept")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(modality.as_ref(), "CT");

    let manufacturer = obj
        .element(tags::MANUFACTURER)
        .expect("Manufacturer should be kept")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(manufacturer.as_ref(), "GE MEDICAL");

    let sex = obj
        .element(tags::PATIENT_SEX)
        .expect("PatientSex should be kept")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(sex.as_ref(), "M");

    // --- Verify REPLACE with var: -------------------------------------------
    let pid = obj
        .element(tags::PATIENT_ID)
        .expect("PatientID should be present")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(pid.as_ref(), "ANON-001");

    let pname = obj
        .element(tags::PATIENT_NAME)
        .expect("PatientName should be present")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(pname.as_ref(), "ANONYMOUS");

    // --- Verify REPLACE with literal ----------------------------------------
    let study_id = obj
        .element_by_name("StudyID")
        .expect("StudyID should be present")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(study_id.as_ref(), "ANONYMIZED");

    // --- Verify JITTER shifted dates ----------------------------------------
    let study_date = obj
        .element(tags::STUDY_DATE)
        .expect("StudyDate should be present")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(
        study_date.as_ref(),
        "20230620",
        "StudyDate should be jittered +5 days"
    );

    // --- Verify BLANK actions -----------------------------------------------
    let birth_date = obj
        .element(tags::PATIENT_BIRTH_DATE)
        .expect("PatientBirthDate should be present but blanked")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(birth_date.as_ref(), "");

    let ref_phys = obj
        .element(tags::REFERRING_PHYSICIAN_NAME)
        .expect("ReferringPhysicianName should be present but blanked")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(ref_phys.as_ref(), "");

    // --- Verify REMOVE actions ----------------------------------------------
    assert!(
        obj.element(tags::INSTITUTION_NAME).is_err(),
        "InstitutionName should be removed"
    );
    assert!(
        obj.element(tags::INSTITUTION_ADDRESS).is_err(),
        "InstitutionAddress should be removed"
    );
    assert!(
        obj.element(tags::STATION_NAME).is_err(),
        "StationName should be removed"
    );
    assert!(
        obj.element(tags::STUDY_DESCRIPTION).is_err(),
        "StudyDescription should be removed"
    );
    assert!(
        obj.element(tags::PERFORMING_PHYSICIAN_NAME).is_err(),
        "PerformingPhysicianName should be removed"
    );
    assert!(
        obj.element(tags::OPERATORS_NAME).is_err(),
        "OperatorName should be removed"
    );
    assert!(
        obj.element(tags::IMAGE_COMMENTS).is_err(),
        "ImageComments should be removed"
    );
    assert!(
        obj.element(tags::PATIENT_ADDRESS).is_err(),
        "PatientAddress should be removed"
    );

    // --- Verify ADD actions -------------------------------------------------
    let identity_removed = obj
        .element_by_name("PatientIdentityRemoved")
        .expect("PatientIdentityRemoved should be added")
        .value()
        .to_str()
        .unwrap();
    assert_eq!(identity_removed.as_ref(), "YES");
}

#[test]
fn pipeline_blacklist_writes_report_file() {
    let tmp = TempDir::new().expect("should create temp dir");
    let input_dir = tmp.path().join("input");
    let output_dir = tmp.path().join("output");
    fs::create_dir_all(&input_dir).expect("create input dir");

    // CT file — should pass through
    let mut ct_file = create_test_file_obj();
    put_str(&mut ct_file, tags::MODALITY, VR::CS, "CT");
    ct_file
        .write_to_file(input_dir.join("ct.dcm"))
        .expect("write CT file");

    // SR file — should be blacklisted by "Reject SR" label
    let mut sr_file = FileDicomObject::new_empty_with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax("1.2.840.10008.1.2.1")
            .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.88.11")
            .media_storage_sop_instance_uid("1.2.3.4.5.6.7.8.10")
            .implementation_class_uid("1.2.3.4")
            .build()
            .expect("valid file meta"),
    );
    put_str(&mut sr_file, tags::MODALITY, VR::CS, "SR");
    sr_file
        .write_to_file(input_dir.join("sr.dcm"))
        .expect("write SR file");

    // OT file — should also be blacklisted by "Reject OT" label
    let mut ot_file = FileDicomObject::new_empty_with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax("1.2.840.10008.1.2.1")
            .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.7")
            .media_storage_sop_instance_uid("1.2.3.4.5.6.7.8.11")
            .implementation_class_uid("1.2.3.4")
            .build()
            .expect("valid file meta"),
    );
    put_str(&mut ot_file, tags::MODALITY, VR::CS, "OT");
    ot_file
        .write_to_file(input_dir.join("ot.dcm"))
        .expect("write OT file");

    let recipe_path = tmp.path().join("recipe.txt");
    fs::write(
        &recipe_path,
        "\
FORMAT dicom

%filter blacklist

LABEL Reject SR
equals Modality SR

LABEL Reject OT
equals Modality OT

%header

REPLACE PatientName ANON
",
    )
    .expect("write recipe");

    let config = DeidConfig {
        input_dir: input_dir.clone(),
        output_dir: output_dir.clone(),
        recipe_path,
        variables: HashMap::new(),
        functions: HashMap::new(),
        remove_private_tags: true,
        remove_unspecified_elements: false,
        quarantine_dir: None,
    };

    let pipeline = DeidPipeline::new(config).expect("should create pipeline");
    let report = pipeline.run().expect("should run pipeline");

    assert_eq!(report.files_processed, 1);
    assert_eq!(report.files_blacklisted, 2);

    // Verify blacklist report file was written
    let report_path = output_dir.join("blacklisted_files.txt");
    assert!(report_path.exists(), "blacklisted_files.txt should exist");

    let content = fs::read_to_string(&report_path).expect("read report");
    let lines: Vec<&str> = content.trim().lines().collect();
    assert_eq!(lines.len(), 2, "should have 2 blacklisted entries");

    // Each line should be "relative_path\tLabel Name"
    for line in &lines {
        assert!(
            line.contains('\t'),
            "line should be tab-separated: {}",
            line
        );
    }

    // Verify both files appear with correct reasons
    let has_sr = lines
        .iter()
        .any(|l| l.contains("sr.dcm") && l.contains("Reject SR"));
    let has_ot = lines
        .iter()
        .any(|l| l.contains("ot.dcm") && l.contains("Reject OT"));
    assert!(has_sr, "report should list sr.dcm with reason 'Reject SR'");
    assert!(has_ot, "report should list ot.dcm with reason 'Reject OT'");
}

#[test]
fn pipeline_no_blacklist_no_report_file() {
    let tmp = TempDir::new().expect("should create temp dir");
    let input_dir = tmp.path().join("input");
    let output_dir = tmp.path().join("output");
    fs::create_dir_all(&input_dir).expect("create input dir");

    let mut ct_file = create_test_file_obj();
    put_str(&mut ct_file, tags::MODALITY, VR::CS, "CT");
    ct_file
        .write_to_file(input_dir.join("ct.dcm"))
        .expect("write CT file");

    let recipe_path = tmp.path().join("recipe.txt");
    fs::write(
        &recipe_path,
        "FORMAT dicom\n%header\nREPLACE PatientName ANON\n",
    )
    .expect("write recipe");

    let config = DeidConfig {
        input_dir,
        output_dir: output_dir.clone(),
        recipe_path,
        variables: HashMap::new(),
        functions: HashMap::new(),
        remove_private_tags: true,
        remove_unspecified_elements: false,
        quarantine_dir: None,
    };

    let pipeline = DeidPipeline::new(config).expect("should create pipeline");
    let report = pipeline.run().expect("should run pipeline");

    assert_eq!(report.files_blacklisted, 0);

    // No blacklist report should be created when no files are blacklisted
    let report_path = output_dir.join("blacklisted_files.txt");
    assert!(
        !report_path.exists(),
        "blacklisted_files.txt should NOT exist when no files are blacklisted"
    );
}

// ============================================================================
// Category 6: File Meta Information group (0002)
// ============================================================================

/// Recursively collect every .dcm file under `dir`.
fn find_dcm_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).expect("readable dir") {
        let path = entry.expect("readable entry").path();
        if path.is_dir() {
            found.extend(find_dcm_files(&path));
        } else if path.extension().is_some_and(|e| e == "dcm") {
            found.push(path);
        }
    }
    found
}

/// Requirements r-3-14-3, r-3-14-4
///
/// The original SOP Instance UID must not survive in the file meta group after a
/// recipe hashes the data set copy. Before group 0002 was handled, `(0002,0003)`
/// still held the original identifying UID in every output file.
#[test]
fn e2e_file_meta_uids_are_deidentified_and_consistent() {
    let tmp = TempDir::new().expect("create temp dir");
    let input_dir = tmp.path().join("input");
    let output_dir = tmp.path().join("output");
    fs::create_dir_all(&input_dir).expect("create input dir");

    const ORIGINAL_UID: &str = "1.2.840.113619.2.55.3.604688119.969.1068842234.928";

    let mut file_obj = FileDicomObject::new_empty_with_meta(
        FileMetaTableBuilder::new()
            .transfer_syntax("1.2.840.10008.1.2.1")
            .media_storage_sop_class_uid("1.2.840.10008.5.1.4.1.1.2")
            .media_storage_sop_instance_uid(ORIGINAL_UID)
            .implementation_class_uid("1.2.3.4")
            .source_application_entity_title("ORIGINATING_SITE")
            .build()
            .expect("valid file meta"),
    );
    put_str(&mut file_obj, tags::PATIENT_NAME, VR::PN, "John^Doe");
    put_str(&mut file_obj, tags::MODALITY, VR::CS, "CT");
    put_str(
        &mut file_obj,
        tags::SOP_CLASS_UID,
        VR::UI,
        "1.2.840.10008.5.1.4.1.1.2",
    );
    put_path_tags(&mut file_obj, "PID001", "20250101", "1", ORIGINAL_UID);
    file_obj
        .write_to_file(input_dir.join("input.dcm"))
        .expect("write DICOM file");

    let recipe_path = tmp.path().join("recipe.txt");
    fs::write(
        &recipe_path,
        "FORMAT dicom\n%header\nREPLACE PatientName ANON\nREPLACE SOPInstanceUID func:hashuid\n",
    )
    .expect("write recipe");

    let config = DeidConfig {
        input_dir: input_dir.clone(),
        output_dir: output_dir.clone(),
        recipe_path,
        variables: HashMap::new(),
        functions: HashMap::new(),
        remove_private_tags: true,
        remove_unspecified_elements: false,
        quarantine_dir: None,
    };

    let report = DeidPipeline::new(config)
        .expect("should create pipeline")
        .run()
        .expect("should run pipeline");
    assert_eq!(report.files_processed, 1);

    let outputs = find_dcm_files(&output_dir);
    assert_eq!(outputs.len(), 1, "expected exactly one output file");
    let result = open_file(&outputs[0]).expect("should open output");

    let dataset_uid = result
        .element(tags::SOP_INSTANCE_UID)
        .expect("SOPInstanceUID should be present")
        .value()
        .to_str()
        .expect("readable")
        .to_string();

    assert_ne!(dataset_uid, ORIGINAL_UID, "data set UID should be hashed");
    assert!(
        dataset_uid.starts_with("2.25."),
        "hashuid emits 2.25.* UIDs"
    );

    assert_eq!(
        result.meta().media_storage_sop_instance_uid(),
        dataset_uid,
        "(0002,0003) must match the de-identified (0008,0018)"
    );
    assert_ne!(
        result.meta().media_storage_sop_instance_uid(),
        ORIGINAL_UID,
        "the original SOP Instance UID must not survive in group 0002"
    );
    assert_eq!(
        result.meta().media_storage_sop_class_uid(),
        "1.2.840.10008.5.1.4.1.1.2",
        "(0002,0002) must match the data set SOPClassUID"
    );
    assert_eq!(
        result.meta().source_application_entity_title,
        None,
        "the originating AE title must be stripped"
    );
    assert_eq!(
        result.meta().transfer_syntax(),
        "1.2.840.10008.1.2.1",
        "transfer syntax must be left alone when no pixel work is done"
    );
}
