use crate::error::DeidError;
use crate::filter_index::FilterIndex;
use crate::functions;
use crate::metadata;
use crate::metadata::DeidFunction;
use crate::pixel;
use crate::recipe::Recipe;
use dicom_core::dictionary::{DataDictionary as _, DataDictionaryEntry as _};
use dicom_object::{open_file, InMemDicomObject};
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
#[cfg(feature = "parallel")]
use std::sync::atomic::{AtomicUsize, Ordering};

/// Configuration for the de-identification pipeline.
pub struct DeidConfig {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub recipe_path: PathBuf,
    pub variables: HashMap<String, String>,
    pub functions: HashMap<String, DeidFunction>,
    /// When `true` (the default), all DICOM tags in odd-numbered groups
    /// are stripped after header actions are applied.  Set to `false` to
    /// preserve private tags.
    pub remove_private_tags: bool,
}

/// Summary report after de-identification completes.
pub struct DeidReport {
    pub files_processed: usize,
    pub files_skipped: usize,
    pub files_blacklisted: usize,
}

/// The main de-identification pipeline.
pub struct DeidPipeline {
    pub config: DeidConfig,
    pub recipe: Recipe,
    filter_index: FilterIndex,
}

pub enum FileOutcome {
    Processed(AuditEntry),
    Blacklisted(String),
}

/// Tags extracted for audit logging, matching CTP's DicomAuditLogger.
const AUDIT_TAGS: &[&str] = &[
    "AccessionNumber",
    "StudyInstanceUID",
    "PatientName",
    "PatientID",
    "PatientSex",
    "Manufacturer",
    "ManufacturerModelName",
    "StudyDescription",
    "StudyDate",
    "SeriesInstanceUID",
    "SOPClassUID",
    "Modality",
    "SeriesDescription",
    "Rows",
    "Columns",
    "InstitutionName",
    "StudyTime",
];

/// A snapshot of tag values extracted from a single DICOM file.
pub type TagSnapshot = HashMap<String, String>;

/// Pre- and post-deid tag snapshots for one file.
pub struct AuditEntry {
    pub pre: TagSnapshot,
    pub post: TagSnapshot,
}

fn extract_tags(obj: &InMemDicomObject, tag_names: &[&str]) -> TagSnapshot {
    let dict = dicom_dictionary_std::StandardDataDictionary;
    let mut snapshot = HashMap::new();
    for &name in tag_names {
        let value = dict
            .by_name(name)
            .and_then(|entry| obj.element(entry.tag()).ok())
            .and_then(|elem| elem.value().to_str().ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_default();
        snapshot.insert(name.to_string(), value);
    }
    snapshot
}

impl DeidPipeline {
    /// Create a new pipeline, parsing the recipe from the configured path.
    ///
    /// Built-in functions (e.g. `hashuid`) are registered automatically.
    /// User-supplied functions in `config.functions` take precedence over
    /// built-in functions with the same name.
    pub fn new(mut config: DeidConfig) -> Result<Self, DeidError> {
        let recipe_text = fs::read_to_string(&config.recipe_path)?;
        let recipe = Recipe::parse(&recipe_text)?;
        let mut merged = functions::default_functions();
        for (name, func) in config.functions.drain() {
            merged.insert(name, func);
        }
        config.functions = merged;
        let filter_index = FilterIndex::new(&recipe);
        Ok(DeidPipeline {
            config,
            recipe,
            filter_index,
        })
    }

    /// Create a new pipeline from recipe text directly (avoids temp files).
    pub fn from_recipe_text(recipe_text: &str, mut config: DeidConfig) -> Result<Self, DeidError> {
        let recipe = Recipe::parse(recipe_text)?;
        let mut merged = functions::default_functions();
        for (name, func) in config.functions.drain() {
            merged.insert(name, func);
        }
        config.functions = merged;
        let filter_index = FilterIndex::new(&recipe);
        Ok(DeidPipeline {
            config,
            recipe,
            filter_index,
        })
    }

    /// Recursively search a directory for DICOM files.
    pub fn find_dicom_files(dir: &Path) -> Result<Vec<PathBuf>, DeidError> {
        let mut results = Vec::new();
        find_dicom_files_recursive(dir, &mut results)?;
        Ok(results)
    }

    /// Run the de-identification pipeline.
    pub fn run(&self) -> Result<DeidReport, DeidError> {
        let files = Self::find_dicom_files(&self.config.input_dir)?;
        let pb = ProgressBar::new(files.len() as u64);
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] [{bar:40}] {pos}/{len} ({eta})")
                .expect("valid progress bar template")
                .progress_chars("=> "),
        );

        let mut report = DeidReport {
            files_processed: 0,
            files_skipped: 0,
            files_blacklisted: 0,
        };
        let mut blacklisted_files: Vec<(PathBuf, String)> = Vec::new();
        let mut audit_entries: Vec<AuditEntry> = Vec::new();

        for file_path in &files {
            match self.process_file(file_path) {
                Ok(FileOutcome::Processed(entry)) => {
                    audit_entries.push(entry);
                    report.files_processed += 1;
                }
                Ok(FileOutcome::Blacklisted(reason)) => {
                    let relative = file_path
                        .strip_prefix(&self.config.input_dir)
                        .unwrap_or(file_path);
                    blacklisted_files.push((relative.to_path_buf(), reason));
                    report.files_blacklisted += 1;
                }
                Err(e) => {
                    let msg = format!("Warning: skipping {}: {}", file_path.display(), e);
                    pb.println(&msg);
                    eprintln!("{}", msg);
                    report.files_skipped += 1;
                }
            }
            pb.inc(1);
        }

        pb.finish_with_message("De-identification complete");

        if !blacklisted_files.is_empty() {
            self.write_blacklist_report(&blacklisted_files)?;
        }

        self.write_audit_files(&audit_entries)?;

        Ok(report)
    }

    pub fn process_file(&self, file_path: &Path) -> Result<FileOutcome, DeidError> {
        let mut obj = open_file(file_path).map_err(|e| {
            DeidError::Dicom(format!("failed to open {}: {}", file_path.display(), e))
        })?;

        // Check blacklist
        if let Some(reason) = self.filter_index.blacklist_reason(&obj) {
            return Ok(FileOutcome::Blacklisted(reason.to_string()));
        }

        // Snapshot tags before de-identification
        let pre = extract_tags(&obj, AUDIT_TAGS);

        // Pixel de-identification
        let regions = self.filter_index.get_graylist_regions(&obj);
        if !regions.is_empty() {
            pixel::decompress_pixel_data(&mut obj)?;
            pixel::apply_pixel_mask(&mut obj, &regions)?;
        }

        // Metadata de-identification
        metadata::apply_header_actions(
            &self.recipe.header,
            &self.config.variables,
            &self.config.functions,
            &mut obj,
        )?;
        if self.config.remove_private_tags {
            metadata::remove_private_tags(&mut obj);
        }

        // Snapshot tags after de-identification
        let post = extract_tags(&obj, AUDIT_TAGS);

        // Compute output path preserving directory structure
        let relative = file_path
            .strip_prefix(&self.config.input_dir)
            .map_err(|e| DeidError::Io(std::io::Error::other(e)))?;
        let output_path = self.config.output_dir.join(relative);

        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }

        obj.write_to_file(&output_path).map_err(|e| {
            DeidError::Dicom(format!("failed to write {}: {}", output_path.display(), e))
        })?;

        Ok(FileOutcome::Processed(AuditEntry { pre, post }))
    }

    /// Run the pipeline with a progress callback instead of a progress bar.
    pub fn run_with_progress(
        &self,
        on_progress: impl Fn(usize, usize, usize),
    ) -> Result<DeidReport, DeidError> {
        let files = Self::find_dicom_files(&self.config.input_dir)?;
        let mut report = DeidReport {
            files_processed: 0,
            files_skipped: 0,
            files_blacklisted: 0,
        };
        let mut blacklisted_files: Vec<(PathBuf, String)> = Vec::new();
        let mut audit_entries: Vec<AuditEntry> = Vec::new();

        for file_path in &files {
            match self.process_file(file_path) {
                Ok(FileOutcome::Processed(entry)) => {
                    audit_entries.push(entry);
                    report.files_processed += 1;
                }
                Ok(FileOutcome::Blacklisted(reason)) => {
                    let relative = file_path
                        .strip_prefix(&self.config.input_dir)
                        .unwrap_or(file_path);
                    blacklisted_files.push((relative.to_path_buf(), reason));
                    report.files_blacklisted += 1;
                }
                Err(e) => {
                    eprintln!("Warning: skipping {}: {}", file_path.display(), e);
                    report.files_skipped += 1;
                }
            }
            on_progress(
                report.files_processed,
                report.files_blacklisted,
                report.files_skipped,
            );
        }

        if !blacklisted_files.is_empty() {
            self.write_blacklist_report(&blacklisted_files)?;
        }

        self.write_audit_files(&audit_entries)?;

        Ok(report)
    }

    /// Run the pipeline using parallel file processing via rayon.
    #[cfg(feature = "parallel")]
    pub fn run_parallel(
        &self,
        num_threads: usize,
        on_progress: impl Fn(usize, usize, usize) + Send + Sync,
    ) -> Result<DeidReport, DeidError> {
        use rayon::prelude::*;

        let files = Self::find_dicom_files(&self.config.input_dir)?;

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .map_err(|e| DeidError::Io(std::io::Error::other(e)))?;

        let processed = AtomicUsize::new(0);
        let blacklisted_count = AtomicUsize::new(0);
        let skipped = AtomicUsize::new(0);

        let blacklisted_files: std::sync::Mutex<Vec<(PathBuf, String)>> =
            std::sync::Mutex::new(Vec::new());

        let audit_entries: std::sync::Mutex<Vec<AuditEntry>> =
            std::sync::Mutex::new(Vec::new());

        pool.install(|| {
            files.par_iter().for_each(|file_path| {
                match self.process_file(file_path) {
                    Ok(FileOutcome::Processed(entry)) => {
                        audit_entries.lock().unwrap().push(entry);
                        processed.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(FileOutcome::Blacklisted(reason)) => {
                        let relative = file_path
                            .strip_prefix(&self.config.input_dir)
                            .unwrap_or(file_path);
                        blacklisted_files
                            .lock()
                            .unwrap()
                            .push((relative.to_path_buf(), reason));
                        blacklisted_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        eprintln!("Warning: skipping {}: {}", file_path.display(), e);
                        skipped.fetch_add(1, Ordering::Relaxed);
                    }
                }
                on_progress(
                    processed.load(Ordering::Relaxed),
                    blacklisted_count.load(Ordering::Relaxed),
                    skipped.load(Ordering::Relaxed),
                );
            });
        });

        let blacklisted_files = blacklisted_files.into_inner().unwrap();
        if !blacklisted_files.is_empty() {
            self.write_blacklist_report(&blacklisted_files)?;
        }

        let audit_entries = audit_entries.into_inner().unwrap();
        self.write_audit_files(&audit_entries)?;

        Ok(DeidReport {
            files_processed: processed.into_inner(),
            files_skipped: skipped.into_inner(),
            files_blacklisted: blacklisted_count.into_inner(),
        })
    }

    fn write_blacklist_report(&self, blacklisted: &[(PathBuf, String)]) -> Result<(), DeidError> {
        fs::create_dir_all(&self.config.output_dir)?;
        let report_path = self.config.output_dir.join("blacklisted_files.txt");
        let mut lines = Vec::with_capacity(blacklisted.len());
        for (path, reason) in blacklisted {
            lines.push(format!("{}\t{}", path.display(), reason));
        }
        fs::write(&report_path, lines.join("\n") + "\n")?;
        Ok(())
    }

    /// Write audit CSV files: metadata.csv, deid_metadata.csv, and linker.csv.
    ///
    /// The audit CSVs mirror CTP's DicomAuditLogger output (study-level,
    /// deduplicated by StudyInstanceUID).  The linker CSV maps original
    /// identifiers to their de-identified counterparts, joined by the
    /// engine's knowledge of which input produced which output.
    fn write_audit_files(&self, entries: &[AuditEntry]) -> Result<(), DeidError> {
        fs::create_dir_all(&self.config.output_dir)?;

        // Deduplicate to study level using original StudyInstanceUID
        let mut seen_pre: HashSet<String> = HashSet::new();
        let mut seen_post: HashSet<String> = HashSet::new();
        let mut pre_rows: Vec<&TagSnapshot> = Vec::new();
        let mut post_rows: Vec<&TagSnapshot> = Vec::new();
        let mut linker_rows: Vec<(&TagSnapshot, &TagSnapshot)> = Vec::new();

        for entry in entries {
            let pre_uid = entry.pre.get("StudyInstanceUID").cloned().unwrap_or_default();
            let post_uid = entry.post.get("StudyInstanceUID").cloned().unwrap_or_default();

            if seen_pre.insert(pre_uid) {
                pre_rows.push(&entry.pre);
            }
            if seen_post.insert(post_uid) {
                post_rows.push(&entry.post);
                // Linker entry: one per de-identified study, linking back to original
                linker_rows.push((&entry.pre, &entry.post));
            }
        }

        // Write metadata.csv (pre-deid audit)
        write_tag_csv(
            &self.config.output_dir.join("metadata.csv"),
            AUDIT_TAGS,
            &pre_rows,
        )?;

        // Write deid_metadata.csv (post-deid audit)
        write_tag_csv(
            &self.config.output_dir.join("deid_metadata.csv"),
            AUDIT_TAGS,
            &post_rows,
        )?;

        // Write linker.csv
        let linker_path = self.config.output_dir.join("linker.csv");
        let mut f = fs::File::create(&linker_path)?;
        writeln!(
            f,
            "Original PatientID,Original PatientName,Original AccessionNumber,Original StudyInstanceUID,\
             Deidentified PatientID,Deidentified PatientName,Deidentified AccessionNumber,Deidentified StudyInstanceUID"
        )?;
        for (pre, post) in &linker_rows {
            writeln!(
                f,
                "{},{},{},{},{},{},{},{}",
                csv_escape(pre.get("PatientID").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(pre.get("PatientName").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(pre.get("AccessionNumber").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(pre.get("StudyInstanceUID").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(post.get("PatientID").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(post.get("PatientName").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(post.get("AccessionNumber").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(post.get("StudyInstanceUID").map(|s| s.as_str()).unwrap_or("")),
            )?;
        }

        Ok(())
    }
}

/// Write a CSV file with the given tag columns from a list of snapshots.
fn write_tag_csv(
    path: &Path,
    columns: &[&str],
    rows: &[&TagSnapshot],
) -> Result<(), DeidError> {
    let mut f = fs::File::create(path)?;
    writeln!(f, "{}", columns.join(","))?;
    for row in rows {
        let values: Vec<String> = columns
            .iter()
            .map(|col| csv_escape(row.get(*col).map(|s| s.as_str()).unwrap_or("")))
            .collect();
        writeln!(f, "{}", values.join(","))?;
    }
    Ok(())
}

/// Escape a value for CSV: quote if it contains commas, quotes, or newlines.
fn csv_escape(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

fn find_dicom_files_recursive(dir: &Path, results: &mut Vec<PathBuf>) -> Result<(), DeidError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            find_dicom_files_recursive(&path, results)?;
        } else if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("dcm"))
        {
            results.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // -- r-1-1 ---------------------------------------------------------------

    /// Requirement r-1-1
    #[test]
    fn r1_1_config_accepts_required_paths() {
        let config = DeidConfig {
            input_dir: PathBuf::from("/tmp/input"),
            output_dir: PathBuf::from("/tmp/output"),
            recipe_path: PathBuf::from("/tmp/recipe.txt"),
            variables: HashMap::new(),
            functions: HashMap::new(),
            remove_private_tags: true,
        };
        assert_eq!(config.input_dir, PathBuf::from("/tmp/input"));
        assert_eq!(config.output_dir, PathBuf::from("/tmp/output"));
        assert_eq!(config.recipe_path, PathBuf::from("/tmp/recipe.txt"));
    }

    // -- r-1-2 ---------------------------------------------------------------

    /// Requirement r-1-2
    #[test]
    fn r1_2_recursive_search_finds_dcm_files() {
        let tmp = TempDir::new().expect("should create temp dir");
        let root = tmp.path();

        // Create nested directory structure with .dcm files
        let sub1 = root.join("sub1");
        let sub2 = root.join("sub1").join("sub2");
        fs::create_dir_all(&sub2).expect("should create dirs");

        fs::write(root.join("file1.dcm"), b"DICM").expect("write");
        fs::write(sub1.join("file2.dcm"), b"DICM").expect("write");
        fs::write(sub2.join("file3.dcm"), b"DICM").expect("write");

        // Also create a non-DICOM file to ensure it's excluded
        fs::write(root.join("notes.txt"), b"not a dicom file").expect("write");

        let files = DeidPipeline::find_dicom_files(root).expect("should find files");
        assert_eq!(files.len(), 3, "should find all 3 .dcm files recursively");
    }

    /// Requirement r-1-2
    #[test]
    fn r1_2_empty_directory_returns_empty() {
        let tmp = TempDir::new().expect("should create temp dir");
        let files = DeidPipeline::find_dicom_files(tmp.path()).expect("should handle empty dir");
        assert!(files.is_empty());
    }

    /// Requirement r-1-2
    #[test]
    fn r1_2_find_skips_non_dcm_files() {
        let tmp = TempDir::new().expect("should create temp dir");
        let root = tmp.path();

        fs::write(root.join("image.dcm"), b"DICM").expect("write");
        fs::write(root.join("readme.txt"), b"text").expect("write");
        fs::write(root.join("data.json"), b"{}").expect("write");
        fs::write(root.join("report.pdf"), b"PDF").expect("write");

        let files = DeidPipeline::find_dicom_files(root).expect("should find files");
        assert_eq!(files.len(), 1, "should only find .dcm files");
        assert!(
            files[0]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(".dcm")
        );
    }

    // -- r-1-3 ---------------------------------------------------------------

    /// Requirement r-1-3: full pipeline run with a valid DICOM file
    #[test]
    fn r1_3_run_processes_dicom_file() {
        use crate::test_helpers::*;

        let tmp = TempDir::new().expect("should create temp dir");
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        fs::create_dir_all(&input_dir).expect("create input dir");

        // Create a minimal valid DICOM file
        let mut file_obj = create_test_file_obj();
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::PATIENT_NAME,
            dicom_core::VR::PN,
            "John^Doe",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::MODALITY,
            dicom_core::VR::CS,
            "CT",
        );
        let dcm_path = input_dir.join("test.dcm");
        file_obj
            .write_to_file(&dcm_path)
            .expect("write test DICOM file");

        // Create a minimal recipe file
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
        };

        let pipeline = DeidPipeline::new(config).expect("should create pipeline");
        let report = pipeline.run().expect("should run pipeline");

        assert_eq!(report.files_processed, 1);
        assert_eq!(report.files_skipped, 0);
        assert_eq!(report.files_blacklisted, 0);

        // Verify output file exists
        let output_file = output_dir.join("test.dcm");
        assert!(output_file.exists(), "output file should exist");

        // Verify the patient name was replaced
        let result_obj = open_file(&output_file).expect("should open output");
        let name = result_obj
            .element_by_name("PatientName")
            .expect("should have PatientName");
        let val = name.value().to_str().expect("should read value");
        assert_eq!(val.as_ref(), "ANON");
    }

    // -- r-6-1 ---------------------------------------------------------------

    /// Requirement r-6-1
    #[test]
    fn r6_1_library_api_is_accessible() {
        use crate::recipe::{
            ActionType, ActionValue, Condition, FilterLabel, FilterSection, FilterType,
            HeaderAction, LogicalOp, Predicate, Recipe, TagSpecifier,
        };

        let recipe = Recipe {
            format: "dicom".into(),
            header: vec![HeaderAction {
                action_type: ActionType::Add,
                tag: TagSpecifier::Keyword("PatientIdentityRemoved".into()),
                value: Some(ActionValue::Literal("YES".into())),
            }],
            filters: vec![FilterSection {
                filter_type: FilterType::Blacklist,
                labels: vec![FilterLabel {
                    name: "Test".into(),
                    conditions: vec![Condition {
                        operator: LogicalOp::First,
                        predicate: Predicate::Missing {
                            field: "Modality".into(),
                        },
                    }],
                    coordinates: vec![],
                }],
            }],
        };

        assert_eq!(recipe.format, "dicom");
        assert_eq!(recipe.header.len(), 1);
        assert_eq!(recipe.filters.len(), 1);
    }

    // -- r-1-3 (parallel) ----------------------------------------------------

    /// Requirement r-1-3: run_parallel produces same results as sequential run
    #[cfg(feature = "parallel")]
    #[test]
    fn r1_3_run_parallel_produces_same_results() {
        use crate::test_helpers::*;

        let tmp = TempDir::new().expect("should create temp dir");
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        fs::create_dir_all(&input_dir).expect("create input dir");

        for i in 0..5 {
            let mut file_obj = create_test_file_obj();
            put_str(
                &mut file_obj,
                dicom_dictionary_std::tags::PATIENT_NAME,
                dicom_core::VR::PN,
                &format!("Patient^{}", i),
            );
            put_str(
                &mut file_obj,
                dicom_dictionary_std::tags::MODALITY,
                dicom_core::VR::CS,
                "CT",
            );
            file_obj
                .write_to_file(input_dir.join(format!("test_{}.dcm", i)))
                .expect("write test DICOM file");
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
        };

        let pipeline = DeidPipeline::new(config).expect("should create pipeline");
        let report = pipeline
            .run_parallel(2, |_, _, _| {})
            .expect("should run parallel pipeline");

        assert_eq!(report.files_processed, 5);
        assert_eq!(report.files_skipped, 0);
        assert_eq!(report.files_blacklisted, 0);

        for i in 0..5 {
            let output_file = output_dir.join(format!("test_{}.dcm", i));
            assert!(output_file.exists(), "output file {} should exist", i);
            let result_obj = open_file(&output_file).expect("should open output");
            let name = result_obj
                .element_by_name("PatientName")
                .expect("should have PatientName");
            let val = name.value().to_str().expect("should read value");
            assert_eq!(val.as_ref(), "ANON");
        }
    }

    /// from_recipe_text avoids needing a recipe file on disk
    #[test]
    fn from_recipe_text_works() {
        use crate::test_helpers::*;

        let tmp = TempDir::new().expect("should create temp dir");
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        fs::create_dir_all(&input_dir).expect("create input dir");

        let mut file_obj = create_test_file_obj();
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::PATIENT_NAME,
            dicom_core::VR::PN,
            "John^Doe",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::MODALITY,
            dicom_core::VR::CS,
            "CT",
        );
        file_obj
            .write_to_file(input_dir.join("test.dcm"))
            .expect("write test DICOM file");

        let recipe_text = "FORMAT dicom\n%header\nREPLACE PatientName ANON\n";
        let config = DeidConfig {
            input_dir,
            output_dir: output_dir.clone(),
            recipe_path: PathBuf::new(),
            variables: HashMap::new(),
            functions: HashMap::new(),
            remove_private_tags: true,
        };

        let pipeline =
            DeidPipeline::from_recipe_text(recipe_text, config).expect("should create pipeline");
        let report = pipeline.run_with_progress(|_, _, _| {});
        assert!(report.is_ok());
        let report = report.unwrap();
        assert_eq!(report.files_processed, 1);

        let result_obj = open_file(output_dir.join("test.dcm")).expect("should open output");
        let val = result_obj
            .element_by_name("PatientName")
            .expect("should have PatientName")
            .value()
            .to_str()
            .expect("should read value");
        assert_eq!(val.as_ref(), "ANON");
    }
}
