use crate::error::DeidError;
use crate::filter_index::FilterIndex;
use crate::functions;
use crate::metadata;
use crate::metadata::DeidFunction;
use crate::pixel;
use crate::recipe::Recipe;
use dicom_core::dictionary::{DataDictionary as _, DataDictionaryEntry as _};
use dicom_object::{InMemDicomObject, open_file};
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
    /// When `true`, tags not targeted by any header action are removed
    /// after processing.  Exempt: SOPClassUID, SOPInstanceUID,
    /// StudyInstanceUID, group 0028, and any keep-groups from the recipe.
    pub remove_unspecified_elements: bool,
    /// When set, files that are blacklisted or fail to parse are copied
    /// into this directory (preserving the relative path from
    /// `input_dir`), and the `blacklisted_files.txt` report is written
    /// here instead of `output_dir`.  When `None`, no copies are made and
    /// the report lands in `output_dir` for backward compatibility.
    pub quarantine_dir: Option<PathBuf>,
}

/// Summary report after de-identification completes.
pub struct DeidReport {
    pub files_processed: usize,
    pub files_skipped: usize,
    pub files_blacklisted: usize,
    /// Files that were blacklisted/skipped but could not be copied into the
    /// quarantine directory (disk full, permission denied, etc.).  The file
    /// is still counted as quarantined for the purposes of
    /// `files_blacklisted` / `files_skipped`; this field surfaces the
    /// persistence failure for downstream reporting.
    pub files_quarantine_copy_failed: usize,
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

/// Build output path from de-identified DICOM tags, matching CTP's structure:
///   `DATE-{StudyDate}--{Modality}--PID-{PatientID}/SER-{SeriesNumber}/{SOPInstanceUID}.dcm`
fn build_output_path(output_dir: &Path, obj: &InMemDicomObject) -> PathBuf {
    let dict = dicom_dictionary_std::StandardDataDictionary;

    let get = |name: &str| -> String {
        dict.by_name(name)
            .and_then(|entry| obj.element(entry.tag()).ok())
            .and_then(|elem| elem.value().to_str().ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    };

    let study_date = get("StudyDate");
    let modality = get("Modality");
    let patient_id = get("PatientID");
    let series_number = get("SeriesNumber");
    let sop_instance_uid = get("SOPInstanceUID");

    let date_part = if study_date.is_empty() {
        "UNKNOWN".to_string()
    } else {
        study_date
    };
    let modality_part = if modality.is_empty() {
        "UN".to_string()
    } else {
        modality
    };
    let pid_part = if patient_id.is_empty() {
        "UNKNOWN".to_string()
    } else {
        patient_id
    };
    let ser_part = if series_number.is_empty() {
        "SER-00000".to_string()
    } else if let Ok(n) = series_number.parse::<u32>() {
        format!("SER-{:05}", n)
    } else {
        format!("SER-{}", series_number)
    };
    let file_name = if sop_instance_uid.is_empty() {
        "unknown.dcm".to_string()
    } else {
        format!("{}.dcm", sop_instance_uid)
    };

    let study_dir = format!("DATE-{}--{}--PID-{}", date_part, modality_part, pid_part);
    output_dir.join(study_dir).join(ser_part).join(file_name)
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
        use std::io::IsTerminal;

        let files = Self::find_dicom_files(&self.config.input_dir)?;
        let total = files.len();
        let is_tty = std::io::stderr().is_terminal();

        let pb = if is_tty {
            let pb = ProgressBar::new(total as u64);
            pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] [{bar:40}] {pos}/{len} ({eta})",
                )
                .expect("valid progress bar template")
                .progress_chars("=> "),
            );
            Some(pb)
        } else {
            eprintln!("Processing {} files", total);
            None
        };

        let mut report = DeidReport {
            files_processed: 0,
            files_skipped: 0,
            files_blacklisted: 0,
            files_quarantine_copy_failed: 0,
        };
        let mut blacklisted_files: Vec<(PathBuf, String)> = Vec::new();
        let mut audit_entries: Vec<AuditEntry> = Vec::new();
        let log_interval = std::cmp::max(total / 20, 1);

        for (i, file_path) in files.iter().enumerate() {
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
                    if let Err(e) = self.copy_to_quarantine(file_path) {
                        eprintln!(
                            "Warning: failed to copy {} to quarantine: {}",
                            file_path.display(),
                            e
                        );
                        report.files_quarantine_copy_failed += 1;
                    }
                }
                Err(e) => {
                    let msg = format!("Warning: skipping {}: {}", file_path.display(), e);
                    if let Some(ref pb) = pb {
                        pb.println(&msg);
                    }
                    eprintln!("{}", msg);
                    report.files_skipped += 1;
                    if let Err(e) = self.copy_to_quarantine(file_path) {
                        eprintln!(
                            "Warning: failed to copy {} to quarantine: {}",
                            file_path.display(),
                            e
                        );
                        report.files_quarantine_copy_failed += 1;
                    }
                }
            }
            if let Some(ref pb) = pb {
                pb.inc(1);
            } else if (i + 1) % log_interval == 0 || i + 1 == total {
                eprintln!(
                    "Progress: {}/{} files ({} processed, {} blacklisted, {} skipped)",
                    i + 1,
                    total,
                    report.files_processed,
                    report.files_blacklisted,
                    report.files_skipped,
                );
            }
        }

        if let Some(pb) = pb {
            pb.finish_with_message("De-identification complete");
        }

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

        // Remove unspecified elements if configured
        if self.config.remove_unspecified_elements {
            metadata::remove_unspecified_elements(&mut obj, &self.recipe);
        }

        // Snapshot tags after de-identification
        let post = extract_tags(&obj, AUDIT_TAGS);

        // Build output path using de-identified tags to match CTP's structure:
        //   DATE-{StudyDate}--{Modality}--PID-{PatientID}/SER-{SeriesNumber}/{SOPInstanceUID}.dcm
        let output_path = build_output_path(&self.config.output_dir, &obj);

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
            files_quarantine_copy_failed: 0,
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
                    if let Err(e) = self.copy_to_quarantine(file_path) {
                        eprintln!(
                            "Warning: failed to copy {} to quarantine: {}",
                            file_path.display(),
                            e
                        );
                        report.files_quarantine_copy_failed += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Warning: skipping {}: {}", file_path.display(), e);
                    report.files_skipped += 1;
                    if let Err(e) = self.copy_to_quarantine(file_path) {
                        eprintln!(
                            "Warning: failed to copy {} to quarantine: {}",
                            file_path.display(),
                            e
                        );
                        report.files_quarantine_copy_failed += 1;
                    }
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

        let audit_entries: std::sync::Mutex<Vec<AuditEntry>> = std::sync::Mutex::new(Vec::new());

        let quarantine_copy_failed = AtomicUsize::new(0);

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
                        if let Err(e) = self.copy_to_quarantine(file_path) {
                            eprintln!(
                                "Warning: failed to copy {} to quarantine: {}",
                                file_path.display(),
                                e
                            );
                            quarantine_copy_failed.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(e) => {
                        eprintln!("Warning: skipping {}: {}", file_path.display(), e);
                        skipped.fetch_add(1, Ordering::Relaxed);
                        if let Err(e) = self.copy_to_quarantine(file_path) {
                            eprintln!(
                                "Warning: failed to copy {} to quarantine: {}",
                                file_path.display(),
                                e
                            );
                            quarantine_copy_failed.fetch_add(1, Ordering::Relaxed);
                        }
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
            files_quarantine_copy_failed: quarantine_copy_failed.into_inner(),
        })
    }

    fn write_blacklist_report(&self, blacklisted: &[(PathBuf, String)]) -> Result<(), DeidError> {
        // Prefer the quarantine directory so the report sits alongside the
        // files it describes; fall back to output_dir for backward
        // compatibility when no quarantine_dir is configured.
        let dest_dir = self
            .config
            .quarantine_dir
            .as_deref()
            .unwrap_or(&self.config.output_dir);
        fs::create_dir_all(dest_dir)?;
        let report_path = dest_dir.join("blacklisted_files.txt");
        let mut lines = Vec::with_capacity(blacklisted.len());
        for (path, reason) in blacklisted {
            lines.push(format!("{}\t{}", path.display(), reason));
        }
        fs::write(&report_path, lines.join("\n") + "\n")?;
        Ok(())
    }

    /// Copy a source file to the configured quarantine directory, preserving
    /// its path relative to `input_dir`.  Returns `Ok(())` when no quarantine
    /// directory is configured (no-op).
    fn copy_to_quarantine(&self, file_path: &Path) -> Result<(), DeidError> {
        let Some(quarantine_dir) = &self.config.quarantine_dir else {
            return Ok(());
        };
        let relative = file_path
            .strip_prefix(&self.config.input_dir)
            .unwrap_or(file_path);
        let dest = quarantine_dir.join(relative);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(file_path, &dest)?;
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
            let pre_uid = entry
                .pre
                .get("StudyInstanceUID")
                .cloned()
                .unwrap_or_default();
            let post_uid = entry
                .post
                .get("StudyInstanceUID")
                .cloned()
                .unwrap_or_default();

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
                csv_escape(
                    pre.get("StudyInstanceUID")
                        .map(|s| s.as_str())
                        .unwrap_or("")
                ),
                csv_escape(post.get("PatientID").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(post.get("PatientName").map(|s| s.as_str()).unwrap_or("")),
                csv_escape(
                    post.get("AccessionNumber")
                        .map(|s| s.as_str())
                        .unwrap_or("")
                ),
                csv_escape(
                    post.get("StudyInstanceUID")
                        .map(|s| s.as_str())
                        .unwrap_or("")
                ),
            )?;
        }

        Ok(())
    }
}

/// Write a CSV file with the given tag columns from a list of snapshots.
fn write_tag_csv(path: &Path, columns: &[&str], rows: &[&TagSnapshot]) -> Result<(), DeidError> {
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

/// Check whether a file is a DICOM file.
///
/// Returns `true` if the file has a `.dcm` extension *or* contains the DICM
/// preamble at byte offset 128.  The preamble check allows detection of
/// DICOM files produced by tools like `storescp` that don't use a `.dcm`
/// extension.
fn is_dicom_file(path: &Path) -> bool {
    if path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("dcm"))
    {
        return true;
    }
    // Check for DICM preamble
    let Ok(mut f) = fs::File::open(path) else {
        return false;
    };
    use std::io::{Read, Seek, SeekFrom};
    if f.seek(SeekFrom::Start(128)).is_err() {
        return false;
    }
    let mut magic = [0u8; 4];
    if f.read_exact(&mut magic).is_err() {
        return false;
    }
    &magic == b"DICM"
}

fn find_dicom_files_recursive(dir: &Path, results: &mut Vec<PathBuf>) -> Result<(), DeidError> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            find_dicom_files_recursive(&path, results)?;
        } else if is_dicom_file(&path) {
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
            remove_unspecified_elements: false,
            quarantine_dir: None,
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
        assert_eq!(files.len(), 1, "should only find DICOM files");
        assert!(
            files[0]
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .ends_with(".dcm")
        );
    }

    /// Files without .dcm extension but with a valid DICM preamble
    /// (e.g. from storescp) should be detected.
    #[test]
    fn r1_2_find_detects_dicom_by_preamble() {
        let tmp = TempDir::new().expect("should create temp dir");
        let root = tmp.path();

        // storescp writes files named by SOP Instance UID, no extension
        let mut preamble_file = vec![0u8; 128];
        preamble_file.extend_from_slice(b"DICM");
        fs::write(root.join("1.2.3.4.5.6.7.8.9"), &preamble_file).expect("write");

        // A non-DICOM file without extension
        fs::write(root.join("blacklisted_files.txt"), b"some text").expect("write");

        let files = DeidPipeline::find_dicom_files(root).expect("should find files");
        assert_eq!(files.len(), 1, "should detect DICOM file by preamble");
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

        // Create a minimal valid DICOM file with tags needed for output path
        let mut file_obj = create_test_file_obj();
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::PATIENT_NAME,
            dicom_core::VR::PN,
            "John^Doe",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::PATIENT_ID,
            dicom_core::VR::LO,
            "PID001",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::MODALITY,
            dicom_core::VR::CS,
            "CT",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::STUDY_DATE,
            dicom_core::VR::DA,
            "20250101",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::SERIES_NUMBER,
            dicom_core::VR::IS,
            "1",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::SOP_INSTANCE_UID,
            dicom_core::VR::UI,
            "1.2.3.4.5.6.7.8.9",
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
            remove_unspecified_elements: false,
            quarantine_dir: None,
        };

        let pipeline = DeidPipeline::new(config).expect("should create pipeline");
        let report = pipeline.run().expect("should run pipeline");

        assert_eq!(report.files_processed, 1);
        assert_eq!(report.files_skipped, 0);
        assert_eq!(report.files_blacklisted, 0);

        // Verify output file exists at CTP-style path
        let output_file = output_dir
            .join("DATE-20250101--CT--PID-PID001")
            .join("SER-00001")
            .join("1.2.3.4.5.6.7.8.9.dcm");
        assert!(output_file.exists(), "output file should exist at CTP-style path: {}", output_file.display());

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
                condition: None,
            }],
            keep_groups: vec![],
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
            remove_unspecified_elements: false,
            quarantine_dir: None,
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
            dicom_dictionary_std::tags::PATIENT_ID,
            dicom_core::VR::LO,
            "PID001",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::MODALITY,
            dicom_core::VR::CS,
            "CT",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::STUDY_DATE,
            dicom_core::VR::DA,
            "20250101",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::SERIES_NUMBER,
            dicom_core::VR::IS,
            "1",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::SOP_INSTANCE_UID,
            dicom_core::VR::UI,
            "1.2.3.4.5.6.7.8.9",
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
            remove_unspecified_elements: false,
            quarantine_dir: None,
        };

        let pipeline =
            DeidPipeline::from_recipe_text(recipe_text, config).expect("should create pipeline");
        let report = pipeline.run_with_progress(|_, _, _| {});
        assert!(report.is_ok());
        let report = report.unwrap();
        assert_eq!(report.files_processed, 1);

        let output_file = output_dir
            .join("DATE-20250101--CT--PID-PID001")
            .join("SER-00001")
            .join("1.2.3.4.5.6.7.8.9.dcm");
        let result_obj = open_file(&output_file).expect("should open output");
        let val = result_obj
            .element_by_name("PatientName")
            .expect("should have PatientName")
            .value()
            .to_str()
            .expect("should read value");
        assert_eq!(val.as_ref(), "ANON");
    }

    // -- quarantine-dir regression -------------------------------------------

    /// Build a minimal valid DICOM at `path` with the tags needed for routing.
    fn write_test_dicom(path: &Path, modality: &str) {
        use crate::test_helpers::*;
        let mut file_obj = create_test_file_obj();
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::PATIENT_NAME,
            dicom_core::VR::PN,
            "Test^Patient",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::PATIENT_ID,
            dicom_core::VR::LO,
            "PID123",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::MODALITY,
            dicom_core::VR::CS,
            modality,
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::STUDY_DATE,
            dicom_core::VR::DA,
            "20250101",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::SERIES_NUMBER,
            dicom_core::VR::IS,
            "1",
        );
        put_str(
            &mut file_obj,
            dicom_dictionary_std::tags::SOP_INSTANCE_UID,
            dicom_core::VR::UI,
            "1.2.3",
        );
        file_obj.write_to_file(path).expect("write test DICOM");
    }

    /// A file matched by the recipe blacklist is copied into `quarantine_dir`
    /// at the same relative path as under `input_dir`.
    #[test]
    fn blacklisted_files_are_copied_to_quarantine_dir() {
        let tmp = TempDir::new().expect("tmp");
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        let quarantine_dir = tmp.path().join("quarantine");
        fs::create_dir_all(input_dir.join("sub")).expect("mkdir");

        // Two files, both CT — both will match the blacklist below.
        write_test_dicom(&input_dir.join("a.dcm"), "CT");
        write_test_dicom(&input_dir.join("sub").join("b.dcm"), "CT");

        let recipe_path = tmp.path().join("recipe.txt");
        fs::write(
            &recipe_path,
            "FORMAT dicom\n%filter blacklist\nLABEL reject_ct\n  equals Modality CT\n",
        )
        .expect("write recipe");

        let config = DeidConfig {
            input_dir: input_dir.clone(),
            output_dir,
            recipe_path,
            variables: HashMap::new(),
            functions: HashMap::new(),
            remove_private_tags: true,
            remove_unspecified_elements: false,
            quarantine_dir: Some(quarantine_dir.clone()),
        };

        let pipeline = DeidPipeline::new(config).expect("pipeline");
        let report = pipeline.run().expect("run");

        assert_eq!(report.files_blacklisted, 2);
        assert_eq!(report.files_processed, 0);
        assert_eq!(report.files_quarantine_copy_failed, 0);
        assert!(
            quarantine_dir.join("a.dcm").exists(),
            "a.dcm should be copied into quarantine root"
        );
        assert!(
            quarantine_dir.join("sub").join("b.dcm").exists(),
            "b.dcm should preserve its relative sub/ path"
        );
    }

    /// A non-DICOM `.dcm` file is counted as skipped (parse failure) and
    /// also copied into `quarantine_dir`.
    #[test]
    fn skipped_files_are_copied_to_quarantine_dir() {
        let tmp = TempDir::new().expect("tmp");
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        let quarantine_dir = tmp.path().join("quarantine");
        fs::create_dir_all(&input_dir).expect("mkdir");

        // A file with .dcm extension but non-DICOM contents → open_file fails.
        let bad = input_dir.join("garbage.dcm");
        fs::write(&bad, b"this is not DICOM").expect("write");

        let recipe_path = tmp.path().join("recipe.txt");
        fs::write(&recipe_path, "FORMAT dicom\n").expect("write recipe");

        let config = DeidConfig {
            input_dir: input_dir.clone(),
            output_dir,
            recipe_path,
            variables: HashMap::new(),
            functions: HashMap::new(),
            remove_private_tags: true,
            remove_unspecified_elements: false,
            quarantine_dir: Some(quarantine_dir.clone()),
        };

        let pipeline = DeidPipeline::new(config).expect("pipeline");
        let report = pipeline.run().expect("run");

        assert_eq!(report.files_skipped, 1);
        assert_eq!(report.files_processed, 0);
        assert_eq!(report.files_quarantine_copy_failed, 0);
        assert!(
            quarantine_dir.join("garbage.dcm").exists(),
            "garbage.dcm should be copied into quarantine"
        );
    }

    /// When a quarantine_dir is configured, `blacklisted_files.txt` lives
    /// in the quarantine dir (not in output_dir).
    #[test]
    fn blacklist_report_written_to_quarantine_dir_when_configured() {
        let tmp = TempDir::new().expect("tmp");
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        let quarantine_dir = tmp.path().join("quarantine");
        fs::create_dir_all(&input_dir).expect("mkdir");
        write_test_dicom(&input_dir.join("a.dcm"), "CT");

        let recipe_path = tmp.path().join("recipe.txt");
        fs::write(
            &recipe_path,
            "FORMAT dicom\n%filter blacklist\nLABEL reject_ct\n  equals Modality CT\n",
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
            quarantine_dir: Some(quarantine_dir.clone()),
        };

        let pipeline = DeidPipeline::new(config).expect("pipeline");
        pipeline.run().expect("run");

        assert!(
            quarantine_dir.join("blacklisted_files.txt").exists(),
            "report should live in quarantine_dir"
        );
        assert!(
            !output_dir.join("blacklisted_files.txt").exists(),
            "report should NOT be written to output_dir when quarantine_dir is set"
        );
    }

    /// Without a quarantine_dir, the report falls back to output_dir
    /// (backward-compat guarantee for direct library consumers).
    #[test]
    fn blacklist_report_falls_back_to_output_dir() {
        let tmp = TempDir::new().expect("tmp");
        let input_dir = tmp.path().join("input");
        let output_dir = tmp.path().join("output");
        fs::create_dir_all(&input_dir).expect("mkdir");
        write_test_dicom(&input_dir.join("a.dcm"), "CT");

        let recipe_path = tmp.path().join("recipe.txt");
        fs::write(
            &recipe_path,
            "FORMAT dicom\n%filter blacklist\nLABEL reject_ct\n  equals Modality CT\n",
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

        let pipeline = DeidPipeline::new(config).expect("pipeline");
        pipeline.run().expect("run");

        assert!(
            output_dir.join("blacklisted_files.txt").exists(),
            "report should still land in output_dir when no quarantine_dir is set"
        );
    }
}
