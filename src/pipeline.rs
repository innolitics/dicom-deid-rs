use crate::error::DeidError;
use crate::filter;
use crate::functions;
use crate::metadata;
use crate::metadata::DeidFunction;
use crate::pixel;
use crate::recipe::Recipe;
use dicom_object::open_file;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Configuration for the de-identification pipeline.
pub struct DeidConfig {
    pub input_dir: PathBuf,
    pub output_dir: PathBuf,
    pub recipe_path: PathBuf,
    pub variables: HashMap<String, String>,
    pub functions: HashMap<String, DeidFunction>,
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
}

enum FileOutcome {
    Processed,
    Blacklisted(String),
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
        // Merge built-in functions; user-supplied functions override defaults.
        let mut merged = functions::default_functions();
        for (name, func) in config.functions.drain() {
            merged.insert(name, func);
        }
        config.functions = merged;
        Ok(DeidPipeline { config, recipe })
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

        for file_path in &files {
            match self.process_file(file_path) {
                Ok(FileOutcome::Processed) => report.files_processed += 1,
                Ok(FileOutcome::Blacklisted(reason)) => {
                    let relative = file_path
                        .strip_prefix(&self.config.input_dir)
                        .unwrap_or(file_path);
                    blacklisted_files.push((relative.to_path_buf(), reason));
                    report.files_blacklisted += 1;
                }
                Err(e) => {
                    pb.println(format!("Warning: skipping {}: {}", file_path.display(), e));
                    report.files_skipped += 1;
                }
            }
            pb.inc(1);
        }

        pb.finish_with_message("De-identification complete");

        if !blacklisted_files.is_empty() {
            self.write_blacklist_report(&blacklisted_files)?;
        }

        Ok(report)
    }

    fn process_file(&self, file_path: &Path) -> Result<FileOutcome, DeidError> {
        let mut obj = open_file(file_path).map_err(|e| {
            DeidError::Dicom(format!("failed to open {}: {}", file_path.display(), e))
        })?;

        // Check blacklist
        if let Some(reason) = filter::blacklist_reason(&self.recipe, &obj) {
            return Ok(FileOutcome::Blacklisted(reason.to_string()));
        }

        // Pixel de-identification
        let regions = filter::get_graylist_regions(&self.recipe, &obj);
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
        metadata::remove_private_tags(&mut obj);

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

        Ok(FileOutcome::Processed)
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
        // Verify that the core library types can be constructed and used
        // programmatically, confirming the software is designed as a library.
        use crate::filter::{evaluate_conditions, is_blacklisted};
        use crate::metadata::apply_header_actions;
        use crate::pixel::apply_pixel_mask;
        use crate::recipe::{
            ActionType, ActionValue, Condition, CoordinateRegion, FilterLabel, FilterSection,
            FilterType, HeaderAction, LogicalOp, Predicate, Recipe, TagSpecifier,
        };

        // Construct a recipe programmatically
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

        // The types compile and can be inspected
        assert_eq!(recipe.format, "dicom");
        assert_eq!(recipe.header.len(), 1);
        assert_eq!(recipe.filters.len(), 1);
    }
}
