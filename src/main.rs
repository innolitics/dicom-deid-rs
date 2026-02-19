use dicom_deid_rs::pipeline::{DeidConfig, DeidPipeline};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <input_dir> <output_dir> <recipe_file>", args[0]);
        process::exit(1);
    }

    let config = DeidConfig {
        input_dir: PathBuf::from(&args[1]),
        output_dir: PathBuf::from(&args[2]),
        recipe_path: PathBuf::from(&args[3]),
        variables: HashMap::new(),
        functions: HashMap::new(),
    };

    let pipeline = match DeidPipeline::new(config) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error initializing pipeline: {}", e);
            process::exit(1);
        }
    };

    match pipeline.run() {
        Ok(report) => {
            println!("De-identification complete:");
            println!("  Files processed:  {}", report.files_processed);
            println!("  Files blacklisted: {}", report.files_blacklisted);
            println!("  Files skipped:    {}", report.files_skipped);
        }
        Err(e) => {
            eprintln!("Error running pipeline: {}", e);
            process::exit(1);
        }
    }
}
