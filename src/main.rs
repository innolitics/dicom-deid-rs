use dicom_deid_rs::pipeline::{DeidConfig, DeidPipeline};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process;

fn print_usage(program: &str) {
    eprintln!(
        "Usage: {} <input_dir> <output_dir> <recipe_file> [--var NAME VALUE]...",
        program
    );
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --var NAME VALUE   Define a recipe variable (can be repeated)");
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        print_usage(&args[0]);
        process::exit(1);
    }

    let mut variables: HashMap<String, String> = HashMap::new();
    let mut i = 4;
    while i < args.len() {
        if args[i] == "--var" {
            if i + 2 >= args.len() {
                eprintln!("Error: --var requires NAME and VALUE arguments");
                print_usage(&args[0]);
                process::exit(1);
            }
            variables.insert(args[i + 1].clone(), args[i + 2].clone());
            i += 3;
        } else {
            eprintln!("Error: unknown argument '{}'", args[i]);
            print_usage(&args[0]);
            process::exit(1);
        }
    }

    let config = DeidConfig {
        input_dir: PathBuf::from(&args[1]),
        output_dir: PathBuf::from(&args[2]),
        recipe_path: PathBuf::from(&args[3]),
        variables,
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
