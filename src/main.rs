use dicom_deid_rs::functions::create_lookup_function;
use dicom_deid_rs::pipeline::{DeidConfig, DeidPipeline};
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;
use std::process;

fn print_usage(program: &str) {
    eprintln!(
        "Usage: {} <input_dir> <output_dir> <recipe_file> [OPTIONS]",
        program
    );
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --var NAME VALUE       Define a recipe variable (can be repeated)");
    eprintln!("  --lookup-table PATH    Load a CTP-format lookup table for func:lookup");
    eprintln!("  --keep-private-tags    Preserve private tags (odd-numbered groups)");
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        print_usage(&args[0]);
        process::exit(1);
    }

    let mut variables: HashMap<String, String> = HashMap::new();
    let mut lookup_table_path: Option<PathBuf> = None;
    let mut remove_private_tags = true;
    let mut i = 4;
    while i < args.len() {
        match args[i].as_str() {
            "--var" => {
                if i + 2 >= args.len() {
                    eprintln!("Error: --var requires NAME and VALUE arguments");
                    print_usage(&args[0]);
                    process::exit(1);
                }
                variables.insert(args[i + 1].clone(), args[i + 2].clone());
                i += 3;
            }
            "--lookup-table" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: --lookup-table requires a PATH argument");
                    print_usage(&args[0]);
                    process::exit(1);
                }
                lookup_table_path = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--keep-private-tags" => {
                remove_private_tags = false;
                i += 1;
            }
            _ => {
                eprintln!("Error: unknown argument '{}'", args[i]);
                print_usage(&args[0]);
                process::exit(1);
            }
        }
    }

    let mut functions = HashMap::new();
    if let Some(ref table_path) = lookup_table_path {
        match create_lookup_function(table_path) {
            Ok(lookup_fns) => {
                for (name, func) in lookup_fns {
                    functions.insert(name, func);
                }
            }
            Err(e) => {
                eprintln!("Error loading lookup table: {}", e);
                process::exit(1);
            }
        }
    }

    let config = DeidConfig {
        input_dir: PathBuf::from(&args[1]),
        output_dir: PathBuf::from(&args[2]),
        recipe_path: PathBuf::from(&args[3]),
        variables,
        functions,
        remove_private_tags,
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
