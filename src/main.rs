use dicom_deid_rs::ctp;
use dicom_deid_rs::functions::create_lookup_function;
use dicom_deid_rs::pipeline::{DeidConfig, DeidPipeline};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process;

fn print_usage(program: &str) {
    eprintln!(
        "Usage: {} <input_dir> <output_dir> <recipe_file> [OPTIONS]",
        program
    );
    eprintln!("       {} translate-ctp <anonymizer.xml> [OPTIONS]", program);
    eprintln!();
    eprintln!("Pipeline options:");
    eprintln!("  --var NAME VALUE              Define a recipe variable (can be repeated)");
    eprintln!("  --lookup-table PATH           Load a CTP-format lookup table for func:lookup");
    eprintln!("  --quarantine-dir PATH         Copy blacklisted/skipped files here for review");
    eprintln!("  --keep-private-tags           Preserve private tags (odd-numbered groups)");
    eprintln!("  --remove-unspecified-elements  Remove tags not targeted by any recipe action");
    eprintln!();
    eprintln!("translate-ctp options:");
    eprintln!("  --pixel PATH           CTP pixel anonymizer script");
    eprintln!("  --filter PATH          CTP filter script (whitelist)");
    eprintln!("  --blacklist PATH       CTP filter script for blacklist (reject matching)");
    eprintln!("  -o, --output PATH      Write recipe to file (default: stdout)");
}

fn main() {
    let args: Vec<String> = env::args().collect();

    // Check for translate-ctp subcommand
    if args.len() >= 2 && args[1] == "translate-ctp" {
        run_translate_ctp(&args);
        return;
    }

    if args.len() < 4 {
        print_usage(&args[0]);
        process::exit(1);
    }

    let mut variables: HashMap<String, String> = HashMap::new();
    let mut lookup_table_path: Option<PathBuf> = None;
    let mut quarantine_dir: Option<PathBuf> = None;
    let mut remove_private_tags = true;
    let mut remove_unspecified_elements = false;
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
            "--quarantine-dir" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: --quarantine-dir requires a PATH argument");
                    print_usage(&args[0]);
                    process::exit(1);
                }
                quarantine_dir = Some(PathBuf::from(&args[i + 1]));
                i += 2;
            }
            "--keep-private-tags" => {
                remove_private_tags = false;
                i += 1;
            }
            "--remove-unspecified-elements" => {
                remove_unspecified_elements = true;
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
        remove_unspecified_elements,
        quarantine_dir,
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
            if report.files_quarantine_copy_failed > 0 {
                println!(
                    "  Files quarantine-copy-failed: {}",
                    report.files_quarantine_copy_failed
                );
            }
        }
        Err(e) => {
            eprintln!("Error running pipeline: {}", e);
            process::exit(1);
        }
    }
}

fn run_translate_ctp(args: &[String]) {
    if args.len() < 3 {
        eprintln!("Usage: {} translate-ctp <anonymizer.xml> [--pixel PATH] [--filter PATH] [-o PATH]", args[0]);
        process::exit(1);
    }

    let anonymizer_path = &args[2];
    let mut pixel_path: Option<String> = None;
    let mut filter_path: Option<String> = None;
    let mut blacklist_path: Option<String> = None;
    let mut output_path: Option<String> = None;

    let mut i = 3;
    while i < args.len() {
        match args[i].as_str() {
            "--pixel" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: --pixel requires a PATH argument");
                    process::exit(1);
                }
                pixel_path = Some(args[i + 1].clone());
                i += 2;
            }
            "--filter" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: --filter requires a PATH argument");
                    process::exit(1);
                }
                filter_path = Some(args[i + 1].clone());
                i += 2;
            }
            "--blacklist" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: --blacklist requires a PATH argument");
                    process::exit(1);
                }
                blacklist_path = Some(args[i + 1].clone());
                i += 2;
            }
            "-o" | "--output" => {
                if i + 1 >= args.len() {
                    eprintln!("Error: --output requires a PATH argument");
                    process::exit(1);
                }
                output_path = Some(args[i + 1].clone());
                i += 2;
            }
            _ => {
                eprintln!("Error: unknown argument '{}'", args[i]);
                process::exit(1);
            }
        }
    }

    let anonymizer_xml = fs::read_to_string(anonymizer_path).unwrap_or_else(|e| {
        eprintln!("Error reading {}: {}", anonymizer_path, e);
        process::exit(1);
    });

    let pixel_text = pixel_path.map(|p| {
        fs::read_to_string(&p).unwrap_or_else(|e| {
            eprintln!("Error reading {}: {}", p, e);
            process::exit(1);
        })
    });

    let filter_text = filter_path.map(|p| {
        fs::read_to_string(&p).unwrap_or_else(|e| {
            eprintln!("Error reading {}: {}", p, e);
            process::exit(1);
        })
    });

    let blacklist_text = blacklist_path.map(|p| {
        fs::read_to_string(&p).unwrap_or_else(|e| {
            eprintln!("Error reading {}: {}", p, e);
            process::exit(1);
        })
    });

    let result = ctp::translate_ctp_scripts_with_blacklist(
        Some(&anonymizer_xml),
        pixel_text.as_deref(),
        filter_text.as_deref(),
        blacklist_text.as_deref(),
    )
    .unwrap_or_else(|e| {
        eprintln!("Error translating CTP scripts: {}", e);
        process::exit(1);
    });

    // Write recipe text
    if let Some(ref path) = output_path {
        fs::write(path, &result.recipe_text).unwrap_or_else(|e| {
            eprintln!("Error writing {}: {}", path, e);
            process::exit(1);
        });
        eprintln!("Recipe written to {}", path);
    } else {
        print!("{}", result.recipe_text);
    }

    // Print variables and config to stderr
    if !result.variables.is_empty() {
        eprintln!("\nVariables:");
        for (name, value) in &result.variables {
            eprintln!("  {} = {}", name, value);
        }
    }
    eprintln!("\nConfig:");
    eprintln!("  remove_private_tags: {}", result.remove_private_tags);
    eprintln!(
        "  remove_unspecified_elements: {}",
        result.remove_unspecified_elements
    );
}
