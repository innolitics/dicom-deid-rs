use dicom_deid_rs::pipeline::DeidConfig;
use std::collections::HashMap;
use std::env;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} <input_dir> <output_dir> <recipe_file>", args[0]);
        std::process::exit(1);
    }

    let _config = DeidConfig {
        input_dir: PathBuf::from(&args[1]),
        output_dir: PathBuf::from(&args[2]),
        recipe_path: PathBuf::from(&args[3]),
        variables: HashMap::new(),
        functions: HashMap::new(),
    };

    todo!("Pipeline execution not yet implemented");
}
