# dicom-deid-rs

A DICOM de-identification tool written in Rust. Removes or masks protected health information (PHI) from DICOM files based on a configurable recipe file.

## Features

- **Metadata de-identification** -- add, replace, blank, remove, keep, or jitter DICOM tags
- **Pixel de-identification** -- mask burned-in PHI in pixel data based on tag-driven filter rules
- **Recipe-driven** -- all operations defined in a human-readable recipe file compatible with CTP conventions
- **Compressed pixel data** -- decompresses JPEG Baseline, JPEG Lossless, JPEG 2000, and RLE Lossless before masking
- **Blacklist filtering** -- exclude files from output entirely based on tag conditions
- **Embeddable** -- designed as a library with a CLI frontend; custom functions and variables can be injected at runtime

## Usage

```
dicom-deid-rs <input_dir> <output_dir> <recipe_file> [--var NAME VALUE]...
```

The tool recursively finds all `.dcm` files in `input_dir`, applies the recipe, and writes de-identified files to `output_dir`, preserving the directory structure.

```
dicom-deid-rs ./input ./output recipe.txt \
  --var PATIENT_ID "ANON-001" \
  --var PATIENT_NAME "Anonymous" \
  --var DATEINC "30"
```

## Recipe Format

Recipes begin with a `FORMAT dicom` declaration followed by `%filter` and `%header` sections.

```
FORMAT dicom

%filter blacklist

LABEL Scanned Documents
  contains ImageType Secondary
  contains Modality OT

%filter graylist

LABEL GE CT Dose Report
  contains Modality CT
  + contains Manufacturer GE
  + contains SeriesDescription Dose Report
  coordinates 0,0,512,110

%header

KEEP Modality YES
REPLACE PatientID var:PATIENT_ID
REPLACE PatientName var:PATIENT_NAME
REPLACE SOPInstanceUID func:hashuid
JITTER StudyDate var:DATEINC
BLANK PatientBirthDate YES
REMOVE InstitutionName YES
ADD PatientIdentityRemoved YES
```

### Filter predicates

`contains`, `notcontains`, `equals`, `notequals`, `missing`, `empty`, `present`

### Logical operators

- `+` (AND) and `||` (OR) between condition lines
- Pipe-separated alternatives in values (e.g. `contains Modality CT|MR`)

### Coordinate types

- `coordinates x,y,xmax,ymax` -- raw pixel region to mask
- `ctpcoordinates x,y,width,height` -- CTP format (converted internally)
- `keepcoordinates` / `ctpkeepcoordinates` -- regions to preserve

### Header actions

| Action    | Description                                        |
|-----------|----------------------------------------------------|
| `ADD`     | Add tag if not already present                     |
| `REPLACE` | Set tag value (creates if missing)                 |
| `REMOVE`  | Delete tag entirely                                |
| `BLANK`   | Clear value but keep tag present                   |
| `KEEP`    | Preserve original value (overrides other actions)  |
| `JITTER`  | Shift date/datetime by N days                      |

Precedence when multiple actions target the same tag: KEEP > ADD > REPLACE > JITTER > REMOVE > BLANK

### Value types

- Literal: `REPLACE StudyID ANONYMIZED`
- Variable: `REPLACE PatientID var:PATIENT_ID`
- Function: `REPLACE SOPInstanceUID func:hashuid`

### Tag formats

- Keyword: `PatientName`
- Bare hex: `00120063`
- Parenthesized: `(0008,0050)`

## Library Usage

```rust
use dicom_deid_rs::pipeline::{DeidConfig, DeidPipeline};
use std::collections::HashMap;
use std::path::PathBuf;

let config = DeidConfig {
    input_dir: PathBuf::from("./input"),
    output_dir: PathBuf::from("./output"),
    recipe_path: PathBuf::from("recipe.txt"),
    variables: HashMap::from([
        ("PATIENT_ID".into(), "ANON-001".into()),
    ]),
    functions: HashMap::new(), // hashuid is built-in
};

let pipeline = DeidPipeline::new(config).unwrap();
let report = pipeline.run().unwrap();
println!("Processed: {}, Blacklisted: {}", report.files_processed, report.files_blacklisted);
```

Custom functions can be supplied via `config.functions` to extend the recipe with application-specific logic.

## Building

```
cargo build --release
```

### Feature flags

| Feature    | Default | Description                          |
|------------|---------|--------------------------------------|
| `jpeg2000` | yes     | JPEG 2000 decompression via OpenJPEG |

To build without JPEG 2000 support:

```
cargo build --release --no-default-features
```

## Testing

```
cargo test
```
