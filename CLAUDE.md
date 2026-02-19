# DICOM De-id

This tool is a dicom de-identification utility written in Rust.

In the resources directory, two different reference dicom deid implementations
are provided. MIRC2 is RSNA's clinical trial processor (CTP), which should be
treated as the canonical reference implementation. Another implementation,
`deid`, is a python implementation leveraging pydicom that is meant to
replicate many behaviors of CTP. An example recipe from the python `deid`
program is included as `recipes.txt`.

## Deidentification Types

There are two key areas of DICOM de-identification:

1. Pixel-based deid-- masking over pixel areas that are known to contain PHI
   based on certain DICOM tags

2. Metadata-based deid-- removing, modifying, or adding new tags to remove or
   mask PHI in DICOM metadata

## Software Requirements

Requirements are recorded in the top-level requirements.md file.

All requirements should be covered by unit tests.

## Quality Checks

- Run cargo format for code formatting
- Run cargo clippy for linting and code quality checks
