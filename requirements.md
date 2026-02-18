r-1 Inputs and Outputs
r-1-1 The software must accept a path to a directory of DICOM files as an input, a path to the output directory, and a path to the recipe file.
r-1-2 The software must recursively search the input directory for all DICOM files
r-1-3 The software must display a progress bar on the console as it processes input files

r-2 De-id Recipe Specification
r-2-1 The software must parse a de-identification recipe file defining the deid operations to be performed

r-3 Metadata De-identification
r-3-1 The software must support adding a DICOM tag with a defined value
r-3-2 The software must support replacing a DICOM tag with a new value
r-3-3 The software must support deleting a DICOM tag entirely
r-3-4 Specifying DICOM tags
r-3-4-1 The software must support specifying a tag by its keyword (e.g. PatientId)
r-3-4-2 The software must support specifying a tag by its tag value (e.g. (0002,0080))
r-3-4-3 The software must support specifying private tags by its group, private creator, and element offset
r-3-5 The software must support pattern matching of tags based on regexes of tag keywords or tag values, and applying deid operations to all tags matching the pattern
r-3-6 The software must support the use of pre-defined functions in the recipe to execute logic
r-3-7 The software must support applying a "jitter" to a date field to shift the date by the specified number of days
r-3-8 The software must support referencing variables within the recpie to allow for dynamic values

r-4 Pixel-based De-identification
r-4-1 The software must support pixel-based de-identification by masking over pixel areas
r-4-2 The software must support defining pixel areas to mask based on DICOM tags (e.g. overlay tags, burn-in tags, etc.)
