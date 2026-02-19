r-1 Inputs and Outputs
r-1-1 The software must accept a path to a directory of DICOM files as an input, a path to the output directory, and a path to the recipe file.
r-1-2 The software must recursively search the input directory for all DICOM files
r-1-3 The software must display a progress bar on the console as it processes input files
r-1-4 The software must preserve the relative directory structure of input files in the output directory (e.g., input/sub/file.dcm → output/sub/file.dcm)
r-1-5 The software must continue processing remaining files when an individual file fails, logging a warning and counting the file as skipped in the final report

r-2 De-id Recipe Specification
r-2-1 The software must parse a de-identification recipe file defining the deid operations to be performed
r-2-2 The recipe must begin with a FORMAT declaration line (e.g. FORMAT dicom). The parser must validate that the declared format is supported.
r-2-3 The recipe must support sections declared with a % prefix: %header for metadata de-id actions, and %filter <name> for named filter groups (e.g. %filter graylist, %filter blacklist).
r-2-4 Lines beginning with # must be treated as comments and ignored. Inline comments after # on action lines must also be stripped.
r-2-5 Under %filter sections, the software must parse LABEL directives that define named filter groups. Each group consists of a LABEL <name> line (with optional # comment), one or more filter condition lines, and zero or more coordinate directives.
r-2-6 Filter conditions
r-2-6-1 The software must support the filter predicate "contains <Field> <Value>" which checks if the field value contains the given substring or regex.
r-2-6-2 The software must support the filter predicate "notcontains <Field> <Value>".
r-2-6-3 The software must support the filter predicate "equals <Field> <Value>" which performs a case-insensitive exact match.
r-2-6-4 The software must support the filter predicate "notequals <Field> <Value>".
r-2-6-5 The software must support the filter predicate "missing <Field>" which checks that the field is not present in the DICOM.
r-2-6-6 The software must support the filter predicate "empty <Field>" which checks that the field is present but has an empty value.
r-2-6-7 The software must support the filter predicate "present <Field>" which checks that the field exists.
r-2-7 Logical operators in filter conditions
r-2-7-1 The software must support the + prefix on a filter line to indicate an AND relationship with the preceding condition.
r-2-7-2 The software must support the || prefix on a filter line to indicate an OR relationship with the preceding condition.
r-2-7-3 The software must support inline || and + within a single line to chain multiple conditions (e.g. "missing Manufacturer || empty Manufacturer").
r-2-7-4 The software must support pipe-separated alternatives within filter values (e.g. "contains ManufacturerModelName A400|A500"), which are treated as regex alternations.
r-2-8 Coordinate directives in filter groups
r-2-8-1 The software must support "coordinates x,y,xmax,ymax" to specify pixel regions to mask in (xmin, ymin, xmax, ymax) format.
r-2-8-2 The software must support "ctpcoordinates x,y,width,height" in CTP format, converting internally to (xmin, ymin, xmin+width, ymin+height).
r-2-8-3 The software must support "keepcoordinates" and "ctpkeepcoordinates" to specify regions to preserve (inverse mask).
r-2-8-4 A filter group may specify multiple coordinate regions.
r-2-9 Header action value types
r-2-9-1 Header action values must support literal string values (e.g. MODIFIED, YES).
r-2-9-2 Header action values must support variable references via var:<NAME> syntax (e.g. var:DATEINC). Variables must be provided at runtime by the caller.
r-2-9-3 Header action values must support function references via func:<name> syntax (e.g. func:hashuid).
r-2-10 Named filter types
r-2-10-1 The software must support "graylist" filters, which flag matching files and apply pixel masking based on the filter group's coordinate directives.
r-2-10-2 The software must support "blacklist" filters, which exclude matching files from the output entirely.

r-3 Metadata De-identification
r-3-1 The software must support adding a DICOM tag with a defined value
r-3-2 The software must support replacing a DICOM tag with a new value
r-3-3 The software must support deleting a DICOM tag entirely
r-3-4 Specifying DICOM tags
r-3-4-1 The software must support specifying a tag by its keyword (e.g. PatientId)
r-3-4-2 The software must support specifying a tag by its tag value in parenthesized format (e.g. (0002,0080)) or bare hex format (e.g. 00120063)
r-3-4-3 The software must support specifying private tags by its group, private creator, and element offset
r-3-5 The software must support pattern matching of tags based on regexes of tag keywords or tag values, and applying deid operations to all tags matching the pattern
r-3-6 The software must support the use of pre-defined functions referenced via func:<name> syntax in the recipe to execute logic. Functions may accept keyword arguments.
r-3-7 The software must support applying a "jitter" to date and datetime fields to shift the value by the specified number of days. DateTime (DT) fields must also be supported, preserving the time component while shifting only the date portion. Jittering a blank or empty date field must be a no-op (no error).
r-3-8 The software must support referencing variables within the recipe via var:<NAME> syntax to allow for dynamic values
r-3-9 The software must support blanking a DICOM tag (setting its value to empty/null) while keeping the tag present in the file
r-3-10 The software must support explicitly keeping a tag's original value unchanged, protecting it from removal by broader rules
r-3-11 When multiple actions apply to the same field, the software must respect a precedence hierarchy: KEEP > ADD > REPLACE > JITTER > REMOVE > BLANK
r-3-12 The software must support bulk removal of private tags from DICOM files

r-4 Pixel-based De-identification
r-4-1 The software must support pixel-based de-identification by masking over pixel areas
r-4-2 The software must support defining pixel areas to mask based on DICOM tags (e.g. overlay tags, burn-in tags, etc.)
r-4-3 The software must support both raw coordinate format (xmin, ymin, xmax, ymax) and CTP coordinate format (x, y, width, height), converting CTP coordinates internally
r-4-4 The software must support "keep" regions that are excluded from masking (inverse of mask regions)
r-4-5 A single filter group may define multiple coordinate regions to mask
r-4-6 Pixel masking regions must only be applied when the associated filter group's conditions match the DICOM file being processed

r-4-7 The software must support decompressing compressed pixel data before applying pixel masking. At minimum, the following transfer syntaxes must be supported: JPEG Baseline (1.2.840.10008.1.2.4.50), JPEG Lossless (1.2.840.10008.1.2.4.70), and RLE Lossless (1.2.840.10008.1.2.5). JPEG 2000 Lossless (1.2.840.10008.1.2.4.90) and JPEG 2000 (1.2.840.10008.1.2.4.91) must be supported when built with the jpeg2000 feature.
r-4-8 After decompressing and masking pixel data, the output must be stored as uncompressed pixel data and the transfer syntax updated to Explicit VR Little Endian (1.2.840.10008.1.2.1).

r-5 File Filtering
r-5-1 The software must support excluding DICOM files from processing entirely based on %filter blacklist rules. Files matching blacklist criteria must not appear in the output.

r-6 Embeddability
r-6-1 The software must be designed as a library, with the main rust entrypoint being a command-line interface to the library.
r-6-2 The software must provide an API for supplying additional functions and variables that can be referenced within the recipe file, allowing for extensibility and custom logic to be injected at runtime.
