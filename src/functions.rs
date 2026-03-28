use crate::error::DeidError;
use crate::metadata::DeidFunction;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Generate a DICOM UID from the SHA-256 hash of the input value.
///
/// The output has the form `2.25.<decimal>` where `<decimal>` is the
/// first 128 bits of the SHA-256 digest interpreted as a big-endian
/// unsigned integer. The result is truncated to 64 characters (the
/// DICOM UID maximum length).
///
/// This is deterministic: the same input always produces the same UID.
fn hashuid(input: &str) -> Result<String, DeidError> {
    let hash = Sha256::digest(input.as_bytes());
    // Take the first 16 bytes (128 bits) as a u128
    let bytes: [u8; 16] = hash[..16].try_into().expect("slice is 16 bytes");
    let num = u128::from_be_bytes(bytes);
    let uid = format!("2.25.{}", num);
    // DICOM UIDs must be at most 64 characters
    Ok(uid[..uid.len().min(64)].to_string())
}

/// Generic hash: SHA-256 of input, truncated to 10 hex characters.
///
/// Matches CTP's `@hash(this,10)` behavior.
fn hash(input: &str) -> Result<String, DeidError> {
    let digest = Sha256::digest(input.as_bytes());
    let hex = format!("{:x}", digest);
    Ok(hex[..hex.len().min(10)].to_string())
}

/// Hash formatted as a DICOM person name (PN VR): `LAST^FIRST`.
///
/// Uses the first 6 hex chars for the last name component and the next
/// 2 hex chars for the first name component, matching CTP's
/// `@hashname(this,6,2)` default behavior.
fn hashname(input: &str) -> Result<String, DeidError> {
    let digest = Sha256::digest(input.as_bytes());
    let hex = format!("{:X}", digest);
    let last = &hex[..6];
    let first = &hex[6..8];
    Ok(format!("{}^{}", last, first))
}

/// Hash mapped to a valid DICOM date (DA VR) in YYYYMMDD format.
///
/// Takes the first 8 bytes of the SHA-256 hash and maps them to a date
/// between 1900-01-01 and 2099-12-31.
fn hashdate(input: &str) -> Result<String, DeidError> {
    let digest = Sha256::digest(input.as_bytes());
    let bytes: [u8; 4] = digest[..4].try_into().expect("slice is 4 bytes");
    let num = u32::from_be_bytes(bytes);

    let year = 1900 + (num % 200) as u16;
    let month = 1 + ((num >> 8) % 12);
    let max_day = match month {
        2 => {
            if year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400)) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    let day = 1 + ((num >> 16) % max_day);

    Ok(format!("{:04}{:02}{:02}", year, month, day))
}

/// Hash formatted as a numeric patient ID.
///
/// Uses the first 8 bytes of the SHA-256 hash to produce a 10-digit
/// decimal number, matching CTP's `@hashptid(this,10)` behavior.
fn hashptid(input: &str) -> Result<String, DeidError> {
    let digest = Sha256::digest(input.as_bytes());
    let bytes: [u8; 8] = digest[..8].try_into().expect("slice is 8 bytes");
    let num = u64::from_be_bytes(bytes) % 10_000_000_000;
    Ok(format!("{:010}", num))
}

/// Create a lookup function from a CTP-format lookup table file.
///
/// The table file uses the format `TagName/OriginalValue = NewValue`,
/// one entry per line. The returned function accepts input in the form
/// `TagName/CurrentValue` and returns the mapped value if found, or
/// the original input value if no mapping exists.
pub fn create_lookup_function(
    table_path: &Path,
) -> Result<HashMap<String, DeidFunction>, DeidError> {
    let content = fs::read_to_string(table_path).map_err(|e| {
        DeidError::Io(std::io::Error::new(
            e.kind(),
            format!("failed to read lookup table {}: {}", table_path.display(), e),
        ))
    })?;

    // Parse the lookup table: group entries by tag name
    let mut tag_tables: HashMap<String, HashMap<String, String>> = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim();
            let value = value.trim().to_string();
            if let Some((tag_name, original_value)) = key.split_once('/') {
                tag_tables
                    .entry(tag_name.to_string())
                    .or_default()
                    .insert(original_value.to_string(), value);
            }
        }
    }

    // Create a lookup function for each tag that has entries
    let mut functions: HashMap<String, DeidFunction> = HashMap::new();

    // Create a single "lookup" function that handles all tags
    let tag_tables = Arc::new(tag_tables);
    let lookup_fn: DeidFunction = {
        let tables = Arc::clone(&tag_tables);
        Box::new(move |input: &str| -> Result<String, DeidError> {
            // Input is the current tag value. We need to search all tag tables
            // for a matching original value and return the mapped value.
            // The tag context is passed as "TagName/Value" format.
            if let Some((tag_name, current_value)) = input.split_once('/')
                && let Some(table) = tables.get(tag_name)
                && let Some(mapped) = table.get(current_value)
            {
                return Ok(mapped.clone());
            }
            // No mapping found -- return original value unchanged
            Ok(input.to_string())
        })
    };
    functions.insert("lookup".into(), lookup_fn);

    Ok(functions)
}

/// Return the default built-in functions available in recipes.
pub fn default_functions() -> HashMap<String, DeidFunction> {
    let mut map: HashMap<String, DeidFunction> = HashMap::new();
    map.insert("hashuid".into(), Box::new(hashuid));
    map.insert("hash".into(), Box::new(hash));
    map.insert("hashname".into(), Box::new(hashname));
    map.insert("hashdate".into(), Box::new(hashdate));
    map.insert("hashptid".into(), Box::new(hashptid));
    map
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn hashuid_deterministic() {
        let a = hashuid("1.2.840.113619.2.55.3.604688119.969.1068842234.928").unwrap();
        let b = hashuid("1.2.840.113619.2.55.3.604688119.969.1068842234.928").unwrap();
        assert_eq!(a, b, "same input should produce same output");
    }

    #[test]
    fn hashuid_different_inputs_different_outputs() {
        let a = hashuid("1.2.3.4.5").unwrap();
        let b = hashuid("1.2.3.4.6").unwrap();
        assert_ne!(a, b, "different inputs should produce different outputs");
    }

    #[test]
    fn hashuid_has_correct_prefix() {
        let uid = hashuid("1.2.3.4.5").unwrap();
        assert!(uid.starts_with("2.25."), "UID should start with 2.25.");
    }

    #[test]
    fn hashuid_max_64_chars() {
        let uid = hashuid("1.2.3.4.5").unwrap();
        assert!(
            uid.len() <= 64,
            "UID length {} exceeds DICOM max of 64",
            uid.len()
        );
    }

    #[test]
    fn hashuid_only_digits_and_dots() {
        let uid = hashuid("1.2.3.4.5").unwrap();
        assert!(
            uid.chars().all(|c| c.is_ascii_digit() || c == '.'),
            "UID should only contain digits and dots, got: {}",
            uid
        );
    }

    #[test]
    fn hashuid_empty_input() {
        let uid = hashuid("").unwrap();
        assert!(uid.starts_with("2.25."));
        assert!(uid.len() <= 64);
    }

    #[test]
    fn default_functions_contains_hashuid() {
        let funcs = default_functions();
        assert!(funcs.contains_key("hashuid"));
        let result = funcs["hashuid"]("1.2.3").unwrap();
        assert!(result.starts_with("2.25."));
    }

    #[test]
    fn hash_deterministic() {
        let a = hash("test_value").unwrap();
        let b = hash("test_value").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn hash_length_10() {
        let result = hash("some input").unwrap();
        assert_eq!(result.len(), 10);
        assert!(result.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_different_inputs() {
        let a = hash("patient1").unwrap();
        let b = hash("patient2").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn hashname_format() {
        let result = hashname("John^Doe").unwrap();
        assert!(result.contains('^'), "hashname should contain ^ separator");
        let parts: Vec<&str> = result.split('^').collect();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].len(), 6);
        assert_eq!(parts[1].len(), 2);
    }

    #[test]
    fn hashname_deterministic() {
        let a = hashname("John^Doe").unwrap();
        let b = hashname("John^Doe").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn hashdate_valid_format() {
        let result = hashdate("20210101").unwrap();
        assert_eq!(result.len(), 8);
        assert!(result.chars().all(|c| c.is_ascii_digit()));
        let year: u16 = result[0..4].parse().unwrap();
        let month: u8 = result[4..6].parse().unwrap();
        let day: u8 = result[6..8].parse().unwrap();
        assert!((1900..=2099).contains(&year));
        assert!((1..=12).contains(&month));
        assert!((1..=31).contains(&day));
    }

    #[test]
    fn hashdate_deterministic() {
        let a = hashdate("20210101").unwrap();
        let b = hashdate("20210101").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn hashptid_format() {
        let result = hashptid("12345").unwrap();
        assert_eq!(result.len(), 10);
        assert!(result.chars().all(|c| c.is_ascii_digit()));
    }

    #[test]
    fn hashptid_deterministic() {
        let a = hashptid("12345").unwrap();
        let b = hashptid("12345").unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn default_functions_contains_all() {
        let funcs = default_functions();
        assert!(funcs.contains_key("hashuid"));
        assert!(funcs.contains_key("hash"));
        assert!(funcs.contains_key("hashname"));
        assert!(funcs.contains_key("hashdate"));
        assert!(funcs.contains_key("hashptid"));
    }

    #[test]
    fn lookup_function_basic() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "PatientID/12345 = ANON001").unwrap();
        writeln!(tmp, "PatientID/67890 = ANON002").unwrap();
        writeln!(tmp, "StudyDate/20210101 = 20220101").unwrap();

        let funcs = create_lookup_function(tmp.path()).unwrap();
        let lookup = &funcs["lookup"];

        assert_eq!(lookup("PatientID/12345").unwrap(), "ANON001");
        assert_eq!(lookup("PatientID/67890").unwrap(), "ANON002");
        assert_eq!(lookup("StudyDate/20210101").unwrap(), "20220101");
    }

    #[test]
    fn lookup_function_missing_key_returns_input() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "PatientID/12345 = ANON001").unwrap();

        let funcs = create_lookup_function(tmp.path()).unwrap();
        let lookup = &funcs["lookup"];

        assert_eq!(lookup("PatientID/99999").unwrap(), "PatientID/99999");
        assert_eq!(lookup("Unknown/value").unwrap(), "Unknown/value");
    }

    #[test]
    fn lookup_function_empty_file() {
        let tmp = NamedTempFile::new().unwrap();
        let funcs = create_lookup_function(tmp.path()).unwrap();
        let lookup = &funcs["lookup"];
        assert_eq!(lookup("PatientID/12345").unwrap(), "PatientID/12345");
    }
}
