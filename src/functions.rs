use crate::error::DeidError;
use crate::metadata::DeidFunction;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

type HmacSha256 = Hmac<Sha256>;

pub trait FromKwargs: Sized {
    fn from_kwargs(kwargs: &HashMap<String, String>) -> Result<Self, DeidError>;
}

/// Typed arguments for `hashuid`.
struct HashUIDArgs {
    /// HMAC key; when absent, an unkeyed SHA-256 hash is used instead.
    key: Option<String>,
}

impl FromKwargs for HashUIDArgs {
    fn from_kwargs(kwargs: &HashMap<String, String>) -> Result<Self, DeidError> {
        Ok(HashUIDArgs {
            key: kwargs.get("key").cloned(),
        })
    }
}

// Returns a wrapper function to generate typed arguments from kwargs, as required by the underlying function
pub fn with_args<A, F>(f: F) -> DeidFunction
where
    A: FromKwargs,
    F: Fn(&str, &A) -> Result<String, DeidError> + Send + Sync + 'static,
{
    Box::new(move |input, kwargs| {
        let args = A::from_kwargs(kwargs)?;
        f(input, &args)
    })
}

pub fn no_args<F>(f: F) -> DeidFunction
where
    F: Fn(&str) -> Result<String, DeidError> + Send + Sync + 'static,
{
    Box::new(move |input, _kwargs| f(input))
}

/// Generate a DICOM UID from the SHA-256 hash of the input value.
///
/// The output has the form `2.25.<decimal>` where `<decimal>` is the
/// first 128 bits of the SHA-256 digest interpreted as a big-endian
/// unsigned integer. The result is truncated to 64 characters (the
/// DICOM UID maximum length).
///
/// This is deterministic: the same input always produces the same UID.
fn hashuid(input: &str, args: &HashUIDArgs) -> Result<String, DeidError> {
    let hash = match &args.key {
        Some(key) => {
            let mut mac =
                HmacSha256::new_from_slice(key.as_bytes()).expect("HMAC can take key of any size");
            mac.update(input.as_bytes());
            mac.finalize().into_bytes()
        }
        None => Sha256::digest(input.as_bytes()),
    };
    // Take the first 16 bytes (128 bits) as a u128
    let bytes: [u8; 16] = hash[..16].try_into().expect("slice is 16 bytes");
    let num = u128::from_be_bytes(bytes);
    let uid = format!("2.25.{}", num);
    // DICOM UIDs must be at most 64 characters
    Ok(uid[..uid.len().min(64)].to_string())
}

/// Return the default built-in functions available in recipes.
pub fn default_functions() -> HashMap<String, DeidFunction> {
    let mut map: HashMap<String, DeidFunction> = HashMap::new();
    map.insert("hashuid".into(), with_args(hashuid));
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn no_args() -> HashUIDArgs {
        HashUIDArgs { key: None }
    }

    #[test]
    fn hashuid_deterministic() {
        let a = hashuid(
            "1.2.840.113619.2.55.3.604688119.969.1068842234.928",
            &no_args(),
        )
        .unwrap();
        let b = hashuid(
            "1.2.840.113619.2.55.3.604688119.969.1068842234.928",
            &no_args(),
        )
        .unwrap();
        assert_eq!(a, b, "same input should produce same output");
    }

    #[test]
    fn hashuid_different_inputs_different_outputs() {
        let a = hashuid("1.2.3.4.5", &no_args()).unwrap();
        let b = hashuid("1.2.3.4.6", &no_args()).unwrap();
        assert_ne!(a, b, "different inputs should produce different outputs");
    }

    #[test]
    fn hashuid_has_correct_prefix() {
        let uid = hashuid("1.2.3.4.5", &no_args()).unwrap();
        assert!(uid.starts_with("2.25."), "UID should start with 2.25.");
    }

    #[test]
    fn hashuid_max_64_chars() {
        let uid = hashuid("1.2.3.4.5", &no_args()).unwrap();
        assert!(
            uid.len() <= 64,
            "UID length {} exceeds DICOM max of 64",
            uid.len()
        );
    }

    #[test]
    fn hashuid_only_digits_and_dots() {
        let uid = hashuid("1.2.3.4.5", &no_args()).unwrap();
        assert!(
            uid.chars().all(|c| c.is_ascii_digit() || c == '.'),
            "UID should only contain digits and dots, got: {}",
            uid
        );
    }

    #[test]
    fn hashuid_empty_input() {
        let uid = hashuid("", &no_args()).unwrap();
        assert!(uid.starts_with("2.25."));
        assert!(uid.len() <= 64);
    }

    #[test]
    fn default_functions_contains_hashuid() {
        let funcs = default_functions();
        assert!(funcs.contains_key("hashuid"));
        let result = funcs["hashuid"]("1.2.3", &HashMap::new()).unwrap();
        assert!(result.starts_with("2.25."));
    }

    #[test]
    fn hashuid_with_hmac_key() {
        let args1 = HashUIDArgs {
            key: Some("my_secret_key".to_string()),
        };
        let args2 = HashUIDArgs {
            key: Some("my_secret_key2".to_string()),
        };
        let a = hashuid("1.2.3.4.5", &args1).unwrap();
        let b = hashuid("1.2.3.4.5", &args2).unwrap();
        assert_ne!(a, b, "different key should produce different output");
    }
}
