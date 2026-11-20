//! `JobData`: the values that flow into and out of jobs.
//!
//! A job's input and output are stored as JSON, published to callers as a shape, and compared to
//! decide whether two requests are the same work. `#[derive(JobData)]` implements this trait for a
//! struct; fields marked `#[job_data(not_compared)]` are left out of the comparison.

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use sha2::{Digest, Sha256};

/// A value a job takes as input or returns as output.
pub trait JobData: Serialize + DeserializeOwned + Send + Sync + 'static {
    /// The part of the value that decides "the same work" and "unchanged". By default, all of it.
    fn identity(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }

    /// A description of the value's fields, published by the API so callers know what to send.
    fn shape() -> Shape;
}

/// The published description of a [`JobData`] type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct Shape {
    /// The Rust type, as written.
    pub type_name: String,
    /// For a struct, its fields; empty for anything else.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FieldShape>,
}

/// One field of a struct's [`Shape`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, serde::Deserialize)]
pub struct FieldShape {
    pub name: String,
    pub type_name: String,
    /// False for a field marked `#[job_data(not_compared)]`.
    pub compared: bool,
}

impl Shape {
    /// The shape of a value with no fields of its own, such as a number or a date.
    pub fn named(type_name: &str) -> Self {
        Shape { type_name: type_name.to_owned(), fields: Vec::new() }
    }
}

macro_rules! plain_job_data {
    ($($ty:ty),* $(,)?) => {$(
        impl JobData for $ty {
            fn shape() -> Shape {
                Shape::named(stringify!($ty))
            }
        }
    )*};
}

plain_job_data!(
    (),
    bool,
    i8,
    i16,
    i32,
    i64,
    u8,
    u16,
    u32,
    u64,
    usize,
    f64,
    String,
    chrono::NaiveDate,
    chrono::NaiveTime,
    chrono::DateTime<chrono::Utc>,
    uuid::Uuid,
    Value,
);

impl<T: JobData> JobData for Option<T> {
    fn identity(&self) -> Value {
        self.as_ref().map_or(Value::Null, JobData::identity)
    }
    fn shape() -> Shape {
        Shape::named(&format!("Option<{}>", T::shape().type_name))
    }
}

impl<T: JobData> JobData for Vec<T> {
    fn identity(&self) -> Value {
        Value::Array(self.iter().map(JobData::identity).collect())
    }
    fn shape() -> Shape {
        Shape::named(&format!("Vec<{}>", T::shape().type_name))
    }
}

macro_rules! tuple_job_data {
    ($($name:ident: $index:tt),+) => {
        impl<$($name: JobData),+> JobData for ($($name,)+) {
            fn identity(&self) -> Value {
                Value::Array(vec![$(self.$index.identity()),+])
            }
            fn shape() -> Shape {
                let parts: Vec<String> = vec![$($name::shape().type_name),+];
                Shape::named(&format!("({})", parts.join(", ")))
            }
        }
    };
}

tuple_job_data!(A: 0, B: 1);
tuple_job_data!(A: 0, B: 1, C: 2);
tuple_job_data!(A: 0, B: 1, C: 2, D: 3);

/// The hash that identifies a value's work: SHA-256 of its identity as canonical JSON.
///
/// Canonical means object keys sorted and null fields left out, so adding an optional field to a
/// type does not change the hash of values stored before it existed.
pub(crate) fn identity_hash(identity: &Value) -> String {
    let mut canonical = String::new();
    write_canonical(identity, &mut canonical);
    hex::encode(Sha256::digest(canonical.as_bytes()))
}

fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.iter().filter(|(_, v)| !v.is_null()).map(|(k, _)| k).collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&Value::String(key.clone()).to_string());
                out.push(':');
                write_canonical(&map[key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => out.push_str(&scalar.to_string()),
    }
}

/// Removes the named top-level fields from a serialized struct; used by `#[derive(JobData)]`.
#[doc(hidden)]
pub fn without_fields(mut value: Value, fields: &[&str]) -> Value {
    if let Value::Object(map) = &mut value {
        for field in fields {
            map.remove(*field);
        }
    }
    value
}

/// Checks that a JSON object sent by a caller has no field the type does not know.
///
/// Stored values are decoded leniently, so old and new builds can run side by side; values that
/// arrive in a request are decoded strictly.
pub(crate) fn unknown_fields(sent: &Value, decoded: &Value) -> Vec<String> {
    match (sent, decoded) {
        (Value::Object(sent), Value::Object(known)) => {
            sent.keys().filter(|key| !known.contains_key(*key)).cloned().collect()
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn hash_ignores_key_order_and_null_fields() {
        let a = identity_hash(&json!({"day": "2026-11-20", "broker": 1}));
        let b = identity_hash(&json!({"broker": 1, "day": "2026-11-20", "note": null}));
        assert_eq!(a, b);
    }

    #[test]
    fn hash_differs_for_different_values() {
        assert_ne!(identity_hash(&json!({"broker": 1})), identity_hash(&json!({"broker": 2})));
    }

    #[test]
    fn finds_fields_a_type_does_not_know() {
        let sent = json!({"day": 1, "typo": 2});
        let decoded = json!({"day": 1});
        assert_eq!(unknown_fields(&sent, &decoded), vec!["typo".to_owned()]);
    }
}
