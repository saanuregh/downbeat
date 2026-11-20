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

    /// A JSON Schema (draft 2020-12) of the value, for a caller in another language or a form.
    /// Types it does not recognise, such as a product's own newtypes, accept any value; the
    /// scheduler still checks every input against the real type.
    pub fn json_schema(&self) -> Value {
        let mut schema = if self.fields.is_empty() {
            schema_of(&self.type_name)
        } else {
            let properties: serde_json::Map<String, Value> =
                self.fields.iter().map(|field| (field.name.clone(), schema_of(&field.type_name))).collect();
            let required: Vec<&str> = self
                .fields
                .iter()
                .filter(|field| !field.type_name.starts_with("Option<"))
                .map(|field| field.name.as_str())
                .collect();
            serde_json::json!({ "type": "object", "properties": properties, "required": required })
        };
        if let Value::Object(map) = &mut schema {
            map.insert("title".to_owned(), Value::String(self.type_name.clone()));
        }
        schema
    }
}

/// The JSON Schema of a Rust type, written as `type_name` gives it.
fn schema_of(type_name: &str) -> Value {
    use serde_json::json;
    let name = type_name.replace(' ', "");
    let inner = |prefix: &str| name.strip_prefix(prefix).and_then(|rest| rest.strip_suffix('>')).map(str::to_owned);
    if let Some(inner) = inner("Option<") {
        return json!({ "anyOf": [schema_of(&inner), { "type": "null" }] });
    }
    if let Some(inner) = inner("Vec<") {
        return json!({ "type": "array", "items": schema_of(&inner) });
    }
    let last = name.rsplit("::").next().unwrap_or(&name);
    match last {
        "()" => json!({ "type": "null" }),
        "bool" => json!({ "type": "boolean" }),
        "String" | "str" => json!({ "type": "string" }),
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" | "usize" | "isize" => json!({ "type": "integer" }),
        "f32" | "f64" => json!({ "type": "number" }),
        "NaiveDate" => json!({ "type": "string", "format": "date" }),
        "NaiveTime" => json!({ "type": "string", "format": "time" }),
        "Uuid" => json!({ "type": "string", "format": "uuid" }),
        _ if last.starts_with("DateTime<") => json!({ "type": "string", "format": "date-time" }),
        _ => json!({}),
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

/// Decodes a value sent by a caller into `T`, refusing any field the type does not know, at any
/// depth, named by its dotted path such as `orders.0.typo`.
///
/// The type's own decoder decides what it knows, so a field it reads but leaves out when it writes
/// itself is still known. A field sent as `null` is not refused. Stored values are decoded
/// leniently instead, so old and new builds can run side by side.
pub(crate) fn decode_strict<T: DeserializeOwned>(sent: &Value) -> Result<T, String> {
    let mut ignored = Vec::new();
    let typed = serde_ignored::deserialize(sent.clone(), |path| ignored.push(segments(&path)))
        .map_err(|error| error.to_string())?;
    let unknown: Vec<String> =
        ignored.into_iter().filter(|path| !is_null_at(sent, path)).map(|path| path.join(".")).collect();
    if unknown.is_empty() { Ok(typed) } else { Err(format!("unknown field(s): {}", unknown.join(", "))) }
}

/// A path as the keys and indexes a caller would write; options and newtypes add none.
fn segments(path: &serde_ignored::Path<'_>) -> Vec<String> {
    use serde_ignored::Path;
    match path {
        Path::Root => Vec::new(),
        Path::Seq { parent, index } => [segments(parent), vec![index.to_string()]].concat(),
        Path::Map { parent, key } => [segments(parent), vec![key.clone()]].concat(),
        Path::Some { parent } | Path::NewtypeStruct { parent } | Path::NewtypeVariant { parent } => segments(parent),
    }
}

fn is_null_at(value: &Value, path: &[String]) -> bool {
    let found = path.iter().try_fold(value, |value, segment| match value {
        Value::Object(map) => map.get(segment),
        Value::Array(items) => segment.parse::<usize>().ok().and_then(|index| items.get(index)),
        _ => None,
    });
    found.is_some_and(Value::is_null)
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
    fn describes_a_struct_as_a_json_schema() {
        let shape = Shape {
            type_name: "Report".into(),
            fields: vec![
                FieldShape { name: "day".into(), type_name: "NaiveDate".into(), compared: true },
                FieldShape { name: "note".into(), type_name: "Option<String>".into(), compared: false },
                FieldShape { name: "ids".into(), type_name: "Vec<u32>".into(), compared: true },
            ],
        };
        assert_eq!(
            shape.json_schema(),
            json!({
                "title": "Report",
                "type": "object",
                "properties": {
                    "day": { "type": "string", "format": "date" },
                    "note": { "anyOf": [{ "type": "string" }, { "type": "null" }] },
                    "ids": { "type": "array", "items": { "type": "integer" } },
                },
                "required": ["day", "ids"],
            })
        );
        assert_eq!(Shape::named("()").json_schema(), json!({ "title": "()", "type": "null" }));
    }

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Order {
        day: u32,
        note: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        tags: Vec<String>,
        lines: Vec<Line>,
    }

    #[derive(Debug, serde::Deserialize)]
    #[allow(dead_code)]
    struct Line {
        sku: String,
    }

    #[test]
    fn strict_decoding_names_unknown_fields_at_any_depth() {
        let sent = json!({"day": 1, "typo": 2, "lines": [{"sku": "a"}, {"sku": "b", "qty": 3}]});
        let refused = decode_strict::<Order>(&sent).unwrap_err();
        assert_eq!(refused, "unknown field(s): lines.1.qty, typo");
    }

    #[test]
    fn strict_decoding_accepts_known_fields_a_type_skips_and_unknown_nulls() {
        let sent = json!({"day": 1, "tags": [], "note": null, "retired": null, "lines": []});
        assert!(decode_strict::<Order>(&sent).is_ok());
    }
}
