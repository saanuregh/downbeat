// Builds a start form from a job's input shape, and turns the form's text back into input JSON.
// Pure, so it can be tested without a browser.
import type { FieldShape, Json, Shape } from "./types";

export type FieldKind =
  "boolean" | "integer" | "number" | "date" | "datetime" | "text" | "json";

export interface FormField {
  name: string;
  typeName: string;
  kind: FieldKind;
  optional: boolean;
}

/** The shape, when the value is one; null otherwise. */
export function asShape(value: unknown): Shape | null {
  if (typeof value !== "object" || value === null) return null;
  const shape = value as { type_name?: unknown; fields?: unknown };
  if (typeof shape.type_name !== "string") return null;
  const fields = Array.isArray(shape.fields)
    ? shape.fields.filter(
        (field): field is FieldShape =>
          typeof field === "object" &&
          field !== null &&
          typeof (field as FieldShape).name === "string" &&
          typeof (field as FieldShape).type_name === "string",
      )
    : [];
  return { type_name: shape.type_name, fields };
}

const integers = new Set([
  "i8",
  "i16",
  "i32",
  "i64",
  "i128",
  "isize",
  "u8",
  "u16",
  "u32",
  "u64",
  "u128",
  "usize",
]);

/** The last path segment of a Rust type with no spaces: "chrono::NaiveDate" → "NaiveDate". */
function baseName(typeName: string): string {
  const compact = typeName.replaceAll(" ", "");
  const generic = compact.indexOf("<");
  const head = generic === -1 ? compact : compact.slice(0, generic);
  const segments = head.split("::");
  const last = segments[segments.length - 1] ?? head;
  return generic === -1 ? last : last + compact.slice(generic);
}

/** How a field of this Rust type is edited. */
export function fieldFor(field: FieldShape): FormField {
  let type = baseName(field.type_name);
  let optional = false;
  const option = /^Option<(.*)>$/.exec(type);
  if (option?.[1] !== undefined) {
    optional = true;
    type = baseName(option[1]);
  }
  let kind: FieldKind = "json";
  if (type === "bool") kind = "boolean";
  else if (integers.has(type)) kind = "integer";
  else if (type === "f32" || type === "f64") kind = "number";
  else if (type === "NaiveDate") kind = "date";
  else if (/^DateTime<.*>$/.test(type)) kind = "datetime";
  else if (
    type === "String" ||
    type === "&str" ||
    type === "str" ||
    type === "Uuid" ||
    type === "char"
  ) {
    kind = "text";
  }
  return { name: field.name, typeName: field.type_name, kind, optional };
}

/** The fields of the form, or an empty list when the shape has none (use a JSON box then). */
export function formFields(shape: Shape | null): FormField[] {
  return (shape?.fields ?? []).map(fieldFor);
}

/** The form's text for each field, taken from an earlier input. */
export function valuesFromInput(
  fields: FormField[],
  input: Json | null | undefined,
): Record<string, string> {
  const source =
    typeof input === "object" && input !== null && !Array.isArray(input)
      ? input
      : {};
  const values: Record<string, string> = {};
  for (const field of fields) {
    const value = source[field.name];
    if (value === undefined || value === null)
      values[field.name] = field.kind === "boolean" ? "false" : "";
    else if (field.kind === "json" || typeof value === "object")
      values[field.name] = JSON.stringify(value);
    else values[field.name] = String(value);
  }
  return values;
}

export type Built =
  { ok: true; input: Json } | { ok: false; errors: Record<string, string> };

/** The input JSON from the form's text, or a sentence per field that cannot be read. */
export function inputFromValues(
  fields: FormField[],
  values: Record<string, string>,
): Built {
  const input: { [key: string]: Json } = {};
  const errors: Record<string, string> = {};
  for (const field of fields) {
    const text = (values[field.name] ?? "").trim();
    if (text === "" && field.kind !== "boolean") {
      if (field.optional) input[field.name] = null;
      else if (field.kind === "text") input[field.name] = "";
      else errors[field.name] = "Required.";
      continue;
    }
    switch (field.kind) {
      case "boolean":
        input[field.name] = text === "true";
        break;
      case "integer":
        if (/^-?\d+$/.test(text)) input[field.name] = Number(text);
        else errors[field.name] = "A whole number.";
        break;
      case "number":
        if (text !== "" && Number.isFinite(Number(text)))
          input[field.name] = Number(text);
        else errors[field.name] = "A number.";
        break;
      case "date":
        if (/^\d{4}-\d{2}-\d{2}$/.test(text)) input[field.name] = text;
        else errors[field.name] = "A date as YYYY-MM-DD.";
        break;
      case "datetime":
        if (!Number.isNaN(Date.parse(text)))
          input[field.name] = new Date(text).toISOString();
        else errors[field.name] = "A time such as 2026-11-20T08:00:00Z.";
        break;
      case "text":
        input[field.name] = values[field.name] ?? "";
        break;
      case "json":
        try {
          input[field.name] = JSON.parse(text) as Json;
        } catch {
          errors[field.name] = "Valid JSON.";
        }
        break;
    }
  }
  return Object.keys(errors).length > 0
    ? { ok: false, errors }
    : { ok: true, input };
}

/** Parses the JSON box used when the shape has no fields; an empty box is null. */
export function inputFromJsonText(text: string): Built {
  if (text.trim() === "") return { ok: true, input: null };
  try {
    return { ok: true, input: JSON.parse(text) as Json };
  } catch (error) {
    return {
      ok: false,
      errors: {
        input: error instanceof Error ? error.message : "Not valid JSON.",
      },
    };
  }
}
