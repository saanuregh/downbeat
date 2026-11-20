import { describe, expect, test } from "bun:test";
import {
  asShape,
  fieldFor,
  formFields,
  inputFromJsonText,
  inputFromValues,
  valuesFromInput,
} from "./shape-form";

const shape = {
  type_name: "CandleInput",
  fields: [
    { name: "day", type_name: "NaiveDate", compared: true },
    { name: "exchange", type_name: "String", compared: true },
    { name: "limit", type_name: "Option<u32>", compared: true },
    { name: "force", type_name: "bool", compared: false },
    { name: "symbols", type_name: "Vec<String>", compared: true },
    {
      name: "since",
      type_name: "chrono::DateTime<chrono::Utc>",
      compared: true,
    },
    { name: "ratio", type_name: "f64", compared: true },
  ],
};

describe("fieldFor", () => {
  test("chooses an editor from the Rust type", () => {
    const kinds = formFields(asShape(shape)).map((field) => [
      field.name,
      field.kind,
      field.optional,
    ]);
    expect(kinds).toEqual([
      ["day", "date", false],
      ["exchange", "text", false],
      ["limit", "integer", true],
      ["force", "boolean", false],
      ["symbols", "json", false],
      ["since", "datetime", false],
      ["ratio", "number", false],
    ]);
  });

  test("handles paths and spaces in type names", () => {
    expect(
      fieldFor({
        name: "d",
        type_name: "Option < chrono::NaiveDate >",
        compared: true,
      }),
    ).toMatchObject({
      kind: "date",
      optional: true,
    });
  });
});

describe("asShape", () => {
  test("accepts a shape without fields and rejects anything else", () => {
    expect(asShape({ type_name: "()" })).toEqual({
      type_name: "()",
      fields: [],
    });
    expect(asShape(null)).toBeNull();
    expect(asShape({ fields: [] })).toBeNull();
    expect(formFields(asShape({ type_name: "serde_json::Value" }))).toEqual([]);
  });
});

describe("a form from a shape", () => {
  const fields = formFields(asShape(shape));
  const lastInput = {
    day: "2026-11-08",
    exchange: "NSE",
    limit: null,
    force: true,
    symbols: ["INFY", "TCS"],
    since: "2026-11-01T00:00:00Z",
    ratio: 0.5,
  };

  test("is prefilled from an earlier input and gives the same input back", () => {
    const values = valuesFromInput(fields, lastInput);
    expect(values).toEqual({
      day: "2026-11-08",
      exchange: "NSE",
      limit: "",
      force: "true",
      symbols: '["INFY","TCS"]',
      since: "2026-11-01T00:00:00Z",
      ratio: "0.5",
    });
    expect(inputFromValues(fields, values)).toEqual({
      ok: true,
      input: { ...lastInput, since: "2026-11-01T00:00:00.000Z" },
    });
  });

  test("starts empty without an earlier input", () => {
    expect(valuesFromInput(fields, null).force).toBe("false");
    expect(valuesFromInput(fields, null).day).toBe("");
  });

  test("names each field that cannot be read", () => {
    const values = {
      ...valuesFromInput(fields, lastInput),
      day: "8 Nov",
      limit: "1.5",
      symbols: "[",
      ratio: "",
    };
    const built = inputFromValues(fields, values);
    expect(built.ok).toBe(false);
    if (!built.ok)
      expect(Object.keys(built.errors).toSorted()).toEqual([
        "day",
        "limit",
        "ratio",
        "symbols",
      ]);
  });
});

describe("the JSON box", () => {
  test("parses JSON and treats an empty box as null", () => {
    expect(inputFromJsonText('{"a":1}')).toEqual({ ok: true, input: { a: 1 } });
    expect(inputFromJsonText("  ")).toEqual({ ok: true, input: null });
    expect(inputFromJsonText("{").ok).toBe(false);
  });
});
