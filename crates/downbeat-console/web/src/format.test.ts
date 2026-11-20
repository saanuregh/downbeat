import { describe, expect, test } from "bun:test";
import {
  addDays,
  formatActor,
  formatStartedBy,
  dateInZone,
  formatClock,
  formatDateTime,
  formatDuration,
  formatRelative,
  formatUtc,
  minuteOfDay,
  secondsBetween,
  shortId,
  statusTone,
  timingAgainstExpected,
  wallTimeToUtc,
} from "./format";

describe("formatDuration", () => {
  test.each([
    [0, "0 s"],
    [45, "45 s"],
    [59.6, "1 min"],
    [125, "2 min 5 s"],
    [720, "12 min"],
    [3600, "1 h"],
    [3900, "1 h 5 min"],
    [86400, "1 d"],
    [2 * 86400 + 3 * 3600 + 59, "2 d 3 h"],
  ])("%p seconds is %p", (seconds, text) => {
    expect(formatDuration(seconds)).toBe(text);
  });

  test("unknown and negative durations are a dash", () => {
    expect(formatDuration(null)).toBe("–");
    expect(formatDuration(undefined)).toBe("–");
    expect(formatDuration(-1)).toBe("–");
    expect(formatDuration(Number.NaN)).toBe("–");
  });
});

describe("secondsBetween", () => {
  test("measures to the end, or to now while running", () => {
    expect(secondsBetween("2026-11-20T08:00:00Z", "2026-11-20T08:12:30Z")).toBe(
      750,
    );
    expect(
      secondsBetween(
        "2026-11-20T08:00:00Z",
        null,
        Date.parse("2026-11-20T08:01:00Z"),
      ),
    ).toBe(60);
    expect(secondsBetween(null, null)).toBeNull();
  });
});

describe("times in a zone", () => {
  const at = "2026-11-20T08:35:00Z";

  test("shows the wall clock of the zone", () => {
    expect(formatDateTime(at, "UTC")).toBe("2026-11-20 08:35");
    expect(formatDateTime(at, "Asia/Kolkata")).toBe("2026-11-20 14:05");
    expect(formatClock(at, "America/New_York")).toBe("03:35");
    expect(formatDateTime(null, "UTC")).toBe("–");
  });

  test("gives the UTC instant for hover", () => {
    expect(formatUtc(at)).toBe("2026-11-20 08:35:00 UTC");
  });

  test("knows the date and minute in the zone", () => {
    expect(dateInZone(new Date("2026-11-20T20:00:00Z"), "Asia/Tokyo")).toBe(
      "2026-11-21",
    );
    expect(minuteOfDay(at, "Asia/Kolkata")).toBe(14 * 60 + 5);
  });

  test("adds days across month ends", () => {
    expect(addDays("2026-11-30", 1)).toBe("2026-12-01");
    expect(addDays("2026-03-01", -1)).toBe("2026-02-28");
  });

  test("turns a wall time in a zone into UTC, across a clock change", () => {
    expect(wallTimeToUtc("2026-11-20T14:05", "Asia/Kolkata")).toBe(
      "2026-11-20T08:35:00.000Z",
    );
    expect(wallTimeToUtc("2026-07-01T09:00", "Europe/London")).toBe(
      "2026-07-01T08:00:00.000Z",
    );
    expect(wallTimeToUtc("2026-12-01T09:00", "Europe/London")).toBe(
      "2026-12-01T09:00:00.000Z",
    );
    expect(wallTimeToUtc("not a time", "UTC")).toBeNull();
  });
});

describe("formatRelative", () => {
  const now = Date.parse("2026-11-20T08:00:00Z");
  test("says how far away", () => {
    expect(formatRelative("2026-11-20T08:05:00Z", now)).toBe("in 5 min");
    expect(formatRelative("2026-11-20T07:48:00Z", now)).toBe("12 min ago");
    expect(formatRelative("2026-11-20T08:00:10Z", now)).toBe("just now");
  });
});

describe("statusTone", () => {
  test("maps each status to its colour family", () => {
    expect(statusTone("succeeded")).toBe("ok");
    expect(statusTone("failed")).toBe("bad");
    expect(statusTone("skipped")).toBe("skip");
    expect(statusTone("running")).toBe("run");
    expect(statusTone("queued")).toBe("wait");
    expect(statusTone("cancelled")).toBe("neutral");
    expect(statusTone("expired")).toBe("neutral");
  });
});

describe("timingAgainstExpected", () => {
  test("compares with the expectation", () => {
    expect(timingAgainstExpected(600, 600, false)).toBe(
      "10 min, about the expected 10 min",
    );
    expect(timingAgainstExpected(1800, 600, true)).toBe(
      "30 min, 3.0 times the expected 10 min; slow",
    );
    expect(timingAgainstExpected(60, 600, false)).toBe(
      "1 min, shorter than the expected 10 min",
    );
    expect(timingAgainstExpected(60, null, false)).toBe(
      "1 min; no expected duration yet",
    );
    expect(timingAgainstExpected(null, 600, false)).toBe("Not started");
  });
});

describe("shortId", () => {
  test("uses the random end of a time-ordered id, so runs made together differ", () => {
    expect(shortId("01a1f9f1-b7bd-76cf-ba10-b77b5eb72c8f")).toBe("5eb72c8f");
    expect(shortId("01a1f9f1-bbbe-706d-a1bd-7538b4668488")).toBe("b4668488");
  });
});

describe("who started a run", () => {
  test("an actor acting through a caller reads as plain words", () => {
    expect(formatActor("asha@example.com (via console)")).toBe(
      "asha@example.com via console",
    );
    expect(formatActor("billing")).toBe("billing");
  });
  test("a schedule names itself; otherwise the actor, else how it started", () => {
    expect(
      formatStartedBy({ started_by: "schedule", schedule: "market_open" }),
    ).toBe("schedule market_open");
    expect(formatStartedBy({ started_by: "person", actor: "a (via c)" })).toBe(
      "a via c",
    );
    expect(formatStartedBy({ started_by: "link" })).toBe("link");
  });
});
