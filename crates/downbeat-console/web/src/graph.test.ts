import { describe, expect, test } from "bun:test";
import { boxWidth, latestStatuses, layerOf, layers, layout } from "./graph";

const pipeline = [
  { job: "report", parents: ["candles", "volumes"] },
  { job: "candles", parents: ["fetch"] },
  { job: "fetch", parents: [] },
  { job: "volumes", parents: ["fetch"] },
  { job: "alert", parents: ["report"] },
];

describe("layers", () => {
  test("puts each job one layer after its deepest parent", () => {
    expect(Object.fromEntries(layerOf(pipeline))).toEqual({
      fetch: 0,
      candles: 1,
      volumes: 1,
      report: 2,
      alert: 3,
    });
    expect(layers(pipeline)).toEqual([
      ["fetch"],
      ["candles", "volumes"],
      ["report"],
      ["alert"],
    ]);
  });

  test("uses the longest path when a job has parents at different depths", () => {
    const nodes = [
      { job: "a", parents: [] },
      { job: "b", parents: ["a"] },
      { job: "c", parents: ["a", "b"] },
    ];
    expect(layers(nodes)).toEqual([["a"], ["b"], ["c"]]);
  });

  test("ignores unknown parents and survives a cycle", () => {
    expect(layers([{ job: "x", parents: ["elsewhere"] }])).toEqual([["x"]]);
    const cyclic = [
      { job: "a", parents: ["b"] },
      { job: "b", parents: ["a"] },
    ];
    expect(layers(cyclic).flat().toSorted()).toEqual(["a", "b"]);
  });

  test("orders a layer near its parents", () => {
    const nodes = [
      { job: "a", parents: [] },
      { job: "b", parents: [] },
      { job: "under-b", parents: ["b"] },
      { job: "under-a", parents: ["a"] },
    ];
    expect(layers(nodes)[1]).toEqual(["under-a", "under-b"]);
  });

  test("an empty pipeline has no layers", () => {
    expect(layers([])).toEqual([]);
    expect(layout([])).toEqual({ boxes: [], edges: [], width: 0, height: 0 });
  });
});

describe("layout", () => {
  test("places layers left to right and draws one edge per link", () => {
    const drawn = layout(pipeline);
    const x = Object.fromEntries(drawn.boxes.map((box) => [box.job, box.x]));
    expect(x.fetch).toBeLessThan(x.candles ?? 0);
    expect(x.candles).toBe(x.volumes);
    expect(drawn.edges).toHaveLength(5);
    expect(drawn.width).toBeGreaterThanOrEqual(4 * boxWidth);
    for (const edge of drawn.edges)
      expect(edge.path.startsWith("M ")).toBe(true);
  });
});

describe("latestStatuses", () => {
  test("takes each job's status from the newest chain that ran it", () => {
    const statuses = latestStatuses([
      {
        started_at: "2026-11-19T08:00:00Z",
        statuses: { fetch: "succeeded", report: "failed" },
      },
      { started_at: "2026-11-20T08:00:00Z", statuses: { fetch: "running" } },
    ]);
    expect(Object.fromEntries(statuses)).toEqual({
      fetch: "running",
      report: "failed",
    });
  });
});
