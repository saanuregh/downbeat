// Lays a pipeline out in layers: each job's layer is its depth from the jobs with no parents, so
// every link points to a later layer. Pure, so it can be tested without a browser.

export interface GraphNode {
  job: string;
  parents: string[];
}

export interface Box {
  job: string;
  layer: number;
  row: number;
  x: number;
  y: number;
}

export interface Edge {
  from: string;
  to: string;
  path: string;
}

export interface Layout {
  boxes: Box[];
  edges: Edge[];
  width: number;
  height: number;
}

export const boxWidth = 176;
export const boxHeight = 48;
const columnGap = 64;
const rowGap = 20;
const margin = 8;

/**
 * The layer of every job: 0 for a job with no known parents, otherwise one more than its
 * deepest parent. A parent that is not in the list is ignored; a cycle is broken where it is
 * found, so the result is always defined.
 */
export function layerOf(nodes: GraphNode[]): Map<string, number> {
  const byJob = new Map(nodes.map((node) => [node.job, node]));
  const found = new Map<string, number>();
  const visiting = new Set<string>();

  function depth(job: string): number {
    const known = found.get(job);
    if (known !== undefined) return known;
    if (visiting.has(job)) return 0;
    visiting.add(job);
    let deepest = -1;
    for (const parent of byJob.get(job)?.parents ?? []) {
      if (byJob.has(parent)) deepest = Math.max(deepest, depth(parent));
    }
    visiting.delete(job);
    const layer = deepest + 1;
    found.set(job, layer);
    return layer;
  }

  for (const node of nodes) depth(node.job);
  return found;
}

/** Jobs grouped by layer; within a layer, ordered near their parents, then by name. */
export function layers(nodes: GraphNode[]): string[][] {
  const layerByJob = layerOf(nodes);
  const byJob = new Map(nodes.map((node) => [node.job, node]));
  const count = Math.max(-1, ...layerByJob.values()) + 1;
  const grouped: string[][] = Array.from({ length: count }, () => []);
  for (const node of nodes)
    grouped[layerByJob.get(node.job) ?? 0]?.push(node.job);

  const rowByJob = new Map<string, number>();
  const weight = (job: string): number => {
    const rows = (byJob.get(job)?.parents ?? [])
      .map((parent) => rowByJob.get(parent))
      .filter((row): row is number => row !== undefined);
    return rows.length === 0
      ? Number.POSITIVE_INFINITY
      : rows.reduce((a, b) => a + b, 0) / rows.length;
  };
  return grouped.map((group, layer) => {
    const ordered =
      layer === 0
        ? group.toSorted((a, b) => a.localeCompare(b))
        : group.toSorted((a, b) => weight(a) - weight(b) || a.localeCompare(b));
    ordered.forEach((job, row) => rowByJob.set(job, row));
    return ordered;
  });
}

/** Positions for every box and a path for every link, left to right. */
export function layout(nodes: GraphNode[]): Layout {
  const grouped = layers(nodes);
  const boxes: Box[] = [];
  const position = new Map<string, Box>();
  const tallest = Math.max(0, ...grouped.map((group) => group.length));
  grouped.forEach((group, layer) => {
    const offset = ((tallest - group.length) * (boxHeight + rowGap)) / 2;
    group.forEach((job, row) => {
      const box: Box = {
        job,
        layer,
        row,
        x: margin + layer * (boxWidth + columnGap),
        y: margin + offset + row * (boxHeight + rowGap),
      };
      boxes.push(box);
      position.set(job, box);
    });
  });

  const edges: Edge[] = [];
  for (const node of nodes) {
    const to = position.get(node.job);
    if (!to) continue;
    for (const parent of node.parents) {
      const from = position.get(parent);
      if (!from) continue;
      const startX = from.x + boxWidth;
      const startY = from.y + boxHeight / 2;
      const endX = to.x;
      const endY = to.y + boxHeight / 2;
      const bend = Math.max(24, (endX - startX) / 2);
      edges.push({
        from: parent,
        to: node.job,
        path: `M ${startX} ${startY} C ${startX + bend} ${startY}, ${endX - bend} ${endY}, ${endX} ${endY}`,
      });
    }
  }

  return {
    boxes,
    edges,
    width:
      grouped.length === 0
        ? 0
        : margin * 2 +
          grouped.length * boxWidth +
          (grouped.length - 1) * columnGap,
    height:
      tallest === 0
        ? 0
        : margin * 2 + tallest * boxHeight + (tallest - 1) * rowGap,
  };
}

/** Each job's status in the newest chain that ran it. */
export function latestStatuses(
  chains: { started_at: string; statuses: Record<string, string> }[],
): Map<string, string> {
  const newestFirst = chains.toSorted(
    (a, b) => Date.parse(b.started_at) - Date.parse(a.started_at),
  );
  const latest = new Map<string, string>();
  for (const chain of newestFirst) {
    for (const [job, status] of Object.entries(chain.statuses)) {
      if (!latest.has(job)) latest.set(job, status);
    }
  }
  return latest;
}
