// A pipeline drawn as boxes and arrows in inline SVG, each job coloured by a status and labelled
// with it in words.
import { useMemo } from "react";
import { boxHeight, boxWidth, layout, type GraphNode } from "../graph";
import { statusTone } from "../format";
import { navigate } from "../router";

export function PipelineGraph(props: {
  nodes: GraphNode[];
  statuses: Map<string, string>;
  /** Where a box links to. */
  hrefOf: (job: string) => string;
  label: string;
}) {
  const drawn = useMemo(() => layout(props.nodes), [props.nodes]);
  if (drawn.boxes.length === 0) return <p className="muted">No jobs.</p>;
  return (
    <figure className="graph-scroll">
      <figcaption className="visually-hidden">{props.label}</figcaption>
      <svg
        className="graph"
        width={drawn.width}
        height={drawn.height}
        viewBox={`0 0 ${drawn.width} ${drawn.height}`}
      >
        <defs>
          <marker
            id="arrow"
            viewBox="0 0 10 10"
            refX="9"
            refY="5"
            markerWidth="7"
            markerHeight="7"
            orient="auto"
          >
            <path d="M 0 0 L 10 5 L 0 10 z" className="graph-arrowhead" />
          </marker>
        </defs>
        {drawn.edges.map((edge) => (
          <path
            key={`${edge.from}->${edge.to}`}
            d={edge.path}
            className="graph-edge"
            markerEnd="url(#arrow)"
          />
        ))}
        {drawn.boxes.map((box) => {
          const status = props.statuses.get(box.job);
          const href = props.hrefOf(box.job);
          return (
            <a
              key={box.job}
              href={href}
              className={`graph-node tone-${status ? statusTone(status) : "none"}`}
              aria-label={`${box.job}: ${status ?? "no recent run"}`}
              onClick={(event) => {
                if (
                  event.metaKey ||
                  event.ctrlKey ||
                  event.shiftKey ||
                  event.button !== 0
                )
                  return;
                event.preventDefault();
                navigate(href);
              }}
            >
              <title>{`${box.job}: ${status ?? "no recent run"}`}</title>
              <rect
                x={box.x}
                y={box.y}
                width={boxWidth}
                height={boxHeight}
                rx={8}
              />
              <text x={box.x + 12} y={box.y + 20} className="graph-job">
                {clip(box.job, 22)}
              </text>
              <text x={box.x + 12} y={box.y + 37} className="graph-status">
                {status ?? "no recent run"}
              </text>
            </a>
          );
        })}
      </svg>
    </figure>
  );
}

function clip(text: string, length: number): string {
  return text.length > length ? `${text.slice(0, length - 1)}…` : text;
}
