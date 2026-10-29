// Draws a StatsBomb-scaled pitch (120x80, origin top-left) directly in SVG
// user units, so StatsBomb's own x/y coordinates can be used as SVG
// coordinates with no separate scale, the SVG's viewBox is "0 0 120 80".
// This is the interactive replacement for the package's matplotlib pitch
// maps, built for the web demo specifically.

import { hideTooltip, isTooltipPinned, pinTooltip, showTooltip } from "./tooltip.js";

const PITCH_LENGTH = 120;
const PITCH_WIDTH = 80;
const LINE = { stroke: "#333", "stroke-width": 0.4, fill: "none" };
// Pointer within this many CSS px of a marker counts as on it; also the
// effective tap target radius, kept above the 24px WCAG minimum diameter.
const HIT_RADIUS_PX = 16;

function styled(selection) {
  return selection
    .attr("stroke", LINE.stroke)
    .attr("stroke-width", LINE["stroke-width"])
    .attr("fill", LINE.fill);
}

function arcPath(cx, cy, r, startDeg, endDeg) {
  const toRad = (d) => (d * Math.PI) / 180;
  const x1 = cx + r * Math.cos(toRad(startDeg));
  const y1 = cy + r * Math.sin(toRad(startDeg));
  const x2 = cx + r * Math.cos(toRad(endDeg));
  const y2 = cy + r * Math.sin(toRad(endDeg));
  return `M ${x1} ${y1} A ${r} ${r} 0 0 1 ${x2} ${y2}`;
}

// Clears svgSelection and draws pitch markings into it. Returns a <g> to
// append markers into, so markers always render above the pitch lines.
export function drawPitch(svgSelection) {
  if (isTooltipPinned(svgSelection.node())) hideTooltip();
  svgSelection.selectAll("*").remove();
  svgSelection.attr("viewBox", `0 0 ${PITCH_LENGTH} ${PITCH_WIDTH}`);

  const lines = svgSelection.append("g").attr("class", "pitch-markings");

  styled(lines.append("rect").attr("x", 0).attr("y", 0).attr("width", PITCH_LENGTH).attr("height", PITCH_WIDTH));
  styled(lines.append("line").attr("x1", 60).attr("y1", 0).attr("x2", 60).attr("y2", 80));
  styled(lines.append("circle").attr("cx", 60).attr("cy", 40).attr("r", 9.15));
  lines.append("circle").attr("cx", 60).attr("cy", 40).attr("r", 0.3).attr("fill", LINE.stroke);

  // penalty boxes
  styled(lines.append("rect").attr("x", 0).attr("y", 18).attr("width", 18).attr("height", 44));
  styled(lines.append("rect").attr("x", 102).attr("y", 18).attr("width", 18).attr("height", 44));
  // six-yard boxes
  styled(lines.append("rect").attr("x", 0).attr("y", 30).attr("width", 6).attr("height", 20));
  styled(lines.append("rect").attr("x", 114).attr("y", 30).attr("width", 6).attr("height", 20));
  // penalty spots
  lines.append("circle").attr("cx", 12).attr("cy", 40).attr("r", 0.3).attr("fill", LINE.stroke);
  lines.append("circle").attr("cx", 108).attr("cy", 40).attr("r", 0.3).attr("fill", LINE.stroke);
  // penalty arcs (only the part outside the box)
  styled(lines.append("path").attr("d", arcPath(12, 40, 9.15, -53, 53)));
  styled(lines.append("path").attr("d", arcPath(108, 40, 9.15, 127, 233)));
  // goals
  styled(lines.append("rect").attr("x", -1.8).attr("y", 36).attr("width", 1.8).attr("height", 8));
  styled(lines.append("rect").attr("x", 120).attr("y", 36).attr("width", 1.8).attr("height", 8));

  return svgSelection.append("g").attr("class", "pitch-markers");
}

// The map is one image to assistive tech; `label` summarizes what's on it
// (the same counts shown in the visible stats line under each map).
export function describePitch(svgSelection, label) {
  svgSelection.attr("role", "img").attr("aria-label", label);
}

// ------------------------------------------------------------------ //
// Tooltips: hover (mouse) and tap (touch) via nearest-marker lookup     //
// ------------------------------------------------------------------ //

// `items` is [{points: [[x, y], ...], html}]: each item can be found from
// any of its points (an arrow uses its start, middle and end), so hovering
// or tapping anywhere along a pass finds that pass. One overlay per pitch
// replaces per-marker hit areas, which would overlap badly on dense maps.
export function enableTooltips(svgSelection, items) {
  const samples = items.flatMap((item, i) => item.points.map(([x, y]) => [x, y, i]));
  const delaunay = samples.length ? d3.Delaunay.from(samples, (s) => s[0], (s) => s[1]) : null;
  const svg = svgSelection.node();

  const nearest = (event) => {
    if (!delaunay) return null;
    const [px, py] = d3.pointer(event, svg);
    const sample = samples[delaunay.find(px, py)];
    const unitsPerPx = PITCH_LENGTH / svg.getBoundingClientRect().width;
    const dist = Math.hypot(sample[0] - px, sample[1] - py);
    return dist <= HIT_RADIUS_PX * unitsPerPx ? { item: items[sample[2]], at: sample } : null;
  };

  const highlight = (hit) => {
    svgSelection.selectAll(".pitch-hover").remove();
    if (!hit) return;
    const [x, y] = hit.item.points[0];
    svgSelection
      .append("circle")
      .attr("class", "pitch-hover")
      .attr("cx", x)
      .attr("cy", y)
      .attr("r", 2)
      .attr("fill", "none")
      .attr("stroke", "#1b1f24")
      .attr("stroke-width", 0.4)
      .attr("pointer-events", "none");
  };

  svgSelection
    .append("rect")
    .attr("class", "pitch-hit-layer")
    .attr("width", PITCH_LENGTH)
    .attr("height", PITCH_WIDTH)
    .attr("fill", "transparent")
    .style("cursor", "pointer")
    .on("pointermove", (event) => {
      if (event.pointerType !== "mouse" || isTooltipPinned()) return;
      const hit = nearest(event);
      highlight(hit);
      if (hit) showTooltip(event.clientX, event.clientY, hit.item.html);
      else hideTooltip();
    })
    .on("pointerleave", (event) => {
      if (event.pointerType === "mouse" && !isTooltipPinned()) hideTooltip();
    })
    .on("click", (event) => {
      event.stopPropagation();
      const hit = nearest(event);
      hideTooltip(); // also clears a highlight left on another map
      if (!hit) return;
      highlight(hit);
      pinTooltip(svg);
      showTooltip(event.clientX, event.clientY, hit.item.html);
    });
}

// ------------------------------------------------------------------ //
// Markers (visual only, interaction comes from enableTooltips)          //
// ------------------------------------------------------------------ //

const SYMBOL_AREA = 4.5;

// Draws one symbol per row at [x, y]. Shapes carry the category alongside
// color, so the map still reads correctly for color-blind viewers.
export function addPointMarkers(markerGroup, data, { x, y, color, symbol = "circle", hollow = false }) {
  const path = d3.symbol().type(SYMBOL_TYPES[symbol]).size(SYMBOL_AREA)();
  markerGroup
    .append("g")
    .selectAll("path")
    .data(data)
    .join("path")
    .attr("d", path)
    .attr("transform", (d) => `translate(${x(d)},${y(d)})`)
    .attr("fill", hollow ? "#ffffff" : color)
    .attr("stroke", hollow ? color : "#1b1f24")
    .attr("stroke-width", hollow ? 0.45 : 0.15)
    .attr("opacity", 0.92)
    .attr("pointer-events", "none");
}

// Draws one arrow per row from [x1, y1] to [x2, y2]. `dashed` marks a
// second category by line style, not just color.
export function addArrowMarkers(markerGroup, data, { x1, y1, x2, y2, color, dashed = false, width = 0.5 }) {
  const svgId = markerGroup.node().ownerSVGElement.id;
  markerGroup
    .append("g")
    .selectAll("line")
    .data(data)
    .join("line")
    .attr("x1", (d) => x1(d))
    .attr("y1", (d) => y1(d))
    .attr("x2", (d) => x2(d))
    .attr("y2", (d) => y2(d))
    .attr("stroke", color)
    .attr("stroke-width", width)
    .attr("stroke-dasharray", dashed ? "1.2 0.8" : null)
    .attr("opacity", 0.85)
    .attr("marker-end", `url(#arrowhead-${svgId})`)
    .attr("pointer-events", "none");
}

export function ensureArrowheadDef(svgSelection, color = "#333") {
  if (!svgSelection.select("defs").empty()) return;
  svgSelection
    .append("defs")
    .append("marker")
    .attr("id", `arrowhead-${svgSelection.attr("id")}`)
    .attr("viewBox", "0 0 10 10")
    .attr("refX", 8)
    .attr("refY", 5)
    .attr("markerWidth", 4)
    .attr("markerHeight", 4)
    .attr("orient", "auto-start-reverse")
    .append("path")
    .attr("d", "M 0 0 L 10 5 L 0 10 z")
    .attr("fill", color);
}

// ------------------------------------------------------------------ //
// Legend swatches matching the markers above                            //
// ------------------------------------------------------------------ //

const SYMBOL_TYPES = {
  circle: d3.symbolCircle,
  diamond: d3.symbolDiamond,
  square: d3.symbolSquare,
  cross: d3.symbolCross,
  triangle: d3.symbolTriangle,
  wye: d3.symbolWye,
  star: d3.symbolStar,
};

export function symbolSwatch(symbol, color, hollow = false) {
  const path = d3.symbol().type(SYMBOL_TYPES[symbol]).size(60)();
  const fill = hollow ? "#ffffff" : color;
  const stroke = hollow ? color : "#1b1f24";
  return `<svg class="legend-symbol" viewBox="-7 -7 14 14" aria-hidden="true"><path d="${path}" fill="${fill}" stroke="${stroke}" stroke-width="${hollow ? 2 : 0.6}"/></svg>`;
}

export function lineSwatch(color, dashed = false, width = 2) {
  const dash = dashed ? ' stroke-dasharray="4 3"' : "";
  return `<svg class="legend-line" viewBox="0 0 22 10" aria-hidden="true"><line x1="1" y1="5" x2="21" y2="5" stroke="${color}" stroke-width="${width}"${dash}/></svg>`;
}
