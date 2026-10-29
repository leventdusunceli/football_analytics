// Plotly chart builders for every chart in this demo except the three
// pitch maps (see pitch.js), which need real hover-per-marker interaction
// on top of a precise pitch coordinate system that Plotly's generic
// scatter traces aren't a natural fit for.

// 20 colors chosen to be as far apart as possible while every one keeps
// at least 3.2:1 contrast on white (the WCAG minimum for chart marks is
// 3:1), so no team's bars or dots fade into the background.
export const DISTINCT_PALETTE = [
  "#1d4ed8", "#00a600", "#d9000e", "#3296a6", "#d90091",
  "#735c00", "#733451", "#00733d", "#bc00d9", "#004b8c",
  "#d97162", "#d97400", "#792a8c", "#a60042", "#658c15",
  "#8c2500", "#5700d9", "#734934", "#d96299", "#d162d9",
];

let teamColorMap = new Map();

// Call once, after meta.teams is known, so every chart agrees on colors.
// `pinned` fixes chosen teams to chosen palette colors; everyone else gets
// the remaining colors in team-name order, so assignments stay stable.
export function setTeamPalette(teams, pinned = {}) {
  const used = new Set(Object.values(pinned));
  const free = DISTINCT_PALETTE.filter((c) => !used.has(c));
  const rest = [...teams].sort().filter((t) => !(t in pinned));
  teamColorMap = new Map([
    ...Object.entries(pinned),
    ...rest.map((team, i) => [team, free[i % free.length]]),
  ]);
}

export function colorForTeam(team) {
  return teamColorMap.get(team) || "#5b6471";
}

// For comparing up to a handful of players side by side (the player
// panel never shows more than two at once), colored by comparison slot
// rather than position, two players in the same position group must
// never end up sharing a color.
const SLOT_PALETTE = ["#1d4ed8", "#c0392b", "#1f9d55", "#9a6324"];

export function colorForSlot(index) {
  return SLOT_PALETTE[index % SLOT_PALETTE.length];
}

const LAYOUT_DEFAULTS = {
  margin: { t: 20, r: 20, b: 55, l: 65 },
  font: { family: "-apple-system, Segoe UI, Roboto, sans-serif", size: 12 },
  hoverlabel: { bgcolor: "#1b1f24", font: { color: "#fff" } },
};

// Plotly 3+ only accepts axis titles as { text }; a plain string is
// silently ignored. automargin keeps long titles from being cut off.
function axis(title) {
  return { title: { text: title || "", standoff: 10 }, automargin: true };
}

// No mode bar: its 22px buttons are below the 24px touch-target minimum,
// and zoom/pan tools add little to a presentation page.
const CONFIG_DEFAULTS = { responsive: true, displayModeBar: false };

// A scatter chart where marker size encodes a third variable. Used where
// the size genuinely carries meaning (e.g. xG per shot).
export function bubbleChart(
  containerId,
  data,
  {
    x,
    y,
    size,
    sizeScale = 18,
    minSize = 10,
    xTitle,
    yTitle,
    colorFn = (d) => colorForTeam(d.team),
  }
) {
  const trace = {
    x: data.map((d) => d[x]),
    y: data.map((d) => d[y]),
    text: data.map((d) => d.display_name),
    mode: "markers+text",
    textposition: "top center",
    textfont: { size: 12 },
    cliponaxis: false, // let labels near an edge spill into the margin instead of being cut
    marker: {
      size: data.map((d) => Math.max(minSize, Math.sqrt(Math.max(d[size], 0)) * sizeScale)),
      color: data.map((d, i) => colorFn(d, i)),
      line: { width: 1, color: "#1b1f24" },
      opacity: 0.85,
    },
    hovertemplate: data.map(
      (d) => `<b>${d.display_name}</b><br>${xTitle}: ${d[x]}<br>${yTitle}: ${d[y]}<extra></extra>`
    ),
  };
  Plotly.newPlot(
    containerId,
    [trace],
    { ...LAYOUT_DEFAULTS, xaxis: axis(xTitle), yaxis: axis(yTitle) },
    CONFIG_DEFAULTS
  );
}

// A scatter chart with a fixed dot size, for comparing two plain metrics
// (no third variable to encode), tolerates a handful of overlapping
// points rather than shrinking dots to the point of being unreadable.
export function dotPlot(
  containerId,
  data,
  { x, y, xTitle, yTitle, dotSize = 16, labelKey = "display_name", colorFn = (d) => colorForTeam(d.team) }
) {
  // Neighbors along x alternate labels above/below the dot, so two close
  // dots (e.g. Real Madrid and Barcelona) don't print over each other.
  const xRank = [];
  [...data.keys()]
    .sort((a, b) => data[a][x] - data[b][x])
    .forEach((idx, rank) => (xRank[idx] = rank));
  const trace = {
    x: data.map((d) => d[x]),
    y: data.map((d) => d[y]),
    text: data.map((d) => d[labelKey]),
    mode: "markers+text",
    textposition: data.map((d, i) => (xRank[i] % 2 ?"bottom center" : "top center")),
    textfont: { size: 12 },
    cliponaxis: false,
    marker: {
      size: dotSize,
      color: data.map((d, i) => colorFn(d, i)),
      line: { width: 1, color: "#1b1f24" },
      opacity: 0.9,
    },
    hovertemplate: data.map(
      (d) => `<b>${d[labelKey]}</b><br>${xTitle}: ${d[x]}<br>${yTitle}: ${d[y]}<extra></extra>`
    ),
  };
  Plotly.newPlot(
    containerId,
    [trace],
    { ...LAYOUT_DEFAULTS, xaxis: axis(xTitle), yaxis: axis(yTitle) },
    CONFIG_DEFAULTS
  );
}

// A horizontal grouped bar chart over an arbitrary set of stat columns,
// one trace per row (a player or a team). Used for the defensive-stats
// breakdown (one or two players) and the team-comparison bar charts.
export function groupedBarChart(
  containerId,
  data,
  {
    statCols,
    statLabels,
    labelKey = "display_name",
    xTitle,
    yTitle,
    colorFn = (d) => colorForTeam(d.team),
  }
) {
  const traces = data.map((row, i) => ({
    type: "bar",
    orientation: "h",
    name: row[labelKey],
    x: statCols.map((col) => row[col]),
    y: statCols.map((col) => statLabels[col] || col),
    marker: { color: colorFn(row, i) },
  }));
  Plotly.newPlot(
    containerId,
    traces,
    {
      ...LAYOUT_DEFAULTS,
      barmode: "group",
      xaxis: axis(xTitle),
      yaxis: axis(yTitle),
    },
    CONFIG_DEFAULTS
  );
}

// A vertical bar chart, one bar per team/row, for a single stat, with
// optional extra context (e.g. a second stat) shown only on hover.
export function singleBarChart(
  containerId,
  data,
  { valueKey, xTitle, yTitle, hoverExtra, colorFn = (d) => colorForTeam(d.team) }
) {
  const trace = {
    type: "bar",
    x: data.map((d) => d.team),
    y: data.map((d) => d[valueKey]),
    marker: { color: data.map((d, i) => colorFn(d, i)) },
    hovertemplate: data.map((d) => {
      const extra = hoverExtra ? `<br>${hoverExtra(d)}` : "";
      return `<b>${d.team}</b><br>${yTitle}: ${d[valueKey]}${extra}<extra></extra>`;
    }),
  };
  Plotly.newPlot(
    containerId,
    [trace],
    { ...LAYOUT_DEFAULTS, xaxis: axis(xTitle), yaxis: axis(yTitle) },
    CONFIG_DEFAULTS
  );
}

export function trendChart(containerId, perSeasonRows, { statKey, statLabel, trendEligibleNames }) {
  const byPlayer = new Map();
  for (const row of perSeasonRows) {
    if (!byPlayer.has(row.player)) byPlayer.set(row.player, []);
    byPlayer.get(row.player).push(row);
  }

  // Colored and shaped per player, not per team: five of the nine players
  // are Barcelona, so team colors would make their lines identical.
  const symbols = ["circle", "square", "diamond", "triangle-up", "x", "star", "cross", "triangle-down", "pentagon"];
  const traces = [...byPlayer].map(([player, rows], i) => {
    const sorted = rows.slice().sort((a, b) => a.season_id - b.season_id);
    const isEligible = trendEligibleNames.includes(player);
    const color = DISTINCT_PALETTE[i % DISTINCT_PALETTE.length];
    return {
      x: sorted.map((r) => r.season_name),
      y: sorted.map((r) => r[statKey]),
      mode: isEligible ? "lines+markers" : "markers",
      name: isEligible
        ? sorted[0].display_name
        : `${sorted[0].display_name} (sparse coverage, no trend line)`,
      marker: { size: 10, color, symbol: symbols[i % symbols.length] },
      line: { width: 2, color },
    };
  });

  Plotly.newPlot(
    containerId,
    traces,
    { ...LAYOUT_DEFAULTS, xaxis: axis("Season"), yaxis: axis(statLabel) },
    CONFIG_DEFAULTS
  );
}
