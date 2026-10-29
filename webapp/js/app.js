import { loadJSON, loadTable } from "./data.js";
import {
  bubbleChart,
  colorForSlot,
  colorForTeam,
  dotPlot,
  groupedBarChart,
  setTeamPalette,
  singleBarChart,
  trendChart,
} from "./charts.js";
import {
  addArrowMarkers,
  addPointMarkers,
  describePitch,
  drawPitch,
  enableTooltips,
  ensureArrowheadDef,
  lineSwatch,
  symbolSwatch,
} from "./pitch.js";
import { enableTermTips, termButton } from "./tooltip.js";

const DATA_DIR = "public/data";
const DEFAULT_TEAM_COMPARISON = ["Real Madrid", "Barcelona", "Atlético Madrid"];

// The default trio gets the three most distinct palette colors, so the
// first chart a visitor sees is the easiest to read.
const PINNED_TEAM_COLORS = {
  Barcelona: "#004b8c",
  "Real Madrid": "#5700d9",
  "Atlético Madrid": "#d9000e",
};

// Eight action types are too many to tell apart by color alone (no
// 8-color set passes a color-blindness check when every marker can sit
// next to every other), so color shows the action's family and each type
// gets its own shape. The four colors pass the dataviz palette validator.
const ACTION_FAMILY_COLORS = {
  win: "#1d4ed8", // winning the ball
  protect: "#17a193", // protecting the goal
  press: "#910884", // pressing
  beaten: "#e90c0c", // beaten or fouled
};
const ACTION_STYLES = {
  tackle: { color: ACTION_FAMILY_COLORS.win, symbol: "circle" },
  interception: { color: ACTION_FAMILY_COLORS.win, symbol: "diamond" },
  ball_recovery: { color: ACTION_FAMILY_COLORS.win, symbol: "triangle" },
  clearance: { color: ACTION_FAMILY_COLORS.protect, symbol: "square" },
  block: { color: ACTION_FAMILY_COLORS.protect, symbol: "cross" },
  pressure: { color: ACTION_FAMILY_COLORS.press, symbol: "wye" },
  dribbled_past: { color: ACTION_FAMILY_COLORS.beaten, symbol: "star" },
  foul_committed: { color: ACTION_FAMILY_COLORS.beaten, symbol: "circle", hollow: true },
};
const ACTION_ORDER = Object.keys(ACTION_STYLES);
const FALLBACK_ACTION_STYLE = { color: "#1b1f24", symbol: "circle" };

// Blue/orange instead of green/red so the passing map works for red-green
// color blindness; incomplete passes are also dashed.
const PASS_COLORS = { complete: "#1d4ed8", incomplete: "#c2410c" };
const SHOT_COLORS = { goal: "#15803d", other: "#1b1f24" };

const DEFENSIVE_STAT_LABELS = {
  tackles: "Tackles",
  interceptions: "Interceptions",
  clearances: "Clearances",
  blocks: "Blocks",
  ball_recoveries: "Ball recoveries",
  pressures: "Pressures",
  got_dribbled_past: "Dribbled past",
  fouls_committed: "Fouls committed",
};

// Plain-language definitions shown when a stat term is hovered, focused
// or tapped. Each matches how the football-analytics package computes it.
const GLOSSARY = {
  xG: "Expected goals: how likely a shot was to be scored (0 to 1), judged from similar shots in StatsBomb's data. Added up, it shows how many goals a team or player 'should' have scored.",
  "xG per shot": "Average xG of a player's shots: higher means they shoot from better chances.",
  "Shot-goal rate": "Goals divided by shots: how often a player's shot ends up in the net.",
  "Avg. minutes per game": "Average time on the pitch per appearance. StatsBomb counts stoppage time, so a full match is usually 92 to 94 minutes.",
  "Shots on target": "Shots that were scored or saved by the goalkeeper.",
  Possession: "Share of the match time a team had the ball, added up over all its matches.",
  "Completion %": "Share of a player's passes that reached a teammate.",
  "Progressive passes": "Passes that end at least 25% closer to the opponent's goal than where they started.",
  "Line-breaking passes": "Passes that cut through an opposing defensive line (StatsBomb's through-ball tag).",
  "Dribbles completed": "Times a player took on an opponent with the ball and got past them, out of all attempts.",
  "Fouls won": "Times the player was fouled by an opponent.",
  Pressures: "Times a player closed down an opponent on the ball to force a mistake.",
  Tackles: "Times a player won the ball from an opponent with a challenge.",
  Interceptions: "Opponent passes a player cut out.",
  "Ball recoveries": "Times a player won back a loose ball.",
  Clearances: "Times a player kicked or headed the ball away from danger, usually near their own goal.",
};

const POSITION_GROUPS = [
  ["forward", "Forwards"],
  ["midfielder", "Midfielders"],
  ["defender", "Defenders"],
  ["goalkeeper", "Goalkeepers"],
];

// ------------------------------------------------------------------ //
// Shareable state: the current view lives in the URL hash               //
// ------------------------------------------------------------------ //

// Keys each mode writes to the URL; other modes' keys stay in memory so
// switching back restores them, but are left out of the shared link.
const MODE_KEYS = {
  team: ["teams"],
  player: ["p1team", "p1", "p2team", "p2", "trend"],
  match: ["mteam", "match", "mscope"],
};
const state = Object.fromEntries(new URLSearchParams(location.hash.slice(1)));

// replaceState (not pushState) so the Back button leaves the page instead
// of stepping through every filter click.
function saveState(updates) {
  Object.assign(state, updates);
  const mode = MODE_KEYS[state.mode] ? state.mode : "team";
  const params = new URLSearchParams({ mode });
  for (const key of MODE_KEYS[mode]) {
    if (state[key]) params.set(key, state[key]);
  }
  history.replaceState(null, "", `#${params}`);
}

function setIfOption(select, value) {
  if (value && [...select.options].some((o) => o.value === value)) select.value = value;
}

// ------------------------------------------------------------------ //
// Shared helpers                                                       //
// ------------------------------------------------------------------ //

const loadStatus = document.getElementById("load-status");

// Marks `element` busy while `task` runs. A first load shows a placeholder
// over the (still empty) cards; a refresh only dims what's already drawn,
// so switching matches doesn't flash. Screen readers hear the status.
// Overlapping loads (fast clicking) are counted, so the busy state ends
// only when the last one finishes.
const pendingLoads = new WeakMap();

async function withBusy(element, label, task, { refresh = false } = {}) {
  pendingLoads.set(element, (pendingLoads.get(element) ?? 0) + 1);
  element.setAttribute("aria-busy", "true");
  element.dataset.loading = refresh ? "refresh" : "first";
  loadStatus.textContent = `Loading ${label}…`;
  try {
    return await task();
  } finally {
    const remaining = pendingLoads.get(element) - 1;
    pendingLoads.set(element, remaining);
    if (remaining === 0) {
      element.removeAttribute("aria-busy");
      delete element.dataset.loading;
      loadStatus.textContent = "";
    }
  }
}

function topN(rows, key, n = 5) {
  return rows.slice().sort((a, b) => b[key] - a[key]).slice(0, n);
}

function renderLeaderboard(containerId, rows, valueKey, labelFn) {
  document.getElementById(containerId).replaceChildren(
    ...rows.map((r) => {
      const li = document.createElement("li");
      li.innerHTML = `<span>${labelFn(r)}</span><span>${r[valueKey]}</span>`;
      return li;
    })
  );
}

// `entries` is [label, swatchHtml], the swatch drawn to match the marker.
function renderLegend(containerId, entries) {
  document.getElementById(containerId).replaceChildren(
    ...entries.map(([label, swatch]) => {
      const span = document.createElement("span");
      span.innerHTML = `${swatch}${label}`;
      return span;
    })
  );
}

// A table label, as an explainable term when the glossary has it.
function statCell(label, term = label) {
  return GLOSSARY[term] ? termButton(term, label) : label;
}

// Fills a <select> with players grouped by position. `players` is
// [{value, label, group}]; option text is set as plain text, not HTML.
function fillPlayerSelect(select, players, { emptyOption } = {}) {
  select.replaceChildren();
  if (emptyOption) select.add(new Option(emptyOption.label, emptyOption.value));
  const known = POSITION_GROUPS.map(([key]) => key);
  for (const [key, title] of [...POSITION_GROUPS, ["other", "Other"]]) {
    const members = players
      .filter((p) => (known.includes(p.group) ? p.group : "other") === key)
      .sort((a, b) => a.label.localeCompare(b.label));
    if (!members.length) continue;
    const group = document.createElement("optgroup");
    group.label = title;
    group.append(...members.map((p) => new Option(p.label, p.value)));
    select.append(group);
  }
}

function fillTeamSelect(select, teams) {
  select.replaceChildren(...teams.map((t) => new Option(t, t)));
}

// Builds a compact comparison table. `columns` is [{label, term?, html}],
// `groups` is [{title?, rows: [{label, term?, cells: [...]}]}].
function compareTable(columns, groups) {
  const head = columns
    .map((c, i) => `<th${i ? ' class="numeric"' : ""} scope="col">${c.html ?? statCell(c.label, c.term)}</th>`)
    .join("");
  const body = groups
    .map((g) => {
      const title = g.title
        ? `<tr class="group-row"><th colspan="${columns.length}" scope="colgroup">${g.title}</th></tr>`
        : "";
      const rows = g.rows
        .map((r) => {
          const cells = r.cells.map((v) => `<td class="numeric">${v}</td>`).join("");
          return `<tr><th scope="row">${r.html ?? statCell(r.label, r.term)}</th>${cells}</tr>`;
        })
        .join("");
      return title + rows;
    })
    .join("");
  const table = document.createElement("table");
  table.className = "compare";
  table.innerHTML = `<thead><tr>${head}</tr></thead><tbody>${body}</tbody>`;
  return table;
}

function renderStandingsTable(rows) {
  const container = document.getElementById("standings-table");
  const table = document.createElement("table");
  table.className = "standings";
  table.innerHTML = `<thead><tr>
    <th>Pos</th><th>Team</th><th class="numeric">P</th><th class="numeric">W</th>
    <th class="numeric">D</th><th class="numeric">L</th><th class="numeric">GF</th>
    <th class="numeric">GA</th><th class="numeric">GD</th><th class="numeric">Pts</th>
  </tr></thead>`;
  const tbody = document.createElement("tbody");
  tbody.replaceChildren(
    ...rows.map((r) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>${r.position}</td><td>${r.team}</td>
        <td class="numeric">${r.played}</td><td class="numeric">${r.won}</td>
        <td class="numeric">${r.drawn}</td><td class="numeric">${r.lost}</td>
        <td class="numeric">${r.goals_for}</td><td class="numeric">${r.goals_against}</td>
        <td class="numeric">${r.goal_difference}</td><td class="numeric">${r.points}</td>`;
      return tr;
    })
  );
  table.appendChild(tbody);
  container.replaceChildren(table);
}

// ------------------------------------------------------------------ //
// Overview: standings + leaderboards                                   //
// ------------------------------------------------------------------ //

async function initOverview() {
  const [standings, shooting, goalsAssists, cleanSheets] = await withBusy(
    document.getElementById("overview"),
    "standings",
    () =>
      Promise.all([
        loadTable(`${DATA_DIR}/standings/standings.json`),
        loadTable(`${DATA_DIR}/players/season-27/shooting.json`),
        loadTable(`${DATA_DIR}/players/season-27/goals_assists.json`),
        loadTable(`${DATA_DIR}/teams/season-27/clean_sheets.json`),
      ])
  );

  renderStandingsTable(standings);
  renderLeaderboard(
    "top-scorers",
    topN(shooting, "goals"),
    "goals",
    (r) => `${r.display_name} (${r.team})`
  );
  renderLeaderboard(
    "top-assists",
    topN(goalsAssists, "assists"),
    "assists",
    (r) => `${r.display_name} (${r.team})`
  );
  renderLeaderboard("top-clean-sheets", topN(cleanSheets, "clean_sheets"), "clean_sheets", (r) => r.team);
}

// ------------------------------------------------------------------ //
// Team-comparison panel                                                //
// ------------------------------------------------------------------ //

let teamPanelData = null;

async function initTeamPanel(meta) {
  if (!teamPanelData) {
    const paths = [
      "standings/standings",
      "teams/season-27/goals_xg",
      "teams/season-27/assists",
      "teams/season-27/possession",
      "teams/season-27/clean_sheets",
      "teams/season-27/pressures_fouls",
    ];
    const tables = await withBusy(document.getElementById("panel-team"), "team data", () =>
      Promise.all(paths.map((p) => loadTable(`${DATA_DIR}/${p}.json`)))
    );

    const byTeam = new Map(meta.teams.map((t) => [t, { team: t }]));
    for (const table of tables) {
      for (const row of table) {
        Object.assign(byTeam.get(row.team) ?? {}, row);
      }
    }
    teamPanelData = { byTeam };
    renderTeamFilter();
  }
  renderTeamCharts();
}

function renderTeamFilter() {
  const { byTeam } = teamPanelData;
  const fromUrl = (state.teams || "").split(",").filter((t) => byTeam.has(t));
  const initial = fromUrl.length ? fromUrl : DEFAULT_TEAM_COMPARISON;

  const container = document.getElementById("team-filter");
  container.replaceChildren(
    ...[...byTeam.keys()].sort().map((team) => {
      const label = document.createElement("label");
      const checked = initial.includes(team) ? "checked" : "";
      label.innerHTML = `<input type="checkbox" value="${team}" ${checked} /> ${team}`;
      return label;
    })
  );
  container.onchange = () => {
    saveState({ teams: selectedTeams().join(",") });
    renderTeamCharts();
  };
}

function selectedTeams() {
  return [...document.querySelectorAll("#team-filter input:checked")].map((i) => i.value);
}

function renderTeamCharts() {
  const { byTeam } = teamPanelData;
  const data = selectedTeams().map((t) => byTeam.get(t)).filter(Boolean);

  dotPlot("team-goals-xg-chart", data, {
    x: "total_xg",
    y: "goals",
    xTitle: "Expected goals (xG)",
    yTitle: "Goals",
    labelKey: "team",
  });
  singleBarChart("team-possession-chart", data, {
    valueKey: "possession_percent",
    xTitle: "Team",
    yTitle: "Possession (%)",
  });
  dotPlot("team-possession-sot-chart", data, {
    x: "possession_percent",
    y: "shots_on_target",
    xTitle: "Possession (%)",
    yTitle: "Shots on target",
    labelKey: "team",
  });
  dotPlot("team-assists-chart", data, {
    x: "position",
    y: "assists",
    xTitle: "League position",
    yTitle: "Assists",
    labelKey: "team",
  });
  groupedBarChart("team-pressures-chart", data, {
    statCols: ["pressures", "fouls_won"],
    statLabels: { pressures: "Pressures", fouls_won: "Fouls won" },
    labelKey: "team",
    xTitle: "Count",
    yTitle: "Statistic",
  });
  renderTeamTable(data);
}

const TEAM_TABLE_COLUMNS = [
  { label: "Pos", value: (t) => t.position },
  { label: "Goals", value: (t) => t.goals },
  { label: "xG", value: (t) => t.total_xg?.toFixed(1) },
  { label: "Possession %", term: "Possession", value: (t) => t.possession_percent?.toFixed(1) },
  { label: "Shots on target", value: (t) => t.shots_on_target },
  { label: "Assists", value: (t) => t.assists },
  { label: "Pressures", value: (t) => t.pressures },
  { label: "Fouls won", value: (t) => t.fouls_won },
];

function renderTeamTable(data) {
  const container = document.getElementById("team-table");
  if (!data.length) {
    container.textContent = "Tick at least one team above to compare.";
    return;
  }
  const rows = data
    .slice()
    .sort((a, b) => a.position - b.position)
    .map((t) => ({
      html: `<span class="key-dot" style="background:${colorForTeam(t.team)}"></span>${t.team}`,
      cells: TEAM_TABLE_COLUMNS.map((c) => c.value(t) ?? "–"),
    }));
  container.replaceChildren(
    compareTable([{ label: "Team" }, ...TEAM_TABLE_COLUMNS], [{ rows }])
  );
}

// ------------------------------------------------------------------ //
// Player-comparison panel                                              //
// ------------------------------------------------------------------ //

let playerPanelData = null;
const playerSlots = [1, 2].map((n) => ({
  n,
  team: document.getElementById(`player-${n}-team`),
  player: document.getElementById(`player-${n}-select`),
  optional: n === 2,
}));

async function initPlayerPanel(meta) {
  if (playerPanelData) return;

  const [roster, shooting, passing, defensive, ballCarrying, minutes, trendShooting, trendGoalsAssists] =
    await withBusy(document.getElementById("panel-player"), "player data", () =>
      Promise.all(
        [
          "season-27/roster",
          "season-27/shooting",
          "season-27/passing",
          "season-27/defensive",
          "season-27/ball_carrying",
          "season-27/minutes",
          "trend/shooting_per90",
          "trend/goals_assists_per90",
        ].map((p) => loadTable(`${DATA_DIR}/players/${p}.json`))
      )
    );

  // minutes.json goes last so its matches_played (appearances, from the
  // lineups) wins over the per-table counts.
  const byPlayer = new Map(roster.map((r) => [r.player, { ...r }]));
  for (const table of [shooting, passing, defensive, ballCarrying, minutes]) {
    for (const row of table) {
      if (byPlayer.has(row.player)) Object.assign(byPlayer.get(row.player), row);
    }
  }
  playerPanelData = { roster, byPlayer, trendShooting, trendGoalsAssists };

  // Each team's default pick is its top scorer (most minutes breaks ties),
  // so choosing a team always shows a meaningful player straight away.
  const ranked = [...byPlayer.values()].sort(
    (a, b) => (b.goals ?? 0) - (a.goals ?? 0) || (b.minutes_played ?? 0) - (a.minutes_played ?? 0)
  );
  const topPlayerOf = (team) => ranked.find((p) => p.team === team)?.player ?? "";

  const validTeam = (t) => meta.teams.includes(t);
  const p1 = byPlayer.has(state.p1) ? state.p1 : ranked[0].player;
  const p1Team = validTeam(state.p1team) ? state.p1team : byPlayer.get(p1).team;
  const p2Team = validTeam(state.p2team) ? state.p2team : p1Team;
  const initial = [
    [p1Team, p1],
    [p2Team, state.p2 ?? ""],
  ];

  for (const [i, slot] of playerSlots.entries()) {
    fillTeamSelect(slot.team, meta.teams);
    slot.team.value = initial[i][0];
    renderSlotOptions(slot);
    setIfOption(slot.player, initial[i][1]);

    slot.team.addEventListener("change", () => {
      renderSlotOptions(slot);
      slot.player.value = topPlayerOf(slot.team.value);
      saveState({ [`p${slot.n}team`]: slot.team.value, [`p${slot.n}`]: slot.player.value });
      renderPlayerPanel();
    });
    slot.player.addEventListener("change", () => {
      saveState({ [`p${slot.n}`]: slot.player.value });
      renderPlayerPanel();
    });
  }
  renderPlayerPanel();

  const trendSelect = document.getElementById("trend-stat");
  setIfOption(trendSelect, state.trend);
  const renderTrend = () => {
    const statKey = trendSelect.value;
    const source =
      statKey === "assists_per90" ? playerPanelData.trendGoalsAssists : playerPanelData.trendShooting;
    trendChart("trend-chart", source, {
      statKey,
      statLabel: trendSelect.selectedOptions[0].textContent,
      trendEligibleNames: meta.trend_eligible_players,
    });
  };
  trendSelect.addEventListener("change", () => {
    saveState({ trend: trendSelect.value });
    renderTrend();
  });
  renderTrend();
}

function renderSlotOptions(slot) {
  const team = slot.team.value;
  const players = playerPanelData.roster
    .filter((r) => r.team === team)
    .map((r) => ({ value: r.player, label: r.display_name, group: r.position_group }));
  fillPlayerSelect(slot.player, players, slot.optional ? { emptyOption: { value: "", label: "None" } } : {});
}

const pct = (value) => (value == null ? "–" : `${value}%`);

const COMPARE_GROUPS = [
  {
    title: "Playing time",
    rows: [
      { label: "Games played", value: (p) => p.matches_played },
      {
        label: "Avg. minutes per game",
        value: (p) => (p.matches_played ? Math.round(p.minutes_played / p.matches_played) : null),
      },
    ],
  },
  {
    title: "Scoring",
    rows: [
      { label: "Goals", value: (p) => p.goals ?? 0 },
      { label: "Assists", value: (p) => p.assists ?? 0 },
      { label: "Shots", value: (p) => p.shots ?? 0 },
      {
        label: "Shot-goal rate",
        value: (p) => (p.shots ? `${((p.goals / p.shots) * 100).toFixed(1)}%` : "0.0%"),
      },
      { label: "xG", value: (p) => (p.total_xg ?? 0).toFixed(1) },
    ],
  },
  {
    title: "Passing",
    rows: [
      { label: "Passes", value: (p) => p.passes ?? 0 },
      { label: "Completion %", value: (p) => pct(p.completion_rate) },
      { label: "Progressive passes", value: (p) => p.progressive_passes ?? 0 },
      { label: "Line-breaking passes", value: (p) => p.line_breaking_passes ?? 0 },
    ],
  },
  {
    title: "Dribbling",
    rows: [
      {
        label: "Dribbles completed",
        value: (p) => `${p.dribbles_completed ?? 0} / ${p.dribbles_attempted ?? 0}`,
      },
      { label: "Fouls won", value: (p) => p.fouls_won ?? 0 },
    ],
  },
  {
    title: "Defending",
    rows: [
      { label: "Tackles", value: (p) => p.tackles ?? 0 },
      { label: "Interceptions", value: (p) => p.interceptions ?? 0 },
      { label: "Pressures", value: (p) => p.pressures ?? 0 },
      { label: "Ball recoveries", value: (p) => p.ball_recoveries ?? 0 },
      { label: "Clearances", value: (p) => p.clearances ?? 0 },
    ],
  },
];

function renderPlayerPanel() {
  if (!playerPanelData) return;
  const { byPlayer } = playerPanelData;
  const selected = playerSlots
    .map((slot) => slot.player.value)
    .filter((name) => byPlayer.has(name))
    .map((name) => byPlayer.get(name));

  const summary = document.getElementById("player-summary");
  if (selected.length === 0) {
    summary.textContent = "Pick a player above to see their season.";
  } else {
    const columns = [
      { label: "Stat" },
      ...selected.map((p, i) => ({
        html:
          `<span class="key-dot" style="background:${colorForSlot(i)}"></span>${p.display_name}` +
          `<span class="compare-team">${p.team}</span>`,
      })),
    ];
    const groups = COMPARE_GROUPS.map((g) => ({
      title: g.title,
      rows: g.rows.map((r) => ({ label: r.label, cells: selected.map((p) => r.value(p) ?? "–") })),
    }));
    summary.replaceChildren(compareTable(columns, groups));
  }

  const colorFn = (d, i) => colorForSlot(i);
  bubbleChart("player-shooting-chart", selected, {
    x: "shots",
    y: "goals",
    size: "xg_per_shot",
    xTitle: "Shots",
    yTitle: "Goals",
    colorFn,
  });
  bubbleChart("player-passing-chart", selected, {
    x: "progressive_passes",
    y: "completion_rate",
    size: "assists",
    xTitle: "Progressive passes",
    yTitle: "Pass completion (%)",
    colorFn,
  });
  bubbleChart("player-dribbling-chart", selected, {
    x: "dribbles_attempted",
    y: "dribble_success_rate",
    size: "fouls_won",
    sizeScale: 6,
    minSize: 7,
    xTitle: "Dribbles attempted",
    yTitle: "Dribble success rate (%)",
    colorFn,
  });
  groupedBarChart("player-defensive-chart", selected, {
    statCols: Object.keys(DEFENSIVE_STAT_LABELS),
    statLabels: DEFENSIVE_STAT_LABELS,
    labelKey: "display_name",
    xTitle: "Count",
    yTitle: "Defensive action",
    colorFn,
  });
}

// ------------------------------------------------------------------ //
// Match-comparison panel                                               //
// ------------------------------------------------------------------ //

let matchIndex = null;
let playerInfo = new Map(); // full name -> {display_name, position_group}
const matchPanel = document.getElementById("panel-match");
const matchTeamSelect = document.getElementById("match-team-select");
const fixtureSelect = document.getElementById("match-fixture-select");
const scopeSelect = document.getElementById("match-player-select");

const displayName = (player) => playerInfo.get(player)?.display_name ?? player;

async function initMatchPanel(meta) {
  if (matchIndex) return;
  const [index, roster] = await withBusy(matchPanel, "fixtures", () =>
    Promise.all([
      loadTable(`${DATA_DIR}/match/index.json`),
      loadTable(`${DATA_DIR}/players/season-27/roster.json`),
    ])
  );
  matchIndex = index;
  playerInfo = new Map(roster.map((r) => [r.player, r]));

  fillTeamSelect(matchTeamSelect, meta.teams);
  matchTeamSelect.value = meta.default_team;
  setIfOption(matchTeamSelect, state.mteam);

  matchTeamSelect.addEventListener("change", () => {
    saveState({ mteam: matchTeamSelect.value, match: "", mscope: "" });
    renderFixtureOptions();
  });
  fixtureSelect.addEventListener("change", () => {
    saveState({ match: fixtureSelect.value, mscope: "" });
    loadMatch();
  });
  scopeSelect.addEventListener("change", () => {
    saveState({ mscope: scopeSelect.value });
    renderMatchMaps();
  });

  await renderFixtureOptions(state.match, state.mscope);
}

function renderFixtureOptions(preferredMatch, preferredScope) {
  const team = matchTeamSelect.value;
  const fixtures = matchIndex
    .filter((m) => m.home_team === team || m.away_team === team)
    .sort((a, b) => a.match_week - b.match_week);

  fixtureSelect.replaceChildren(
    ...fixtures.map((m) => {
      const opponent = m.home_team === team ? m.away_team : m.home_team;
      const venue = m.home_team === team ? "H" : "A";
      return new Option(
        `MW ${m.match_week}: vs ${opponent} (${venue}), ${m.home_score}-${m.away_score}`,
        m.match_id
      );
    })
  );
  setIfOption(fixtureSelect, preferredMatch);
  return loadMatch(preferredScope);
}

let currentMatch = null;
let matchRequest = 0;

async function loadMatch(preferredScope) {
  const matchId = fixtureSelect.value;
  if (!matchId) return;
  // Ignore responses that arrive after the user already picked another
  // match, so a slow earlier request can't overwrite the newer one.
  const request = ++matchRequest;
  const matchDir = `${DATA_DIR}/match/${matchId}`;
  const [shots, passes, defensiveEvents, lineup] = await withBusy(
    matchPanel,
    "match events",
    () =>
      Promise.all(
        ["shots", "passes", "defensive_events", "lineup"].map((n) => loadTable(`${matchDir}/${n}.json`))
      ),
    { refresh: currentMatch !== null }
  );
  if (request !== matchRequest) return;
  currentMatch = { shots, passes, defensiveEvents, lineup };

  renderScopeOptions(preferredScope);
  renderDefensiveToggles();
  renderPassingToggles();
  renderMatchMaps();
}

// Every player of the selected team who played in this match (from the
// lineup, not the events), so players with no shots or passes still show.
function renderScopeOptions(preferred) {
  const team = matchTeamSelect.value;
  const players = currentMatch.lineup
    .filter((r) => r.team === team)
    .map((r) => ({
      value: r.player,
      label: `${displayName(r.player)} (${Math.round(r.minutes_played)} min)`,
      group: playerInfo.get(r.player)?.position_group,
    }));
  fillPlayerSelect(scopeSelect, players, { emptyOption: { value: "", label: "Whole team" } });
  setIfOption(scopeSelect, preferred);
}

function actionLabel(type) {
  return type.replace("_", " ");
}

// Family order (same-colored actions together); unknown types go last.
function byActionOrder(types) {
  const rank = (t) => (ACTION_ORDER.includes(t) ? ACTION_ORDER.indexOf(t) : ACTION_ORDER.length);
  return types.slice().sort((a, b) => rank(a) - rank(b));
}

function renderDefensiveToggles() {
  const actionTypes = byActionOrder([...new Set(currentMatch.defensiveEvents.map((d) => d.action_type))]);
  const container = document.getElementById("defensive-action-toggles");
  container.replaceChildren(
    ...actionTypes.map((type) => {
      const label = document.createElement("label");
      const checked = type === "pressure" ? "" : "checked";
      label.innerHTML = `<input type="checkbox" value="${type}" ${checked} /> ${actionLabel(type)}`;
      return label;
    })
  );
  container.onchange = renderDefensiveMap;
}

function renderPassingToggles() {
  const container = document.getElementById("passing-filter-toggles");
  container.replaceChildren(
    ...[
      ["progressive", "Progressive"],
      ["linebreak", "Line-breaking"],
      ["assist", "Assist"],
    ].map(([value, label]) => {
      const el = document.createElement("label");
      el.innerHTML = `<input type="checkbox" value="${value}" /> ${label}`;
      return el;
    })
  );
  container.onchange = () => renderMatchMaps();
}

// Rows for the selected team, narrowed to one player if one is chosen.
function scopedRows(rows) {
  const team = matchTeamSelect.value;
  const player = scopeSelect.value;
  return rows.filter((r) => r.team === team && (!player || r.player === player));
}

// e.g. "Sergio Busquets had no shots in this match."
function noEventsText(kind) {
  const subject = scopeSelect.value ? displayName(scopeSelect.value) : matchTeamSelect.value;
  return `${subject} had no ${kind} in this match.`;
}

function renderMatchMaps() {
  if (!currentMatch) return;
  renderShotMap(scopedRows(currentMatch.shots));
  renderPassingMap(scopedRows(currentMatch.passes));
  renderDefensiveMap();
}

// Start, middle and end of an arrow, so hovering or tapping anywhere along
// it finds the event.
function arrowPoints(start, end) {
  return [
    [start[0], start[1]],
    [(start[0] + end[0]) / 2, (start[1] + end[1]) / 2],
    [end[0], end[1]],
  ];
}

const plural = (n, one, many = `${one}s`) => `${n} ${n === 1 ? one : many}`;

function renderShotMap(shots) {
  const svg = d3.select("#shot-map");
  const markerGroup = drawPitch(svg);
  ensureArrowheadDef(svg);

  const arrow = {
    x1: (d) => d.location[0],
    y1: (d) => d.location[1],
    x2: (d) => d.shot_end_location[0],
    y2: (d) => d.shot_end_location[1],
  };
  const goals = shots.filter((s) => s.shot_outcome === "Goal");
  addArrowMarkers(markerGroup, shots.filter((s) => s.shot_outcome !== "Goal"), {
    ...arrow,
    color: SHOT_COLORS.other,
  });
  addArrowMarkers(markerGroup, goals, { ...arrow, color: SHOT_COLORS.goal, width: 0.9 });

  enableTooltips(
    svg,
    shots.map((d) => ({
      points: arrowPoints(d.location, d.shot_end_location),
      html: `<b>${displayName(d.player)}</b><br>${d.minute}' ${d.shot_outcome}<br>xG ${d.shot_statsbomb_xg.toFixed(2)}`,
    }))
  );
  renderLegend("shot-legend", [
    ["Goal", lineSwatch(SHOT_COLORS.goal, false, 3)],
    ["Other shot", lineSwatch(SHOT_COLORS.other)],
  ]);

  const totalXg = shots.reduce((sum, s) => sum + s.shot_statsbomb_xg, 0);
  const summary = shots.length
    ? `${plural(shots.length, "shot")} · ${plural(goals.length, "goal")} · ${totalXg.toFixed(2)} xG`
    : noEventsText("shots");
  document.getElementById("shot-stats").textContent = summary;
  describePitch(svg, `Shot map: ${summary}`);
}

function renderPassingMap(passes) {
  const svg = d3.select("#passing-map");
  const markerGroup = drawPitch(svg);
  ensureArrowheadDef(svg);

  const checked = [...document.querySelectorAll("#passing-filter-toggles input:checked")].map(
    (i) => i.value
  );
  const matchesFilter = (p) => {
    if (checked.length === 0) return true;
    return (
      (checked.includes("progressive") && p.is_progressive) ||
      (checked.includes("linebreak") && p.pass_through_ball) ||
      (checked.includes("assist") && p.pass_goal_assist)
    );
  };
  const shown = passes.filter(matchesFilter);

  const arrow = {
    x1: (d) => d.location[0],
    y1: (d) => d.location[1],
    x2: (d) => d.pass_end_location[0],
    y2: (d) => d.pass_end_location[1],
  };
  addArrowMarkers(markerGroup, shown.filter((p) => p.pass_outcome), {
    ...arrow,
    color: PASS_COLORS.incomplete,
    dashed: true,
  });
  addArrowMarkers(markerGroup, shown.filter((p) => !p.pass_outcome), {
    ...arrow,
    color: PASS_COLORS.complete,
  });

  enableTooltips(
    svg,
    shown.map((d) => {
      const tags = [];
      if (d.is_progressive) tags.push("progressive");
      if (d.pass_through_ball) tags.push("line-breaking");
      if (d.pass_goal_assist) tags.push("assist");
      const outcome = d.pass_outcome || "Complete";
      const tagText = tags.length ? `<br>${tags.join(", ")}` : "";
      return {
        points: arrowPoints(d.location, d.pass_end_location),
        html: `<b>${displayName(d.player)}</b><br>${d.minute}' · ${outcome}${tagText}`,
      };
    })
  );
  renderLegend("passing-legend", [
    ["Complete", lineSwatch(PASS_COLORS.complete)],
    ["Incomplete", lineSwatch(PASS_COLORS.incomplete, true)],
  ]);

  const stats = document.getElementById("passing-stats");
  if (!passes.length) {
    stats.textContent = noEventsText("passes");
    describePitch(svg, `Passing map: ${stats.textContent}`);
    return;
  }
  const completeCount = passes.filter((p) => !p.pass_outcome).length;
  const shownComplete = shown.filter((p) => !p.pass_outcome).length;
  const progCount = passes.filter((p) => p.is_progressive).length;
  const lineCount = passes.filter((p) => p.pass_through_ball).length;
  const assistCount = passes.filter((p) => p.pass_goal_assist).length;
  stats.textContent =
    `Showing ${shown.length} of ${passes.length} passes (${completeCount} complete, ` +
    `${passes.length - completeCount} incomplete) · Progressive: ${progCount} · ` +
    `Line-breaking: ${lineCount} · Assists: ${assistCount}`;
  describePitch(
    svg,
    `Passing map, attacking left to right: ${plural(shown.length, "pass", "passes")} shown, ` +
      `${shownComplete} complete and ${shown.length - shownComplete} incomplete.`
  );
}

function renderDefensiveMap() {
  if (!currentMatch) return;
  const scoped = scopedRows(currentMatch.defensiveEvents);

  const checkedTypes = [...document.querySelectorAll("#defensive-action-toggles input:checked")].map(
    (i) => i.value
  );
  const events = scoped.filter((d) => checkedTypes.includes(d.action_type));

  const svg = d3.select("#defensive-map");
  const markerGroup = drawPitch(svg);
  const byType = {};
  for (const e of events) (byType[e.action_type] ??= []).push(e);

  const styleFor = (type) => ACTION_STYLES[type] || FALLBACK_ACTION_STYLE;
  for (const [actionType, rows] of Object.entries(byType)) {
    addPointMarkers(markerGroup, rows, {
      x: (d) => d.location[0],
      y: (d) => d.location[1],
      ...styleFor(actionType),
    });
  }

  enableTooltips(
    svg,
    events.map((d) => ({
      points: [[d.location[0], d.location[1]]],
      html: `<b>${displayName(d.player)}</b><br>${d.minute}' · ${actionLabel(d.action_type)}`,
    }))
  );
  renderLegend(
    "defensive-legend",
    byActionOrder(Object.keys(byType)).map((t) => {
      const { color, symbol, hollow } = styleFor(t);
      return [actionLabel(t), symbolSwatch(symbol, color, hollow)];
    })
  );

  const stats = document.getElementById("defensive-stats");
  if (!scoped.length) {
    stats.textContent = noEventsText("defensive actions");
  } else {
    const allCounts = {};
    for (const e of scoped) allCounts[e.action_type] = (allCounts[e.action_type] || 0) + 1;
    stats.textContent = byActionOrder(Object.keys(allCounts))
      .map((type) => `${actionLabel(type)}: ${allCounts[type]}`)
      .join(" · ");
  }
  const shownCounts = Object.entries(byType).map(([t, rows]) => `${actionLabel(t)}: ${rows.length}`);
  describePitch(
    svg,
    `Defensive actions map, attacking left to right: ` +
      `${shownCounts.length ? shownCounts.join(", ") : stats.textContent}.`
  );
}

// ------------------------------------------------------------------ //
// Mode switch + entry point                                            //
// ------------------------------------------------------------------ //

function initModeSwitch(meta) {
  const select = document.getElementById("mode-select");
  const panels = {
    team: document.getElementById("panel-team"),
    player: document.getElementById("panel-player"),
    match: matchPanel,
  };
  const initializers = {
    team: () => initTeamPanel(meta),
    player: () => initPlayerPanel(meta),
    match: () => initMatchPanel(meta),
  };

  const show = (mode) => {
    for (const [name, panel] of Object.entries(panels)) {
      panel.hidden = name !== mode;
    }
    return initializers[mode]();
  };

  select.addEventListener("change", () => {
    saveState({ mode: select.value });
    show(select.value);
  });

  setIfOption(select, state.mode);
  return show(select.value);
}

async function main() {
  enableTermTips(GLOSSARY);
  const meta = await loadJSON(`${DATA_DIR}/meta.json`);
  setTeamPalette(meta.teams, PINNED_TEAM_COLORS);
  // Overview and the selected panel load in parallel.
  await Promise.all([initOverview(), initModeSwitch(meta)]);
}

main().catch((err) => {
  console.error(err);
  document.querySelector(".intro-text").textContent =
    "Could not load the precomputed data. If you're viewing this file directly " +
    "(file://), serve webapp/ with a local HTTP server instead, see webapp/README.md.";
});
