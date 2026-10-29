// Fetch helpers for the precomputed {"columns": [...], "rows": [...]} JSON
// files written by scripts/data_pipeline/build.py.

export async function loadJSON(path) {
  const res = await fetch(path);
  if (!res.ok) {
    throw new Error(`Failed to load ${path}: ${res.status}`);
  }
  return res.json();
}

// Converts a {"columns": [...], "rows": [...]} payload into an array of
// plain objects, which is far easier to work with in Plotly/D3 code than
// parallel column/row arrays.
export async function loadTable(path) {
  const { columns, rows } = await loadJSON(path);
  return rows.map((row) => Object.fromEntries(columns.map((col, i) => [col, row[i]])));
}
