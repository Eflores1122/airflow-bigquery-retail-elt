import * as duckdb from "@duckdb/duckdb-wasm";
import duckdbMvp from "@duckdb/duckdb-wasm/dist/duckdb-mvp.wasm?url";
import mvpWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-mvp.worker.js?url";
import duckdbEh from "@duckdb/duckdb-wasm/dist/duckdb-eh.wasm?url";
import ehWorker from "@duckdb/duckdb-wasm/dist/duckdb-browser-eh.worker.js?url";

function absoluteUrl(url) {
  if (!url) return url;
  return new URL(url, window.location.href).href;
}

const BUNDLES = {
  mvp: { mainModule: absoluteUrl(duckdbMvp), mainWorker: absoluteUrl(mvpWorker) },
  eh: { mainModule: absoluteUrl(duckdbEh), mainWorker: absoluteUrl(ehWorker) },
};

const TASKS = [
  "resolve_window",
  "generate_and_land",
  "load_raw_and_stage",
  "publish_marts",
  "data_quality",
];

let db = null;
let conn = null;
let manifest = null;
let bundle = null;
let files = [];
let version = "";

function normalize(value) {
  if (value == null) return null;
  if (typeof value === "bigint") return Number(value);
  if (value instanceof Date) {
    const year = value.getUTCFullYear();
    const month = String(value.getUTCMonth() + 1).padStart(2, "0");
    const day = String(value.getUTCDate()).padStart(2, "0");
    if (
      value.getUTCHours() === 0 &&
      value.getUTCMinutes() === 0 &&
      value.getUTCSeconds() === 0 &&
      value.getUTCMilliseconds() === 0
    ) {
      return `${year}-${month}-${day}`;
    }
    return value.toISOString().replace(".000Z", "Z");
  }
  if (typeof value === "number" || typeof value === "boolean" || typeof value === "string") {
    return value;
  }
  if (typeof value === "object" && typeof value.toString === "function") {
    const text = String(value);
    if (text && text !== "[object Object]") return text;
  }
  return value;
}

export function rowsFrom(table) {
  if (!table || !table.schema) return [];
  const fields = table.schema.fields.map((field) => field.name);
  const rows = [];
  for (let index = 0; index < table.numRows; index += 1) {
    const row = {};
    for (const name of fields) {
      row[name] = normalize(table.getChild(name)?.get(index));
    }
    rows.push(row);
  }
  return rows;
}

async function openDatabase() {
  const selected = await duckdb.selectBundle(BUNDLES);
  const worker = await duckdb.createWorker(selected.mainWorker);
  const database = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
  await database.instantiate(selected.mainModule, selected.pthreadWorker);
  await database.open({ path: ":memory:" });
  const connection = await database.connect();
  const reported = await database.getVersion();
  return { database, connection, reported };
}

async function registerBuffers(database) {
  for (const file of files) {
    // registerFileBuffer transfers the buffer into the worker.
    await database.registerFileBuffer(file.path, file.bytes.slice());
  }
}

export async function boot(onStatus) {
  onStatus("load");
  const base = import.meta.env.BASE_URL;
  const [manifestResponse, sqlResponse] = await Promise.all([
    fetch(`${base}sample/manifest.json`),
    fetch(`${base}sample/sql.json`),
  ]);
  if (!manifestResponse.ok || !sqlResponse.ok) {
    throw new Error("sample json unavailable");
  }
  manifest = await manifestResponse.json();
  bundle = await sqlResponse.json();
  files = [];
  for (const file of manifest.files) {
    const response = await fetch(`${base}sample/${file.path}`);
    if (!response.ok) throw new Error(file.path);
    files.push({ path: file.path, bytes: new Uint8Array(await response.arrayBuffer()) });
  }
  onStatus("boot");
  const opened = await openDatabase();
  db = opened.database;
  conn = opened.connection;
  version = opened.reported;
  await registerBuffers(db);
  for (const statement of bundle.boot) {
    await conn.query(statement);
  }
  return { manifest, bundle, version };
}

export function getManifest() {
  return manifest;
}

export function getBundle() {
  return bundle;
}

export function getVersion() {
  return version;
}

export function taskNames() {
  return TASKS;
}

async function query(sql) {
  try {
    const table = await conn.query(sql);
    return rowsFrom(table);
  } catch (error) {
    const message = error && error.message ? error.message : String(error);
    const failure = new Error(message);
    failure.sql = sql;
    throw failure;
  }
}

export async function runTask(runId, task) {
  const run = bundle.runs[runId];
  if (!run) throw new Error(runId);
  const started = performance.now();
  let rows = [];
  if (task !== "data_quality") {
    for (const statement of run.tasks[task]) {
      rows = await query(statement);
    }
  }
  if (task === "publish_marts") {
    const guard = await query(
      `SELECT (SELECT COUNT(*) FROM stg_order_lines) AS staged,
              (SELECT COUNT(*) FROM int_order_line_enriched) AS enriched`,
    );
    const staged = Number(guard[0].staged);
    const enriched = Number(guard[0].enriched);
    if (staged !== enriched || staged === 0) {
      throw new Error(`star join dropped rows (staging=${staged}, enriched=${enriched})`);
    }
  }
  let checks = null;
  if (task === "data_quality") {
    checks = [];
    for (let index = 0; index < run.tasks.data_quality.length; index += 1) {
      const result = await query(run.tasks.data_quality[index]);
      checks.push({
        name: run.quality_names[index],
        violations: Number(result[0].violations || 0),
      });
    }
    rows = checks.map((check) => ({ name: check.name, violations: check.violations }));
  }
  return {
    rows,
    checks,
    elapsedMs: Math.round(performance.now() - started),
    sql: run.tasks[task].join(";\n\n"),
  };
}

export async function measure(windowDates) {
  const totals = await query(
    `SELECT COUNT(*) AS fact_rows,
            COALESCE(SUM(line_amount_centavos), 0) AS cents
     FROM fact_order_lines`,
  );
  let windowRows = 0;
  if (windowDates && windowDates.length) {
    const found = await query(
      `SELECT COUNT(*) AS rows
       FROM fact_order_lines
       WHERE order_date BETWEEN DATE '${windowDates[0]}' AND DATE '${windowDates[windowDates.length - 1]}'`,
    );
    windowRows = Number(found[0].rows);
  }
  const byDay = await query(
    `SELECT CAST(order_date AS VARCHAR) AS order_date,
            COUNT(*) AS rows,
            SUM(line_amount_centavos) AS cents
     FROM fact_order_lines
     GROUP BY 1
     ORDER BY 1`,
  );
  const sample = await query(
    `SELECT order_line_id,
            CAST(order_date AS VARCHAR) AS order_date,
            line_type,
            status,
            channel,
            category,
            quantity,
            line_amount_centavos,
            store_id
     FROM fact_order_lines
     ORDER BY order_date DESC, order_line_id
     LIMIT 8`,
  );
  const categories = await query(
    `SELECT category,
            COUNT(*) AS rows,
            SUM(CASE WHEN status = 'completed' THEN line_amount_centavos ELSE 0 END) AS completed_centavos
     FROM fact_order_lines
     GROUP BY 1
     ORDER BY completed_centavos DESC, category`,
  );
  let stagingRows = null;
  try {
    const staged = await query(`SELECT COUNT(*) AS rows FROM stg_order_lines`);
    stagingRows = Number(staged[0].rows);
  } catch {
    stagingRows = null;
  }
  return {
    factRows: Number(totals[0].fact_rows),
    cents: Number(totals[0].cents),
    windowRows,
    outsideRows: Number(totals[0].fact_rows) - windowRows,
    byDay,
    sample,
    categories,
    stagingRows,
  };
}

export function splitSql(sql) {
  const parts = [];
  let current = "";
  let quote = null;
  for (let index = 0; index < sql.length; index += 1) {
    const char = sql[index];
    if (quote) {
      current += char;
      if (char === quote) {
        if (quote === "'" && sql[index + 1] === "'") {
          current += sql[index + 1];
          index += 1;
          continue;
        }
        quote = null;
      }
      continue;
    }
    if (char === "'" || char === '"') {
      quote = char;
      current += char;
      continue;
    }
    if (char === "-" && sql[index + 1] === "-") {
      const end = sql.indexOf("\n", index);
      current += end === -1 ? sql.slice(index) : sql.slice(index, end + 1);
      index = end === -1 ? sql.length : end;
      continue;
    }
    if (char === ";") {
      if (current.trim()) parts.push(current.trim());
      current = "";
      continue;
    }
    current += char;
  }
  if (current.trim()) parts.push(current.trim());
  return parts;
}

export async function runEditorSql(sql) {
  const statements = splitSql(sql);
  if (!statements.length) throw new Error("empty sql");
  const started = performance.now();
  let rows = [];
  let columns = [];
  for (const statement of statements) {
    const table = await conn.query(statement);
    const next = rowsFrom(table);
    if (table.schema && table.schema.fields.length && (next.length || !columns.length)) {
      rows = next;
      columns = table.schema.fields.map((field) => field.name);
    }
  }
  return {
    rows,
    columns,
    statements: statements.length,
    elapsedMs: Math.round(performance.now() - started),
  };
}

export async function runFullRefresh() {
  const opened = await openDatabase();
  try {
    await registerBuffers(opened.database);
    const started = performance.now();
    for (const statement of bundle.full_refresh.statements) {
      await opened.connection.query(statement);
    }
    const totals = rowsFrom(
      await opened.connection.query(
        `SELECT COUNT(*) AS fact_rows,
                COALESCE(SUM(line_amount_centavos), 0) AS cents
         FROM fact_order_lines`,
      ),
    )[0];
    let violations = 0;
    for (const statement of bundle.full_refresh.quality_sql) {
      const row = rowsFrom(await opened.connection.query(statement))[0];
      violations += Number(row.violations || 0);
    }
    return {
      factRows: Number(totals.fact_rows),
      cents: Number(totals.cents),
      violations,
      elapsedMs: Math.round(performance.now() - started),
      version: opened.reported,
    };
  } finally {
    await opened.database.terminate();
  }
}

export async function resetWarehouse() {
  if (db) {
    try {
      await db.terminate();
    } catch {
      /* already closed */
    }
  }
  const opened = await openDatabase();
  db = opened.database;
  conn = opened.connection;
  version = opened.reported;
  await registerBuffers(db);
  for (const statement of bundle.boot) {
    await conn.query(statement);
  }
  return version;
}
