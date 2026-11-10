import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const duckdb = require("@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs");

const here = path.dirname(fileURLToPath(import.meta.url));
const sampleDir = path.join(here, "public", "sample");
const duckdbDist = path.dirname(
  require.resolve("@duckdb/duckdb-wasm/dist/duckdb-node-blocking.cjs"),
);

function rowsFrom(table) {
  const fields = table.schema.fields.map((field) => field.name);
  const rows = [];
  for (let index = 0; index < table.numRows; index += 1) {
    const row = {};
    for (const name of fields) {
      let value = table.getChild(name).get(index);
      if (typeof value === "bigint") value = Number(value);
      row[name] = value;
    }
    rows.push(row);
  }
  return rows;
}

function snapshot(conn) {
  const totals = rowsFrom(
    conn.query(
      `SELECT COUNT(*) AS fact_rows,
              COALESCE(SUM(line_amount_centavos), 0) AS cents,
              (SELECT COUNT(*) FROM stg_order_lines) AS staging_rows
       FROM fact_order_lines`,
    ),
  )[0];
  const byDay = rowsFrom(
    conn.query(
      `SELECT CAST(order_date AS VARCHAR) AS order_date,
              COUNT(*) AS rows,
              SUM(line_amount_centavos) AS cents
       FROM fact_order_lines
       GROUP BY 1
       ORDER BY 1`,
    ),
  ).map((row) => ({
    order_date: String(row.order_date).slice(0, 10),
    rows: Number(row.rows),
    cents: Number(row.cents),
  }));
  return {
    fact_rows: Number(totals.fact_rows),
    cents: Number(totals.cents),
    staging_rows: Number(totals.staging_rows),
    by_day: byDay,
  };
}

async function openDb() {
  const database = await duckdb.createDuckDB(
    {
      mvp: {
        mainModule: path.join(duckdbDist, "duckdb-mvp.wasm"),
        mainWorker: path.join(duckdbDist, "duckdb-node-mvp.worker.cjs"),
      },
      eh: {
        mainModule: path.join(duckdbDist, "duckdb-eh.wasm"),
        mainWorker: path.join(duckdbDist, "duckdb-node-eh.worker.cjs"),
      },
    },
    new duckdb.ConsoleLogger(),
    duckdb.NODE_RUNTIME,
  );
  await database.instantiate();
  database.open({ path: ":memory:" });
  return database;
}

function registerSample(database, manifest) {
  for (const file of manifest.files) {
    const bytes = fs.readFileSync(path.join(sampleDir, file.path));
    database.registerFileBuffer(file.path, bytes);
  }
}

function same(label, actual, expected) {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  if (left !== right) {
    throw new Error(`${label}\n${left}\n${right}`);
  }
}

const manifest = JSON.parse(fs.readFileSync(path.join(sampleDir, "manifest.json"), "utf8"));
const bundle = JSON.parse(fs.readFileSync(path.join(sampleDir, "sql.json"), "utf8"));

const database = await openDb();
const conn = database.connect();
console.log("wasm", database.getVersion());
registerSample(database, manifest);
for (const statement of bundle.boot) conn.query(statement);

for (const runId of manifest.run_order) {
  const run = bundle.runs[runId];
  for (const task of manifest.tasks) {
    for (const statement of run.tasks[task]) conn.query(statement);
  }
  const guard = rowsFrom(
    conn.query(
      `SELECT (SELECT COUNT(*) FROM stg_order_lines) AS staged,
              (SELECT COUNT(*) FROM int_order_line_enriched) AS enriched`,
    ),
  )[0];
  if (Number(guard.staged) !== Number(guard.enriched) || Number(guard.staged) === 0) {
    throw new Error(`${runId} star join ${guard.staged} ${guard.enriched}`);
  }
  let violations = 0;
  for (const statement of run.tasks.data_quality) {
    violations += Number(rowsFrom(conn.query(statement))[0].violations || 0);
  }
  if (violations !== 0) throw new Error(`${runId} violations ${violations}`);
  same(runId, snapshot(conn), run.expected);
  console.log(runId, "ok", run.expected.fact_rows);
}

const second = await openDb();
const secondConn = second.connect();
registerSample(second, manifest);
for (const statement of bundle.full_refresh.statements) secondConn.query(statement);
let fullViolations = 0;
for (const statement of bundle.full_refresh.quality_sql) {
  fullViolations += Number(rowsFrom(secondConn.query(statement))[0].violations || 0);
}
if (fullViolations !== 0) throw new Error(`full refresh violations ${fullViolations}`);
same("full_refresh", snapshot(secondConn), bundle.full_refresh.expected);
console.log("full_refresh ok", bundle.full_refresh.expected.fact_rows);
