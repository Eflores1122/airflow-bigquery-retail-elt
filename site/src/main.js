import "@fontsource/atkinson-hyperlegible/400.css";
import "@fontsource/atkinson-hyperlegible/700.css";
import "@fontsource/ibm-plex-mono/400.css";
import benchmark from "../../benchmarks/results.json";
import "./styles.css";
import {
  boot,
  getBundle,
  getManifest,
  getVersion,
  measure,
  resetWarehouse,
  runEditorSql,
  runFullRefresh,
  runTask,
  taskNames,
} from "./engine.js";
import { applyStaticCopy, getLang, initLang, setLang, t } from "./i18n.js";

const TASKS = taskNames();
const DAY_IDS = [
  "inc-2026-03-02",
  "inc-2026-03-03",
  "inc-2026-03-04",
  "inc-2026-03-05",
  "inc-2026-03-06",
];
const reduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

const statusNode = document.querySelector("#status");
const errorNode = document.querySelector("#run-error");
const nextButton = document.querySelector("#next");
const stepBox = document.querySelector("#step");
const replayButton = document.querySelector("#replay");
const lookbackButton = document.querySelector("#lookback");
const correctionButton = document.querySelector("#correction");
const resetButton = document.querySelector("#reset");
const fullButton = document.querySelector("#full");
const sqlBox = document.querySelector("#sql-text");
const sqlError = document.querySelector("#sql-error");
const sqlResult = document.querySelector("#sql-result");

let busy = false;
let statusText = () => t("loading");
let introduced = false;
let dayIndex = 0;
let taskIndex = 0;
let active = null;
let lastSql = "";
let sqlDirty = false;
let lastMeasure = null;
let lastWindow = null;
let lastChecks = null;
const log = [];

function setStatus(next) {
  statusText = typeof next === "function" ? next : () => next;
  statusNode.textContent = statusText();
}

function delay(ms) {
  if (reduced) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function integer(value) {
  return new Intl.NumberFormat(getLang() === "en" ? "en-US" : "es-MX").format(value);
}

function showError(node, error) {
  const message = error && error.message ? error.message : String(error);
  node.hidden = false;
  node.textContent = error && error.sql ? `${message}\n${error.sql.slice(0, 500)}` : message;
}

function clearError(node) {
  node.hidden = true;
  node.textContent = "";
}

function renderDag() {
  const list = document.querySelector("#dag");
  list.replaceChildren();
  for (const task of TASKS) {
    const item = document.createElement("li");
    const index = TASKS.indexOf(task);
    if (active && active.task === task) item.classList.add("is-active");
    else if (active && index < TASKS.indexOf(active.task)) item.classList.add("is-done");
    else if (!active && taskIndex > 0 && index < taskIndex) item.classList.add("is-done");
    else if (!active && taskIndex === 0 && dayIndex > 0) item.classList.add("is-done");
    const id = document.createElement("span");
    id.className = "task-id";
    id.textContent = task;
    const note = document.createElement("span");
    note.className = "task-note";
    note.textContent = t(`task-${task}`);
    item.append(id, note);
    list.append(item);
  }
}

function renderLog() {
  const list = document.querySelector("#log");
  list.replaceChildren();
  if (!log.length) {
    const item = document.createElement("li");
    item.textContent = t("log-empty");
    list.append(item);
    return;
  }
  for (const entry of log) {
    const item = document.createElement("li");
    const run = getBundle().runs[entry.runId];
    const comparable = entry.task === "publish_marts" || entry.task === "data_quality";
    const expected = run.expected;
    const matched =
      comparable && entry.factRows === expected.fact_rows && entry.cents === expected.cents;
    const matchText = !comparable
      ? ""
      : matched
        ? getLang() === "en"
          ? " Matches sql.json."
          : " Coincide con sql.json."
        : getLang() === "en"
          ? ` sql.json has ${integer(expected.fact_rows)} rows and ${integer(expected.cents)} centavos.`
          : ` sql.json guarda ${integer(expected.fact_rows)} filas y ${integer(expected.cents)} centavos.`;
    item.textContent =
      getLang() === "en"
        ? `${entry.runId}, ${entry.task}: ${integer(entry.factRows)} fact rows, ${integer(entry.windowRows)} in the window, ${entry.elapsedMs} ms.${matchText}`
        : `${entry.runId}, ${entry.task}: ${integer(entry.factRows)} filas en el hecho, ${integer(entry.windowRows)} en la ventana, ${entry.elapsedMs} ms.${matchText}`;
    list.append(item);
  }
}

function renderDays(state) {
  const body = document.querySelector("#by-day tbody");
  body.replaceChildren();
  const rows = state ? state.byDay : [];
  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 4;
    cell.textContent = t("by-day-empty");
    row.append(cell);
    body.append(row);
    return;
  }
  const max = Math.max(...rows.map((row) => Number(row.rows)));
  const windowSet = new Set(lastWindow || []);
  for (const item of rows) {
    const row = document.createElement("tr");
    if (windowSet.has(String(item.order_date))) row.classList.add("is-window");
    const date = document.createElement("td");
    date.textContent = String(item.order_date);
    const count = document.createElement("td");
    count.textContent = integer(Number(item.rows));
    const cents = document.createElement("td");
    cents.textContent = integer(Number(item.cents));
    const scale = document.createElement("td");
    const bar = document.createElement("span");
    bar.className = "bar";
    bar.style.width = `${Math.max(4, Math.round((Number(item.rows) / max) * 100))}%`;
    bar.setAttribute("aria-hidden", "true");
    scale.append(bar);
    row.append(date, count, cents, scale);
    body.append(row);
  }
}

function renderFact(state) {
  const caption = document.querySelector("#fact-caption");
  const body = document.querySelector("#fact tbody");
  body.replaceChildren();
  if (!state || state.factRows === 0) {
    caption.textContent = t("fact-caption-empty");
    return;
  }
  caption.textContent = t("fact-caption", {
    fact: integer(state.factRows),
    window: integer(state.windowRows),
    outside: integer(state.outsideRows),
    start: lastWindow ? lastWindow[0] : "",
    end: lastWindow ? lastWindow[lastWindow.length - 1] : "",
    cents: integer(state.cents),
  });
  for (const item of state.sample) {
    const row = document.createElement("tr");
    for (const key of [
      "order_line_id",
      "order_date",
      "line_type",
      "status",
      "channel",
      "category",
      "quantity",
      "line_amount_centavos",
      "store_id",
    ]) {
      const cell = document.createElement("td");
      const value = item[key];
      cell.textContent =
        key === "quantity" || key === "line_amount_centavos" ? integer(Number(value)) : String(value);
      if (key === "order_line_id") cell.className = "mono";
      row.append(cell);
    }
    body.append(row);
  }
}

function renderCategories(state) {
  const body = document.querySelector("#categories tbody");
  body.replaceChildren();
  const rows = state ? state.categories : [];
  if (!rows.length) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 3;
    cell.textContent = t("cat-empty");
    row.append(cell);
    body.append(row);
    return;
  }
  for (const item of rows) {
    const row = document.createElement("tr");
    for (const key of ["category", "rows", "completed_centavos"]) {
      const cell = document.createElement("td");
      cell.textContent = key === "category" ? String(item[key]) : integer(Number(item[key]));
      row.append(cell);
    }
    body.append(row);
  }
}

function renderQuality() {
  const summary = document.querySelector("#quality-summary");
  const list = document.querySelector("#quality-list");
  list.replaceChildren();
  if (!lastChecks) {
    summary.textContent = t("quality-pending");
    return;
  }
  const violations = lastChecks.reduce((sum, check) => sum + check.violations, 0);
  summary.textContent = t("quality-summary", {
    checks: lastChecks.length,
    violations,
  });
  for (const check of lastChecks) {
    const item = document.createElement("li");
    item.textContent = `${check.name}: ${check.violations}`;
    list.append(item);
  }
}

function renderArch() {
  const root = document.querySelector("#arch");
  root.replaceChildren();
  const keys = ["node-gen", "node-land", "node-raw", "node-stg", "node-dims", "node-fact", "node-dq"];
  keys.forEach((key, index) => {
    if (index > 0) {
      const arrow = document.createElement("span");
      arrow.className = "arch-arrow";
      arrow.setAttribute("aria-hidden", "true");
      root.append(arrow);
    }
    const node = document.createElement("span");
    node.className = "arch-node";
    node.textContent = t(key);
    root.append(node);
  });
}

function renderStrategies() {
  const body = document.querySelector("#strategies-body");
  body.replaceChildren();
  const rows = [
    ["row-files", "cell-files-full", "cell-files-inc"],
    ["row-publish", "cell-publish-full", "cell-publish-inc"],
    ["row-outside", "cell-outside-full", "cell-outside-inc"],
    ["row-cost", "cell-cost-full", "cell-cost-inc"],
  ];
  for (const [topic, full, inc] of rows) {
    const row = document.createElement("tr");
    for (const key of [topic, full, inc]) {
      const cell = document.createElement("td");
      cell.textContent = t(key);
      row.append(cell);
    }
    body.append(row);
  }
}

function renderBenchmark() {
  const source = document.querySelector("#bench-source");
  const machine = benchmark.machine;
  const data = benchmark.dataset;
  source.textContent =
    getLang() === "en"
      ? `${machine.platform}, Python ${machine.python}, DuckDB ${machine.duckdb}, ${machine.cpu_count} CPUs. Measured ${benchmark.measured_at_utc}. Dataset ${data.start} to ${data.end}, ${integer(data.fact_order_lines)} fact rows, ${integer(data.products)} products, ${integer(data.stores)} stores, ${integer(data.customers)} customers, seed ${data.seed}. Median of ${data.iterations} runs after ${data.warmup} warmup.`
      : `${machine.platform}, Python ${machine.python}, DuckDB ${machine.duckdb}, ${machine.cpu_count} CPUs. Medido ${benchmark.measured_at_utc}. Dataset del ${data.start} al ${data.end}, ${integer(data.fact_order_lines)} filas de hecho, ${integer(data.products)} productos, ${integer(data.stores)} tiendas, ${integer(data.customers)} clientes, semilla ${data.seed}. Mediana de ${data.iterations} corridas tras ${data.warmup} calentamiento.`;
  document.querySelector("#bench-disclaimer").textContent = benchmark.disclaimer;
  const body = document.querySelector("#benchmark tbody");
  body.replaceChildren();
  for (const [name, scenario] of Object.entries(benchmark.scenarios)) {
    const row = document.createElement("tr");
    const cells = [
      t(`scenario-${name}`),
      scenario.seconds_median,
      scenario.load_and_stage_seconds_median,
      scenario.publish_seconds_median,
      scenario.quality_seconds_median,
      scenario.order_line_files_read,
    ];
    cells.forEach((value, index) => {
      const cell = document.createElement("td");
      cell.textContent = index === 0 ? String(value) : String(value);
      row.append(cell);
    });
    body.append(row);
  }
  const full = benchmark.scenarios.full_refresh_all_history;
  const one = benchmark.scenarios.incremental_lookback_zero;
  const two = benchmark.scenarios.incremental_lookback_one_day;
  const totalRatio = (full.seconds_median / one.seconds_median).toFixed(2);
  const lookRatio = (full.seconds_median / two.seconds_median).toFixed(2);
  const fullWork = full.load_and_stage_seconds_median + full.publish_seconds_median;
  const oneWork = one.load_and_stage_seconds_median + one.publish_seconds_median;
  const twoWork = two.load_and_stage_seconds_median + two.publish_seconds_median;
  const workOne = (fullWork / oneWork).toFixed(2);
  const workTwo = (fullWork / twoWork).toFixed(2);
  document.querySelector("#ratios").textContent =
    getLang() === "en"
      ? `Median total, full refresh / one partition: ${totalRatio}x. Full refresh / lookback 1: ${lookRatio}x. The same ratios for load, stage, and publish, excluding quality: ${workOne}x and ${workTwo}x.`
      : `Total mediano, full refresh / una partición: ${totalRatio}x. Full refresh / lookback 1: ${lookRatio}x. Las mismas razones para carga, stage y publicación, sin los chequeos: ${workOne}x y ${workTwo}x.`;
}

function renderSample() {
  const manifest = getManifest();
  const generator = manifest.generator;
  const correction = manifest.correction;
  document.querySelector("#sample-note").textContent =
    getLang() === "en"
      ? `Generated by ${generator.module}, seed ${generator.seed}, ${generator.products} products, ${generator.stores} stores, ${generator.customers} customers, ${generator.order_lines_per_day} base order lines per day, ${generator.start} to ${generator.end}, weekday volume on. dim_date has ${integer(manifest.dim_date_rows)} rows, ${manifest.dim_date_span[0]} to ${manifest.dim_date_span[1]}. The 4 March correction changes ${correction.updated_order_line_id} from quantity ${correction.quantity_before} to ${correction.quantity_after} and drops ${correction.dropped_order_line_id}. Python DuckDB ${manifest.duckdb_python} measured the expected counts in sql.json.`
      : `Generado con ${generator.module}, semilla ${generator.seed}, ${generator.products} productos, ${generator.stores} tiendas, ${generator.customers} clientes, ${generator.order_lines_per_day} líneas base por día, del ${generator.start} al ${generator.end}, con volumen por día de semana. dim_date tiene ${integer(manifest.dim_date_rows)} filas, del ${manifest.dim_date_span[0]} al ${manifest.dim_date_span[1]}. La corrección del 4 de marzo cambia ${correction.updated_order_line_id} de cantidad ${correction.quantity_before} a ${correction.quantity_after} y quita ${correction.dropped_order_line_id}. DuckDB ${manifest.duckdb_python} de Python midió los conteos esperados en sql.json.`;
  document.querySelector("#engine-note").textContent =
    getLang() === "en"
      ? `This page is running ${getVersion()}. That build is DuckDB-WASM 1.32.0. The repository warehouse and the benchmark use DuckDB ${manifest.duckdb_python}. The statement text is the DuckDB dialect from sql_builder.py.`
      : `Esta página está corriendo ${getVersion()}. Esa build es DuckDB-WASM 1.32.0. El almacén del repositorio y el benchmark usan DuckDB ${manifest.duckdb_python}. El texto de las sentencias es el dialecto DuckDB de sql_builder.py.`;
}

function renderControls() {
  const daysDone = dayIndex >= DAY_IDS.length && taskIndex === 0;
  const midDay = taskIndex > 0;
  nextButton.disabled = busy || !introduced || daysDone;
  if (stepBox.checked && !daysDone) nextButton.textContent = t("next-task");
  else if (taskIndex > 0) nextButton.textContent = t("next-finish");
  else if (dayIndex === 0) nextButton.textContent = t("next-first");
  else if (!daysDone) nextButton.textContent = t("next-day");
  else nextButton.textContent = t("next-done");
  const sideReady = daysDone && !busy;
  for (const button of [replayButton, lookbackButton, correctionButton]) {
    button.hidden = !introduced || midDay;
    button.disabled = !sideReady;
  }
  replayButton.textContent = t("replay");
  lookbackButton.textContent = t("lookback");
  correctionButton.textContent = t("correction");
  resetButton.disabled = busy || !introduced;
  fullButton.disabled = busy || !introduced;
  document.querySelector("#sql-run").disabled = busy || !introduced;
}

function renderAll() {
  applyStaticCopy();
  statusNode.textContent = statusText();
  renderDag();
  renderLog();
  renderDays(lastMeasure);
  renderFact(lastMeasure);
  renderCategories(lastMeasure);
  renderQuality();
  renderArch();
  renderStrategies();
  renderBenchmark();
  if (getManifest()) renderSample();
  renderControls();
}

function setSql(text) {
  if (!sqlDirty) sqlBox.value = text;
  lastSql = text;
}

async function refreshMeasure(windowDates) {
  lastWindow = windowDates;
  lastMeasure = await measure(windowDates);
  renderDays(lastMeasure);
  renderFact(lastMeasure);
  renderCategories(lastMeasure);
}

async function executeTask(runId) {
  const task = TASKS[taskIndex];
  active = { runId, task };
  renderDag();
  setStatus(
    () => (getLang() === "en" ? `Running ${runId}, ${task}.` : `Corriendo ${runId}, ${task}.`),
  );
  const outcome = await runTask(runId, task);
  const run = getBundle().runs[runId];
  const state = await measure(run.window);
  lastWindow = run.window;
  lastMeasure = state;
  if (outcome.checks) lastChecks = outcome.checks;
  log.push({
    runId,
    task,
    factRows: state.factRows,
    cents: state.cents,
    windowRows: state.windowRows,
    elapsedMs: outcome.elapsedMs,
  });
  setSql(outcome.sql);
  renderAll();
  taskIndex += 1;
  if (taskIndex >= TASKS.length) {
    taskIndex = 0;
    if (DAY_IDS[dayIndex] === runId) dayIndex += 1;
  }
}

async function runGuided() {
  const runId = DAY_IDS[dayIndex];
  if (!runId) return;
  if (stepBox.checked) {
    await executeTask(runId);
    return;
  }
  const stop = TASKS.length;
  while (taskIndex < stop && DAY_IDS[dayIndex] === runId) {
    await executeTask(runId);
    if (taskIndex !== 0) await delay(160);
  }
}

async function runSide(runId) {
  taskIndex = 0;
  for (const task of TASKS) {
    active = { runId, task };
    renderDag();
    setStatus(
      () => (getLang() === "en" ? `Running ${runId}, ${task}.` : `Corriendo ${runId}, ${task}.`),
    );
    const outcome = await runTask(runId, task);
    const run = getBundle().runs[runId];
    lastMeasure = await measure(run.window);
    lastWindow = run.window;
    if (outcome.checks) lastChecks = outcome.checks;
    log.push({
      runId,
      task,
      factRows: lastMeasure.factRows,
      cents: lastMeasure.cents,
      windowRows: lastMeasure.windowRows,
      elapsedMs: outcome.elapsedMs,
    });
    setSql(outcome.sql);
    renderAll();
    await delay(120);
  }
  taskIndex = 0;
  active = null;
  if (lastMeasure) {
    setStatus(
      () =>
        getLang() === "en"
          ? `${runId}: ${integer(lastMeasure.factRows)} fact rows, ${integer(lastMeasure.windowRows)} inside the window.`
          : `${runId}: ${integer(lastMeasure.factRows)} filas en el hecho, ${integer(lastMeasure.windowRows)} dentro de la ventana.`,
    );
  }
}

async function withLock(work) {
  if (busy) return;
  busy = true;
  clearError(errorNode);
  renderControls();
  try {
    await work();
  } catch (error) {
    showError(errorNode, error);
  } finally {
    busy = false;
    active = null;
    renderAll();
  }
}

function renderSqlResult(result) {
  sqlResult.replaceChildren();
  const note = document.createElement("p");
  if (!result.columns.length) {
    note.textContent = t("sql-empty");
    sqlResult.append(note);
    return;
  }
  note.textContent =
    result.rows.length > 50
      ? t("sql-truncated", { count: result.rows.length })
      : t("sql-rows", { count: result.rows.length });
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  const table = document.createElement("table");
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  for (const column of result.columns) {
    const cell = document.createElement("th");
    cell.textContent = column;
    headRow.append(cell);
  }
  head.append(headRow);
  const body = document.createElement("tbody");
  for (const row of result.rows.slice(0, 50)) {
    const line = document.createElement("tr");
    for (const column of result.columns) {
      const cell = document.createElement("td");
      const value = row[column];
      cell.textContent = value == null ? "" : String(value);
      line.append(cell);
    }
    body.append(line);
  }
  table.append(head, body);
  wrap.append(table);
  sqlResult.append(note, wrap);
}

function renderFull(result) {
  const expected = getBundle().full_refresh.expected;
  const host = document.querySelector("#full-result");
  host.replaceChildren();
  const wrap = document.createElement("div");
  wrap.className = "table-wrap";
  const table = document.createElement("table");
  const caption = document.createElement("caption");
  caption.textContent = t("full-result-caption");
  const head = document.createElement("thead");
  const headRow = document.createElement("tr");
  for (const key of ["full-metric", "full-browser", "full-python"]) {
    const cell = document.createElement("th");
    cell.textContent = t(key);
    headRow.append(cell);
  }
  head.append(headRow);
  const body = document.createElement("tbody");
  const rows = [
    [t("full-rows"), result.factRows, expected.fact_rows],
    [t("full-cents"), result.cents, expected.cents],
    [t("full-quality"), result.violations, 0],
  ];
  for (const [label, browser, python] of rows) {
    const row = document.createElement("tr");
    for (const value of [label, integer(browser), integer(python)]) {
      const cell = document.createElement("td");
      cell.textContent = String(value);
      row.append(cell);
    }
    body.append(row);
  }
  table.append(caption, head, body);
  wrap.append(table);
  const note = document.createElement("p");
  const matched =
    result.factRows === expected.fact_rows &&
    result.cents === expected.cents &&
    result.violations === 0;
  note.textContent = `${matched ? t("full-match") : t("full-mismatch")} ${result.elapsedMs} ms, ${result.version}.`;
  host.append(wrap, note);
}

async function start() {
  initLang();
  renderAll();
  try {
    const opened = await boot((phase) => {
      setStatus(() => (phase === "boot" ? t("booting") : t("loading")));
    });
    introduced = true;
    const manifest = opened.manifest;
    setStatus(() =>
      t("boot-ready", {
        dim: integer(manifest.dim_date_rows),
        start: manifest.dim_date_span[0],
        end: manifest.dim_date_span[1],
        version: opened.version,
      }),
    );
    await refreshMeasure(null);
    setSql(getBundle().runs[DAY_IDS[0]].tasks.resolve_window.join(";\n\n"));
    renderAll();
    await withLock(async () => {
      await runGuided();
      setStatus(
        () =>
          getLang() === "en"
            ? `2026-03-02 is in the fact. ${integer(lastMeasure.factRows)} rows measured in this browser.`
            : `2026-03-02 ya está en el hecho. ${integer(lastMeasure.factRows)} filas medidas en este navegador.`,
      );
    });
  } catch (error) {
    showError(errorNode, error);
    setStatus(() =>
      getLang() === "en" ? "DuckDB-WASM did not start." : "DuckDB-WASM no arrancó.",
    );
  }
}

nextButton.addEventListener("click", () => {
  withLock(async () => {
    await runGuided();
    if (lastMeasure) {
      setStatus(
        () =>
          getLang() === "en"
            ? `${integer(lastMeasure.factRows)} fact rows measured in this browser.`
            : `${integer(lastMeasure.factRows)} filas medidas en este navegador.`,
      );
    }
  });
});

for (const [button, runId] of [
  [replayButton, "replay-2026-03-06"],
  [lookbackButton, "lookback-2026-03-06"],
  [correctionButton, "correction-2026-03-04"],
]) {
  button.addEventListener("click", () => {
    withLock(async () => {
      await runSide(runId);
    });
  });
}

resetButton.addEventListener("click", () => {
  withLock(async () => {
    await resetWarehouse();
    dayIndex = 0;
    taskIndex = 0;
    log.length = 0;
    lastChecks = null;
    lastMeasure = await measure(null);
    lastWindow = null;
    sqlDirty = false;
    setSql(getBundle().runs[DAY_IDS[0]].tasks.resolve_window.join(";\n\n"));
    setStatus(() =>
      t("boot-ready", {
        dim: integer(getManifest().dim_date_rows),
        start: getManifest().dim_date_span[0],
        end: getManifest().dim_date_span[1],
        version: getVersion(),
      }),
    );
  });
});

fullButton.addEventListener("click", () => {
  withLock(async () => {
    setStatus(() => t("full-working"));
    const result = await runFullRefresh();
    renderFull(result);
    setStatus(
      () =>
        getLang() === "en"
          ? `Full refresh measured ${integer(result.factRows)} rows in a second database.`
          : `El full refresh midió ${integer(result.factRows)} filas en una segunda base.`,
    );
  });
});

document.querySelector("#sql-run").addEventListener("click", () => {
  withLock(async () => {
    clearError(sqlError);
    try {
      const result = await runEditorSql(sqlBox.value);
      renderSqlResult(result);
      lastMeasure = await measure(lastWindow);
      renderDays(lastMeasure);
      renderFact(lastMeasure);
      renderCategories(lastMeasure);
    } catch (error) {
      showError(sqlError, error);
    }
  });
});

document.querySelector("#sql-restore").addEventListener("click", () => {
  sqlDirty = false;
  sqlBox.value = lastSql;
  clearError(sqlError);
});

sqlBox.addEventListener("input", () => {
  sqlDirty = true;
});

document.querySelector("#lang").addEventListener("click", () => {
  setLang(getLang() === "es" ? "en" : "es");
  renderAll();
});

stepBox.addEventListener("change", () => renderControls());

start();
