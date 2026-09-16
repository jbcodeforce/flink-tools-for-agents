# Plan: VSCode Plugin for `flink-tools-for-agents`

## Overview

Build a developer-facing VSCode extension at `vscode-extension/` inside this repository.
The extension surfaces all CLI commands (`flink-sql-deploy`, `flink-sql-snapshot`,
`flink-sql-stream`, `flink-sql-manifest`, `flink-sql-cleanup`, `flink-sql-register`,
`flink-sql-migrate-dbt`, `sl-dbt`) through a command palette and form-based UI so
developers do not have to remember flags.

**Stack:** TypeScript + VSCode Extension API (`vscode.window.createWebviewPanel` for forms,
`vscode.window.createOutputChannel` for streaming, `vscode.SecretStorage` for credentials).  
**Subprocess model:** `child_process.spawn` calling `uv run <entry-point>` from the workspace root — no bundled Python.  
**Location:** `vscode-extension/` subfolder inside `flink-tools-for-agents`.

### Scope

**In-scope:**
- Form-based UI for every CLI sub-command across all three domains (Flink, Kafka, dbt)
- Credentials manager using VSCode SecretStorage + optional `.env` file path setting
- Real-time streaming output to a dedicated OutputChannel
- Structured results viewer (JSON/table webview) for snapshot, manifest list, cleanup list
- Confirmation dialogs with a dry-run preview before destructive operations (undeploy, drop, delete --permanent)
- Activity Bar panel + full Command Palette registration

**Out of scope:**
- Bundling Python or `uv` — both must be installed on the host machine
- Schema Registry schema editor or Avro/JSON syntax support
- CI/CD publishing to the VSCode Marketplace (that is a follow-on task)

---

## Sub-Tasks

---

### Sub-Task 1 — Scaffold the extension

**Status:** `[x] done`

**Intent**
Create the extension folder with all project boilerplate so the extension compiles,
activates, and is debuggable in VSCode without any features yet.

**Expected Outcomes**
- `vscode-extension/` exists with `package.json`, `tsconfig.json`, `esbuild.js`, `.vscodeignore`
- `src/extension.ts` exports `activate` and `deactivate`, registers one placeholder command
- `npm run compile` produces `dist/extension.js` with no TypeScript errors
- `F5` in VSCode launches the Extension Development Host without errors
- `.vscode/launch.json` and `tasks.json` configured for debugging

**Todo List**
1. Create `vscode-extension/` directory with `package.json` — set `name`, `displayName`,
   `publisher`, `engines.vscode`, `main: "./dist/extension"`, `activationEvents`, and
   an empty `contributes` block (commands, views, menus)
2. Add `devDependencies`: `@types/vscode`, `@types/node`, `typescript`, `esbuild`
3. Add `scripts`: `compile`, `watch`, `package` (vsce package)
4. Write `tsconfig.json` targeting `ES2020`, `module: commonjs`, `outDir: dist`, strict mode
5. Write `esbuild.js` bundling `src/extension.ts` → `dist/extension.js` as CommonJS external
   to `vscode`
6. Write `src/extension.ts` with `activate` (logs "flink-tools activated") and `deactivate`
7. Write `.vscode/launch.json` with "Run Extension" launch config
8. Write `.vscode/tasks.json` pre-launch build task
9. Write `.vscodeignore` excluding `src/`, `node_modules/`, `esbuild.js`
10. Run `npm install` and `npm run compile`; verify zero errors

**Relevant Context**
- VSCode Extension API version should match `engines.vscode: "^1.85.0"` (current LTS-stable)
- `esbuild` bundler is preferred over `webpack` for speed
- `child_process` and `path` are Node built-ins; no extra packages needed for subprocess

---

### Sub-Task 2 — Credentials Manager

**Status:** `[x] done`

**Intent**
Provide a secure, persistent way to store and retrieve the Flink, Kafka, and Schema Registry
environment variables. Credentials are stored in `vscode.SecretStorage` (OS keychain-backed).
A settings entry `flinkTools.envFilePath` lets users point to an existing `.env` file instead.

**Expected Outcomes**
- `src/credentials.ts` exports `CredentialsManager` class
- `setCredential(key, value)` and `getCredential(key)` persist to SecretStorage
- `buildEnv()` returns a `Record<string, string>` merged from SecretStorage + process env,
  with an optional `.env` file overlay when `flinkTools.envFilePath` is set
- A "Flink Tools: Configure Credentials" command opens a multi-step `QuickInput` wizard
  cycling through all required env var keys grouped by domain
- `package.json` declares `configuration` contribution with `flinkTools.envFilePath` string setting
- Credentials are never logged or included in error messages

**Todo List**
1. Write `src/credentials.ts`:
   - Define `FLINK_ENV_VARS`, `KAFKA_ENV_VARS`, `DBT_ENV_VARS` constant arrays listing every
     required key (sourced from the env var tables in `skills/flink-deploy/SKILL.md`,
     `skills/kafka/SKILL.md`)
   - Implement `CredentialsManager` with constructor taking `vscode.ExtensionContext`
   - `setCredential(key, value)` — stores via `context.secrets.store`
   - `getCredential(key)` — reads via `context.secrets.get`
   - `buildEnv()` — merges in order: `process.env` → SecretStorage values → `.env` file values
     (parse the `.env` file manually with a 10-line regex; no dotenv npm package needed)
2. Register "Flink Tools: Configure Credentials" command in `extension.ts`
3. Implement the multi-step `QuickInput` wizard: groups (Flink / Kafka / dbt), key prompt,
   masked input (`password: true`), skip-able steps
4. Add `flinkTools.envFilePath` to `contributes.configuration` in `package.json`
5. Export a singleton `getCredentialsManager()` for use by the executor

**Relevant Context**
- Full list of Flink env vars: `FLINK_API_KEY`, `FLINK_API_SECRET`, `FLINK_REST_ENDPOINT`,
  `FLINK_ENV_ID`, `FLINK_ORG_ID`, `FLINK_COMPUTE_POOL_ID`, `FLINK_DATABASE_NAME`,
  `CLOUD_PROVIDER`, `CLOUD_REGION`
- Full list of Kafka env vars: `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_API_KEY`,
  `SCHEMA_REGISTRY_API_SECRET`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_API_KEY`, `KAFKA_API_SECRET`
- `skills/flink-deploy/SKILL.md` and `skills/kafka/SKILL.md` are the authoritative source
- The CLI tools also accept `CONFLUENT_ENV_FILE` pointing to a `.env` file, but the VSCode
  extension manages this transparently via `buildEnv()`

---

### Sub-Task 3 — Command Executor

**Status:** `[x] done`

**Intent**
Create a reusable execution layer that spawns `uv run <entry-point>` subprocesses, streams
stdout/stderr to a shared OutputChannel, captures structured JSON output for results viewer,
handles cancellation, and signals the caller with exit code.

**Expected Outcomes**
- `src/executor.ts` exports `CommandExecutor` class
- `run(args, opts)` spawns `child_process.spawn('uv', ['run', ...args])` from workspace root
- Stdout/stderr lines stream in real-time to the "Flink Tools" OutputChannel
- `run()` returns a `Promise<ExecutionResult>` with `{ exitCode, stdout, stderr, cancelled }`
- Cancellation via `AbortController` sends `SIGTERM` and resolves with `cancelled: true`
- If stdout is valid JSON, `ExecutionResult.json` is populated
- `WorkspaceRootNotFoundError` is thrown when no workspace folder is open

**Todo List**
1. Write `src/executor.ts`:
   - `ExecutionResult` interface: `exitCode: number`, `stdout: string`, `stderr: string`,
     `cancelled: boolean`, `json?: unknown`
   - `CommandExecutor` constructor takes `OutputChannel` and `CredentialsManager`
   - `run(args: string[], opts?: { cwd?: string, cancellationToken?: vscode.CancellationToken })`
   - Detect workspace root from `vscode.workspace.workspaceFolders[0].uri.fsPath`
   - Spawn `uv run ...args` with `env` from `credentialsManager.buildEnv()` merged with current env
   - Pipe stdout/stderr chunks to OutputChannel line-by-line
   - Buffer full stdout; on process exit try `JSON.parse(stdout)` to populate `result.json`
   - Map `CancellationToken.onCancellationRequested` to `process.kill('SIGTERM')`
2. Wire `OutputChannel` creation in `extension.ts`: `vscode.window.createOutputChannel('Flink Tools')`
3. Export singleton `getExecutor()` for use by all command handlers

**Relevant Context**
- `uv` must be on PATH; show actionable error message if `uv` is not found (ENOENT)
- Long-running commands: `flink-sql-stream` and `flink-sql-deploy` may run for minutes
- JSON output commands: `flink-sql-snapshot --output json`, `flink-sql-manifest --dry-run`,
  `flink-sql-cleanup list --dry-run`, `flink-sql-register list --dry-run`
- Cancellation is especially important for `flink-sql-stream`

---

### Sub-Task 4 — Flink Commands UI

**Status:** `[x] done`

**Intent**
Build form-based webview panels for all four Flink entry points: `flink-sql-deploy`,
`flink-sql-snapshot`, `flink-sql-stream`, and `flink-sql-manifest`. Destructive sub-commands
(undeploy, drop-tables) run a dry-run preview first and require explicit confirmation.

**Expected Outcomes**
- Four webview panels, one per entry point, each with labelled inputs for every flag
- "Deploy" form: SQL dir picker, group input, "Deploy" button, "Undeploy" button (→ confirmation)
- "Snapshot" form: table/SQL radio, columns, where, limit, output format, run button
- "Stream" form: same fields as Snapshot plus max-rows, keep-statement checkbox, Cancel button
- "Manifest" form: SQL dir picker, prefix, dry-run checkbox, dbt checkbox, generate button
- "Undeploy" and "Drop Tables" show a confirmation modal before executing
- All forms show real-time output in the OutputChannel; Snapshot/Manifest results open ResultsViewer

**Todo List**
1. Create `src/panels/FlinkDeployPanel.ts` — webview panel for deploy/undeploy/drop-tables/groups
   - Folder picker for `--sql-dir`, text input for `--group`
   - "Deploy" button: runs `flink-sql-deploy --sql-dir <dir> deploy [--group <g>]`
   - "Undeploy" button: first runs `--dry-run` equivalent (groups list), then shows
     `vscode.window.showWarningMessage` confirmation, then runs undeploy
   - "Drop Tables" button: same confirmation gate before execute
2. Create `src/panels/FlinkSnapshotPanel.ts` — webview for `flink-sql-snapshot`
   - Radio buttons: "Table name" vs "SQL query", conditional text fields
   - Dropdowns for `--output` (table/json/csv), checkboxes for `--keep-statement`, `--as-dict`
   - On submit: run command, if output=json open ResultsViewer with returned rows
3. Create `src/panels/FlinkStreamPanel.ts` — webview for `flink-sql-stream`
   - Same fields as Snapshot plus `--max-rows`, `--timeout`
   - "Start" wires a CancellationTokenSource; "Stop" button cancels it
4. Create `src/panels/FlinkManifestPanel.ts` — webview for `flink-sql-manifest`
   - Folder picker for `--sql-dir`, checkbox `--dbt`, `--overwrite`, `--dry-run`
   - On success with `--dry-run`: open ResultsViewer with JSON manifest
5. Create `src/webview/formTemplate.ts` — shared HTML/CSS template function used by all panels
   (VS Code Codicon styles, consistent field layout, VSCode theme variables)
6. Register four commands in `package.json` `contributes.commands` and wire in `extension.ts`

**Relevant Context**
- `tools/flink/cc_deploy/deploy_flink_statements.py` — argparse CLI; sub-commands: deploy,
  undeploy, drop-tables, groups
- `tools/flink/cc_deploy/run_snapshot_query.py` and `run_streaming_query.py` — argparse CLIs
- `tools/flink/manifest/manifest_cli.py` — Typer CLI
- For `flink-sql-deploy`, `--sql-dir` is a workspace-relative path so workspace root auto-fill
  is helpful
- Groups sub-command output can be used to populate the group dropdown dynamically

---

### Sub-Task 5 — Kafka Commands UI

**Status:** `[x] done`

**Intent**
Build form-based webview panels for `flink-sql-register` (schema register/list/delete/debug-refs)
and `flink-sql-cleanup` (list/drop). Delete with `--permanent` and drop both require a dry-run
preview and confirmation dialog.

**Expected Outcomes**
- "Schema Registry" panel with a tab/section for each sub-command (register, list, debug-refs, delete)
- "Table Cleanup" panel with list and drop sections
- Dry-run preview shown in ResultsViewer before destructive operations (delete, drop)
- `--permanent` delete shows a second warning message

**Todo List**
1. Create `src/panels/KafkaRegisterPanel.ts` — webview for `flink-sql-register`
   - **Register** section: file picker for schema path, text input for `--subject`, dropdown
     `--type` (AVRO/JSON), run button
   - **List** section: file path for `--output`, dry-run checkbox, run button (opens ResultsViewer)
   - **Debug-refs** section: text input for SUBJECT, run button (output to OutputChannel)
   - **Delete** section: manifest path picker, dry-run checkbox, `--permanent` checkbox,
     `--resolve-references` checkbox; dry-run preview first, then confirmation modal before live delete
2. Create `src/panels/KafkaCleanupPanel.ts` — webview for `flink-sql-cleanup`
   - **List** section: output path, dry-run checkbox, include-internal checkbox, run button
     (opens ResultsViewer on success)
   - **Drop** section: manifest path picker, dry-run checkbox; dry-run preview first, then
     confirmation modal before live drop
3. Register two commands in `package.json` and wire in `extension.ts`

**Relevant Context**
- `tools/kafka/register_schema.py` — argparse CLI, sub-commands: register, list, debug-refs, delete
- `tools/kafka/table_cleanup.py` — argparse CLI, sub-commands: list, drop
- Env vars required: `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_API_KEY`, `SCHEMA_REGISTRY_API_SECRET`,
  and for cleanup: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_API_KEY`, `KAFKA_API_SECRET`, `FLINK_DATABASE_NAME`

---

### Sub-Task 6 — dbt Commands UI

**Status:** `[x] done`

**Intent**
Build form-based webview panels for `flink-sql-migrate-dbt` (migrate-one-file,
migrate-sl-folder) and `sl-dbt` (init, add-data-product, add-table, add-raw-topic).
All migration commands default to dry-run; a `--write` checkbox triggers the live run
with a confirmation step.

**Expected Outcomes**
- "Migrate DML to dbt" panel with two sections: migrate-one-file and migrate-sl-folder
- "dbt Project Scaffold" panel with init and add-* commands
- `--write` is not sent unless the user explicitly enables it and confirms
- Dry-run output opens in the OutputChannel (plain text diff/log)

**Todo List**
1. Create `src/panels/DbtMigratePanel.ts` — webview for `flink-sql-migrate-dbt`
   - **migrate-sl-folder** section: pipeline dir picker, dbt project dir picker,
     exclude-file multi-input, product name, `--write` checkbox (gated by confirmation),
     `--force` checkbox, run button
   - **migrate-one-file** section: statement file picker, target dir picker, ddl-file picker,
     model-name input, materialized dropdown, ref-table repeatable key=value input,
     `--write` checkbox (gated by confirmation), advanced options (dbt-project-dir, dbt-target,
     source-project-dir, source-name, no-sources, seed-name), run button
2. Create `src/panels/DbtScaffoldPanel.ts` — webview for `sl-dbt`
   - **init** section: project root picker, type dropdown, profile name, force checkbox, run button
   - **add-data-product** section: project root picker, name input, run button
   - **add-table** section: project root picker, name input, data-product name, table-type dropdown
   - **add-raw-topic** section: project root picker, topic name input, run button
3. Register two commands in `package.json` and wire in `extension.ts`

**Relevant Context**
- `tools/dbt/flink_dbt_migrate/migrate_dml_to_dbt.py` — Typer CLI
- `tools/dbt/sl_dbt.py` — Typer CLI
- Migration defaults to dry-run (no `--write` flag) so showing output without `--write` is safe
- dbt project directories must contain `dbt_project.yml`; folder picker should validate this

---

### Sub-Task 7 — Results Viewer

**Status:** `[x] done`

**Intent**
Implement a shared webview panel that renders structured JSON output as a table or raw JSON.
Used by Snapshot, Manifest (dry-run), Cleanup list, and Schema Registry list commands.

**Expected Outcomes**
- `src/panels/ResultsViewerPanel.ts` with a static `show(title, data)` factory method
- When `data` is an array of objects: renders an HTML table with sortable column headers
- When `data` is any other JSON: renders syntax-highlighted JSON using a `<pre>` block
- Panel is retained (reused) across calls with the same title rather than reopening
- VSCode theme CSS variables applied for consistent dark/light mode appearance

**Todo List**
1. Create `src/panels/ResultsViewerPanel.ts`:
   - Static `panels: Map<string, ResultsViewerPanel>` to reuse existing panels
   - `static show(title: string, data: unknown, context: vscode.ExtensionContext)` factory
   - Webview HTML: if `Array.isArray(data)` and all items are objects, render `<table>`;
     otherwise render `<pre>` with `JSON.stringify(data, null, 2)`
   - Table: `<thead>` from `Object.keys(data[0])`, `<tbody>` rows, CSS zebra striping,
     click-to-sort on column headers (plain JS, no framework)
   - Style using VSCode theme variables: `--vscode-editor-background`,
     `--vscode-editor-foreground`, `--vscode-list-hoverBackground`
2. Wire `ResultsViewerPanel.show()` calls into FlinkSnapshotPanel, FlinkManifestPanel,
   KafkaRegisterPanel (list), and KafkaCleanupPanel (list) after successful JSON result
3. Export `showResults(title, data, context)` convenience function from `src/panels/index.ts`

**Relevant Context**
- `flink-sql-snapshot --output json` returns an array of row objects
- `flink-sql-manifest --dry-run` returns a `deploy_manifest.json`-shaped object
- `flink-sql-cleanup list --dry-run` and `flink-sql-register list --dry-run` return JSON arrays

---

### Sub-Task 8 — Activity Bar + Command Palette

**Status:** `[x] done`

**Intent**
Wire the Activity Bar tree view so users can open any form panel from a sidebar,
and register all commands in the Command Palette under a "Flink Tools:" prefix.

**Expected Outcomes**
- "Flink Tools" Activity Bar icon (using a Codicon) with a tree view listing all domains and commands
- Clicking a tree item opens the corresponding webview panel
- All commands also accessible via `Ctrl+Shift+P` → "Flink Tools: ..."
- `package.json` `contributes` fully populated: `commands`, `views`, `viewsContainers`, `menus`
- README.md inside `vscode-extension/` documents install, configuration, and command reference

**Todo List**
1. Add `contributes.viewsContainers.activitybar` entry in `package.json` with a Codicon icon
   and `id: "flinkTools"`
2. Add `contributes.views.flinkTools` tree with one view `id: "flinkToolsCommands"`
3. Create `src/providers/FlinkToolsTreeProvider.ts` implementing `vscode.TreeDataProvider`
   - Top-level nodes: "Flink SQL", "Kafka / Schema Registry", "dbt"
   - Children: one node per command (Deploy, Snapshot, Stream, Manifest, Register, Cleanup,
     Migrate DML, Scaffold dbt)
   - Each leaf node has a `command` property that fires the corresponding registered command
4. Register tree view and provider in `extension.ts`
5. Populate `contributes.commands` in `package.json` with all 8+ commands under "Flink Tools:" prefix,
   each with a Codicon icon
6. Add `contributes.menus["view/title"]` entries for quick-access buttons in the tree view header
7. Write `vscode-extension/README.md` with: prerequisites (uv, Python), installation, credential
   setup steps, and a command reference table mapping UI labels to underlying CLI commands
8. Run `npm run compile` and verify zero TypeScript errors across all new source files

**Relevant Context**
- Codicons available via `$(codicon-name)` syntax in `TreeItem.iconPath` and command titles
- Suitable icons: `$(rocket)` for Flink deploy, `$(database)` for Kafka, `$(symbol-structure)` for dbt,
  `$(list-tree)` for manifest, `$(search)` for snapshot, `$(broadcast)` for stream
- `vscode.commands.registerCommand` in `extension.ts` wires command IDs to panel `.show()` calls
- The extension should have a single `activate()` that sets up CredentialsManager, Executor,
  TreeProvider, and all command registrations in sequence
