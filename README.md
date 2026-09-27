# dsh-web-research

A DeepSeek Harness Host plugin that ports the **Hermes-agent `web-research`
plugin** — a local/Open-Source-first web **search and extract pipeline** — as
three agent tools plus two `ctx.web` seam adapters, without reinventing
anything Hermes already solved.

| Piece | Hermes original | This bundle |
|---|---|---|
| Pipeline orchestration | `webresearch/core.py` | `python/webresearch/core.py` (ported verbatim) |
| Backends | Hister cache, SearXNG, wreq venv, Camofox | same, unchanged |
| Tool surface | `@hermes` tool handlers (`__init__.py`) | `index.js` → subprocess `cli.py` |
| `web_search`/`web_fetch` adaptation | `web_backend.py` (Hermes `ctx`) | seam providers in `index.js` |

The Python package is a faithful copy of the Hermes modules (only the log-file
location changed from `~/.hermes/logs` to `$DSH_HOME/logs`, default
`~/.dsh/logs`) plus a new `cli.py` subprocess entry. The Node plugin adds no
dependencies: tools are plain duck-typed definitions passed to
`ctx.tools.register`, and the pipeline runs as a subprocess.

## Tools

| Tool | Command | What it does |
|---|---|---|
| `web_recall` | `recall` | Search **only** the Hister cache; return full cleaned text for each hit (no live crawling). |
| `web_research` | `research` | SearXNG + Hister recall → dedupe → fetch top uncached pages via **wreq** (TLS-fingerprint) with **Camofox** browser fallback → ingest into Hister → return clean, citable text. |
| `web_read` | `read` | One URL, cache-first (max age 10 days), else fetch → ingest → clean text. |

All pipelines return JSON documents (results + `diagnostics` with per-stage
counts) and log structured lines to `~/.dsh/logs/web-research.log`.

## Configuration

Backend settings are never committed to the repo — they live in a gitignored
`.env` file next to the bundle. Copy the template and edit:

```sh
cp .env.example .env
```

Config precedence per field: **row `config` > process env > `.env` > Python
default**. The plugin loads `.env` (or the file named by
`DSH_WEBRESEARCH_ENV_FILE`) at startup and passes the pipeline its
`WEBRESEARCH_HISTER_URL`, `WEBRESEARCH_HISTER_TOKEN`, `WEBRESEARCH_SEARXNG_URL`,
`WEBRESEARCH_WREQ_PY`, `CAMOFOX_URL`, `CAMOFOX_API_KEY`,
`WEBRESEARCH_CAMOFOX_FALLBACK`, ... contract, so an existing Hermes environment
or a `.env` file works unchanged. To pin values in the profile instead of the
environment, add a profile-patch row (config replaces wholesale):

```yaml
- id: web-research
  name: '@local/dsh-web-research'
  config:
    histerUrl: http://127.0.0.1:4434
    histerToken: <token>
    searxngUrl: http://127.0.0.1:8888
    wreqPython: /path/to/wreq-venv/bin/python
    camofoxUrl: http://127.0.0.1:9377
    camofoxFallback: true
```

Full field list (each falls back to the env variable named in parentheses):

- `histerUrl` (`WEBRESEARCH_HISTER_URL`), `histerToken`
  (`WEBRESEARCH_HISTER_TOKEN`), `histerBin` (`WEBRESEARCH_HISTER_BIN`)
- `searxngUrl` (`WEBRESEARCH_SEARXNG_URL`)
- `wreqPython` (`WEBRESEARCH_WREQ_PY`), `workDir` (`WEBRESEARCH_WORK_DIR`)
- `camofoxUrl` (`CAMOFOX_URL`), `camofoxApiKey` (`CAMOFOX_API_KEY`),
  `camofoxFallback` (`WEBRESEARCH_CAMOFOX_FALLBACK`),
  `cookiesFile` (`WEBRESEARCH_COOKIES_FILE`)
- `refreshMaxDays` (`WEBRESEARCH_REFRESH_MAX_DAYS`)
- `blockedDomains`, `allowedDomains`, `rerankUrl`, `rerankModel`,
  `rerankToken`, `rerankMaxChars`, `searchTextChars`, `label`, `logsDir`
  (same names with `WEBRESEARCH_` prefix)
- `pythonBin` (also `DSH_WEBRESEARCH_PYTHON_BIN`), default `python3`
- `toolTimeoutMs`, default `240000`
- `registerWebBackend`, default `false` — see next section

## Built-in `web_search` / `web_fetch` routing (optional)

Set `registerWebBackend: true` (row config) to register two `ctx.web`
providers backed by the same pipeline:

- search provider id **`web-research-search`** — Hister-recall-first metadata
  discovery, topped up with SearXNG (no page bodies fetched; the Hermes
  `WebResearchSearchProvider` contract).
- fetch provider id **`web-research-extract`** — one URL through the full
  fetch → ingest → clean-text path, served as `kind: 'text'`.

`available()` is false unless `histerUrl` + `histerToken` resolve, so leaving
the flag on is harmless when the pipeline is unconfigured. To route the
built-in tools through it, override the `web` row (higher priority than the
bundle layer) in the profile patch:

```yaml
- id: web
  name: '@deepseek-ai/dsh-web'
  config:
    searchProvider: web-research-search
    fetchProvider: web-research-extract
```

> Note: with `web-fetch-http` also installed, both fetch providers would be
> *usable*, so `fetchProvider` must be set explicitly (the seam rejects an
> ambiguous auto-selection).

## Layout

```
index.js              Host plugin: tools, system-prompt guidance, seam adapters
cordis.patch.yml      bundle patch (one insert row; backend settings via .env)
python/webresearch/   ported pipeline + cli.py
locale/en.json        display metadata
```

## Development

- Pipeline only: `cd python/webresearch && python3 cli.py <cmd>` with one JSON
  object on stdin (`recall | research | read | search | search_meta | refresh_doc`).
- Host side: `node --check index.js`; see `/tmp/dsh-wr-smoke.mjs` for the stub-ctx
  smoke test used during development.
