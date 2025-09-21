## Metabase Question Source Switcher

Python CLI to duplicate a Metabase question, remap its MBQL dataset from one database to another by matching schema/table/field paths, and create a new question on the target database.

- Auth: API keys via `X-API-KEY`.
- Supports self-signed certs with `--insecure`.
- Supports saving to root collection by passing `--collection-id root`.

References: `Metabase API` docs: https://www.metabase.com/docs/latest/api and `API keys` docs: https://www.metabase.com/docs/latest/people-and-groups/api-keys

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

Set your API key (recommended):
```bash
export METABASE_API_KEY='your_key_here'
```

Run the CLI (single-command app; no subcommand). Provide exactly one of --question-id or --dashboard-id.

Switch a question:
```bash
python -m source_switcher.cli \
  --host https://metabase.example.com \
  --api-key "$METABASE_API_KEY" \
  --source-db-id 2 \
  --target-db-id 5 \
  --question-id 123 \
  --collection-id root \
  [--insecure] \
  [--dry-run]
```

Switch a dashboard (and all its questions):
```bash
python -m source_switcher.cli \
  --host https://metabase.example.com \
  --api-key "$METABASE_API_KEY" \
  --source-db-id 2 \
  --target-db-id 5 \
  --dashboard-id 456 \
  --collection-id root \
  [--insecure] \
  [--dry-run]
```

Behavior:
- **Question mode**: Duplicates the original question for safety (by POSTing a copy with a new name). Loads source/target metadata; remaps top-level `query.source-table`, MBQL field references `["field", <id>, ...]`, and nested `source-field` integers to corresponding IDs in the target DB by `schema.table.field` path. Creates a new question with transformed `dataset_query` and original visualization settings.
- **Dashboard mode**: Fetches the dashboard, switches each question in its dashcards to the target DB, remaps parameter_mappings field IDs, creates a new dashboard with the same structure (name, description, collection, parameters, dashcards with new card_ids and same positions/size_x/size_y), and updates it.

Notes:
- `--collection-id root` saves to the root collection (omits `collection_id` in payload). To save to a specific collection, pass its numeric ID, e.g. `--collection-id 42`.
- `--insecure` sets TLS verification off for local/self-signed instances.
- Assumes the target DB has matching schema/table/field names. Unmatched fields won’t be remapped.
- Native SQL questions aren’t rewritten; this tool targets MBQL questions.
