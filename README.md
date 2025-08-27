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

Run the CLI (single-command app; no subcommand):
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

Behavior:
- Duplicates the original question for safety (by POSTing a copy with a new name).
- Loads source/target metadata; remaps:
  - Top-level `query.source-table` to the target table with the same `schema.table`.
  - All MBQL field references `["field", <id>, ...]` to the corresponding field id in the target DB by `schema.table.field` path.
  - Any nested `source-field` integers within MBQL option objects.
- Creates a new question with the transformed `dataset_query` and original visualization settings.

Notes:
- `--collection-id root` saves to the root collection (omits `collection_id` in payload). To save to a specific collection, pass its numeric ID, e.g. `--collection-id 42`.
- `--insecure` sets TLS verification off for local/self-signed instances.
- Assumes the target DB has matching schema/table/field names. Unmatched fields won’t be remapped.
- Native SQL questions aren’t rewritten; this tool targets MBQL questions.
