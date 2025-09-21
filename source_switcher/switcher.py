from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
from copy import deepcopy
from rich import print
import uuid

from .client import MetabaseClient


def remap_parameter_mappings(parameter_mappings: List[Dict[str, Any]], field_mapping: Dict[int, int]) -> List[Dict[str, Any]]:
    """Remap field IDs in parameter_mappings targets."""
    new_mappings = []
    for pm in parameter_mappings:
        pm_copy = deepcopy(pm)
        target = pm_copy.get("target")
        if isinstance(target, list) and len(target) >= 2 and target[0] == "dimension":
            dimension = target[1]
            if isinstance(dimension, list) and len(dimension) >= 3 and dimension[0] == "field" and isinstance(dimension[1], int):
                old_field_id = dimension[1]
                new_field_id = field_mapping.get(old_field_id, old_field_id)
                pm_copy["target"][1][1] = new_field_id
        new_mappings.append(pm_copy)
    return new_mappings


def generate_param_id() -> str:
    """Generate a random parameter ID like '431e0d86'."""
    return uuid.uuid4().hex[:8]


def remap_param_fields(param_fields: Dict[str, List[Dict[str, Any]]], field_mapping: Dict[int, int]) -> Dict[str, List[Dict[str, Any]]]:
    """Remap field IDs in param_fields field objects."""
    new_param_fields = {}
    for param_id, fields in param_fields.items():
        new_fields = []
        for field in fields:
            field_copy = deepcopy(field)
            field_id = field_copy.get("id")
            if isinstance(field_id, int):
                new_field_id = field_mapping.get(field_id, field_id)
                field_copy["id"] = new_field_id
            # Also remap table_id if needed
            table_id = field_copy.get("table_id")
            if isinstance(table_id, int):
                # Assuming table_id is mapped similarly, but we might need a table mapping
                # For now, keep as is since tables are remapped in queries
                pass
            new_fields.append(field_copy)
        new_param_fields[param_id] = new_fields
    return new_param_fields


class MetadataIndex:
    def __init__(self, db_meta: Dict[str, Any]):
        self.tables_by_schema_and_name: Dict[Tuple[Optional[str], str], Dict[str, Any]] = {}
        self.fields_by_path: Dict[Tuple[Optional[str], str, str], Dict[str, Any]] = {}
        for table in db_meta.get("tables", []):
            schema = table.get("schema")
            name = table.get("name")
            self.tables_by_schema_and_name[(schema, name)] = table
            for field in table.get("fields", []):
                field_name = field.get("name")
                self.fields_by_path[(schema, name, field_name)] = field

    def find_field(self, schema: Optional[str], table: str, field: str) -> Optional[Dict[str, Any]]:
        return self.fields_by_path.get((schema, table, field))

    def find_table(self, schema: Optional[str], table: str) -> Optional[Dict[str, Any]]:
        return self.tables_by_schema_and_name.get((schema, table))


def extract_used_field_ids(card: Dict[str, Any]) -> List[int]:
    dataset_query = card.get("dataset_query") or {}
    query = dataset_query.get("query") or {}

    used: List[int] = []

    def visit(node: Any):
        if isinstance(node, dict):
            # Look for field refs like ["field", field_id, opts]
            if node.get("field") is not None and isinstance(node.get("field"), list):
                arr = node.get("field")
                if len(arr) >= 2 and isinstance(arr[1], int):
                    used.append(arr[1])
            for v in node.values():
                visit(v)
        elif isinstance(node, list):
            # MBQL expressions, e.g., ["field", 123, {...}] inside arrays
            if len(node) >= 2 and node[0] == "field" and isinstance(node[1], int):
                used.append(node[1])
            for v in node:
                visit(v)

    visit(query)

    # Deduplicate
    return sorted(set(used))


def build_field_path_map(client: MetabaseClient, field_ids: List[int]) -> Dict[int, Tuple[Optional[str], str, str]]:
    id_to_path: Dict[int, Tuple[Optional[str], str, str]] = {}
    for fid in field_ids:
        f = client.get_field(fid)
        table = f.get("table")
        schema = table.get("schema") if table else None
        table_name = table.get("name") if table else None
        field_name = f.get("name")
        if table_name and field_name:
            id_to_path[fid] = (schema, table_name, field_name)
    return id_to_path


def build_table_id_to_path(db_meta: Dict[str, Any]) -> Dict[int, Tuple[Optional[str], str]]:
    mapping: Dict[int, Tuple[Optional[str], str]] = {}
    for table in db_meta.get("tables", []):
        table_id = table.get("id")
        if table_id is not None:
            mapping[table_id] = (table.get("schema"), table.get("name"))
    return mapping


def collect_source_field_ids(dataset_query: Dict[str, Any]) -> List[int]:
    # Traverse the query object to gather any integers referenced under the key 'source-field'
    ids: List[int] = []
    query = (dataset_query or {}).get("query") or {}

    def visit(node: Any):
        if isinstance(node, dict):
            for k, v in node.items():
                if k == "source-field" and isinstance(v, int):
                    ids.append(v)
                visit(v)
        elif isinstance(node, list):
            for v in node:
                visit(v)

    visit(query)
    return sorted(set(ids))


def transform_dataset_query(
    dataset_query: Dict[str, Any],
    source_paths: Dict[int, Tuple[Optional[str], str, str]],
    target_index: MetadataIndex,
    target_db_id: int,
    source_table_id_to_path: Optional[Dict[int, Tuple[Optional[str], str]]] = None,
) -> Dict[str, Any]:
    dq = deepcopy(dataset_query)
    dq["database"] = target_db_id

    # Build reverse lookup: path -> target field id
    path_to_target_field_id: Dict[Tuple[Optional[str], str, str], int] = {}
    for path, _ in {(path, fid) for fid, path in source_paths.items()}:
        target_field = target_index.find_field(*path)
        if target_field:
            path_to_target_field_id[path] = target_field["id"]

    # Map top-level source table if present and we can resolve it
    query_obj = dq.get("query") or {}
    if source_table_id_to_path and isinstance(query_obj.get("source-table"), int):
        src_tid = query_obj.get("source-table")
        path = source_table_id_to_path.get(src_tid)
        if path:
            tgt_table = target_index.find_table(path[0], path[1])
            if tgt_table:
                query_obj["source-table"] = tgt_table.get("id")
                dq["query"] = query_obj

    # Build a direct mapping: source_field_id -> target_field_id for quick lookups
    source_field_id_to_target: Dict[int, int] = {}
    for fid, path in source_paths.items():
        tgt_field = target_index.find_field(*path)
        if tgt_field:
            source_field_id_to_target[fid] = tgt_field["id"]

    def replace(node: Any) -> Any:
        if isinstance(node, list):
            # Field reference shape: ["field", field_id, opts]
            if len(node) >= 2 and node[0] == "field" and isinstance(node[1], int):
                fid = node[1]
                path = source_paths.get(fid)
                if path and path in path_to_target_field_id:
                    # Also transform the trailing elements (e.g., options dict with source-field)
                    transformed_tail = [replace(v) for v in node[2:]]
                    return ["field", path_to_target_field_id[path]] + transformed_tail
            return [replace(v) for v in node]
        if isinstance(node, dict):
            new_obj: Dict[str, Any] = {}
            for k, v in node.items():
                if k == "source-field" and isinstance(v, int):
                    mapped = source_field_id_to_target.get(v)
                    new_obj[k] = mapped if mapped is not None else v
                else:
                    new_obj[k] = replace(v)
            return new_obj
        return node

    dq["query"] = replace(dq.get("query"))
    return dq


def switch_question(
    client: MetabaseClient,
    source_db_id: int,
    target_db_id: int,
    question_id: int,
    collection_id: Optional[object] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    # 1) Duplicate the question to preserve original
    original_card = client.fetch_card(question_id)
    cloned_card = client.duplicate_card(question_id)

    print(f"[cyan]Cloned original question to ID {cloned_card['id']}")

    # 2) Build metadata indices
    src_meta = client.list_tables(source_db_id)
    tgt_meta = client.list_tables(target_db_id)
    target_index = MetadataIndex(tgt_meta)
    src_table_id_to_path = build_table_id_to_path(src_meta)

    # 3) Extract used field IDs
    used_field_ids = extract_used_field_ids(original_card)
    # Also include any 'source-field' references in MBQL option objects
    extra_source_field_ids = collect_source_field_ids(original_card.get("dataset_query"))
    all_field_ids = sorted(set(used_field_ids + extra_source_field_ids))
    id_to_path = build_field_path_map(client, all_field_ids)

    # 4) Transform dataset_query
    new_dq = transform_dataset_query(
        dataset_query=original_card.get("dataset_query"),
        source_paths=id_to_path,
        target_index=target_index,
        target_db_id=target_db_id,
        source_table_id_to_path=src_table_id_to_path,
    )

    if dry_run:
        print("[yellow]Dry-run: would create new question with transformed dataset_query")
        return {"id": None, "dataset_query": new_dq}

    # 5) Create new question
    payload = {
        "name": f"{original_card.get('name')} (switched to DB {target_db_id})",
        "description": original_card.get("description"),
        "dataset_query": new_dq,
        "display": original_card.get("display"),
        "visualization_settings": original_card.get("visualization_settings") or {},
    }

    # Determine destination collection. For root, omit the key; otherwise pass numeric ID
    if collection_id is None:
        dest_collection = original_card.get("collection_id")
    elif collection_id == "root":
        dest_collection = None
    else:
        dest_collection = collection_id

    if dest_collection is not None:
        payload["collection_id"] = dest_collection

    new_card = client.create_card(payload)
    return new_card


def switch_dashboard(
    client: MetabaseClient,
    source_db_id: int,
    target_db_id: int,
    dashboard_id: int,
    collection_id: Optional[object] = None,
    dry_run: bool = False,
) -> Dict[str, Any]:
    # 1) Fetch the original dashboard
    original_dashboard = client.fetch_dashboard(dashboard_id)
    print(f"[cyan]Fetched dashboard '{original_dashboard.get('name')}' with {len(original_dashboard.get('dashcards', []))} cards and {len(original_dashboard.get('tabs', []))} tabs")

    # 2) Build metadata indices
    src_meta = client.list_tables(source_db_id)
    tgt_meta = client.list_tables(target_db_id)
    target_index = MetadataIndex(tgt_meta)

    # 3) Collect all field IDs from parameter_mappings across dashcards and param_fields
    all_field_ids: List[int] = []
    original_param_fields = original_dashboard.get("param_fields", {})

    # From dashcard parameter_mappings
    for dashcard in original_dashboard.get("dashcards", []):
        for pm in dashcard.get("parameter_mappings", []):
            target = pm.get("target")
            if isinstance(target, list) and len(target) >= 2 and target[0] == "dimension":
                dimension = target[1]
                if isinstance(dimension, list) and len(dimension) >= 3 and dimension[0] == "field" and isinstance(dimension[1], int):
                    all_field_ids.append(dimension[1])

    # From param_fields
    for fields in original_param_fields.values():
        for field in fields:
            field_id = field.get("id")
            if isinstance(field_id, int):
                all_field_ids.append(field_id)

    # Build field path map for remapping
    field_mapping = {}
    if all_field_ids:
        id_to_path = build_field_path_map(client, all_field_ids)
        for fid, path in id_to_path.items():
            tgt_field = target_index.find_field(*path)
            if tgt_field:
                field_mapping[fid] = tgt_field["id"]

    # 3.5) Build tab ID mapping (generate new IDs for tabs)
    original_tabs = original_dashboard.get("tabs", [])
    tab_id_mapping: Dict[int, str] = {}
    new_tabs = []
    for tab in original_tabs:
        old_id = tab.get("id")
        new_id = generate_param_id()
        tab_id_mapping[old_id] = new_id
        new_tab = deepcopy(tab)
        new_tab["id"] = new_id
        new_tabs.append(new_tab)

    # 4) Switch each question in dashcards
    card_id_mapping: Dict[int, int] = {}
    for dashcard in original_dashboard.get("dashcards", []):
        card_id = dashcard.get("card_id")
        if card_id:
            new_card = switch_question(
                client=client,
                source_db_id=source_db_id,
                target_db_id=target_db_id,
                question_id=card_id,
                collection_id=collection_id,
                dry_run=dry_run,
            )
            if not dry_run:
                card_id_mapping[card_id] = new_card["id"]

    if dry_run:
        print("[yellow]Dry-run: would create new dashboard with switched cards")
        return {"id": None}

    # 5) Create new dashboard
    dashboard_payload = {
        "name": f"{original_dashboard.get('name')} (switched to DB {target_db_id})",
        "description": original_dashboard.get("description"),
    }
    if collection_id is not None:
        dashboard_payload["collection_id"] = collection_id

    new_dashboard = client.create_dashboard(dashboard_payload)

    # 6) Prepare updated dashcards and parameters
    original_parameters = original_dashboard.get("parameters", [])
    param_id_mapping: Dict[str, str] = {}
    new_parameters = []
    new_param_fields = {}

    # Generate new IDs for parameters and build mappings
    for param in original_parameters:
        old_id = param.get("id")
        new_id = generate_param_id()
        param_id_mapping[old_id] = new_id
        param_copy = deepcopy(param)
        param_copy["id"] = new_id
        new_parameters.append(param_copy)

    # Remap param_fields
    if original_param_fields:
        remapped_param_fields = remap_param_fields(original_param_fields, field_mapping)
        for old_param_id, fields in remapped_param_fields.items():
            new_param_id = param_id_mapping.get(old_param_id, old_param_id)
            new_param_fields[new_param_id] = fields

    # Prepare updated dashcards
    updated_dashcards = []
    for dashcard in original_dashboard.get("dashcards", []):
        dashcard_copy = deepcopy(dashcard)
        old_card_id = dashcard_copy.get("card_id")
        if old_card_id and old_card_id in card_id_mapping:
            dashcard_copy["card_id"] = card_id_mapping[old_card_id]
        # Update dashboard_tab_id to new tab ID
        old_tab_id = dashcard_copy.get("dashboard_tab_id")
        if old_tab_id and old_tab_id in tab_id_mapping:
            dashcard_copy["dashboard_tab_id"] = tab_id_mapping[old_tab_id]
        # Remap parameter_mappings: update parameter_id, card_id, and targets
        pm_copy = remap_parameter_mappings(
            dashcard_copy.get("parameter_mappings", []), field_mapping
        )
        for pm in pm_copy:
            old_param_id = pm.get("parameter_id")
            new_param_id = param_id_mapping.get(old_param_id, old_param_id)
            pm["parameter_id"] = new_param_id
            # Update card_id in parameter_mapping to the new card_id
            old_pm_card_id = pm.get("card_id")
            if old_pm_card_id and old_pm_card_id in card_id_mapping:
                pm["card_id"] = card_id_mapping[old_pm_card_id]
        dashcard_copy["parameter_mappings"] = pm_copy
        updated_dashcards.append(dashcard_copy)

    # 7) Update dashboard with dashcards, parameters, param_fields, and tabs
    update_payload = {
        "dashcards": updated_dashcards,
        "parameters": new_parameters,
    }
    if new_param_fields:
        update_payload["param_fields"] = new_param_fields
    if new_tabs:
        update_payload["tabs"] = new_tabs

    client.update_dashboard(new_dashboard["id"], update_payload)

    print(f"[green]Created new dashboard with {len(updated_dashcards)} cards")
    return new_dashboard
