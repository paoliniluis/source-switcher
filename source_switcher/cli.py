from typer import Typer, Option
from rich import print
from .client import MetabaseClient
from .switcher import switch_question

app = Typer(add_completion=False)

@app.command()
def run(
    host: str = Option(..., help="Metabase host, e.g., https://metabase.example.com"),
    api_key: str = Option(..., help="Metabase API key (X-API-KEY)"),
    source_db_id: int = Option(..., help="Source database ID"),
    target_db_id: int = Option(..., help="Target database ID"),
    question_id: int = Option(..., help="Question (card) ID to duplicate and switch"),
    collection_id: str = Option(None, help='Optional collection to save new question into ("root" or numeric ID)'),
    insecure: bool = Option(False, help="Disable TLS verification (accept self-signed certs)"),
    dry_run: bool = Option(False, help="Show planned changes without creating the new question"),
):
    client = MetabaseClient(host=host, api_key=api_key, insecure=insecure)

    # Normalize collection_id: allow "root" or numeric strings
    normalized_collection: object
    if collection_id is None:
        normalized_collection = None
    elif collection_id == "root":
        normalized_collection = "root"
    else:
        try:
            normalized_collection = int(collection_id)
        except ValueError:
            raise ValueError("collection_id must be either 'root' or a numeric ID")

    new_card = switch_question(
        client=client,
        source_db_id=source_db_id,
        target_db_id=target_db_id,
        question_id=question_id,
        collection_id=normalized_collection,
        dry_run=dry_run,
    )

    if dry_run:
        print("[yellow]Dry run complete. No changes made.")
    else:
        print(f"[green]Created new question ID: {new_card['id']}")

if __name__ == "__main__":
    app()
