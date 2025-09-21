from __future__ import annotations
import requests
from typing import Dict, Any, Optional
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3 import disable_warnings

class MetabaseClient:
    def __init__(self, host: str, api_key: Optional[str] = None, session: Optional[requests.Session] = None, insecure: bool = False):
        self.host = host.rstrip('/')
        self.session = session or requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})
        # Use API key auth per docs: set X-API-KEY header
        if api_key:
            self.session.headers.update({"X-API-KEY": api_key})
        # Optionally disable TLS verification for self-signed endpoints
        if insecure:
            self.session.verify = False
            disable_warnings(InsecureRequestWarning)

    def get(self, path: str, **kwargs) -> requests.Response:
        return self.session.get(f"{self.host}{path}", **kwargs)

    def post(self, path: str, **kwargs) -> requests.Response:
        return self.session.post(f"{self.host}{path}", **kwargs)

    def put(self, path: str, **kwargs) -> requests.Response:
        return self.session.put(f"{self.host}{path}", **kwargs)

    def delete(self, path: str, **kwargs) -> requests.Response:
        return self.session.delete(f"{self.host}{path}", **kwargs)

    # Convenience wrappers for common API endpoints
    def fetch_card(self, card_id: int) -> Dict[str, Any]:
        r = self.get(f"/api/card/{card_id}")
        r.raise_for_status()
        return r.json()

    def duplicate_card(self, card_id: int, name_suffix: str = " (copy)") -> Dict[str, Any]:
        # There is no /clone endpoint; to duplicate, fetch the card and create a new one with a different name
        original = self.fetch_card(card_id)
        payload: Dict[str, Any] = {
            "name": f"{original.get('name')}{name_suffix}",
            "description": original.get("description"),
            "dataset_query": original.get("dataset_query"),
            "display": original.get("display"),
            "visualization_settings": original.get("visualization_settings") or {},
            "collection_id": original.get("collection_id"),
        }
        r = self.post("/api/card", json=payload)
        r.raise_for_status()
        return r.json()

    def create_card(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.post("/api/card", json=payload)
        try:
            r.raise_for_status()
        except requests.HTTPError as e:
            # Include server response body to help diagnose 4xx errors
            detail = None
            try:
                detail = r.json()
            except Exception:
                detail = r.text
            raise requests.HTTPError(f"{e} | response={detail}", response=r) from None
        return r.json()

    def list_tables(self, db_id: int) -> Dict[str, Any]:
        # /api/database/:id/metadata returns tables and fields
        r = self.get(f"/api/database/{db_id}/metadata")
        r.raise_for_status()
        return r.json()

    def get_field(self, field_id: int) -> Dict[str, Any]:
        r = self.get(f"/api/field/{field_id}")
        r.raise_for_status()
        return r.json()

    def list_fields_for_table(self, table_id: int) -> Dict[str, Any]:
        r = self.get(f"/api/table/{table_id}/query_metadata")
        r.raise_for_status()
        return r.json()

    # Dashboard methods
    def fetch_dashboard(self, dashboard_id: int) -> Dict[str, Any]:
        r = self.get(f"/api/dashboard/{dashboard_id}")
        r.raise_for_status()
        return r.json()

    def create_dashboard(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.post("/api/dashboard", json=payload)
        r.raise_for_status()
        return r.json()

    def update_dashboard(self, dashboard_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
        r = self.put(f"/api/dashboard/{dashboard_id}", json=payload)
        r.raise_for_status()
        return r.json()
