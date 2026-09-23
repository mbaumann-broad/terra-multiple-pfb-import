"""Sam client (Terra identity / authorization service).

Used by QC to read a TDR snapshot's authorization domain, to verify the workspace's auth domain
matches it. See docs/export_import_flow.md section 6.
"""

from __future__ import annotations

from .base import BaseClient


class SamClient(BaseClient):
    def snapshot_auth_domain(self, snapshot_id: str) -> list[str]:
        """GET the auth-domain group names protecting a TDR snapshot (``[]`` for open-access)."""
        resp = self.request("GET", f"/api/resources/v2/datasnapshot/{snapshot_id}/authDomain")
        return list(resp.json())
