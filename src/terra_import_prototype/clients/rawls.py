"""Rawls client: create the destination workspace, and read back its data tables.

Two jobs here. Before the import, create the workspace with an empty ``authorizationDomain`` -- the
import applies the real one for a controlled-access source, which is a privileged operation we
**verify** rather than perform. After the import, read the entity-type metadata: that is the whole
of this tool's QC (see ``qc.py``), because the question it answers is "did N fanned-out import jobs
actually put data in the workspace?" and not "does that data match a reference".
"""

from __future__ import annotations

from typing import Optional

from .base import BaseClient


class RawlsClient(BaseClient):
    def create_workspace(
        self,
        namespace: str,
        name: str,
        *,
        description: str = "",
        auth_domain: Optional[list[dict]] = None,
        bucket_location: str = "US-CENTRAL1",
        enhanced_bucket_logging: bool = True,
    ) -> dict:
        """POST /api/workspaces. ``authorizationDomain`` defaults to ``[]`` (the import applies it)."""
        body = {
            "namespace": namespace,
            "name": name,
            "authorizationDomain": list(auth_domain or []),
            "attributes": {"description": description},
            "copyFilesWithPrefix": "notebooks/",
            "bucketLocation": bucket_location,
            "enhancedBucketLogging": enhanced_bucket_logging,
            "addUsers": [],
        }
        return self.request("POST", "/api/workspaces", json_body=body).json()

    def get_workspace(
        self,
        namespace: str,
        name: str,
        fields: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> dict:
        params = {"fields": fields} if fields else None
        return self.request(
            "GET", f"/api/workspaces/{namespace}/{name}", params=params, timeout=timeout
        ).json()

    def entity_type_metadata(self, namespace: str, name: str) -> dict:
        """GET /api/workspaces/{ns}/{name}/entities.

        Returns ``{entityType: {"count": N, "attributeNames": [...], "idName": "..."}}`` -- the
        structural overview the post-import check reads. One call covers every table, so the check
        costs the same whether the fan-out imported one PFB or eighteen.
        """
        return self.request("GET", f"/api/workspaces/{namespace}/{name}/entities").json()


def workspace_auth_domains(workspace: dict) -> list[str]:
    """Extract the auth-domain group names from a workspace response.

    Used by the create-workspace adopt path: a workspace this tool just created must have **none**,
    because the import applies it afterwards. A non-empty domain on a workspace we are about to
    adopt means it is not the one we created.
    """
    ws = workspace.get("workspace", workspace)
    return [g["membersGroupName"] for g in (ws.get("authorizationDomain") or [])]
