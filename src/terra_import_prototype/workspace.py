"""Terra workspace naming.

Name = ``<user-local-part>_<infix>_<sanitized label>_YYYYMMDDHHMM`` (run start time,
minute-granular -- so a rerun after a transient failure does not collide on an existing workspace).
Any non-``[A-Za-z0-9_-]`` run is collapsed to a single ``_``.

The leading ``qc`` segment keeps these workspaces easy to identify, group and eventually delete: one
workspace is retained per run, so they accumulate. The infix also names the **import shape** that
produced the workspace, which is the whole point of this tool -- being able to tell at a glance
whether a workspace came from a single PFB or from an N-way manifest fan-out is what makes a pile of
result workspaces readable.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Optional

_NON_NAME_CHARS = re.compile(r"[^A-Za-z0-9_-]+")

#: Workspace-name infix per import shape (the leading ``qc`` keeps them grouped/identifiable).
AVRO_NAME_INFIX = "qc_bdc_avro"
MANIFEST_NAME_INFIX = "qc_bdc_manifest"
#: Several operator-supplied sources merged into one workspace. Its own infix because a workspace
#: fed by three sources is a different experiment from one fed by a single PFB or a single manifest,
#: and telling them apart at a glance is the whole reason the infix exists.
MULTI_NAME_INFIX = "qc_bdc_multi"

#: Terra caps workspace names; keep the generated name comfortably inside it. The timestamp and
#: infix are never truncated -- only the label is -- so uniqueness and identifiability survive.
MAX_NAME_LENGTH = 100


def sanitize(text: str) -> str:
    """Collapse any run of disallowed characters to a single underscore; trim leading/trailing ``_``."""
    return _NON_NAME_CHARS.sub("_", text).strip("_")


#: Import shape -> infix. An unrecognised shape falls back to the manifest infix, which is the
#: conservative reading: "more than one PFB may have landed here".
_INFIX_BY_KIND = {
    "avro": AVRO_NAME_INFIX,
    "manifest": MANIFEST_NAME_INFIX,
    "multi": MULTI_NAME_INFIX,
}


def infix_for(kind: str) -> str:
    """The workspace-name infix for an import shape (``avro`` / ``manifest`` / ``multi``)."""
    return _INFIX_BY_KIND.get(kind, MANIFEST_NAME_INFIX)


def workspace_name(
    user_email: str,
    label: str,
    when: Optional[datetime] = None,
    *,
    infix: str = AVRO_NAME_INFIX,
    max_length: int = MAX_NAME_LENGTH,
) -> str:
    """Build the per-run workspace name.

    ``label`` describes the import source -- typically the export's filename. It is truncated rather
    than the timestamp, because two runs a minute apart must produce different names and a run's
    shape must stay readable; a shortened label only costs a little context.
    """
    when = when or datetime.now()
    user_local_part = sanitize(user_email.split("@", 1)[0])
    timestamp = when.strftime("%Y%m%d%H%M")
    fixed = f"{user_local_part}_{infix}__{timestamp}"  # the parts that must never be trimmed
    budget = max(0, max_length - len(fixed))
    return f"{user_local_part}_{infix}_{sanitize(label)[:budget]}_{timestamp}".replace("__", "_")
