"""
matrix_adapter.py — PersonalStorageForge → MATRIX → harmony bus bridge.

ARCHITECTURE DECISION:
  PersonalStorageForge does NOT own the harmony bus publish logic.
  MATRIX owns it. PSF imports from MATRIX.

  This avoids the dual-maintenance problem where brain_bus_publisher.py
  was copied into both repos and drifted apart. The canonical transport
  lives in MATRIX/harmony_publisher_base.py.

  Import chain:
    PSF pipeline code
         ↓
    PSFPublisher (this file)
         ↓
    MATRIX/harmony_publisher_base.HarmonyPublisher
         ↓
    harmony-engine-protocol (ws://localhost:9002/harmony)
         ↓
    harmony_subscriber.py in the-brain

FALLBACK:
  If MATRIX is not on the path (MATRIX_PATH unset, MATRIX not installed),
  this file falls back to its own local HarmonyPublisher built from
  harmony_publisher_base if available, or a no-op stub. PSF never crashes
  because MATRIX isn’t available — it just loses the richer provenance.

ENVIRONMENT VARIABLES:
  MATRIX_PATH   Absolute path to the root of your MATRIX clone.
                Example: /Users/vinny/dev/MATRIX
                If unset, we try to find MATRIX as a sibling directory.
  HARMONY_WS_URL, HARMONY_TOKEN, HARMONY_FALLBACK  (see harmony_publisher_base)

USAGE:
  from matrix_adapter import PSFPublisher

  pub = PSFPublisher()
  pub.publish_file_indexed("/Users/vinny/Documents/report.pdf",
                           size_bytes=42000, tags=["work", "finance"])
  pub.publish_file_deduped(original="/foo/a.pdf", duplicate="/bar/b.pdf")
  pub.publish_storage_stats(total_files=14200, dedup_savings_mb=3400.0)
"""
from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Step 1: Locate and import from MATRIX
# ---------------------------------------------------------------------------

def _find_matrix_path() -> Path | None:
    """Try MATRIX_PATH env var, then sibling-directory heuristic."""
    env = os.environ.get("MATRIX_PATH")
    if env:
        p = Path(env)
        if p.exists() and (p / "harmony_publisher_base.py").exists():
            return p

    # Heuristic: look for MATRIX as sibling of this repo
    here = Path(__file__).resolve().parent
    for candidate in [
        here.parent / "MATRIX",
        here.parent.parent / "MATRIX",
        Path.home() / "dev" / "MATRIX",
        Path.home() / "MATRIX",
    ]:
        if candidate.exists() and (candidate / "harmony_publisher_base.py").exists():
            return candidate

    return None


_matrix_path = _find_matrix_path()
_HarmonyPublisher = None
_publish_artifact_promoted = None
_publish_kg_patch = None
_build_event = None

if _matrix_path is not None:
    if str(_matrix_path) not in sys.path:
        sys.path.insert(0, str(_matrix_path))
    try:
        from harmony_publisher_base import (  # type: ignore
            HarmonyPublisher as _HarmonyPublisher,
            publish_artifact_promoted as _publish_artifact_promoted,
            publish_kg_patch as _publish_kg_patch,
            build_event as _build_event,
        )
        log.info("[PSF matrix_adapter] Imported harmony_publisher_base from %s", _matrix_path)
    except ImportError as e:
        log.warning("[PSF matrix_adapter] Could not import from MATRIX: %s", e)
else:
    log.warning("[PSF matrix_adapter] MATRIX not found on path. Set MATRIX_PATH env var. "
                "Using no-op fallback.")


# ---------------------------------------------------------------------------
# Step 2: Fallback stub (if MATRIX unavailable)
# ---------------------------------------------------------------------------

class _NoOpPublisher:
    """Dropped-in when MATRIX is unavailable. Logs a warning, returns False."""

    def __init__(self, source: str):
        self.source = source

    def sync_publish(self, event_type: str, payload: dict | None = None,
                     run_id: str | None = None) -> bool:
        log.warning("[PSF matrix_adapter] MATRIX unavailable: event %s dropped", event_type)
        return False


def _make_publisher(source: str) -> Any:
    if _HarmonyPublisher is not None:
        return _HarmonyPublisher(source=source)
    return _NoOpPublisher(source=source)


# ---------------------------------------------------------------------------
# Step 3: PSFPublisher — storage-domain event wrappers
# ---------------------------------------------------------------------------

class PSFPublisher:
    """
    PersonalStorageForge’s publish API.

    All harmony bus events emitted by PSF go through this class.
    Storage-domain events are distinct from MATRIX media events:
    - PSF deals with files of all types (docs, code, configs, archives)
    - MATRIX deals with media assets (photos, videos, audio)
    Both converge in the-brain’s artifacts table and KG.
    """

    def __init__(self, source: str = "PersonalStorageForge"):
        self.source = source
        self._pub = _make_publisher(source)

    def publish_file_indexed(
        self,
        path: str,
        size_bytes: int = 0,
        file_type: str = "",
        tags: list[str] | None = None,
        run_id: str | None = None,
    ) -> bool:
        """
        Emit when PSF indexes (ingests/watches) a new file.
        Creates a kg_patch node for the file + an artifact_promoted event.
        """
        rid = run_id or f"psf-idx-{uuid.uuid4().hex[:8]}"
        name = Path(path).name
        size_mb = round(size_bytes / 1_048_576, 3) if size_bytes else 0.0

        # Artifact promoted event (written into brain.db artifacts table)
        if _publish_artifact_promoted is not None:
            try:
                _publish_artifact_promoted(
                    artifact_name=path,
                    status="indexed",
                    trace_id=rid,
                    notes=json.dumps({"size_mb": size_mb, "type": file_type,
                                      "tags": tags or []}),
                    source=self.source,
                    run_id=rid,
                )
            except Exception as e:
                log.debug("[PSF] artifact_promoted error: %s", e)

        # KG patch: add the file as a node
        node: dict[str, Any] = {
            "node_id": f"file:{path}",
            "node_type": "storage_file",
            "label": name,
            "properties": {
                "source": self.source,
                "run_id": rid,
                "size_mb": size_mb,
                "file_type": file_type,
                "tags": ",".join(tags or []),
            },
        }
        if _publish_kg_patch is not None:
            try:
                _publish_kg_patch(nodes=[node], source=self.source, run_id=rid)
            except Exception as e:
                log.debug("[PSF] kg_patch error: %s", e)
                return False
            return True

        return self._pub.sync_publish(
            "file_indexed",
            {"path": path, "size_mb": size_mb, "tags": tags or [], "file_type": file_type},
            rid,
        )

    def publish_file_deduped(
        self,
        original: str,
        duplicate: str,
        run_id: str | None = None,
    ) -> bool:
        """
        Emit when PSF detects and suppresses a duplicate file.
        Writes a KG edge duplicate_of between the two file nodes.
        """
        rid = run_id or f"psf-dedup-{uuid.uuid4().hex[:8]}"
        if _publish_kg_patch is not None:
            try:
                return _publish_kg_patch(
                    nodes=[
                        {"node_id": f"file:{original}", "node_type": "storage_file",
                         "label": Path(original).name},
                        {"node_id": f"file:{duplicate}", "node_type": "storage_file",
                         "label": Path(duplicate).name},
                    ],
                    edges=[{
                        "source_id": f"file:{duplicate}",
                        "target_id": f"file:{original}",
                        "relation": "duplicate_of",
                        "weight": 0.99,
                    }],
                    source=self.source,
                    run_id=rid,
                )
            except Exception as e:
                log.debug("[PSF] dedup kg_patch error: %s", e)

        return self._pub.sync_publish(
            "file_deduped",
            {"original": original, "duplicate": duplicate},
            rid,
        )

    def publish_file_promoted(
        self,
        path: str,
        classification: str = "",
        tags: list[str] | None = None,
        destination: str = "",
        run_id: str | None = None,
    ) -> bool:
        """
        Emit when PSF promotes (classifies/tags/moves) a file to its
        canonical storage location.
        """
        rid = run_id or f"psf-promo-{uuid.uuid4().hex[:8]}"
        if _publish_artifact_promoted is not None:
            try:
                return _publish_artifact_promoted(
                    artifact_name=path,
                    status="promoted",
                    trace_id=rid,
                    notes=json.dumps({
                        "classification": classification,
                        "tags": tags or [],
                        "destination": destination,
                    }),
                    source=self.source,
                    run_id=rid,
                )
            except Exception as e:
                log.debug("[PSF] promote error: %s", e)

        return self._pub.sync_publish(
            "file_promoted",
            {"path": path, "classification": classification,
             "tags": tags or [], "destination": destination},
            rid,
        )

    def publish_storage_stats(
        self,
        total_files: int = 0,
        dedup_savings_mb: float = 0.0,
        indexed_today: int = 0,
        run_id: str | None = None,
    ) -> bool:
        """
        Emit a periodic stats snapshot as a kg_patch on the PSF node.
        Conductor can subscribe to these for dashboard KPIs.
        """
        rid = run_id or f"psf-stats-{uuid.uuid4().hex[:8]}"
        if _publish_kg_patch is not None:
            try:
                return _publish_kg_patch(
                    nodes=[{
                        "node_id": "PersonalStorageForge",
                        "node_type": "system_component",
                        "label": "PersonalStorageForge",
                        "properties": {
                            "total_files": total_files,
                            "dedup_savings_mb": dedup_savings_mb,
                            "indexed_today": indexed_today,
                            "last_stats_run": rid,
                        },
                    }],
                    source=self.source,
                    run_id=rid,
                )
            except Exception as e:
                log.debug("[PSF] stats error: %s", e)

        return self._pub.sync_publish(
            "storage_stats",
            {"total_files": total_files, "dedup_savings_mb": dedup_savings_mb,
             "indexed_today": indexed_today},
            rid,
        )
