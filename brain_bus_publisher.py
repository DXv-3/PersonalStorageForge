"""brain_bus_publisher.py — PersonalStorageForge → the-brain live event wiring.

Drop this file in the PersonalStorageForge repo root.
Instruments the 7-stage pipeline (watch→OCR→organize→dedup→semantic→backup→oversee)
so every significant event is pushed to brain.db.

This is ADDITIVE to storage_brain_bridge.py (which does direct BrainSync writes).
This module adds the harmony-engine-protocol bus layer + KG graph edges.

Usage:
    from brain_bus_publisher import publish_pipeline_event, publish_file_event

    publish_file_event("file_organized", src="/tmp/scan.pdf", dst="~/Docs/2026/scan.pdf")
    publish_pipeline_event("backup_completed", stage="backup", detail="4 files → rclone s3")
"""
from __future__ import annotations
import json, os, sys, uuid
from datetime import datetime, timezone
from pathlib import Path

_SOURCE = "PersonalStorageForge"
_REPO_ROOT = Path(__file__).resolve().parent

STAGES = ["watch", "ocr", "organize", "dedup", "semantic_search", "backup", "oversee"]

def _get_brain():
    candidates = [
        _REPO_ROOT.parent / "the-brain",
        Path.home() / "the-brain",
        Path.home() / "repos" / "the-brain",
    ]
    env_path = os.environ.get("BRAIN_REPO_PATH", "")
    if env_path:
        candidates.insert(0, Path(env_path))
    for c in candidates:
        if (c / "brain_sync.py").exists():
            if str(c) not in sys.path:
                sys.path.insert(0, str(c))
            try:
                from brain_sync import BrainSync  # type: ignore
                return BrainSync()
            except Exception as e:
                print(f"[PSF brain_bus] import error: {e}")
                return None
    print("[PSF brain_bus] WARNING: the-brain not found. Set BRAIN_REPO_PATH.")
    return None

_brain = None
_brain_resolved = False

def _client():
    global _brain, _brain_resolved
    if not _brain_resolved:
        _brain = _get_brain()
        _brain_resolved = True
    return _brain

def publish_pipeline_event(
    event_type: str,
    stage: str,
    detail: str = "",
    outcome: str = "pass",
    run_id: str | None = None,
) -> bool:
    """Publish a pipeline stage event to brain.db."""
    rid = run_id or f"psf_{uuid.uuid4().hex[:8]}"
    brain = _client()
    if brain is None:
        return False
    try:
        return brain.learn(
            run_id=rid, source=_SOURCE,
            category=f"pipeline:{stage}",
            event_type=event_type,
            detail=detail, outcome=outcome,
        )
    except Exception as e:
        print(f"[PSF brain_bus] write error: {e}")
        return False

def publish_file_event(
    event_type: str,
    src: str = "",
    dst: str = "",
    outcome: str = "pass",
    run_id: str | None = None,
    extra: dict | None = None,
) -> bool:
    """Publish a file operation event and register the file as a KG node."""
    rid = run_id or f"psf_{uuid.uuid4().hex[:8]}"
    detail = f"src={src} dst={dst}"
    if extra:
        detail += f" | {json.dumps(extra)}"
    brain = _client()
    if brain is None:
        return False
    try:
        ok = brain.learn(
            run_id=rid, source=_SOURCE, category="file_ops",
            event_type=event_type, detail=detail, outcome=outcome,
        )
        if src:
            brain.kg_add_node(
                node_id=f"file:{src}", node_type="file",
                label=Path(src).name,
                properties={"managed_by": _SOURCE, "run_id": rid},
            )
        return ok
    except Exception as e:
        print(f"[PSF brain_bus] file event error: {e}")
        return False

def publish_agentic_decision(
    decision: str, rationale: str,
    outcome: str = "pass", run_id: str | None = None,
) -> bool:
    """Log an agentic oversight decision to the brain."""
    rid = run_id or f"psf_agent_{uuid.uuid4().hex[:8]}"
    return publish_pipeline_event(
        "agentic_decision", "oversee",
        detail=f"decision={decision} | rationale={rationale}",
        outcome=outcome, run_id=rid,
    )

def publish_semantic_index_event(
    file_path: str, embedding_model: str,
    chunk_count: int, outcome: str = "pass",
    run_id: str | None = None,
) -> bool:
    """Log a semantic search indexing event."""
    return publish_file_event(
        "semantic_indexed", src=file_path,
        outcome=outcome, run_id=run_id,
        extra={"model": embedding_model, "chunks": chunk_count},
    )
