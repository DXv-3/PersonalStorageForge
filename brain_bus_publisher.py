"""brain_bus_publisher.py — PersonalStorageForge → the-brain live event wiring.

Instruments the 7-stage pipeline (watch→OCR→organize→dedup→semantic→backup→oversee).
This is ADDITIVE to storage_brain_bridge.py (direct BrainSync writes).
This module uses get_brain() singleton + writes KG graph edges per file op.

Usage:
    from brain_bus_publisher import publish_pipeline_event, publish_file_event
    from brain_bus_publisher import publish_agentic_decision

    publish_file_event("file_organized", src="/tmp/scan.pdf", dst="~/Docs/2026/scan.pdf")
    publish_pipeline_event("backup_completed", stage="backup", detail="4 files → rclone s3")

Requires:
    Set BRAIN_SYNC_PATH env var to the directory containing brain_sync.py.
"""
from __future__ import annotations
import json, uuid
from pathlib import Path
from _brain_client import get_client

_SOURCE = "PersonalStorageForge"

def publish_pipeline_event(
    event_type: str,
    stage: str,
    detail: str = "",
    outcome: str = "pass",
    run_id: str | None = None,
) -> bool:
    """Publish a pipeline stage event to brain.db."""
    rid = run_id or f"psf_{uuid.uuid4().hex[:8]}"
    brain = get_client()
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
    brain = get_client()
    if brain is None:
        return False
    try:
        ok = brain.learn(
            run_id=rid, source=_SOURCE, category="file_ops",
            event_type=event_type, detail=detail, outcome=outcome,
        )
        if src:
            brain.kg_add_node(
                node_id=f"file:{src}",
                node_type="file",
                label=Path(src).name,
                properties={"managed_by": _SOURCE, "run_id": rid},
            )
            if dst:
                brain.kg_add_edge(
                    source_id=f"file:{src}",
                    target_id=f"file:{dst}",
                    relation="moved_to",
                    weight=1.0,
                )
        return ok
    except Exception as e:
        print(f"[PSF brain_bus] file event error: {e}")
        return False

def publish_agentic_decision(
    decision: str, rationale: str,
    outcome: str = "pass", run_id: str | None = None,
) -> bool:
    """Log an agentic oversight decision from the oversee stage."""
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

def publish_ocr_result(
    file_path: str, page_count: int,
    char_count: int, outcome: str = "pass",
    run_id: str | None = None,
) -> bool:
    """Log an OCR extraction result from the ocr stage."""
    return publish_file_event(
        "ocr_completed", src=file_path,
        outcome=outcome, run_id=run_id,
        extra={"pages": page_count, "chars": char_count},
    )

def publish_backup_event(
    destination: str, file_count: int,
    size_mb: float, outcome: str = "pass",
    run_id: str | None = None,
) -> bool:
    """Log a backup operation completion from the backup stage."""
    return publish_pipeline_event(
        "backup_completed", "backup",
        detail=f"dest={destination} files={file_count} size_mb={size_mb:.1f}",
        outcome=outcome, run_id=run_id,
    )
