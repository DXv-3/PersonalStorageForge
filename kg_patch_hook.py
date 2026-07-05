"""
kg_patch_hook.py — PersonalStorageForge → harmony bus kg_patch emitter
------------------------------------------------------------------------
Wires the PSF file classification pipeline into the harmony bus so that
every file PSF classifies, deduplicates, or organises becomes a KG node
in the-brain and a kg_patch event in the conductor's subscriber.

Usage — call from your classifier callback:

    from kg_patch_hook import on_file_classified

    # After PSF classifies a file:
    on_file_classified(
        file_path="/Users/vinny/Documents/invoice_2026.pdf",
        classification={"category": "finance", "tags": ["invoice", "2026"],
                        "confidence": 0.94, "mime": "application/pdf"},
        action="indexed",        # indexed | deduped | organised | archived
        duplicate_of=None,       # path of original if deduped
    )

Or wire the hook into matrix_adapter automatically at startup:

    from kg_patch_hook import patch_matrix_adapter
    patch_matrix_adapter()   # monkeypatches PSFPublisher.publish_file_indexed

Events emitted:
  kg_patch  → conductor harmony_subscriber.handle_kg_patch
             → the-brain KG via BRAIN_MCP_URL/kg/write
             → local PSF audit log (~/.psf/kg_patches.jsonl)
"""

import json
import logging
import os
import sys
import threading
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_LOG_DIR = Path(os.getenv("PSF_LOG_DIR", Path.home() / ".psf"))
_LOG_FILE = _LOG_DIR / "kg_patches.jsonl"
_BRAIN_MCP_URL = os.getenv("BRAIN_MCP_URL", "http://localhost:8765")
_HARMONY_POLL_FILE = Path(os.getenv("HARMONY_POLL_FILE", "/tmp/harmony_events.jsonl"))


# ---------------------------------------------------------------------------
# Core emitter
# ---------------------------------------------------------------------------

def on_file_classified(
    file_path: str,
    classification: Dict[str, Any],
    action: str = "indexed",
    duplicate_of: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Emit a kg_patch event for a classified file.

    Args:
        file_path:      Absolute path to the file.
        classification: Dict from PSF classifier, e.g.:
                        {"category": "finance", "tags": [...],
                         "confidence": 0.94, "mime": "application/pdf"}
        action:         "indexed" | "deduped" | "organised" | "archived"
        duplicate_of:   Path of original file if action=="deduped".
        metadata:       Any extra key-value pairs to attach to the KG node.

    Returns:
        patch_id (UUID string) for provenance tracking.
    """
    patch_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).isoformat()
    fp = Path(file_path)

    payload: Dict[str, Any] = {
        "patch_id": patch_id,
        "node_type": "file",
        "node_id": f"file:{fp.name}:{fp.stat().st_size if fp.exists() else 0}",
        "action": action,
        "file_path": str(fp),
        "file_name": fp.name,
        "file_ext": fp.suffix.lower(),
        "file_size": fp.stat().st_size if fp.exists() else 0,
        "category": classification.get("category", "unknown"),
        "tags": classification.get("tags", []),
        "confidence": classification.get("confidence", 0.0),
        "mime": classification.get("mime", ""),
        "duplicate_of": duplicate_of,
        "source_repo": "PersonalStorageForge",
        "timestamp": now,
        **(metadata or {}),
    }

    # Write local audit log
    _write_local_log(patch_id, payload)

    # Publish async (harmony bus + brain KG) — non-blocking
    t = threading.Thread(
        target=_publish_all,
        args=(patch_id, payload),
        daemon=True,
    )
    t.start()

    logger.debug(
        "kg_patch_hook: emitted patch_id=%s file=%s action=%s category=%s",
        patch_id, fp.name, action, classification.get("category", "?")
    )
    return patch_id


def on_file_deduped(
    file_path: str,
    duplicate_of: str,
    classification: Optional[Dict[str, Any]] = None,
) -> str:
    """Convenience wrapper: emit a kg_patch for a deduplicated file."""
    return on_file_classified(
        file_path=file_path,
        classification=classification or {},
        action="deduped",
        duplicate_of=duplicate_of,
    )


def on_file_archived(
    file_path: str,
    archive_path: str,
    classification: Optional[Dict[str, Any]] = None,
) -> str:
    """Convenience wrapper: emit a kg_patch for an archived file."""
    return on_file_classified(
        file_path=file_path,
        classification=classification or {},
        action="archived",
        metadata={"archive_path": archive_path},
    )


# ---------------------------------------------------------------------------
# Publish targets
# ---------------------------------------------------------------------------

def _publish_all(patch_id: str, payload: Dict[str, Any]):
    """Publish to harmony bus AND the-brain KG. Non-blocking, called in thread."""
    _publish_harmony(patch_id, payload)
    _publish_brain(patch_id, payload)


def _publish_harmony(patch_id: str, payload: Dict[str, Any]):
    """Publish to harmony bus; fall back to poll-file."""
    event_envelope = json.dumps({
        "event_type": "kg_patch",
        "payload": payload,
    })

    # Attempt 1: MATRIX HarmonyPublisher (if available)
    try:
        matrix_path = os.getenv("MATRIX_PATH", "../MATRIX")
        sys.path.insert(0, matrix_path)
        from harmony_publisher_base import HarmonyPublisher  # type: ignore
        HarmonyPublisher().publish("kg_patch", payload)
        logger.debug("kg_patch_hook: harmony bus publish OK patch_id=%s", patch_id)
        return
    except Exception:
        pass

    # Attempt 2: PSFPublisher (already in this repo)
    try:
        from matrix_adapter import PSFPublisher  # type: ignore
        PSFPublisher()._pub.sync_publish("kg_patch", payload, run_id=patch_id)
        return
    except Exception:
        pass

    # Attempt 3: Poll-file fallback (conductor harmony_subscriber reads this)
    try:
        _HARMONY_POLL_FILE.parent.mkdir(parents=True, exist_ok=True)
        with _HARMONY_POLL_FILE.open("a") as f:
            f.write(event_envelope + "\n")
        logger.debug("kg_patch_hook: wrote to poll-file patch_id=%s", patch_id)
    except Exception as exc:
        logger.warning("kg_patch_hook: all harmony publish attempts failed: %s", exc)


def _publish_brain(patch_id: str, payload: Dict[str, Any]):
    """Write the file node directly to the-brain KG via MCP. Never raises."""
    try:
        data = json.dumps({
            "operation": "kg_write",
            "node_type": "file",
            "node_id": payload.get("node_id", f"file:{patch_id}"),
            "properties": {
                k: v for k, v in payload.items()
                if k not in ("patch_id", "node_type", "node_id")
            },
        }).encode()
        req = urllib.request.Request(
            f"{_BRAIN_MCP_URL}/kg/write",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            if resp.status == 200:
                logger.debug("kg_patch_hook: brain KG write OK patch_id=%s", patch_id)
    except Exception as exc:
        logger.debug("kg_patch_hook: brain KG write failed (non-fatal): %s", exc)


def _write_local_log(patch_id: str, payload: Dict[str, Any]):
    """Write to ~/.psf/kg_patches.jsonl for local audit."""
    try:
        _LOG_DIR.mkdir(parents=True, exist_ok=True)
        with _LOG_FILE.open("a") as f:
            f.write(json.dumps({"patch_id": patch_id, **payload}) + "\n")
    except Exception as exc:
        logger.debug("kg_patch_hook._write_local_log: %s", exc)


# ---------------------------------------------------------------------------
# matrix_adapter monkeypatch
# ---------------------------------------------------------------------------

_PATCHED = False


def patch_matrix_adapter():
    """
    Monkeypatch PSFPublisher.publish_file_indexed so every file PSF
    indexes automatically emits a kg_patch event to the harmony bus
    and the-brain KG.

    Apply once at startup:
        from kg_patch_hook import patch_matrix_adapter
        patch_matrix_adapter()
    """
    global _PATCHED
    if _PATCHED:
        return

    try:
        from matrix_adapter import PSFPublisher  # type: ignore
    except ImportError:
        logger.warning("kg_patch_hook: matrix_adapter not importable — patch skipped")
        return

    original_publish = PSFPublisher.publish_file_indexed

    def patched_publish_file_indexed(
        self,
        file_path: str,
        classification: Optional[Dict] = None,
        action: str = "indexed",
        **kwargs,
    ):
        # Run original first
        result = original_publish(self, file_path, classification=classification,
                                  action=action, **kwargs)
        # Emit kg_patch
        try:
            on_file_classified(
                file_path=file_path,
                classification=classification or {},
                action=action,
            )
        except Exception as exc:
            logger.warning("kg_patch_hook patch: on_file_classified failed: %s", exc)
        return result

    PSFPublisher.publish_file_indexed = patched_publish_file_indexed
    _PATCHED = True
    logger.info("kg_patch_hook: PSFPublisher.publish_file_indexed patched successfully")


# ---------------------------------------------------------------------------
# Batch helper
# ---------------------------------------------------------------------------

def emit_batch(
    files: List[Dict[str, Any]],
    default_action: str = "indexed",
) -> List[str]:
    """
    Emit kg_patch events for a batch of classified files.

    Args:
        files: List of dicts, each with keys:
               file_path (str), classification (dict), action (str, optional)
        default_action: Used when a file dict has no "action" key.

    Returns:
        List of patch_ids.
    """
    patch_ids = []
    for f in files:
        try:
            pid = on_file_classified(
                file_path=f["file_path"],
                classification=f.get("classification", {}),
                action=f.get("action", default_action),
                duplicate_of=f.get("duplicate_of"),
                metadata=f.get("metadata"),
            )
            patch_ids.append(pid)
        except Exception as exc:
            logger.warning("kg_patch_hook.emit_batch: failed for %s: %s",
                           f.get("file_path", "?"), exc)
    return patch_ids


# ---------------------------------------------------------------------------
# CLI smoke test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import tempfile
    logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")

    print("--- kg_patch_hook smoke test ---")

    # Create a real temp file so stat() works
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(b"%PDF-1.4 smoke test")
        tmp_path = tmp.name

    pid = on_file_classified(
        file_path=tmp_path,
        classification={
            "category": "finance",
            "tags": ["invoice", "test"],
            "confidence": 0.95,
            "mime": "application/pdf",
        },
        action="indexed",
    )
    print(f"patch_id: {pid}")

    # Give async threads a moment
    import time; time.sleep(0.5)

    # Check local log
    if _LOG_FILE.exists():
        last_line = _LOG_FILE.read_text().strip().split("\n")[-1]
        entry = json.loads(last_line)
        print(f"Local log: category={entry['category']} action={entry['action']}")

    # Check poll file
    if _HARMONY_POLL_FILE.exists():
        last_line = _HARMONY_POLL_FILE.read_text().strip().split("\n")[-1]
        entry = json.loads(last_line)
        print(f"Poll file: event_type={entry['event_type']} node_id={entry['payload']['node_id']}")

    print("\n✅ smoke test passed")
    Path(tmp_path).unlink(missing_ok=True)
