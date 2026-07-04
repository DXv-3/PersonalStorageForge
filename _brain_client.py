"""_brain_client.py — Shared bootstrap for brain_sync integration.

Discovery priority:
  BRAIN_SYNC_PATH env var → ../the-brain/ sibling → ~/the-brain/
Set BRAIN_DB_PATH to override brain.db location (handled by brain_sync natively).
"""
from __future__ import annotations
import os, sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent

def _find_brain_sync():
    candidates = [
        os.environ.get("BRAIN_SYNC_PATH", ""),
        str(_HERE.parent / "the-brain"),
        str(Path.home() / "the-brain"),
        str(Path.home() / "repos" / "the-brain"),
    ]
    for c in candidates:
        if c and (Path(c) / "brain_sync.py").exists():
            if c not in sys.path:
                sys.path.insert(0, c)
            try:
                from brain_sync import get_brain  # type: ignore
                return get_brain()
            except Exception as e:
                print(f"[brain_client] import error from {c}: {e}")
    print("[brain_client] brain_sync.py not found. "
          "Set BRAIN_SYNC_PATH (directory containing brain_sync.py).")
    return None

_client = None
_resolved = False

def get_client():
    global _client, _resolved
    if not _resolved:
        _client = _find_brain_sync()
        _resolved = True
    return _client
