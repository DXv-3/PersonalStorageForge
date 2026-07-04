#!/usr/bin/env python3
"""storage_brain_bridge.py — PersonalStorageForge → brain.db integration.

PersonalStorageForge watches the filesystem, organizes files with AI,
deduplicates, creates semantic indexes, and manages immutable backups.
This bridge writes every significant storage event to brain.db so:

    - conductor knows the state of local storage
    - the-brain has semantic search over file descriptions
    - the KG maps file relationships (supersedes, backup_of, tagged_with)
    - MATRIX and PersonalStorageForge dedup events cross-reference each other

Usage:
    from storage_brain_bridge import StorageBrainBridge

    bridge = StorageBrainBridge()

    # File watcher detected new file
    bridge.file_detected("/Users/vinny/Downloads/report.pdf",
                          trigger="inotify", size_bytes=450_000)

    # AI organized it
    bridge.file_organized(
        original_path="/Users/vinny/Downloads/report.pdf",
        new_path="/Users/vinny/Documents/Projects/report.pdf",
        ai_model="claude-3-5-sonnet",
        confidence=0.91,
        category="document",
    )

    # Immutable backup created
    bridge.backup_created("/Users/vinny/Documents/Projects/report.pdf",
                           backup_hash="sha256:abc...", store="b2")
"""
from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "harmony-engine-protocol"))
try:
    from brain_bus import BrainBusPublisher
    _BUS_AVAILABLE = True
except ImportError:
    _BUS_AVAILABLE = False


class StorageBrainBridge:
    """Write PersonalStorageForge storage events to brain.db via the brain bus."""

    def __init__(self, source_repo: str = "PersonalStorageForge"):
        self.source_repo = source_repo
        self._pub: BrainBusPublisher | None = None
        if _BUS_AVAILABLE:
            self._pub = BrainBusPublisher(source_repo=source_repo)

    def _run_id(self, prefix: str = "psf") -> str:
        return f"{prefix}-{str(uuid.uuid4())[:8]}"

    def _learn(self, event_type: str, category: str, detail: str, outcome: str = "pass") -> bool:
        if self._pub is None:
            print(f"[storage_bridge][no-bus] {event_type}: {detail[:80]}")
            return False
        return self._pub.publish_learn(
            run_id=self._run_id(),
            source=self.source_repo,
            category=category,
            event_type=event_type,
            detail=detail,
            outcome=outcome,
        )

    # ---------------------------------------------------------------- #
    #  File watcher events                                              #
    # ---------------------------------------------------------------- #

    def file_detected(
        self,
        file_path: str,
        trigger: str = "watcher",
        size_bytes: int = 0,
        extension: str = "",
    ) -> bool:
        return self._learn(
            "FILE_CATALOGED", "file_watcher",
            json.dumps({"path": str(file_path), "trigger": trigger,
                        "size_bytes": size_bytes, "extension": extension or Path(file_path).suffix}),
        )

    def file_organized(
        self,
        original_path: str,
        new_path: str,
        ai_model: str = "",
        confidence: float = 0.0,
        category: str = "",
        tags: list[str] | None = None,
    ) -> bool:
        """Log AI-driven file organization (move + categorize)."""
        ok = self._learn(
            "FILE_MOVED", "ai_organization",
            json.dumps({"from": str(original_path), "to": str(new_path),
                        "ai_model": ai_model, "confidence": round(confidence, 3),
                        "category": category, "tags": tags or []}),
        )
        # Add to KG: ORGANIZED_INTO edge from file to category
        if self._pub and category:
            self._pub.publish_kg_node(
                node_id=f"category:{category.lower().replace(' ', '_')}",
                node_type="file_category",
                label=category,
            )
            filename = Path(new_path).name
            self._pub.publish_kg_node(
                node_id=f"file:{filename}",
                node_type="file",
                label=filename,
                properties={"path": str(new_path), "category": category},
            )
            self._pub.publish_kg_edge(
                source_id=f"file:{filename}",
                target_id=f"category:{category.lower().replace(' ', '_')}",
                relation="ORGANIZED_INTO",
                weight=round(confidence, 3),
            )
        return ok

    def duplicate_removed(
        self,
        original_path: str,
        removed_path: str,
        hash_value: str = "",
    ) -> bool:
        """Log a deduplication removal. Cross-references with MATRIX."""
        return self._learn(
            "DUPLICATE_FOUND", "deduplication",
            json.dumps({"original": str(original_path), "removed": str(removed_path),
                        "hash": hash_value, "kept": "original"}),
        )

    def semantic_index_updated(
        self,
        indexed_path: str,
        num_files: int = 0,
        index_type: str = "vector",
    ) -> bool:
        """Log a semantic index rebuild."""
        return self._learn(
            "SKILL_EXPORTED", "semantic_index",
            json.dumps({"path": str(indexed_path), "num_files": num_files,
                        "index_type": index_type}),
        )

    def backup_created(
        self,
        file_path: str,
        backup_hash: str = "",
        store: str = "local",
        size_bytes: int = 0,
    ) -> bool:
        """Log an immutable backup creation."""
        ok = self._learn(
            "FILE_ARCHIVED", "backup",
            json.dumps({"path": str(file_path), "backup_hash": backup_hash,
                        "store": store, "size_bytes": size_bytes}),
        )
        if self._pub:
            self._pub.publish_artifact(
                artifact_name=Path(file_path).name,
                promotion_status="promoted",
                trace_id=backup_hash[:16] if backup_hash else "",
                notes=f"Immutable backup in {store}",
            )
        return ok

    def agent_decision(
        self,
        decision: str,
        file_path: str = "",
        reasoning: str = "",
        outcome: str = "pass",
    ) -> bool:
        """Log an agentic oversight decision."""
        return self._learn(
            "GATE_PASSED" if outcome == "pass" else "GATE_FAILED",
            "agentic_oversight",
            json.dumps({"decision": decision[:200], "file_path": str(file_path),
                        "reasoning": reasoning[:300]}),
            outcome=outcome,
        )

    def error(
        self,
        file_path: str,
        error_message: str,
        stage: str = "",
    ) -> bool:
        return self._learn(
            "PROCESSING_ERROR", "error",
            json.dumps({"path": str(file_path), "error": error_message[:400], "stage": stage}),
            outcome="fail",
        )

    def ingest_file_description(
        self,
        file_path: str,
        description: str,
        session_prefix: str = "psf-semantic",
    ) -> bool:
        """Ingest an AI-generated file description into brain FTS5 search.

        This makes your files semantically searchable via the-brain.
        """
        if self._pub is None:
            return False
        session_id = f"{session_prefix}-{Path(file_path).stem[:20]}"
        return self._pub.publish_ingest(
            session_id=session_id,
            role="assistant",
            content=f"FILE DESCRIPTION: {file_path}\n\n{description}",
            source=self.source_repo,
        )
