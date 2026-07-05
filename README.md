# PersonalStorageForge

Local-first Mac personal storage sovereignty engine: file watching → AI organization
→ multi-layer deduplication → semantic search → immutable backup → agentic oversight.

Zero cloud dependency. All inference runs on-device.

## Architecture

```
File System (macOS)
      ↓  (fsevents / watchdog)
PersonalStorageForge pipeline
      ↓  (matrix_adapter.PSFPublisher)
MATRIX / harmony_publisher_base
      ↓  (WebSocket)
harmony-engine-protocol (ws://localhost:9002/harmony)
      ↓  (harmony_subscriber.py)
the-brain (brain.db → SQLite + vector + KG)
      ↓  (MCP tools)
conductor-protocol-v2 / self-improving-system-builder
```

PersonalStorageForge does **not** own the harmony bus publish logic.
MATRIX owns it. PSF imports from MATRIX via `matrix_adapter.py`.
This is intentional: one source of truth for event envelopes.

## Quick Start

```bash
# 1. Clone both repos side-by-side
git clone https://github.com/DXv-3/MATRIX
git clone https://github.com/DXv-3/PersonalStorageForge

# 2. Set MATRIX_PATH (or rely on auto-discovery if repos are siblings)
export MATRIX_PATH=/Users/vinny/dev/MATRIX

# 3. Install dependencies
pip install -e .

# 4. Copy and configure environment
cp .env.example .env
# Edit .env: set BRAIN_SYNC_PATH, BRAIN_DB_PATH, HARMONY_WS_URL

# 5. Run storage bridge
python storage_brain_bridge.py
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MATRIX_PATH` | auto | Path to MATRIX repo root |
| `BRAIN_SYNC_PATH` | — | Path to the-brain repo root |
| `BRAIN_DB_PATH` | — | Path to brain.db |
| `HARMONY_WS_URL` | `ws://localhost:9002/harmony` | Harmony bus URL |
| `HARMONY_TOKEN` | — | Bearer token for harmony bus |
| `HARMONY_FALLBACK` | `1` | Fall back to brain_client if bus unreachable |

## Publishing Events

```python
from matrix_adapter import PSFPublisher

pub = PSFPublisher()

# File ingested by the watcher
pub.publish_file_indexed("/Users/vinny/Documents/report.pdf",
                          size_bytes=42000, tags=["work", "finance"])

# Duplicate suppressed
pub.publish_file_deduped(original="/archive/a.pdf", duplicate="/inbox/b.pdf")

# File promoted to canonical location after AI classification
pub.publish_file_promoted("/archive/finance/report-2026-q2.pdf",
                           classification="finance", destination="/archive/finance")

# Periodic stats (call from a cron or after each pipeline run)
pub.publish_storage_stats(total_files=14200, dedup_savings_mb=3400.0, indexed_today=47)
```

## Related Repos

- [MATRIX](https://github.com/DXv-3/MATRIX) — canonical harmony bus transport + media intelligence
- [the-brain](https://github.com/DXv-3/the-brain) — central knowledge store
- [harmony-engine-protocol](https://github.com/DXv-3/harmony-engine-protocol) — message bus
- [conductor-protocol-v2](https://github.com/DXv-3/conductor-protocol-v2) — agent orchestrator
