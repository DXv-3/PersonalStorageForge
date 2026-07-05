"""
brain_bus_publisher.py — DEPRECATED SHIM.

This file exists only for backwards compatibility. New PSF code should
import from matrix_adapter.PSFPublisher instead.

    from matrix_adapter import PSFPublisher
    pub = PSFPublisher()
    pub.publish_file_indexed(path, ...)

The canonical publish logic now lives in:
  MATRIX/harmony_publisher_base.py  (transport layer)
  matrix_adapter.py                 (PSF domain wrappers)
"""
import warnings
warnings.warn(
    "brain_bus_publisher is deprecated. Import from matrix_adapter instead.",
    DeprecationWarning,
    stacklevel=2,
)

try:
    from matrix_adapter import PSFPublisher as _PSF
    _pub = _PSF()

    def publish_event(event_type, category="storage", detail="", outcome="pass",
                      run_id=None, metadata=None):
        return _pub._pub.sync_publish(event_type, {"category": category,
                                                    "detail": detail,
                                                    "outcome": outcome,
                                                    **(metadata or {})}, run_id)
except Exception:
    def publish_event(*args, **kwargs):
        return False
