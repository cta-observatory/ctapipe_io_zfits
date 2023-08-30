"""
EventSource implementations for protozfits files
"""
from .dl0 import ProtozfitsDL0EventSource
from .version import __version__

__all__ = [
    "__version__",
    "ProtozfitsDL0EventSource",
]
