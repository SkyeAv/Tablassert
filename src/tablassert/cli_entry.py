"""Console-script launcher for the optional ``[cli]`` extra."""

from __future__ import annotations

from importlib import import_module

from tablassert import extras


def main() -> None:
    """Run the command-line application after checking its optional runtime."""
    extras.require("cli")
    import_module("tablassert.cli").APP()
