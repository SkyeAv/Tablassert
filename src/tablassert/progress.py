from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from importlib.metadata import version as get_version
from pathlib import Path
from time import monotonic
from types import TracebackType
from typing import TYPE_CHECKING

from rich.console import Console, Group
from rich.live import Live
from rich.padding import Padding
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskID, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.text import Text

if TYPE_CHECKING:
    from tablassert.lib import Tcode


def format_section_compact(x: Tcode) -> str:
    """Build a one-line section identifier (config stem plus 8-char hash stem).

    Args:
        x: Section task descriptor.

    Returns:
        Compact ``"<config stem> · <hash[:8]>"`` label.
    """
    return f"{Path(x.config.name).stem} · {x.store.stem[:8]}"


def _truncate(s: str, max_width: int) -> str:
    """Truncate ``s`` to ``max_width`` with a trailing ellipsis if too long.

    Args:
        s: String to truncate.
        max_width: Maximum number of characters to keep.

    Returns:
        The original string if it fits, otherwise a ``max_width``-length string
        ending in ``…`` (or just ``…`` when ``max_width <= 1``).
    """
    if len(s) <= max_width:
        return s
    if max_width <= 1:
        return "…"
    return s[: max_width - 1] + "…"


class PipelineProgress(AbstractContextManager["PipelineProgress"]):
    def __init__(self: PipelineProgress, total_stages: int) -> None:
        self.total_stages: int = total_stages
        self.console: Console = Console(stderr=True)
        # Middle row: label + bar + count + elapsed + ETA. No description, no spinner.
        self.progress: Progress = Progress(
            TextColumn("[bold cyan]{task.fields[label]}"),
            BarColumn(bar_width=None),
            MofNCompleteColumn(),
            TextColumn("[dim]·[/dim]"),
            TimeElapsedColumn(),
            TextColumn("[dim]·[/dim]"),
            TimeRemainingColumn(),
            console=self.console,
            transient=False,
            expand=True,
        )
        # Top row: stage header.
        self.stage_text: Text = Text("")
        # Bottom row: indented item detail with optional phase tag.
        self.detail_text: Text = Text("")
        self._group: Group = Group(self.stage_text, Padding(self.progress, (0, 0)), self.detail_text)
        self.live: Live = Live(self._group, console=self.console, refresh_per_second=10, transient=False)
        self.section_task: TaskID | None = None
        self.stage_step: int = 0
        self._stage_name: str = ""
        self._stage_started_at: float = monotonic()
        self._current_detail: str = ""
        self._current_phase: str = ""

    def __enter__(self: PipelineProgress) -> PipelineProgress:
        self.console.line(1)
        self.live.start()
        self._render_stage(f"Stage 0 of {self.total_stages}  ·  STARTING")
        return self

    def __exit__(self: PipelineProgress, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None) -> None:
        # Print a final completion line for the last stage before tearing down.
        if self.stage_step >= 1:
            self._print_completion()
        self.live.stop()
        self.console.line(1)

    def stage(self: PipelineProgress, name: str) -> None:
        """Advance to the next pipeline stage.

        Emits a completion line for the previous stage (if any) before advancing
        the stage counter and rendering the new stage header.

        Args:
            name: Display name of the stage being entered.
        """
        # Emit a completion line for the previous stage before advancing.
        if self.stage_step >= 1:
            self._print_completion()
        self.stage_step += 1
        self._stage_name = name
        self._stage_started_at = monotonic()
        self.end_section_task()
        self._clear_detail()
        self._render_stage(f"Stage {self.stage_step} of {self.total_stages}  ·  {name.upper()}")

    def section_loop(self: PipelineProgress, total: int, label: str) -> tuple[Callable[[str], None], Callable[[], None], Callable[[str], None]]:
        """Start a progress loop over the sections of the current stage.

        Args:
            total: Number of items the loop will iterate.
            label: Upper-cased label rendered on the progress bar.

        Returns:
            A ``(start, advance, sub_step)`` tuple of callbacks. ``start`` sets
            the current detail line; ``advance`` ticks the bar counter; ``sub_step``
            appends a phase tag to the detail line. Detail lives on its own row,
            NOT in the bar description.
        """
        # Detail lives on its own row, NOT in the bar description.
        self.end_section_task()
        self.section_task = self.progress.add_task(description="", total=total, label=label.upper())
        self._clear_detail()

        def start(detail: str) -> None:
            assert self.section_task is not None
            self._current_detail = detail
            self._current_phase = ""
            self._render_detail()

        def sub_step(phase: str) -> None:
            assert self.section_task is not None
            self._current_phase = phase
            self._render_detail()

        def advance() -> None:
            assert self.section_task is not None
            self._current_phase = ""
            self.progress.update(self.section_task, advance=1)
            self._render_detail()

        return start, advance, sub_step

    def end_section_task(self: PipelineProgress) -> None:
        if self.section_task is not None:
            self.progress.update(self.section_task, visible=False)
            self.section_task = None

    def dynamic_loop(self: PipelineProgress, label: str) -> Callable[[int, int, int, str], None]:
        """Start a progress task driven by an external ``(phase, completed, total, detail)`` callback.

        Used for the fullmap build, where Rust reports progress across phases
        (equivalents / synonyms / writing) whose totals differ and are not all
        known up front. A fresh bar is created on each phase change; a phase
        whose ``total`` is ``0`` renders as an indeterminate bar until a later
        callback supplies the real total. The reverse (a phase that starts with
        a real total then reports ``total=0``) re-creates the bar as
        indeterminate so a later, larger count never overflows the earlier total.

        Args:
            label: Fallback label rendered on the progress bar.

        Returns:
            A ``(phase, completed, total, detail)`` callback to pass as
            ``rs.build_fullmap_db(progress=...)``.
        """
        self.end_section_task()
        self._clear_detail()
        phase_labels: dict[int, str] = {0: "EQUIVALENTS", 1: "SYNONYMS", 2: "WRITING"}
        # Rust fires the progress callback from parallel rayon threads; the GIL
        # serialises the calls but NOT in completion order, so ``completed`` can
        # arrive out of order (e.g. 6, 3, 21, 8).  Feeding a regression to rich
        # makes the bar jump backwards and breaks the ETA (``--:--:--``).  Track
        # the high-water mark per phase and clamp to keep the bar monotonic.
        state: dict[str, int] = {"phase": -1, "max_completed": 0, "determinate": 0}

        def update(phase: int, completed: int, total: int, detail: str) -> None:
            if phase != state["phase"]:
                state["phase"] = phase
                state["max_completed"] = 0
                state["determinate"] = 1 if total > 0 else 0
                self.end_section_task()
                self.section_task = self.progress.add_task(
                    description="", total=total if total > 0 else None, label=phase_labels.get(phase, label.upper())
                )
            elif total == 0 and state["determinate"]:
                # Same phase switched from a real total to indeterminate: phase 0
                # scatters per-file counts (total = number of class files) and then
                # the equiv-merge reports entry counts in the millions with total=0.
                # Re-create the bar as indeterminate and restart the high-water mark
                # so a huge entry count never overflows the small file-count total.
                state["determinate"] = 0
                state["max_completed"] = 0
                self.end_section_task()
                self.section_task = self.progress.add_task(description="", total=None, label=phase_labels.get(phase, label.upper()))
            if completed > state["max_completed"]:
                state["max_completed"] = completed
            mono = state["max_completed"]
            assert self.section_task is not None
            if total > 0:
                self.progress.update(self.section_task, completed=mono, total=total)
            else:
                self.progress.update(self.section_task, completed=mono)
            self._current_detail = detail
            self._current_phase = ""
            self._render_detail()

        return update

    def log_sink(self: PipelineProgress, message: str) -> None:
        self.console.print(message, end="", highlight=False, markup=False)

    def _render_stage(self: PipelineProgress, text: str) -> None:
        # Stage header: bold stage count + dimmed version, single line, no metadata.
        self.stage_text.plain = ""
        self.stage_text.append(text, style="bold")
        self.stage_text.append(f"   tablassert v{get_version('tablassert')}", style="dim")

    def _render_detail(self: PipelineProgress) -> None:
        # Compose indented item detail with optional phase suffix, truncated to console width.
        body: str = f"  ↳ {self._current_detail}" if self._current_detail else "  ↳ …"
        if self._current_phase:
            body = f"{body}  →  {self._current_phase}"
        max_width: int = max(20, self.console.width - 2)
        self.detail_text.plain = ""
        self.detail_text.append(_truncate(body, max_width), style="dim")

    def _clear_detail(self: PipelineProgress) -> None:
        self._current_detail = ""
        self._current_phase = ""
        self.detail_text.plain = ""

    def _print_completion(self: PipelineProgress) -> None:
        # Print above the live region: ✓ Stage N · NAME · 0:00:42
        elapsed: float = monotonic() - self._stage_started_at
        mins: int = int(elapsed) // 60
        secs: float = elapsed - mins * 60
        line: str = f"✓ Stage {self.stage_step} · {self._stage_name.upper()} · {mins}:{secs:05.2f}"
        self.console.print(line, style="green", highlight=False, markup=False)
