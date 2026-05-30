"""Plan incremental feature-window computation without database side effects."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WindowPlan:
    """Classify expected feature windows for an incremental run."""

    keep: tuple[dict, ...]
    compute_new: tuple[dict, ...]
    recompute_empty: tuple[dict, ...]
    recompute_retry: tuple[dict, ...]
    recompute_recent: tuple[dict, ...]

    @property
    def to_compute(self) -> tuple[dict, ...]:
        return self.compute_new + self.recompute_empty + self.recompute_retry + self.recompute_recent

    def summary(self) -> dict[str, int]:
        return {
            "kept": len(self.keep),
            "new": len(self.compute_new),
            "empty": len(self.recompute_empty),
            "retry": len(self.recompute_retry),
            "recent": len(self.recompute_recent),
            "to_compute": len(self.to_compute),
        }


def plan_incremental_windows(
    window_specs: list[dict],
    existing_tables: set[str],
    empty_tables: set[str],
    retry_tables: set[str],
    recompute_last_n: int,
) -> WindowPlan:
    """Plan new, empty, retry, and recent window tables for deterministic recompute."""
    if recompute_last_n < 0:
        raise ValueError("recompute_last_n must be >= 0")

    recent_tables = _select_recent_tables(window_specs, existing_tables, recompute_last_n)
    keep: list[dict] = []
    compute_new: list[dict] = []
    recompute_empty: list[dict] = []
    recompute_retry: list[dict] = []
    recompute_recent: list[dict] = []

    for spec in window_specs:
        short_name = _short_table_name(spec)
        if short_name not in existing_tables:
            compute_new.append(spec)
        elif short_name in empty_tables:
            recompute_empty.append(spec)
        elif short_name in retry_tables:
            recompute_retry.append(spec)
        elif short_name in recent_tables:
            recompute_recent.append(spec)
        else:
            keep.append(spec)

    return WindowPlan(
        keep=tuple(keep),
        compute_new=tuple(compute_new),
        recompute_empty=tuple(recompute_empty),
        recompute_retry=tuple(recompute_retry),
        recompute_recent=tuple(recompute_recent),
    )


def _select_recent_tables(
    window_specs: list[dict],
    existing_tables: set[str],
    recompute_last_n: int,
) -> set[str]:
    if recompute_last_n == 0:
        return set()

    by_size: dict[int, list[dict]] = {}
    for spec in window_specs:
        by_size.setdefault(int(spec["window_size"]), []).append(spec)

    recent_tables: set[str] = set()
    for specs in by_size.values():
        ordered_specs = sorted(specs, key=lambda item: (item["end_ym"], item["start_ym"]))
        recent_tables.update(
            short_name
            for spec in ordered_specs[-recompute_last_n:]
            if (short_name := _short_table_name(spec)) in existing_tables
        )
    return recent_tables


def _short_table_name(spec: dict) -> str:
    return str(spec["table_name"]).split(".")[-1]
