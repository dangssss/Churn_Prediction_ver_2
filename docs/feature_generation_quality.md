# Feature Generation Quality Gates

Feature generation writes operational quality audits to
`data_static.feature_generation_quality`.

## Audit Artifacts

| `window_table` value | Meaning |
|---|---|
| `__lifetime__` | Latest compatibility `data_static.cus_lifetime` validation |
| `data_window.cus_feature_{W}m_{YYMM}_{YYMM}` | Per-window validation and summary metrics |
| `__batch_consistency__` | Cross-window and lifetime consistency checks |

Each audit row is keyed by `(run_id, window_table)` and stores:

- `status`: `success` or `failed`
- `violations`: JSON array of failed quality rules
- `metrics`: JSON summary for diagnosis and drift analysis
- `checked_at`: audit timestamp

## Enforced Rules

Lifetime validation checks customer uniqueness, blank IDs, critical nulls,
non-finite values, ratio ranges, non-negative totals, and activity ranges.

Window validation checks customer uniqueness, blank IDs, critical nulls,
non-finite values, ratio ranges, non-negative totals, activity ranges,
`recency`, and embedded window metadata.

Batch consistency checks that nested windows ending in the same month do not
lose item or revenue totals as the window grows. It also checks the largest
current window against the matching row in
`data_static.cus_lifetime_snapshot`.

The pipeline fails before downstream dataset preparation if any enforced rule
fails.

## Activity Semantics

- Sliding-window `recency` is the inclusive ordinal day of the latest BCCP
  activity relative to the window start. For example, `2025-01-01` through
  `2025-02-02` yields `33`.
- `lifetime_months_active` is the number of distinct `report_month` values
  with `item_count > 0`, bounded from `2025-01-01` through the snapshot
  cutoff month.
- A no-service gap is a consecutive sequence of days without BCCP activity
  inside the sliding window. Leading and trailing window boundaries are
  included.
- `avg_noservice_days` is the average length of positive no-service gaps.
- `max_consecutive_inactive` is the longest no-service gap.
- If a customer has no BCCP activity in a window, both no-service features
  equal the full inclusive window duration.
