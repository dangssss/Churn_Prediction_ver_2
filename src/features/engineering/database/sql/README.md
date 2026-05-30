# Feature Generation SQL

This directory is the canonical owner of SQL templates used by feature
generation.

Runtime loading is implemented in
`features.engineering.feature_gen.template_engine`. Keep feature-generation
SQL changes here only. The former duplicate templates under
`data/preprocessing/database/sql` were removed because they were not loaded at
runtime and the sliding aggregate had diverged from the optimized staging-table
implementation.
