# Archived SQL Server Files

These files were used when the tool-sync system read TipQA data directly from
SQL Server via JTDS JDBC. As of April 2026 the data source was migrated to
Databricks Unity Catalog (`manufacturing.bronze_tipqa.gt_master`).

## Contents

- `jtds-1.3.1.jar` -- JTDS JDBC driver for SQL Server
- `tipqa_tools.sql` -- Full TipQA query (SQL Server T-SQL dialect)
- `tipqa_tools_subset.sql` -- Parameterized subset query (SQL Server T-SQL dialect)
