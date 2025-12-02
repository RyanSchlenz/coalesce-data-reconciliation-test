# Coalesce Data Reconciliation Test

A production-tested macro for Coalesce that performs full data reconciliation between source and target tables, identifying specific missing records rather than just row count mismatches.

## The Problem

Existing Coalesce test utilities can tell you that row counts don't match between tables, but they can't tell you **which records are missing or why**. When your ETL pipeline drops records, you need to know:
- Which specific records are missing
- What percentage of records are affected
- Whether the issue exceeds acceptable tolerance thresholds

## The Solution

This macro uses SQL `EXCEPT` to identify actual missing records between source and target tables, with:
- **Column mapping** - Handle source-to-target column name differences
- **Temporal filtering** - Optional reference table pattern to avoid false positives during active loads
- **Soft delete awareness** - Configurable deleted flag columns for CDC patterns
- **Tolerance thresholds** - Only alert when discrepancy exceeds defined percentage
- **Diagnostic output** - Returns missing records with full context for investigation

## Production Status

**In production use for 6+ months** at PetIQ for validating transformation pipelines between ingestion and consumption layers.

## Installation

1. Copy `expect_table_data_reconciliation.sql` to your Coalesce workspace macros (Build Settings > Macros)
2. Reference it in your node tests
3. See [usage-guide.md](usage-guide.md) for implementation details

## Quick Start

### Basic Reconciliation


{{ expect_table_data_reconciliation(
    source_table='RAW.SOURCE_TABLE',
    target_table='ANALYTICS.TARGET_TABLE',
    columns_mapping=[
        {'source': 'SOURCE_ID', 'target': 'ID'},
        {'source': 'SOURCE_NAME', 'target': 'NAME'},
        {'source': 'SOURCE_VALUE', 'target': 'VALUE'}
    ],
    source_id_column='SOURCE_ID',
    target_id_column='ID',
    tolerance_percentage=1.0
) }}


### With Reference Table (Recommended for Active Pipelines)


{{ expect_table_data_reconciliation(
    source_table='RAW.SOURCE_TABLE',
    target_table='ANALYTICS.TARGET_TABLE',
    reference_table='ANALYTICS.REFERENCE_TABLE',
    columns_mapping=[
        {'source': 'ID', 'target': 'ID'},
        {'source': 'NAME', 'target': 'NAME'}
    ],
    tolerance_percentage=0.5
) }}


See [usage-guide.md](usage-guide.md) for detailed examples and patterns.

## Key Features

### 1. Actual Record Identification
Unlike row count comparisons, this returns the actual missing records:

ID | NAME | VALUE | total_source_records | total_missing_records | missing_percentage
123 | Test | 456 | 10000 | 5 | 0.05


### 2. Reference Table Pattern
Prevents false positives by only comparing records that have been fully processed. 

**The Problem:** When testing on actively loading pipelines, you get false positives for records still in-flight.

**The Solution:** Use a reference table (typically the target table itself) to establish a stable comparison window. The test only compares source records that existed before the reference table's latest timestamp.

**Example:**

-- Reference table last updated: 9:55 AM
-- Only compares records created/updated before 9:55 AM
-- Ignores records from 9:55 AM onwards (still in-flight)


**When to use:**
- Pipeline runs continuously or frequently (every 5-60 minutes)
- You need stable, repeatable test results
- False positives from in-flight data are a problem

**Most common pattern:** Use target table as its own reference

reference_table='ANALYTICS.TARGET_TABLE'  -- Same as target_table


### 3. Column Mapping
Handle schema differences between source and target:

columns_mapping=[
    {'source': 'CUSTOMER_ID', 'target': 'CUST_ID'},
    {'source': 'FULL_NAME', 'target': 'NAME'}
]


### 4. Tolerance-Based Alerting
Only fails when discrepancy exceeds threshold, preventing noise from acceptable data lag:
- `tolerance_percentage=1.0` - Fail if >1% of records missing
- `tolerance_percentage=0.0` - Fail on any missing records

## Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `source_table` | Yes | - | Source table reference |
| `target_table` | Yes | - | Target table reference |
| `columns_mapping` | Yes | - | List of source-to-target column mappings |
| `reference_table` | No | `none` | Reference table for temporal filtering |
| `source_id_column` | No | `'ID'` | Source table ID column |
| `target_id_column` | No | `'ID'` | Target table ID column |
| `source_deleted_flag` | No | `'_FIVETRAN_DELETED'` | Source soft delete column |
| `target_deleted_flag` | No | `'_FIVETRAN_DELETED'` | Target soft delete column |
| `source_created_col` | No | `'CREATED_AT'` | Source creation timestamp |
| `source_updated_col` | No | `'UPDATED_AT'` | Source update timestamp |
| `target_created_col` | No | `'CREATED_AT'` | Target creation timestamp |
| `target_updated_col` | No | `'UPDATED_AT'` | Target update timestamp |
| `reference_created_col` | No | `'CREATED_AT'` | Reference creation timestamp |
| `reference_updated_col` | No | `'UPDATED_AT'` | Reference update timestamp |
| `tolerance_percentage` | No | `1.0` | Alert threshold (% of source records) |

## Performance Considerations

**CRITICAL:** `EXCEPT` operations can be expensive on large tables.

### Table Size Guidelines
| Table Size | Execution Time | Recommendation |
|------------|----------------|----------------|
| <100K rows | <5 seconds | Safe for frequent testing |
| 100K-1M | 10-60 seconds | Schedule during test runs |
| 1M-10M | 1-5 minutes | Use sparingly, maybe nightly |
| >10M rows | 5+ minutes | Add filtering or partition testing |

### Optimization Strategies
1. **Reduce column count** - Only compare critical columns (fastest optimization)
2. **Limit comparison window** - Add date filters to reduce data volume
3. **Use appropriate schedule** - Don't run on every build for large tables
4. **Add indexes** - On ID columns, deleted flags, and timestamp columns
5. **Consider partitioning** - For tables >10M rows, test by partition or date range

### When NOT to Use
- Real-time validation (too slow)
- Tables >50M rows (without optimization)
- High-frequency testing on large tables
- Wide tables (>50 columns) unless you reduce column count

## Use Cases

1. **ETL Pipeline Validation** - Verify no records lost during transformation
2. **CDC Pipeline Monitoring** - Ensure all source changes propagate to target
3. **Historical Load Verification** - Validate full table reloads
4. **Cross-Environment Reconciliation** - Compare dev/prod data consistency

## Limitations

- **Not for real-time validation** - Best suited for batch/scheduled testing
- **Performance scales with table size** - Test on representative data volumes first
- **Requires timestamp columns** - For reference table pattern (optional otherwise)
- **Fivetran-centric defaults** - Column names optimized for Fivetran CDC but fully configurable

## Documentation

- [usage-guide.md](usage-guide.md) - Complete parameter reference, patterns, and troubleshooting

## Contributing

Issues and pull requests welcome. This macro is actively maintained and used in production at PetIQ.


