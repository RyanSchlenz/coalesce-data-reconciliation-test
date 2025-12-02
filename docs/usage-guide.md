# Usage Guide

## Installation

### Step 1: Copy Macro to Coalesce
1. Download `expect_table_data_reconciliation.sql` from this repo
2. In Coalesce, navigate to your workspace
3. Go to Build Settings > Macros
4. Create new macro or upload file
5. Save and verify syntax

### Step 2: Create Test in Node
1. Open the node you want to test
2. Navigate to Testing Configuration tab
3. Click "New Test"
4. Paste macro call with your parameters
5. Click "Run" to execute test

## Parameter Reference

### Required Parameters

#### source_table
**Type:** String  
**Description:** Fully qualified source table name


source_table='RAW.SALESFORCE_ORDERS'
source_table='FIVETRAN_DB.HUBSPOT.CONTACTS'


#### target_table
**Type:** String  
**Description:** Fully qualified target table name


target_table='ANALYTICS.ORDERS_FACT'
target_table='MART.CONTACTS_DIM'


#### columns_mapping
**Type:** List of dictionaries  
**Description:** Maps source columns to target columns for comparison


-- Simple mapping (same column names)
columns_mapping=[
    {'source': 'ID', 'target': 'ID'},
    {'source': 'NAME', 'target': 'NAME'}
]

-- Schema transformation mapping (different names)
columns_mapping=[
    {'source': 'CUSTOMER_ID', 'target': 'CUST_ID'},
    {'source': 'FULL_NAME', 'target': 'NAME'},
    {'source': 'ORDER_AMOUNT', 'target': 'AMOUNT'}
]


**Critical:** 
- Column order must match between source and target mappings
- Both source and target must have same number of columns
- Column data types should be compatible

### Optional Parameters

#### reference_table
**Type:** String  
**Default:** `none`  
**Description:** Reference table for temporal filtering


-- Use target as its own reference (most common)
reference_table='ANALYTICS.ORDERS_FACT'

-- Use separate reference snapshot
reference_table='ANALYTICS.ORDERS_REFERENCE'

-- Omit for no temporal filtering
-- reference_table parameter not included


#### source_id_column / target_id_column
**Type:** String  
**Default:** `'ID'`  
**Description:** Primary key column names


source_id_column='ORDER_ID'
target_id_column='ID'


**Use case:** When source and target have different primary key column names.

#### source_deleted_flag / target_deleted_flag
**Type:** String  
**Default:** `'_FIVETRAN_DELETED'`  
**Description:** Soft delete flag column names


-- Fivetran default
source_deleted_flag='_FIVETRAN_DELETED'
target_deleted_flag='_FIVETRAN_DELETED'

-- Custom soft delete
source_deleted_flag='IS_DELETED'
target_deleted_flag='DELETED_FLAG'

-- No soft delete column
-- Still provide column name, ensure it exists with FALSE values


**Critical:** These columns must exist and contain boolean values (TRUE/FALSE).

#### timestamp columns
**Type:** String  
**Default:** `'CREATED_AT'` and `'UPDATED_AT'`  
**Description:** Timestamp columns for temporal filtering


-- Standard naming
source_created_col='CREATED_AT'
source_updated_col='UPDATED_AT'
target_created_col='CREATED_AT'
target_updated_col='UPDATED_AT'

-- Custom naming
source_created_col='CREATED_DATE'
source_updated_col='LAST_MODIFIED_DATE'
target_created_col='CREATE_TS'
target_updated_col='UPDATE_TS'


**Required when:** Using reference_table parameter  
**Can ignore when:** Not using reference_table

#### tolerance_percentage
**Type:** Float  
**Default:** `1.0`  
**Description:** Percentage threshold for test failure


-- Fail if more than 1% missing
tolerance_percentage=1.0

-- Fail if more than 0.5% missing
tolerance_percentage=0.5

-- Zero tolerance - fail on any missing records
tolerance_percentage=0.0

-- More lenient - fail only if >5% missing
tolerance_percentage=5.0


**Use case:** 
- Set low (0.1-0.5%) for critical financial data
- Set medium (1.0-2.0%) for standard operational data
- Set high (5.0+%) for less critical or eventually consistent data

## Common Patterns

### Pattern 1: Basic Reconciliation (No Reference Table)

**Use when:** Pipeline is idle during testing or you want to catch everything


{{ expect_table_data_reconciliation(
    source_table='RAW.ORDERS',
    target_table='ANALYTICS.ORDERS',
    columns_mapping=[
        {'source': 'ID', 'target': 'ID'},
        {'source': 'AMOUNT', 'target': 'AMOUNT'},
        {'source': 'STATUS', 'target': 'STATUS'}
    ],
    tolerance_percentage=1.0
) }}


### Pattern 2: Reconciliation with Reference Table

**Use when:** Pipeline is actively loading and you want stable results


{{ expect_table_data_reconciliation(
    source_table='RAW.ORDERS',
    target_table='ANALYTICS.ORDERS',
    reference_table='ANALYTICS.ORDERS',  -- Same as target
    columns_mapping=[
        {'source': 'ID', 'target': 'ID'},
        {'source': 'AMOUNT', 'target': 'AMOUNT'}
    ],
    tolerance_percentage=0.5
) }}


### Pattern 3: Schema Transformation Reconciliation

**Use when:** Source and target have different column names


{{ expect_table_data_reconciliation(
    source_table='FIVETRAN.SALESFORCE_ACCOUNT',
    target_table='ANALYTICS.CUSTOMER_DIM',
    columns_mapping=[
        {'source': 'ID', 'target': 'CUSTOMER_ID'},
        {'source': 'NAME', 'target': 'CUSTOMER_NAME'},
        {'source': 'INDUSTRY', 'target': 'INDUSTRY_CODE'},
        {'source': 'ANNUAL_REVENUE', 'target': 'REVENUE'}
    ],
    source_id_column='ID',
    target_id_column='CUSTOMER_ID',
    tolerance_percentage=1.0
) }}


### Pattern 4: CDC Pipeline Reconciliation

**Use when:** Testing Fivetran or similar CDC replication


{{ expect_table_data_reconciliation(
    source_table='FIVETRAN.SALESFORCE_ORDERS',
    target_table='ANALYTICS.ORDERS_FACT',
    reference_table='ANALYTICS.ORDERS_FACT',
    columns_mapping=[
        {'source': 'ID', 'target': 'ORDER_ID'},
        {'source': 'ACCOUNT_ID', 'target': 'CUSTOMER_ID'},
        {'source': 'AMOUNT', 'target': 'ORDER_AMOUNT'},
        {'source': 'STATUS', 'target': 'ORDER_STATUS'}
    ],
    source_deleted_flag='_FIVETRAN_DELETED',
    target_deleted_flag='IS_DELETED',
    source_created_col='SYSTEMMODSTAMP',
    source_updated_col='SYSTEMMODSTAMP',
    tolerance_percentage=0.5
) }}


### Pattern 5: Custom Soft Delete Logic

**Use when:** Not using standard soft delete columns


{{ expect_table_data_reconciliation(
    source_table='RAW.CUSTOMERS',
    target_table='ANALYTICS.CUSTOMERS',
    columns_mapping=[
        {'source': 'ID', 'target': 'ID'},
        {'source': 'NAME', 'target': 'NAME'}
    ],
    source_deleted_flag='ACTIVE_FLAG',  -- TRUE = active, FALSE = deleted
    target_deleted_flag='IS_ACTIVE',
    tolerance_percentage=1.0
) }}


**Note:** Ensure your custom column uses TRUE for active records, FALSE for deleted.

## Reading Test Results

### Test Passes (Within Tolerance)

-- Empty result set
-- OR
-- Zero rows returned

**Meaning:** All records reconciled within tolerance threshold.

### Test Fails (Exceeds Tolerance)

ID | NAME | AMOUNT | total_source_records | total_missing_records | missing_percentage
---+------+--------+---------------------+----------------------+-------------------
123 | John | 500.00 | 10000 | 5 | 0.05
124 | Jane | 750.00 | 10000 | 5 | 0.05
...


**Columns:**
- **ID, NAME, AMOUNT** - The actual missing records from source
- **total_source_records** - Total records in source (after filtering)
- **total_missing_records** - Count of records in source but not in target
- **missing_percentage** - Percentage calculated as (missing/source * 100)

**Action:** Investigate why these specific records are missing in target.

## Troubleshooting

### Issue: Test returns unexpected results

**Check 1: Verify column mapping**

-- Run this to see actual columns
SELECT * FROM source_table LIMIT 1;
SELECT * FROM target_table LIMIT 1;

Ensure column names match exactly (case-sensitive).

**Check 2: Verify deleted flag logic**

-- Check if deleted flag exists and has correct values
SELECT deleted_flag, COUNT(*) 
FROM source_table 
GROUP BY deleted_flag;

Should return TRUE and FALSE, not NULL or other values.

**Check 3: Check timestamp columns (if using reference table)**

-- Verify timestamps exist and aren't NULL
SELECT 
  COUNT(*) as total,
  COUNT(created_at) as has_created,
  COUNT(updated_at) as has_updated
FROM source_table;


### Issue: Test is too slow

See Performance Considerations section in [README.md](../README.md) for optimization strategies.

### Issue: Too many false positives

**Solution:** Use reference_table parameter

reference_table='ANALYTICS.TARGET_TABLE'  -- Same as target


### Issue: Test always fails even when data looks correct

**Check tolerance setting:**

-- Maybe tolerance is too strict
tolerance_percentage=0.0  -- Change to 1.0 or higher


**Check for data type mismatches:**

-- String vs. number can cause EXCEPT to fail matching
-- Example:
-- Source: '123' (string)
-- Target: 123 (number)
-- These won't match even though they look the same


### Issue: Can't find certain records

**Check if records are truly missing or soft-deleted:**

-- In source
SELECT * FROM source_table WHERE id = 123;

-- In target
SELECT * FROM target_table WHERE id = 123;

-- Check deleted flags
SELECT id, _fivetran_deleted FROM source_table WHERE id = 123;
SELECT id, is_deleted FROM target_table WHERE id = 123;


## Best Practices

1. **Start with small column count** - Test 3-5 key columns first, expand if needed
2. **Use reference table for active pipelines** - Eliminates false positives
3. **Set appropriate tolerance** - Balance between catching issues and noise
4. **Test on schedule, not on every build** - Conserve warehouse resources
5. **Monitor test performance** - Set alerts for slow-running tests
6. **Document expected missing percentage** - Some lag is normal, know your baseline
7. **Investigate failures promptly** - Don't let reconciliation drift go unnoticed