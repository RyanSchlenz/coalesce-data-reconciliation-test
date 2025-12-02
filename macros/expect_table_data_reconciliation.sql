{#
  Macro: expect_table_data_reconciliation
  
  Purpose: Performs full data reconciliation between source and target tables,
           identifying specific missing records rather than just count mismatches.
  
  Production Status: In use for 6+ months at PetIQ
  
  Key Features:
  - Uses EXCEPT to identify actual missing records
  - Optional reference table pattern for temporal filtering
  - Configurable column mapping for schema differences
  - Tolerance-based alerting
  - Soft delete handling for CDC patterns
  
  Performance Warning: 
  EXCEPT operations can be expensive on large tables. Test with representative
  data volumes before production use. Consider adding WHERE clauses to limit
  comparison window for very large tables.
  
  Parameters:
    source_table (string, required): 
      Source table reference (e.g., 'RAW.SOURCE_TABLE')
    
    target_table (string, required): 
      Target table reference (e.g., 'ANALYTICS.TARGET_TABLE')
    
    columns_mapping (list, required): 
      List of dicts mapping source to target columns
      Example: [{'source': 'SOURCE_ID', 'target': 'ID'}]
    
    reference_table (string, optional, default=none): 
      Reference table for temporal filtering. When provided, only compares
      records that existed before the latest timestamp in reference table.
      This prevents false positives during active loads.
    
    source_id_column (string, optional, default='ID'): 
      Primary key column name in source table
    
    target_id_column (string, optional, default='ID'): 
      Primary key column name in target table
    
    source_deleted_flag (string, optional, default='_FIVETRAN_DELETED'): 
      Soft delete flag column in source table
    
    target_deleted_flag (string, optional, default='_FIVETRAN_DELETED'): 
      Soft delete flag column in target table
    
    source_created_col (string, optional, default='CREATED_AT'): 
      Creation timestamp column in source table
    
    source_updated_col (string, optional, default='UPDATED_AT'): 
      Update timestamp column in source table
    
    target_created_col (string, optional, default='CREATED_AT'): 
      Creation timestamp column in target table
    
    target_updated_col (string, optional, default='UPDATED_AT'): 
      Update timestamp column in target table
    
    reference_created_col (string, optional, default='CREATED_AT'): 
      Creation timestamp column in reference table
    
    reference_updated_col (string, optional, default='UPDATED_AT'): 
      Update timestamp column in reference table
    
    tolerance_percentage (float, optional, default=1.0): 
      Percentage threshold for alerting. Test fails only when missing
      percentage exceeds this value. Set to 0.0 for zero tolerance.
  
  Returns:
    SQL query that returns missing records when tolerance is exceeded.
    Empty result set when within tolerance.
    
    Output columns:
    - All mapped columns from source table
    - total_source_records: Total count in source
    - total_missing_records: Count of missing records
    - missing_percentage: Percentage of records missing
  
  Example Usage:
    
    Basic reconciliation:
    {{ expect_table_data_reconciliation(
        source_table='RAW.ORDERS',
        target_table='ANALYTICS.ORDERS',
        columns_mapping=[
            {'source': 'ORDER_ID', 'target': 'ID'},
            {'source': 'CUSTOMER_ID', 'target': 'CUSTOMER_ID'},
            {'source': 'AMOUNT', 'target': 'AMOUNT'}
        ],
        source_id_column='ORDER_ID',
        target_id_column='ID',
        tolerance_percentage=0.5
    ) }}
    
    With reference table:
    {{ expect_table_data_reconciliation(
        source_table='RAW.ORDERS',
        target_table='ANALYTICS.ORDERS',
        reference_table='ANALYTICS.ORDERS_REFERENCE',
        columns_mapping=[
            {'source': 'ID', 'target': 'ID'},
            {'source': 'AMOUNT', 'target': 'AMOUNT'}
        ],
        tolerance_percentage=1.0
    ) }}
#}

{%- macro expect_table_data_reconciliation(
    source_table, 
    target_table, 
    columns_mapping,
    reference_table=none,
    source_id_column='ID', 
    target_id_column='ID',
    source_deleted_flag='_FIVETRAN_DELETED',
    target_deleted_flag='_FIVETRAN_DELETED',
    source_created_col='CREATED_AT',
    source_updated_col='UPDATED_AT',
    target_created_col='CREATED_AT', 
    target_updated_col='UPDATED_AT',
    reference_created_col='CREATED_AT',
    reference_updated_col='UPDATED_AT',
    tolerance_percentage=1.0
) -%}

{# Branch logic based on whether reference table is provided #}
{% if reference_table %}
{# WITH REFERENCE TABLE: Compare only records that existed before reference table's latest timestamp #}
WITH latest_target_timestamps AS (
  SELECT
    MAX({{ reference_created_col }}) AS max_created_at,
    MAX({{ reference_updated_col }}) AS max_updated_at
  FROM {{ reference_table }}
),

source_records AS (
  SELECT 
      {{ source_id_column }}{% if columns_mapping %},{% endif %}
      {% for mapping in columns_mapping %}
      {{ mapping['source'] }}{{ "," if not loop.last }}
      {% endfor %}
  FROM {{ source_table }}
  WHERE 
      {{ source_deleted_flag }} = FALSE
      AND {{ source_created_col }} IS NOT NULL
      AND {{ source_updated_col }} IS NOT NULL
      AND {{ source_created_col }} < (SELECT max_created_at FROM latest_target_timestamps)
      AND {{ source_updated_col }} < (SELECT max_updated_at FROM latest_target_timestamps)
),

target_records AS (
  SELECT 
      {{ target_id_column }}{% if columns_mapping %},{% endif %}
      {% for mapping in columns_mapping %}
      {{ mapping['target'] }}{{ "," if not loop.last }}
      {% endfor %}
  FROM ({{ target_table }})
  WHERE
      {{ target_deleted_flag }} = FALSE
      AND {{ target_created_col }} IS NOT NULL
      AND {{ target_updated_col }} IS NOT NULL
      AND {{ target_created_col }} < (SELECT max_created_at FROM latest_target_timestamps)
      AND {{ target_updated_col }} < (SELECT max_updated_at FROM latest_target_timestamps)
),
{% else %}
{# WITHOUT REFERENCE TABLE: Compare all non-deleted records #}
WITH source_records AS (
  SELECT 
      {{ source_id_column }}{% if columns_mapping %},{% endif %}
      {% for mapping in columns_mapping %}
      {{ mapping['source'] }}{{ "," if not loop.last }}
      {% endfor %}
  FROM {{ source_table }}
  WHERE {{ source_deleted_flag }} = FALSE
),

target_records AS (
  SELECT 
      {{ target_id_column }}{% if columns_mapping %},{% endif %}
      {% for mapping in columns_mapping %}
      {{ mapping['target'] }}{{ "," if not loop.last }}
      {% endfor %}
  FROM ({{ target_table }})
  WHERE {{ target_deleted_flag }} = FALSE
),
{% endif %}

{# Identify records in source but not in target using EXCEPT #}
missing_records AS (
  SELECT * FROM source_records
  EXCEPT
  SELECT * FROM target_records
),

{# Calculate counts for reporting #}
source_count AS (
  SELECT COUNT(*) AS total_source_records
  FROM source_records
),

missing_count AS (
  SELECT COUNT(*) AS total_missing_records
  FROM missing_records
),

{# Calculate missing percentage and compare to tolerance #}
discrepancy_check AS (
  SELECT 
    sc.total_source_records,
    mc.total_missing_records,
    CASE 
      WHEN sc.total_source_records = 0 THEN 0
      ELSE (mc.total_missing_records * 100.0) / sc.total_source_records
    END AS missing_percentage
  FROM source_count sc
  CROSS JOIN missing_count mc
)

{# Return missing records only if they exceed tolerance threshold #}
SELECT 
  mr.*,
  dc.total_source_records,
  dc.total_missing_records,
  dc.missing_percentage
FROM missing_records mr
CROSS JOIN discrepancy_check dc
WHERE dc.missing_percentage >= {{ tolerance_percentage }}

{%- endmacro -%}