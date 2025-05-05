TRUNCATE TABLE `x-marketing.data_catalog.data_catalog` ;

INSERT INTO `x-marketing.data_catalog.data_catalog` (
  _raw_dataset,
  _table,
  _field,
  _crm_source,
  _data_type,
  table_created,
  _client_name
)
WITH column_name AS (
  SELECT
    table_schema AS _raw_dataset,
    table_name AS _table,
    field_path AS _field,
    CASE
      WHEN table_schema LIKE '%_hubspot' THEN 'hubspot'
      WHEN table_schema LIKE '%_salesforce' THEN 'salesforce'
      WHEN table_schema LIKE '%linkedin%' THEN 'linkedin'
      WHEN table_schema LIKE '%google%' THEN 'google'
      WHEN table_schema LIKE '%_pardot' THEN 'pardot'
      WHEN table_schema LIKE '%_marketo' THEN 'marketo'
      WHEN table_schema LIKE '%bing%' THEN 'bing'
      WHEN table_schema LIKE '%facebook%' THEN 'facebook'
      WHEN table_schema LIKE '%sfmc%' THEN 'sfmc'
      WHEN table_schema LIKE '%mailchimp%' THEN 'mailchimp'
      ELSE table_schema
    END AS _crm_source,
    data_type AS _data_type
  FROM `x-marketing.region-us.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
  WHERE data_type NOT LIKE 'STRUCT%'
    AND data_type NOT LIKE '%ARRAY%'
    AND field_path NOT LIKE '%sdc%'
    AND table_schema NOT LIKE '%_mysql'
    AND table_schema NOT LIKE 'analytic%'
    AND table_name NOT LIKE '_sdc_%'
    AND table_schema NOT LIKE '%_sheet%'
),
table_created_time AS (
  SELECT DISTINCT
    table_schema,
    table_name,
    TIMESTAMP(creation_time) AS table_created
  FROM `x-marketing.region-us.INFORMATION_SCHEMA.TABLES`
),
main_data AS (
  SELECT
    column_name.*,
    table_created_time.table_created
  FROM column_name
  LEFT JOIN table_created_time
    ON column_name._raw_dataset = table_created_time.table_schema
    AND column_name._table = table_created_time.table_name
)
SELECT 
  *,
  SPLIT(_raw_dataset, '_')[OFFSET(0)] AS _client_name
FROM main_data
WHERE _crm_source IN ('hubspot', 'salesforce', 'linkedin', 'google', 'pardot', 'marketo', 'bing', 'facebook', 'sfmc', 'mailchimp');