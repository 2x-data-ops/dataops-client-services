CREATE OR REPLACE TABLE `x-marketing.data_catalog.data_catalog` AS  
WITH main_data AS (
  WITH column_name AS (
    SELECT
      table_schema AS raw_dataset,
      table_name AS table,
      field_path AS field,
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
      END AS crm_source,
      data_type AS data_type
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
      FORMAT_TIMESTAMP('%F %R', TIMESTAMP(creation_time)) AS table_created
    FROM `x-marketing.region-us.INFORMATION_SCHEMA.TABLES`
  )
  SELECT
    column_name.*,
    table_created_time. * EXCEPT (table_schema, table_name)
  FROM column_name
  LEFT JOIN table_created_time
    ON column_name.raw_dataset = table_created_time.table_schema
    AND column_name.table = table_created_time.table_name
),
crm_combined AS (
  WITH hubspot_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'hubspot'
  ),
  salesforce_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'salesforce'
  ),
  linkedin_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'linkedin'
  ),
  google_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'google'
  ),
  pardot_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'pardot'
  ),
  marketo_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'marketo'
  ),
  bing_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'bing'
  ),
  facebook_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'facebook'
  ),
  sfmc_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'sfmc'
  ),
  mailchimp_source AS (
    SELECT *
    FROM main_data
    WHERE crm_source = 'mailchimp'
  )
  SELECT * FROM hubspot_source
  UNION ALL
  SELECT * FROM salesforce_source
  UNION ALL
  SELECT * FROM linkedin_source
  UNION ALL
  SELECT * FROM google_source
  UNION ALL
  SELECT * FROM pardot_source
  UNION ALL
  SELECT * FROM marketo_source
  UNION ALL
  SELECT * FROM bing_source
  UNION ALL
  SELECT * FROM facebook_source
  UNION ALL
  SELECT * FROM sfmc_source
  UNION ALL
  SELECT * FROM mailchimp_source
)
SELECT *,
  SPLIT(raw_dataset, '_')[OFFSET(0)] AS client_name
FROM crm_combined