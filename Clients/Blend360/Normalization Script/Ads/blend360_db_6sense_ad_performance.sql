-- 6sense Ad Performance
TRUNCATE TABLE `x-marketing.blend360.db_6sense_ad_performance`;
INSERT INTO `x-marketing.blend360.db_6sense_ad_performance` (
  _campaign_id,	
  _advariation,	
  _adid,	
  _spend,	
  _clicks,	
  _impressions,	
  _date,	
  _campaign_name,	
  _ad_group,	
  _screenshot,	
  _adtype,	
  _platform,	
  _segment,	
  _month_year
)

WITH ads_li AS (
SELECT DISTINCT 
  _licampaignid AS _campaign_id,
  _name AS _advariation,
  _6senseliid AS _adid,
  CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
  CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
  CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
  CASE 
    WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
    WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date) 
  END AS _date,
FROM `x-marketing.blend360_mysql.db_6s_li_daily_campaign_performance`
WHERE _datatype = 'Ad' 
  AND _sdc_deleted_at IS NULL
),

airtable_fields_li AS (
SELECT DISTINCT 
  _campaignid AS _campaign_id, 
  _adid AS _ad_id,
  _adname,
  _campaignname AS _campaign_name,
  _adgroup AS _ad_group,
  _screenshot,
  _adtype,
  _platform,
  _segment
FROM `x-marketing.blend360_mysql.optimization_airtable_ads_linkedin`
),

combined_data_li AS (
SELECT
  ads.*,
  airtable_fields._campaign_name,
  airtable_fields._ad_group,
  airtable_fields._screenshot,
  airtable_fields._adtype,
  airtable_fields._platform,
  airtable_fields._segment,
  DATE_TRUNC(_date, MONTH) AS _month_year
FROM ads_li ads
JOIN airtable_fields_li airtable_fields
  ON ads._adid = airtable_fields._ad_id
),

ads_6sense AS (
SELECT DISTINCT 
  _campaignid AS _campaign_id,
  _name AS _advariation,
  _6senseid AS _adid,
  CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
  CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
  CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
  CASE 
    WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
    WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date) 
  END AS _date,
  FROM `x-marketing.blend360_mysql.db_6s_daily_campaign_performance`
  WHERE _datatype = 'Ad'
    AND _sdc_deleted_at IS NULL
  QUALIFY ROW_NUMBER() OVER (
  PARTITION BY _campaignid, _6senseid, _date 
  ORDER BY 
    CASE 
      WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate) 
      WHEN _extractdate LIKE '%-%' THEN PARSE_DATE('%F', _extractdate) 
    END
  ) = 1
),

airtable_fields_6sense AS (
SELECT DISTINCT 
  _campaignid AS _campaign_id, 
  _adid AS _ad_id,
  _adname,
  _campaignname AS _campaign_name,
  _adgroup AS _ad_group,
  _screenshot,
  _adtype,
  _platform,
  _segment
FROM `x-marketing.blend360_mysql.optimization_airtable_ads_6sense`
),

combined_data_6sense AS (
SELECT
  ads.*,
  airtable_fields._campaign_name,
  airtable_fields._ad_group,
  airtable_fields._screenshot,
  airtable_fields._adtype,
  airtable_fields._platform,
  airtable_fields._segment,
  DATE_TRUNC(_date, MONTH) AS _month_year
FROM ads_6sense ads
JOIN airtable_fields_6sense airtable_fields 
  ON ads._adid = airtable_fields._ad_id
  AND ads._campaign_id = airtable_fields._campaign_id
)
SELECT * FROM combined_data_li
UNION ALL
SELECT * FROM combined_data_6sense;

-- 6sense Account Performance
TRUNCATE TABLE `x-marketing.blend360.db_6sense_account_performance`;
INSERT INTO `x-marketing.blend360.db_6sense_account_performance` (
  _standardizedcompanyname,	
  new_industry,	
  icp_tier,	
  customer_segment,	
  _campaignid,	
  _campaignname,	
  _impressions,	
  _spent,	
  _clicks,	
  _date,	
  _platform
)

WITH airtable_6sense AS (
SELECT DISTINCT 
  _campaignid,
  _campaignname
FROM `x-marketing.blend360_mysql.optimization_airtable_ads_6sense`
),

data_6sense AS (
SELECT 
  std_name,
  master.new_industry,
  ad_name,
  master.icp_tier,
  master.customer_segment,
  CASE 
    WHEN _extractdate = '6/4/2024' THEN PARSE_DATE('%m/%d/%Y', '5/31/2024')
    WHEN _extractdate = '4/1/2024' THEN PARSE_DATE('%m/%d/%Y', '3/31/2024')
    ELSE PARSE_DATE('%m/%d/%Y', _extractdate) 
  END AS _date,
  SUM(CAST(REPLACE(_impressions, ',', '') AS INT64)) AS _impressions,
  SUM(CAST(_clicks AS INT64)) AS _clicks,
  CASE 
    WHEN _extractdate = '6/4/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
    WHEN _extractdate = '4/1/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
    ELSE EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', _extractdate)) 
  END AS year,
  CASE 
    WHEN _extractdate = '6/4/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
    WHEN _extractdate = '4/1/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
    ELSE EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', _extractdate)) 
  END AS month,
  acc._campaignID,
  airtable._campaignname,
  SUM(CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64)) AS _spent
FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` master
LEFT JOIN `x-marketing.blend360_mysql.db_6s_account_reached` acc 
  ON _6s_ad_name = _6sensecompanyname
LEFT JOIN airtable_6sense airtable 
  ON airtable._campaignid = acc._campaignid
WHERE _impressions IS NOT NULL 
  AND acc._sdc_deleted_at IS NULL
GROUP BY ALL
),

max_date_per_month_6sense AS (
SELECT 
  std_name,
  _campaignID,
  FORMAT_TIMESTAMP('%Y-%m', _date) AS month,
  MAX(_date) AS max_date
FROM data_6sense
GROUP BY 1,2,3
),

max_date_6sense AS (
SELECT 
  data_6sense.* EXCEPT (year, month),
  max_month.max_date
FROM data_6sense
JOIN max_date_per_month_6sense max_month
  ON data_6sense.std_name = max_month.std_name 
  AND data_6sense._campaignID = max_month._campaignID 
  AND data_6sense._date = max_month.max_date
),

lagged_6sense AS (
SELECT
  max_date_6sense.*,
  LAG(_impressions) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_impressions,
  LAG(_spent) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_spent,
  LAG(_clicks) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_clicks
FROM max_date_6sense
), 

combined_data_impr_6sense AS (
SELECT
  lagged_6sense.*,
  IFNULL(_impressions - prev_impressions, _impressions) AS change_impressions,
  IFNULL(_spent - prev_spent, _spent) AS change_spent,
  IFNULL(_clicks - prev_clicks, _clicks) AS change_clicks
FROM lagged_6sense
),

final_base_6sense AS (
SELECT
  std_name AS _standardizedcompanyname,
  new_industry,
  icp_tier,
  customer_segment,
  _campaignid,
  ANY_VALUE(_campaignname) AS _campaignname, 
  SUM(change_impressions) AS _impressions,
  SUM(change_spent) AS _spent,
  SUM(change_clicks) AS _clicks,
  _date,
  '6sense' AS _platform
FROM combined_data_impr_6sense
GROUP BY _standardizedcompanyname, new_industry, icp_tier, customer_segment, _date, _campaignid
),

airtable_linkedin AS (
SELECT DISTINCT 
  _campaignid,
  _campaignname
FROM `x-marketing.blend360_mysql.optimization_airtable_ads_linkedin`
),

data_li_6sense AS (
SELECT 
  std_name,
  master.new_industry,
  ad_name,
  master.icp_tier,
  master.customer_segment,
  CASE 
    WHEN _liextractdate = '6/4/2024' THEN PARSE_DATE('%m/%d/%Y', '5/31/2024')
    WHEN _liextractdate = '4/1/2024' THEN PARSE_DATE('%m/%d/%Y', '3/31/2024')
    ELSE PARSE_DATE('%m/%d/%Y', _liextractdate) 
  END AS _date,
  SUM(CAST(REPLACE(_impressions, ',', '') AS INT64)) AS _impressions,
  SUM(CAST(_clicks AS INT64)) AS _clicks,
  CASE 
    WHEN _liextractdate = '6/4/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
    WHEN _liextractdate = '4/1/2024' THEN EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
    ELSE EXTRACT(YEAR FROM PARSE_DATE('%m/%d/%Y', _liextractdate)) 
  END AS year,
  CASE 
    WHEN _liextractdate = '6/4/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '5/31/2024'))
    WHEN _liextractdate = '4/1/2024' THEN EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', '3/31/2024'))
    ELSE EXTRACT(MONTH FROM PARSE_DATE('%m/%d/%Y', _liextractdate)) 
  END AS month,
  acc._campaignID,
  airtable._campaignname,
  SUM(CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64)) AS _spent
FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` master
LEFT JOIN `x-marketing.blend360_mysql.db_6s_li_account_reached` acc 
  ON _6s_li_ad_name = _6sensecompanyname
LEFT JOIN airtable_linkedin airtable 
  ON airtable._campaignid = acc._campaignid
WHERE _impressions IS NOT NULL 
  AND acc._sdc_deleted_at IS NULL
GROUP BY ALL
),

max_date_per_month_li_6sense AS (
SELECT 
  std_name,
  _campaignID,
  FORMAT_TIMESTAMP('%Y-%m', _date) AS month,
  MAX(_date) AS max_date
FROM data_li_6sense
GROUP BY 1,2,3
),

max_date_li_6sense AS (
SELECT 
  data_li_6sense.* EXCEPT (year, month),
  max_date.max_date
FROM data_li_6sense
JOIN max_date_per_month_li_6sense max_date
  ON data_li_6sense.std_name = max_date.std_name 
  AND data_li_6sense._campaignID = max_date._campaignID 
  AND data_li_6sense._date = max_date.max_date
),

lagged_li_6sense AS (
SELECT
  max_date.*,
  LAG(_impressions) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_impressions,
  LAG(_spent) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_spent,
  LAG(_clicks) OVER (PARTITION BY std_name, _campaignID ORDER BY _date) AS prev_clicks
FROM max_date_li_6sense max_date
), 

combined_data_impr_li_6sense AS (
SELECT
  lagged_li_6sense.*,
  IFNULL(_impressions - prev_impressions, _impressions) AS change_impressions,
  IFNULL(_spent - prev_spent, _spent) AS change_spent,
  IFNULL(_clicks - prev_clicks, _clicks) AS change_clicks
FROM lagged_li_6sense
),

final_base_li_6sense AS (
SELECT
  std_name AS _standardizedcompanyname,
  new_industry,
  icp_tier,
  customer_segment,
  _campaignid,
  ANY_VALUE(_campaignname) AS _campaignname, 
  SUM(change_impressions) AS _impressions,
  SUM(change_spent) AS _spent,
  SUM(change_clicks) AS _clicks,
  _date,
  'LinkedIn' AS _platform
FROM combined_data_impr_li_6sense
GROUP BY _standardizedcompanyname, new_industry, icp_tier, customer_segment, _date, _campaignid
)

SELECT * 
FROM final_base_6sense
UNION ALL
SELECT *
FROM final_base_li_6sense