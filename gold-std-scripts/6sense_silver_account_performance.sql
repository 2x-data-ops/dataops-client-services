-- 6sense Account Performance
TRUNCATE TABLE `x-marketing.bluemantis.db_6sense_account_performance`;
INSERT INTO `x-marketing.bluemantis.db_6sense_account_performance` (
  _6sense_company_name,
  _6sense_country,
  _6sense_domain,
  _business_segment,
  _campaign_id,
  _campaign_name,
  _is_reached,
  _has_clicks,
  _has_impressions,
  _account_spend,
  _impressions,
  _clicks,
  _batch_id,
  _platform
)
-- Get all target accounts and their campaigns
WITH campaign_data AS (
  SELECT 
    li_id.* EXCEPT(_campaign_id),
    sixsense_id._campaignid
  FROM `x-marketing.bluemantis_google_sheets.db_ads_optimization` li_id
  JOIN `x-marketing.bluemantis_mysql.db_campaign_daily_performance` sixsense_id
    ON li_id._campaign_id = sixsense_id._campaignid
),
target_accounts AS (
  SELECT DISTINCT
    main._6sensecompanyname AS _6sense_company_name,
    main._6sensecountry AS _6sense_country,
    main._6sensedomain AS _6sense_domain,            
    campaign_data._business_segment,
    campaign_data._campaignid AS _campaign_id,
    campaign_data._campaign_name
  FROM `x-marketing.bluemantis_mysql.db_target_account` main
  JOIN campaign_data
    ON main._segmentname = campaign_data._business_segment
),
-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (
  SELECT DISTINCT 
    main.*,
    CASE 
      WHEN side._campaignid IS NOT NULL THEN TRUE
    END AS _is_reached,
    CASE
      WHEN CAST(REPLACE(side._clicks, ',', '') AS INTEGER) > 0 THEN TRUE 
    END AS _has_clicks,
    CASE 
      WHEN CAST(REPLACE(side._impressions, ',', '') AS INTEGER) > 0 THEN TRUE 
    END AS _has_impressions,
    CAST(REGEXP_EXTRACT(side._spend, r'[\d.]+') AS FLOAT64) AS _account_spend,
    CAST(REPLACE(side._impressions, ',', '') AS INT64) AS _impressions,
    CAST(REPLACE(side._clicks, ',', '') AS INT64) AS _clicks,
    side._batchid AS _batch_id,
    "6sense" AS _platform
  FROM target_accounts AS main
  LEFT JOIN `x-marketing.bluemantis_mysql.db_campaign_accounts_reached` side 
    ON main._6sense_company_name = side._6sensecompanyname
    AND main._6sense_country = side._6sensecountry
    AND main._6sense_domain = side._6sensedomain
    AND main._campaign_id = side._campaignid
),
--Just select the latest batch
latest_batch AS (
  SELECT 
    MAX(_batchid) AS _batch_id
  FROM `x-marketing.bluemantis_mysql.db_campaign_accounts_reached`
)
SELECT 
  reached.* 
FROM reached_accounts AS reached
JOIN latest_batch AS batch
  ON reached._batch_id = batch._batch_id;