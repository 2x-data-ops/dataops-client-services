----6sense ad performance
TRUNCATE TABLE `x-marketing.bluemantis.db_6sense_ad_performance`;
INSERT INTO `x-marketing.bluemantis.db_6sense_ad_performance` (
  _campaign_id,
  _ad_variation,
  _ad_id,
  _spend,
  _clicks,
  _impressions,
  _date,
  _campaign_name,
  _campaign_type,
  _campaign_status,
  _start_date,
  _end_date,
  _newly_engaged_accounts,
  _increased_engagement_accounts,
  _segment,
  _ad_group,
  _screenshot,
  _target_accounts,
  _reached_accounts,
  _occurrence,
  _reduced_newly_engaged_accounts,
  _reduced_increased_engagement_accounts,
  _reduced_target_accounts,
  _reduced_reached_accounts
)
WITH ads AS (
  SELECT DISTINCT 
    _campaignid AS _campaign_id,
    _name AS _ad_variation,
    _6senseid AS _ad_id,
    CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
    CAST(REPLACE(_clicks, '.0', '') AS INTEGER) AS _clicks,
    CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions,
    CASE 
      WHEN _date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _date)
      WHEN _date LIKE '%-%' THEN PARSE_DATE('%F', _date)
    END AS _date
  FROM `x-marketing.bluemantis_mysql.db_campaign_daily_performance`
  WHERE _datatype = 'Ad'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _campaignid, _6senseid, _date 
    ORDER BY _extractdate) = 1
),
-- Get campaign level fields
campaign_fields AS (
  SELECT
    _campaignid AS _campaign_id,
    _segment,
    CASE
      WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _extractdate)
      ELSE PARSE_DATE('%F', _extractdate)
    END AS _extractdate,
    CASE 
      WHEN _startDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _startDate)
      WHEN _startDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _startDate)
    END AS _start_date,
    CASE 
      WHEN _endDate LIKE '%/%' THEN PARSE_DATE('%m/%d/%Y', _endDate)
      WHEN _endDate LIKE '%-%' THEN PARSE_DATE('%d-%h-%y', _endDate)
    END AS _end_date,
    _status AS _campaign_status,
    _name AS _campaign_name,
    _campaigntype AS _campaign_type,
    CASE 
      WHEN _accountsnewlyengagedlifetime = '-' THEN 0
      ELSE SAFE_CAST(_accountsnewlyengagedlifetime AS INT64)
    END AS _newly_engaged_accounts,
    CASE 
      WHEN _accountswithincreasedengagementlifetime = '-' THEN 0
      ELSE SAFE_CAST(_accountswithincreasedengagementlifetime AS INT64)
    END AS _increased_engagement_accounts
  FROM `x-marketing.bluemantis_mysql.db_campaign_daily_performance`
  WHERE _datatype = 'Campaign'
  QUALIFY ROW_NUMBER () OVER (
    PARTITION BY _campaignid
    ORDER BY _extractdate DESC) = 1
),
ads_campaign_combined AS (
  SELECT 
    ads.* EXCEPT (_campaign_id),
    campaign_fields._campaign_id,
    campaign_fields._campaign_name,
    campaign_fields._campaign_type,
    campaign_fields._campaign_status,
    campaign_fields._start_date,
    campaign_fields._end_date,
    campaign_fields._newly_engaged_accounts,
    campaign_fields._increased_engagement_accounts,
    campaign_fields._segment
  FROM ads
  JOIN campaign_fields 
    ON ads._campaign_id = campaign_fields._campaign_id
),
airtable_fields AS (
  SELECT DISTINCT 
    _campaign_id, 
    _ad_id,
    _ad_name,
    _ad_group AS _ad_group,
    '' AS _screenshot
  FROM `x-marketing.bluemantis_google_sheets.db_ads_optimization`
  WHERE _campaign_id != ''
),
combined_data AS (
  SELECT
    ads_campaign_combined.*,
    airtable_fields._ad_group,
    airtable_fields._screenshot
  FROM ads_campaign_combined
  LEFT JOIN airtable_fields
    ON ads_campaign_combined._ad_id = airtable_fields._ad_id
    AND ads_campaign_combined._campaign_id = airtable_fields._campaign_id
  LEFT JOIN campaign_fields
    ON ads_campaign_combined._campaign_id = campaign_fields._campaign_id
),
-- Add campaign numbers to each ad
unique_target_list AS (
  SELECT DISTINCT 
    main._6sensecompanyname,
    main._6sensecountry,
    main._6sensedomain,
    main._segmentname,
    side._campaign_id
  FROM `x-marketing.bluemantis_mysql.db_target_account` main
  JOIN `x-marketing.bluemantis_google_sheets.db_ads_optimization` side
    ON main._segmentname = side._business_segment
),
campaign_number_target AS (
  SELECT DISTINCT 
    _campaign_id,
    COUNT(*) AS _target_accounts
  FROM unique_target_list
  GROUP BY 1
),
unique_reach_list AS (
  SELECT main.*
  FROM unique_target_list AS main
  JOIN `x-marketing.bluemantis_mysql.db_campaign_accounts_reached` extra
    ON main._6sensecompanyname = extra._6sensecompanyname
    AND main._6sensecountry = extra._6sensecountry
    AND main._6sensedomain = extra._6sensedomain
    AND main._campaign_id = extra._campaignid
),
campaign_number_reach AS (
  SELECT DISTINCT 
    _campaign_id,
    COUNT(*) AS _reached_accounts
  FROM unique_reach_list
  GROUP BY 1
),
campaign_numbers AS (
  SELECT
    *
  FROM combined_data 
  LEFT JOIN campaign_number_target
    USING (_campaign_id)
  LEFT JOIN campaign_number_reach
    USING (_campaign_id)
),
-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (
  SELECT
    *,
    COUNT(*) OVER (PARTITION BY _campaign_id) AS _occurrence
  FROM campaign_numbers
),
-- Reduced the campaign numbers by the occurrence
reduced_campaign_numbers AS (
  SELECT
    *,
    _newly_engaged_accounts / _occurrence AS _reduced_newly_engaged_accounts,
    _increased_engagement_accounts / _occurrence AS _reduced_increased_engagement_accounts,
    _target_accounts / _occurrence AS _reduced_target_accounts,
    _reached_accounts / _occurrence AS _reduced_reached_accounts,
  FROM total_ad_occurrence_per_campaign
)
SELECT 
  * 
FROM reduced_campaign_numbers;