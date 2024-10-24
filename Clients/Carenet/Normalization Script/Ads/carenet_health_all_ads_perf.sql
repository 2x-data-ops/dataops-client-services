CREATE OR REPLACE TABLE `x-marketing.carenet_health.all_ads_performance` AS 
--+/ _google_sem \+--
WITH carenet_health_ads_google AS (
  SELECT 
    activity.day AS _timestamp, 
    activity.campaign_id AS _campaign_id, 
    activity.ad_group_id AS _ad_group_id, 
    ad_id AS _ad_id, 
    ad_group_name AS _ad_group_name, 
    activity.campaign_name AS _campaign_name,
    ads.name AS _ad_name,
    campaign.start_date AS _start_date,
    campaign.end_date AS _end_date,
    'Google SEM' AS _platform, 
    'Carenet Health' AS _client,
    'Ads' AS _type,
    CASE 
      WHEN activity.campaign_name LIKE '%Website Traffic%' THEN 'Website_Traffic'
      WHEN activity.campaign_name LIKE 'Search || NB || Pentesting' THEN 'Lead_Generation' 
      ELSE 'Lead_Generation' 
    END AS _campaign_objective,
    '' AS _landing_page,
    '' AS _screenshot,
    INITCAP(ads.type,'_') AS _ads_type,
    activity.cost AS _spent, 
    activity.impressions AS _impressions, 
    activity.clicks AS _clicks, 
    activity.conversions AS _conversions, 
    NULL AS _leads,
    NULL AS _landingpageclick
  FROM `x-marketing.carenet_health.google_search_adsvariation_performance` activity
  LEFT JOIN `x-marketing.carenet_google_ads.ads` ads 
    ON ads.id = activity.ad_id
  JOIN `x-marketing.carenet_google_ads.campaigns` campaign 
    ON campaign.id = activity.campaign_id
),
carenet_health_campaign_google AS (
  SELECT 
    activity.day AS _timestamp, 
    activity.campaign_id AS _campaign_id, 
    NULL AS _ad_group_id, 
    NULL AS _ad_id, 
    '' AS _ad_group_name, 
    activity.campaign_name AS _campaign_name,
    '' AS _ad_name,
    campaign.start_date AS _start_date,
    campaign.end_date AS _end_date,
    'Google SEM' AS _platform, 
    'PlexTrac' AS _client,
    'Campaign' AS _type,
    CASE 
      WHEN activity.campaign_name LIKE '%Website Traffic%' THEN 'Website_Traffic'
      WHEN activity.campaign_name LIKE 'Search || NB || Pentesting' THEN 'Lead_Generation' 
      ELSE 'Lead_Generation' 
    END AS _campaign_objective,
    '' AS _landing_page,
    '' AS _screenshot,
    '' AS _ads_type,
    activity.cost AS _spent, 
    activity.impressions AS _impressions, 
    activity.clicks AS _clicks, 
    activity.conversions AS _conversions, 
    NULL AS _leads,
    NULL AS _landingpageclick
  FROM `x-marketing.carenet_health.google_search_campaign_performance` activity
  JOIN `x-marketing.carenet_google_ads.campaigns` campaign 
    ON campaign.id = activity.campaign_id
),
_google_sem AS (
  SELECT DISTINCT *
  FROM (
    SELECT * FROM carenet_health_ads_google
    UNION ALL
    SELECT * FROM carenet_health_campaign_google
  )
),
--+/ _6sense \+--
carenet_ads_6sense AS (
  SELECT 
    CAST(PARSE_DATE('%m/%e/%Y', _6sense._date) AS TIMESTAMP) AS _timestamp, 
    CAST(_6sense._campaignid AS INT64) AS _campaign_id, 
    -- _adgroupid AS _ad_group_id, 
    CAST(NULL AS INT64) AS _ad_group_id,
    CAST(_6sense._6senseid AS INT64) AS _ad_id, 
    -- _adgroup AS _ad_group_name,
    _6sense._name AS _ad_group_name, 
    _campaignname AS _campaign_name, 
    -- _advariation AS _ad_name,
    '' AS _ad_name,
    CASE 
      WHEN _6sense._startdate = '-' 
      THEN NULL 
      WHEN _6sense._startdate = 'No End Date' 
      THEN NULL 
      ELSE CAST(PARSE_DATE('%m/%e/%Y', _6sense._startdate) AS TIMESTAMP) 
    END,
    CASE 
      WHEN _6sense._enddate = '-' 
      THEN NULL 
      WHEN _6sense._enddate = 'No End Date' 
      THEN NULL 
      ELSE CAST(PARSE_DATE('%m/%e/%Y', _6sense._enddate) AS TIMESTAMP) 
    END,
    '6Sense' AS _platform, 
    'Carenet Health' AS _client,
    'Ads' AS _type,
    'Brand_Awareness' AS _campaign_objective,
  --   '' AS _landing_page,
    -- _adscreenshot AS _screenshot,
  --   '' AS _screenshot,
  --   '' AS _ads_type,
    CAST(REPLACE(REPLACE(_6sense._spend, '$', ''), ',', '') AS FLOAT64) AS _spent, 
    CAST(REPLACE(_6sense._impressions, ',', '') AS INTEGER) AS _impressions, 
    CAST(_6sense._clicks AS INTEGER) AS _clicks, 
    NULL AS _conversions,
    NULL AS _leads, 
    NULL AS _landingpageclick,
    CAST(NULL AS STRING) AS _status
  FROM `x-marketing.carenet_health_mysql.carenet_db_daily_campaign_performance__refurbished` _6sense
  -- JOIN `carenet_health_mysql.db_airtable_6sense_campaign` c ON CAST(c._advariationid AS STRING)  = CAST(a._6senseid AS STRING)
  WHERE _datatype = 'Ad'
),
carenet_campaign_6sense AS (
  SELECT 
    CAST(PARSE_DATE('%m/%e/%Y', _6sense._date) AS TIMESTAMP) AS _timestamp, 
    -- CAST(_campaignid AS INT64) AS _campaign_id
    CAST(_6sense._6senseid AS INT64) AS _campaign_id, 
    -- _adgroupid AS _ad_group_id, 
    CAST(NULL AS INT64) AS _ad_group_id, 
    CAST(NULL AS INT64) AS _ad_id, 
    -- _adgroup AS _ad_group_name,
    _6sense._name AS _ad_group_name, 
    _campaignname AS _campaign_name, 
    '' AS _ad_name,
    CASE 
      WHEN _6sense._startdate = '-' 
      THEN NULL 
      WHEN _6sense._startdate = 'No End Date' 
      THEN NULL 
      ELSE CAST(PARSE_DATE('%d-%b-%y', _6sense._startdate) AS TIMESTAMP) 
    END,
    CASE 
      WHEN _6sense._enddate = '-' 
      THEN NULL 
      WHEN _6sense._enddate = 'No End Date' 
      THEN NULL 
      ELSE CAST(PARSE_DATE('%d-%b-%y', _6sense._enddate) AS TIMESTAMP) 
    END,
    '6Sense' AS _platform, 
    'Carenet Health' AS _client,
    'Campaign' AS _type,
    'Brand_Awareness' AS _campaign_objective,
  --   '' AS _landing_page,
    -- _adscreenshot AS _screenshot,
  --   '' AS _screenshot,
  --   '' AS _ads_type,
    CAST(REPLACE(REPLACE(_6sense._spend, '$', ''), ',', '') AS FLOAT64) AS _spent, 
    CAST(REPLACE(_6sense._impressions, ',', '') AS INTEGER) AS _impressions, 
    CAST(_6sense._clicks AS INTEGER) AS _clicks, 
    NULL AS _conversions,
    NULL AS _leads, 
    NULL AS _landingpageclick,
    CAST(NULL AS STRING) AS _status
  FROM `x-marketing.carenet_health_mysql.carenet_db_daily_campaign_performance__refurbished` _6sense
  -- JOIN `carenet_health_mysql.db_airtable_6sense_campaign` c ON CAST(c._campaignid AS STRING)  = CAST(a._6senseid AS STRING)
  WHERE _datatype = 'Campaign'
  QUALIFY ROW_NUMBER() OVER(
    PARTITION BY _6sense._6senseid, _6sense._date 
    ORDER BY CAST(PARSE_DATE('%m/%e/%Y', _6sense._date) AS TIMESTAMP) DESC
  ) = 1
),
_6sense AS (
  SELECT *
  FROM (
    SELECT * FROM carenet_ads_6sense
    UNION ALL
    SELECT * FROM carenet_campaign_6sense
  )
),
--+/ _6sense \+--
--+/ _linkedin \+--
LI_ads AS (
  SELECT 
    start_at AS _timestamp,
    creative_id,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    one_click_leads AS _leads,
    landing_page_clicks AS _landingpageclicks
  FROM `x-marketing.carenet_health_linkedin.ad_analytics_by_creative`
),
ads_title AS (
  SELECT 
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `x-marketing.carenet_health_linkedin.creatives`
),
campaigns AS (
  SELECT 
    id AS _campaignID,
    name AS _campaignname,
    campaign_group_id,
    run_schedule.start AS _start_date,
    run_schedule.end AS _end_date,
    INITCAP(objective_type,'_') AS _campaign_objective
  FROM `x-marketing.carenet_health_linkedin.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status AS _status
  FROM `carenet_health_linkedin.campaign_groups`
),
carenet_health_ads_linkedin AS (
  SELECT 
    LI_ads._timestamp,
    campaigns._campaignID,
    campaign_group.groupID,
    LI_ads.creative_id,
    campaign_group._groupName,
    campaigns._campaignname,
    -- airtable_info._creativename,
    '' AS _creativename,
    campaigns._start_date,
    campaigns._end_date,
    'LinkedIn' AS _platform,
    'Carenet Health' AS _client,
    'Ads' AS _type,
    _campaign_objective,
    -- '' AS _landing_page,
    -- '' AS _screenshot,
    -- '' AS _ads_type,
    _spent,
    _impressions,
    _clicks,
    _conversions,
    _leads,
    _landingpageclicks,
    campaign_group._status
  FROM LI_ads
  RIGHT JOIN ads_title 
    ON CAST( LI_ads.creative_id AS STRING) = ads_title.cID
  LEFT JOIN campaigns 
    ON ads_title.campaign_id = campaigns._campaignID
  JOIN campaign_group 
    ON campaigns.campaign_group_id = campaign_group.groupID
  WHERE _timestamp IS NOT NULL
),
LI_campaign AS (
  SELECT 
    start_at AS _timestamp,
    campaign_id,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    one_click_leads AS _leads,
    landing_page_clicks AS _landingpageclicks
  FROM `x-marketing.carenet_health_linkedin.ad_analytics_by_campaign`
),
carenet_health_campaign_linkedin AS (
  SELECT 
    LI_campaign._timestamp,
    campaigns._campaignID,
    campaign_group.groupID,
    NULL AS creative_id,
    campaign_group._groupName,
    campaigns._campaignname,
    campaigns._start_date,
    campaigns._end_date,
    'LinkedIn' AS _platform,
    'Carenet Health' AS _client,
    'Campaign' AS _type,
    _campaign_objective,
    '' AS _landing_page,
    '' AS _screenshot,
    '' AS _ads_type,
    _spent,
    _impressions,
    _clicks,
    _conversions,
    _leads,
    _landingpageclicks,
    campaign_group._status
  FROM LI_campaign
  LEFT JOIN campaigns 
    ON LI_campaign.campaign_id = campaigns._campaignID
  JOIN campaign_group 
    ON campaigns.campaign_group_id = campaign_group.groupID
  WHERE _timestamp IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY LI_campaign._timestamp, campaigns._campaignID 
    ORDER BY LI_campaign._timestamp DESC
  ) = 1
),
_linkedin AS (
  SELECT * 
  FROM (
    SELECT * FROM carenet_health_ads_linkedin
    -- UNION ALL
    -- SELECT * FROM carenet_health_campaign_linkedin
  )
),
--+/ _linkedin \+--
airtable_info AS (
    SELECT 
      *
    -- FROM `x-marketing.carenet_health_mysql.carenet_optimization_airtable_ads_linkedin`
    FROM `x-marketing.carenet_health_mysql.carenet_db_campaign_ad_id`
)
SELECT 
  _all.*, 
  CASE 
    WHEN _end_date IS NOT NULL 
    THEN CONCAT(CAST(_start_date AS DATE),' ','-',' ',CAST(_end_date AS DATE)) 
    WHEN _start_date IS NULL 
    THEN 'Not Stated'
    ELSE CONCAT(CAST(_start_date AS DATE),' ','-',' ','No End Date') 
  END AS _campaign_date_range,
  airtable_info.* EXCEPT(_campaignid,_campaignname)
FROM (
  SELECT * FROM _linkedin
  -- SELECT * FROM _google_sem 
  UNION ALL 
  SELECT * FROM _6sense
) _all
LEFT JOIN airtable_info 
  ON CAST(_all.creative_id AS STRING) = CAST(airtable_info._adid AS STRING)