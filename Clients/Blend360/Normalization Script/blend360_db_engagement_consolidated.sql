-- Create a table for joined engagement data
-- CREATE OR REPLACE TABLE `x-marketing.blend360.multiple_engagement_data` AS
TRUNCATE TABLE `x-marketing.blend360.multiple_engagement_data`;

INSERT INTO `x-marketing.blend360.multiple_engagement_data` (
    _standardizedcompanyname,
    industry,
    ad_name,
    icp_tier,
    customer_segment,
    q1_2024__6s_,
    _ads_engagements,
    _visitoridleadfeeder,
    _impressions,
    _engagement,
    fd_click_delivered,
    db_clicks,
    _reacheds
  )
  WITH email_clicks AS (
    -- Select email engagement data within the last 90 days
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      q1_2024__6s_,
      NULL AS _ads_engagements,
      CAST(NULL AS STRING) AS _visitoridleadfeeder,
      NULL AS _impressions,
      'Clicked' AS _engagement,
      NULL AS fd_click_delivered,
      NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360.db_campaign_analysis`
      ON hs_name = _company
    WHERE _engagement = 'Clicked'
      AND _emailfilters = 'Campaign'
      AND CAST(_timestamp AS DATE) >= DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL -90 DAY
      )
      AND CAST(_timestamp AS DATE) < DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL 1 DAY
      )
  ),
  ads_engagement AS (
    -- Select ads engagement data from Google Sheets
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      q1_2024__6s_,
      CAST(ad_engagements AS INT64) AS _ads_engagements,
      CAST(NULL AS STRING) AS _visitoridleadfeeder,
      impressions AS _impressions,
      '' AS _engagement,
      NULL AS fd_click_delivered,
      NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_campaign_data.LI_Account_Engagement`
      ON li_name = li_company_name
  ),
  unique_web AS (
    -- Select unique web visits within the last 90 days
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      q1_2024__6s_,
      NULL AS _ads_engagements,
      _visitoridleadfeeder,
      NULL AS _impressions,
      '' AS _engagement,
      NULL AS fd_click_delivered,
      NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits`
      ON df_name = _companyname
    WHERE PARSE_DATE('%m/%d/%Y', _visitstartdate) >= DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL -90 DAY
      )
      AND PARSE_DATE('%m/%d/%Y', _visitstartdate) < DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL 1 DAY
      )
  ),
  foundry_performance AS (
    -- Select performance data from Foundry for Google Sheets
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      q1_2024__6s_,
      NULL AS _ads_engagements,
      CAST(NULL AS STRING) AS _visitoridleadfeeder,
      impressions_delivered AS _impressions,
      '' AS _engagement,
      clicks_delivered AS fd_click_delivered,
      NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_campaign_data.FD_Account_Data`
      ON fd_name = company
  ),
  demandbase_performance AS (
    -- Select performance data from Demandbase for Google Sheets
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      q1_2024__6s_,
      NULL AS _ads_engagements,
      CAST(NULL AS STRING) AS _visitoridleadfeeder,
      impressions,
      '' AS _engagement,
      NULL AS fd_click_delivered,
      clicks AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_campaign_data.DB_Domain_Summary`
      ON db_name = domain_name
  ),
  all_data AS (
    -- Union all engagement data sources
    SELECT
      *
    FROM email_clicks
    UNION ALL
    SELECT
      *
    FROM ads_engagement
    UNION ALL
    SELECT
      *
    FROM unique_web
    UNION ALL
    SELECT
      *
    FROM foundry_performance
    UNION ALL
    SELECT
      *
    FROM demandbase_performance
  ),
  reached AS (
    -- Determine if companies reached based on impressions
    SELECT
      _standardizedcompanyname,
      industry,
      IF(SUM(_impressions) >= 1, 'Y', 'N')
      AS _reacheds
    FROM all_data
    GROUP BY 1, 2
  ) -- Finalize the table by joining with 'reached' data
SELECT
  all_data.*,
  _reacheds
FROM all_data
LEFT JOIN reached
  ON all_data._standardizedcompanyname = reached._standardizedcompanyname
  AND all_data.industry = reached.industry;

-- Create a table for aggregated dealfront web visits
-- CREATE OR REPLACE TABLE `x-marketing.blend360.aggregated_dealfront_web_visit` AS 
TRUNCATE TABLE `x-marketing.blend360.aggregated_dealfront_web_visit`;

INSERT INTO `x-marketing.blend360.aggregated_dealfront_web_visit` (
    _standardizedcompanyname,
    industry,
    ad_name,
    icp_tier,
    customer_segment,
    _data,
    unique_visit,
    last_visit_date
  )
  WITH _90days AS (
    -- Count unique visits within the last 90 days
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      '90 days' AS _data,
      COUNT(DISTINCT _visitoridleadfeeder) AS unique_visit,
      MAX(PARSE_DATE('%m/%d/%Y', _visitstartdate)) AS last_visit_date
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits`
      ON df_name = _companyname
    WHERE PARSE_DATE('%m/%d/%Y', _visitstartdate) >= DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL -90 DAY
      )
      AND PARSE_DATE('%m/%d/%Y', _visitstartdate) < DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL 1 DAY
      )
    GROUP BY 1, 2, 3, 4, 5
  ),
  _all AS (
    -- Count overall unique visits
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      'Overall' AS _data,
      COUNT(DISTINCT _visitoridleadfeeder) AS unique_visit,
      MAX(PARSE_DATE('%m/%d/%Y', _visitstartdate)) AS last_visit_date
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits`
      ON df_name = _companyname
    GROUP BY 1, 2, 3, 4, 5
  ) -- Finalize the table by combining results
SELECT
  *
FROM _90days
UNION ALL
SELECT
  *
FROM _all;

-- Create a table for detailed dealfront web visit activities
-- CREATE OR REPLACE TABLE `x-marketing.blend360.dealfront_web_visit` AS
TRUNCATE TABLE `x-marketing.blend360.dealfront_web_visit`;

INSERT INTO `x-marketing.blend360.dealfront_web_visit` (
    _standardizedcompanyname,
    industry,
    ad_name,
    icp_tier,
    customer_segment,
    page_visit,
    visit_date,
    _timeonpageseconds,
    _data
  )
  WITH _90days AS (
    -- Select detailed web visit data within the last 90 days
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      CASE
        WHEN _url LIKE '%utm_%' THEN 'www.blend360.com/'
        WHEN _url LIKE '%hsa_%' THEN SPLIT(_url, '?hsa_acc')[SAFE_OFFSET(0)]
        ELSE _url
      END AS page_visit,
      PARSE_DATE('%m/%d/%Y', _visitstartdate) AS visit_date,
      _timeonpageseconds,
      '90 days' AS _data
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits`
      ON df_name = _companyname
    WHERE PARSE_DATE('%m/%d/%Y', _visitstartdate) >= DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL -90 DAY
      )
      AND PARSE_DATE('%m/%d/%Y', _visitstartdate) < DATE_ADD(
        DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)),
        INTERVAL 1 DAY
      )
  ),
  _all AS (
    -- Select overall detailed web visit data
    SELECT
      std_name AS _standardizedcompanyname,
      new_industry AS industry,
      ad_name,
      icp_tier,
      customer_segment,
      CASE
        WHEN _url LIKE '%utm_%' THEN 'www.blend360.com/'
        WHEN _url LIKE '%hsa_%' THEN SPLIT(_url, '?hsa_acc')[SAFE_OFFSET(0)]
        ELSE _url
      END AS page_visit,
      PARSE_DATE('%m/%d/%Y', _visitstartdate) AS visit_date,
      _timeonpageseconds,
      'Overall' AS _data
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits`
      ON df_name = _companyname
    WHERE _url IS NOT NULL
  ) -- Finalize the table by combining results
SELECT
  *
FROM _90days
UNION ALL
SELECT
  *
FROM _all;

-- Create a table for aggregated engagement data
-- CREATE OR REPLACE TABLE `x-marketing.blend360.aggregated_engagement_data` AS
TRUNCATE TABLE `x-marketing.blend360.aggregated_engagement_data`;

INSERT INTO `x-marketing.blend360.aggregated_engagement_data` (
    _standardizedcompanyname,
    industry,
    ad_name,
    icp_tier,
    customer_segment,
    _reacheds,
    unique_visitor,
    email_clicks,
    li_ad_engagements,
    fd_click,
    db_click,
    total_engagement
  )
SELECT
  _standardizedcompanyname,
  industry,
  ad_name,
  icp_tier,
  customer_segment,
  _reacheds,
  COUNT(DISTINCT _visitoridleadfeeder) AS unique_visitor,
  SUM(IF(_engagement = 'Clicked', 1, 0)) AS email_clicks,
  CAST(AVG(_ads_engagements) AS INT64) AS li_ad_engagements,
  SUM(fd_click_delivered) AS fd_click,
  SUM(db_clicks) AS db_click,
  CASE
    WHEN SUM(_ads_engagements) > 0 AND SUM(db_clicks) >= 0 AND SUM(fd_click_delivered) >= 0 THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + AVG(_ads_engagements) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered) + SUM(db_clicks)
    )
    WHEN SUM(_ads_engagements) > 0 AND SUM(db_clicks) >= 0 THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + AVG(_ads_engagements) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(db_clicks)
    )
    WHEN SUM(_ads_engagements) > 0 AND SUM(fd_click_delivered) >= 0 THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + AVG(_ads_engagements) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered)
    )
    WHEN SUM(fd_click_delivered) >= 0 AND SUM(db_clicks) >= 0 THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered) + SUM(db_clicks)
    )
    WHEN SUM(fd_click_delivered) >= 0 THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered)
    )
    WHEN SUM(db_clicks) >= 0 THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(db_clicks)
    )
    WHEN SUM(_ads_engagements) IS NULL AND SUM(fd_click_delivered) IS NULL AND SUM(db_clicks) IS NULL THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + COUNT(DISTINCT _visitoridleadfeeder)
    )
    WHEN SUM(_ads_engagements) IS NULL THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered) + SUM(db_clicks)
    )
    WHEN SUM(fd_click_delivered) IS NULL THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + COUNT(DISTINCT _visitoridleadfeeder) + AVG(_ads_engagements)
    )
    WHEN SUM(db_clicks) IS NULL THEN (
      SUM(IF(_engagement = 'Clicked', 1, 0)) + COUNT(DISTINCT _visitoridleadfeeder) + AVG(_ads_engagements)
    )
  END AS total_engagement
FROM `x-marketing.blend360.multiple_engagement_data`
GROUP BY 1, 2, 3, 4, 5, 6;