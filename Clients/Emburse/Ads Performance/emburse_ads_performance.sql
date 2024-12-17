-----------Consolidate Ads------------------
TRUNCATE TABLE `x-marketing.emburse.consolidate_ad_performance`;
INSERT INTO `x-marketing.emburse.consolidate_ad_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  ad_group_id,	
  ad_group_name,	
  day,	
  ad_id,	
  campaign_status,
  _google_account_name,	
  _platform,	
  spent,	
  impressions,	
  clicks,	
  conversions
)
WITH unique_rows_google_ads AS (
  SELECT
    ads.campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ads.ad_group_id,
    ad_group_name,
    date AS day,
    ads.id AS ad_id,
    cost_micros/1000000 AS spent,
    impressions,
    clicks,
    conversions,
    INITCAP(campaign_status) AS campaign_status,
    customer_descriptive_name AS _google_account_name
  FROM `x-marketing.emburse_google_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (
    PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id
    ORDER BY ads._sdc_received_at DESC) = 1
),

google_ads AS (
  SELECT
    campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    day,
    ad_id,
    campaign_status,
    _google_account_name,
    'Google' AS _platform,
    SUM(spent) AS spent,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions
  FROM unique_rows_google_ads
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

-- unique_rows_campaign_level AS (
--   SELECT
--     ads.campaign_id,
--     campaign_name,
--     CASE 
--       WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
--       WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
--       WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
--       WHEN campaign_name LIKE '%UK%' THEN 'UK'
--       WHEN campaign_name LIKE '%US%' THEN 'US'
--       WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
--       WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
--       WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
--       ELSE NULL 
--     END AS campaign_country_region,
--     SAFE_CAST('' AS INT64) AS ad_group_id, --ads.ad_group_id, -- Not in campaign performance
--     '' AS ad_group_name, --ad_group_name, -- Not in campaign performance
--     date AS day,
--     SAFE_CAST('' AS INT64) AS ad_id, --ads.id AS ad_id, -- Not in campaign performance
--     cost_micros/1000000 AS spent,
--     impressions,
--     clicks,
--     conversions,
--     INITCAP(campaign_status) AS campaign_status,
--     customer_descriptive_name AS _google_account_name
--   FROM `x-marketing.emburse_google_ads.campaign_performance_report` ads
--   WHERE campaign_id IN (21672109159,20541949184,19976833107,20998416897)
--   QUALIFY RANK() OVER (
--     PARTITION BY date, campaign_id
--     ORDER BY ads._sdc_received_at DESC) = 1
-- ),

-- google_campaign_level AS (
--   SELECT
--     campaign_id,
--     campaign_name,
--     campaign_country_region,
--     ad_group_id,
--     ad_group_name,
--     day,
--     ad_id,
--     campaign_status,
--     _google_account_name,
--     'Google' AS _platform,
--     SUM(spent) AS spent,
--     SUM(impressions) AS impressions,
--     SUM(clicks) AS clicks,
--     SUM(conversions) AS conversions
--   FROM unique_rows_campaign_level
--   GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
-- ),
--Google Display
unique_campaign_list AS (
  SELECT DISTINCT 
    campaign_id,
    campaign_name,
    customer_descriptive_name
  FROM `x-marketing.emburse_google_ads.campaign_performance_report`
  WHERE campaign_advertising_channel_type <> 'SEARCH'
),

ad_counts_display AS (
  SELECT
    ad.ad_group_id,
    ad_group_name,
    report.campaign_name,
    date,
    COUNT(DISTINCT ad.id) AS ad_count
  FROM `x-marketing.emburse_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.emburse_google_ads.ads` ad 
    ON ad.ad_group_id = report.ad_group_id
  LEFT JOIN unique_campaign_list
    ON unique_campaign_list.campaign_id = report.campaign_id
  WHERE unique_campaign_list.campaign_id IS NOT NULL
  GROUP BY ad.ad_group_id, ad_group_name, campaign_name, date
),

adjusted_metrics AS (
  SELECT
    CAST(ad.id AS STRING) AS _adid,
    --airtable._adname,
    '' AS _adcopy,
    --_screenshot,
    '' AS _ctacopy,
    report.ad_group_id, 
    report.ad_group_name, 
    report.campaign_id,
    report.campaign_name,
    CASE 
      WHEN report.campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN report.campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN report.campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN report.campaign_name LIKE '%UK%' THEN 'UK'
      WHEN report.campaign_name LIKE '%US%' THEN 'US'
      WHEN report.campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN report.campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN report.campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    ad.name AS ad_name, 
    report.date AS _date,
    report.campaign_status,
    EXTRACT(YEAR FROM report.date) AS year,
    EXTRACT(MONTH FROM report.date) AS month,
    EXTRACT(QUARTER FROM report.date) AS quarter,
    CONCAT('Q', EXTRACT(YEAR FROM report.date), '-', EXTRACT(QUARTER FROM report.date)) AS quarteryear,
    cost_micros / 1000000 / c.ad_count AS adjusted_spent, 
    conversions / c.ad_count AS adjusted_conversions,
    clicks / c.ad_count AS adjusted_clicks, 
    impressions / c.ad_count AS adjusted_impressions,
    ad_count,
    report.customer_descriptive_name AS _google_account_name
  FROM `x-marketing.emburse_google_ads.ad_group_performance_report` report
  JOIN `x-marketing.emburse_google_ads.ads` ad 
    ON ad.ad_group_id = report.ad_group_id
  JOIN ad_counts_display c 
    ON ad.ad_group_id = c.ad_group_id 
    AND report.date = c.date
  LEFT JOIN unique_campaign_list
    ON unique_campaign_list.campaign_id = report.campaign_id
  WHERE unique_campaign_list.campaign_id IS NOT NULL
  QUALIFY RANK() OVER (
    PARTITION BY ad.id, campaign_id, report.date 
    ORDER BY report.date DESC) = 1
),

google_display AS (
  SELECT
    CAST(campaign_id AS INT64) AS campaign_id,
    campaign_name,
    campaign_country_region,
    ad_group_id,
    ad_group_name,
    _date AS day,
    CAST(_adid AS INT64) AS ad_id,
    campaign_status,
    _google_account_name,
    'Google' AS _platform, 
    SUM(CAST(adjusted_spent AS FLOAT64)) AS spent,
    SUM(CAST(adjusted_impressions AS INT64)) AS impressions,
    SUM(CAST(adjusted_clicks AS INT64)) AS clicks,
    SUM(CAST(adjusted_conversions AS FLOAT64)) AS conversions,
  FROM adjusted_metrics
  GROUP BY ALL
),

bing_ads AS (
  SELECT
    ads.campaignid, 
    ads.campaignname,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM' 
      ELSE NULL 
    END AS campaign_country_region,
    adgroupid,
    adgroupname,
    ads.timeperiod, 
    ads.adid, 
    ads.campaignstatus,
    '' AS _google_account_name,
    'Bing' AS _platform,
    ads.spend AS cost, 
    ads.impressions, 
    ads.clicks, 
    ads.conversions
  FROM `x-marketing.emburse_bing_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (
    PARTITION BY ads.timeperiod, ads.adid 
    ORDER BY ads._sdc_report_datetime DESC) = 1
),

LI_ads AS (
  SELECT
    start_at AS _date,
    creative_id,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
  FROM `x-marketing.emburse_linkedin_ads.ad_analytics_by_creative` 

),
LI_ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `x-marketing.emburse_linkedin_ads.creatives`

),
LI_campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    CASE 
      WHEN name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN name LIKE '%US/CA%' THEN 'US/CA'
      WHEN name LIKE '%Germany%' THEN 'Germany'
      WHEN name LIKE '%UK%' THEN 'UK'
      WHEN name LIKE '%US%' THEN 'US'
      WHEN name LIKE '%APAC%' THEN 'APAC'
      WHEN name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    INITCAP(status) AS status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id
  FROM `x-marketing.emburse_linkedin_ads.campaigns`
),

LI_campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    INITCAP(status)
  FROM `x-marketing.emburse_linkedin_ads.campaign_groups`
),

linkedin_ads AS (
  SELECT
    LI_campaigns.campaignID,
    LI_campaigns._campaignNames,
    LI_campaigns.campaign_country_region,
    LI_campaign_group.groupID,
    LI_campaign_group._groupName,
    LI_ads._date,
    LI_ads.creative_id,
    LI_campaigns.status,
    '' AS _google_account_name,
    'LinkedIn' AS _platform,
    LI_ads._spent,
    LI_ads._impressions,
    LI_ads._clicks,
    LI_ads._conversions
  FROM LI_ads
  RIGHT JOIN LI_ads_title
    ON CAST(LI_ads.creative_id AS STRING) = LI_ads_title.cID
  LEFT JOIN LI_campaigns
    ON LI_ads_title.campaign_id = LI_campaigns.campaignID
  LEFT JOIN LI_campaign_group
    ON LI_campaigns.campaign_group_id = LI_campaign_group.groupID
  WHERE _date IS NOT NULL
)
SELECT 
  * 
FROM google_ads
UNION ALL
-- SELECT 
--   * 
-- FROM google_campaign_level
-- UNION ALL
SELECT 
  * 
FROM google_display
UNION ALL
SELECT 
  * 
FROM bing_ads
UNION ALL
SELECT 
  * 
FROM linkedin_ads;

---------------Google Ads----------------------
---Google Campaign Performance
TRUNCATE TABLE `x-marketing.emburse.google_search_campaign_performance`;
INSERT INTO `x-marketing.emburse.google_search_campaign_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  day,	
  currency,	
  campaign_status,	
  customer_time_zone,	
  campaign_advertising_channel_type,	
  _account_name,
  cost,	
  impressions,	
  search_impressions,	
  clicks,	
  conversions,	
  view_through_conv,	
  conv_value
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    date AS day,
    customer_currency_code AS currency,
    campaign_budget_amount_micros/1000000 AS budget,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    conversions,
    view_through_conversions AS view_through_conv,
    conversions_value AS conv_value,
    campaign_status,
    customer_time_zone,
    INITCAP(campaign_advertising_channel_type) AS campaign_advertising_channel_type,
    account.descriptive_name AS _account_name
  FROM `x-marketing.emburse_google_ads.campaign_performance_report` report
  LEFT JOIN `x-marketing.emburse_google_ads.accounts` account
    ON account.id = report.customer_id
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id 
    ORDER BY report._sdc_received_at DESC) = 1

)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  day,
  currency,
  campaign_status,
  customer_time_zone,
  campaign_advertising_channel_type,
  _account_name,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv,
  SUM(conv_value) AS conv_value
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY day, campaign_id;

-- Google Search Ads Variation Performance
TRUNCATE TABLE `x-marketing.emburse.google_search_adsvariation_performance`;
INSERT INTO `x-marketing.emburse.google_search_adsvariation_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  ad_group_id,	
  ad_group_name,	
  day,	
  ad_id,	
  headlines,	
  final_urls,	
  currency,	
  ad_group_status,	
  customer_time_zone,	
  _account_name,
  cost,	
  impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv,	
  conv_value
)
WITH unique_rows AS (
  SELECT
    ads.campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ads.ad_group_id,
    ad_group_name,
    date AS day,
    ads.id AS ad_id,
    CASE
      WHEN ads.type = 'RESPONSIVE_SEARCH_AD' THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(ads.responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
    END AS headlines,
    TRIM(ads.final_urls, "[']") AS final_urls,
    customer_currency_code AS currency,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    conversions_value AS conv_value,
    ad_group_status,
    customer_time_zone,
    account.descriptive_name AS _account_name
  FROM `emburse_google_ads.ad_performance_report` ads
  LEFT JOIN `x-marketing.emburse_google_ads.accounts` account
    ON account.id = ads.customer_id
  QUALIFY RANK() OVER (
    PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id
    ORDER BY ads._sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  ad_group_id,
  ad_group_name,
  day,
  ad_id,
  headlines,
  final_urls,
  currency,
  ad_group_status,
  customer_time_zone,
  _account_name,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv,
  SUM(conv_value) AS conv_value
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13
ORDER BY day, campaign_id, ad_group_id, ad_id;

-- Google Seach Keyword Performance
TRUNCATE TABLE `x-marketing.emburse.google_search_keyword_performance`;
INSERT INTO `x-marketing.emburse.google_search_keyword_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  ad_group_id,	
  ad_group_name,	
  match_type,	
  keyword,	
  quality_score,	
  day,	
  currency,	
  ad_group_criterion_status,	
  customer_time_zone,	
  _account_name,
  cost,	
  impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ad_group_id,
    ad_group_name,
    ad_group_criterion_keyword.match_type AS match_type,
    ad_group_criterion_keyword.text AS keyword,
    ad_group_criterion_quality_info.quality_score AS quality_score,
    date AS day,
    customer_currency_code AS currency,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    ad_group_criterion_status,
    customer_time_zone,
    account.descriptive_name AS _account_name
  FROM `x-marketing.emburse_google_ads.keywords_performance_report` keywords
  LEFT JOIN `x-marketing.emburse_google_ads.accounts` account
    ON account.id = keywords.customer_id
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
    ORDER BY keywords._sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  ad_group_id,
  ad_group_name,
  match_type,
  keyword,
  quality_score,
  day,
  currency,
  ad_group_criterion_status,
  customer_time_zone,
  _account_name,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,10,11,12,13
ORDER BY day, campaign_id, ad_group_id, keyword;

-- Google Search Query Performance
TRUNCATE TABLE `x-marketing.emburse.google_search_query_performance`;
INSERT INTO `x-marketing.emburse.google_search_query_performance` (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  ad_group_id,	
  ad_group_name,	
  keyword,	
  search_term,	
  day,	
  currency,	
  campaign_status,	
  ad_group_status,	
  customer_time_zone,	
  _account_name,
  cost,	
  impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    ad_group_id,
    ad_group_name,
    keyword.info.text AS keyword,
    -- search_term_match_type AS match_type,
    search_term_view_search_term AS search_term,
    date AS day,
    customer_currency_code AS currency,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'SEARCH' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    campaign_status,
    ad_group_status,
    customer_time_zone,
    account.descriptive_name AS _account_name
  FROM `x-marketing.emburse_google_ads.search_query_performance_report` report
  LEFT JOIN `x-marketing.emburse_google_ads.accounts` account
    ON account.id = customer_id
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY date, campaign_id, ad_group_id, keyword/*.info.text*/, search_term_view_search_term
    ORDER BY report._sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  ad_group_id,
  ad_group_name,
  keyword,
  -- match_type,
  search_term,
  day,
  currency,
  campaign_status,
  ad_group_status,
  customer_time_zone,
  _account_name,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8,9,10, 11,12,13
ORDER BY day, campaign_id, ad_group_id, keyword, search_term;

-- Google Display Campaign Performance
TRUNCATE TABLE `x-marketing.emburse.google_display_campaign_performance`;
INSERT INTO `x-marketing.emburse.google_display_campaign_performance`  (
  campaign_id,	
  campaign_name,	
  campaign_country_region,	
  day,	
  currency,	
  budget,	
  campaign_status,	
  customer_time_zone,	
  _account_name,
  cost,	
  impressions,	
  active_view_impressions,	
  search_impressions,	
  abs_top_impr_value,	
  clicks,	
  conversions,	
  view_through_conv
)
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    date AS day,
    customer_currency_code AS currency,
    campaign_budget_amount_micros/1000000 AS budget,
    cost_micros/1000000 AS cost,
    impressions,
    active_view_impressions,
    CASE
      WHEN ad_network_type = 'DISPLAY' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    campaign_status,
    customer_time_zone,
    account.descriptive_name AS _account_name
  FROM `x-marketing.emburse_google_ads.campaign_performance_report` report
  LEFT JOIN `x-marketing.emburse_google_ads.accounts` account
    ON account.id = customer_id
  WHERE campaign_advertising_channel_type = 'DISPLAY'
  QUALIFY RANK() OVER (
    PARTITION BY date, campaign_id
    ORDER BY report._sdc_received_at DESC) = 1
)
SELECT
  campaign_id,
  campaign_name,
  campaign_country_region,
  day,
  currency,
  budget,
  campaign_status,
  customer_time_zone,
  _account_name,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(active_view_impressions) AS active_view_impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5,6,7,8,9
ORDER BY day, campaign_id;

--- google video performance
TRUNCATE TABLE `x-marketing.emburse.video_performance`;
INSERT INTO `x-marketing.emburse.video_performance` (
  campaign_id,
  campaign_name,	
  campaign_country_region,	
  day,	
  currency,	
  network_type,	
  video_title,	
  video_channel_id,	
  group_status,	
  campaign_status,	
  customer_time_zone,	
  _account_name,
  cost,	
  impressions,
  search_impressions,
  clicks,
  conversions,
  view_through_conv,
  view_views
)
WITH unique_rows AS (
  SELECT
    report.campaign_id, 
    report.campaign_name,
    CASE 
      WHEN campaign_name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaign_name LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaign_name LIKE '%Germany%' THEN 'Germany'
      WHEN campaign_name LIKE '%UK%' THEN 'UK'
      WHEN campaign_name LIKE '%US%' THEN 'US'
      WHEN campaign_name LIKE '%APAC%' THEN 'APAC'
      WHEN campaign_name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaign_name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    report.date AS day, 
    report.customer_currency_code AS currency,
    report.cost_micros/1000000 AS cost, 
    report.impressions, 
    CASE
      WHEN report.ad_network_type = 'VIDEO'
      THEN report.impressions
      ELSE NULL
    END search_impressions,
    report.clicks, 
    report.conversions, 
    report.view_through_conversions AS view_through_conv,
    report.ad_network_type AS network_type, 
    video_title AS video_title,
    video_channel_id,
    video_id,
    ad_group_status AS group_status,
    report.campaign_status AS campaign_status,
    report.video_views AS _view_views,
    customer_time_zone,
    customer_descriptive_name AS _account_name
  FROM `x-marketing.emburse_google_ads.video_performance_report` report 
  QUALIFY RANK() OVER (
  PARTITION BY date, campaign_id,report.video_id
    ORDER BY report._sdc_received_at DESC) = 1
)
SELECT
  campaign_id, 
  campaign_name, 
  campaign_country_region,
  day,
  currency,
  network_type,
  video_title,
  video_channel_id,
  group_status,
  campaign_status,
  customer_time_zone,
  _account_name,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv,
  SUM(_view_views) AS view_views
FROM unique_rows
GROUP BY 1, 2, 3, 4, 5,6,7,8,9,10, 11,12
ORDER BY day, campaign_id;

---bings ads performance
---bing keyword performance
TRUNCATE TABLE `x-marketing.emburse.bing_keyword_performance`;
INSERT INTO `x-marketing.emburse.bing_keyword_performance` (
  adgroupid,	
  timeperiod,	
  campaignname,	
  campaign_country_region,	
  campaignid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  ctr,	
  avgcpc,	
  conversions,	
  conversionrate,	
  deliveredmatchtype,	
  keyword,	
  qualityscore,	
  bidmatchtype,	
  dailybudget	
)
WITH keywords AS (
  SELECT
    adgroupid, 
    keywords.timeperiod, 
    keywords.campaignname,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region,
    keywords.campaignid, 
    addistribution,
    keywords.currencycode, 
    keywords.spend AS cost, 
    keywords.impressions , 
    keywords.clicks, 
    CAST(REPLACE(keywords.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent, 
    keywords.ctr, 
    keywords.averagecpc AS avgcpc,
    keywords.conversions , 
    keywords.conversionrate , 
    keywords.deliveredmatchtype, 
    keywords.keyword, 
    keywords.qualityscore, 
    bidmatchtype
  FROM `x-marketing.emburse_bing_ads.keyword_performance_report` keywords
  QUALIFY RANK() OVER (
    PARTITION BY keywords.timeperiod, keywords.keywordid
    ORDER BY keywords._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  keywords.*, 
  budget.dailybudget 
FROM keywords 
JOIN budget 
  ON keywords.campaignid = budget.campaignid;

---bing ads variation performance
TRUNCATE TABLE `x-marketing.emburse.bing_adsvariation_performance`;
INSERT INTO `x-marketing.emburse.bing_adsvariation_performance` (
  adgroupid,
  titlepart1,	
  titlepart2,	
  titlepart3,	
  campaignname,	
  campaign_country_region,	
  campaignid,
  adid,
  timeperiod,	
  currencycode,	
  cost,	
  impressions,
  clicks,
  AbsoluteTopImpressionRatePercent,	
  conversions,	
  conversionrate,	
  _sdc_report_datetime,	
  dailybudget
)
WITH ads AS (
  SELECT
    adgroupid, 
    titlepart1, 
    titlepart2, 
    titlepart3, 
    ads.campaignname,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    ads.campaignid, 
    ads.adid, 
    ads.timeperiod, 
    ads.currencycode, 
    ads.spend AS cost, 
    ads.impressions , 
    ads.clicks, 
    CAST(REPLACE(ads.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent, 
    ads.conversions, 
    ads.conversionrate,
    ads._sdc_report_datetime
  FROM `x-marketing.emburse_bing_ads.ad_performance_report` ads
  QUALIFY RANK() OVER (
    PARTITION BY ads.timeperiod, ads.adid
    ORDER BY ads._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  ads.*, 
  budget.dailybudget 
FROM ads
JOIN budget
  ON ads.campaignid = budget.campaignid;

---Bing Campaign Performance
TRUNCATE TABLE `x-marketing.emburse.bing_campaign_performance`;
INSERT INTO `x-marketing.emburse.bing_campaign_performance` (
  timeperiod,	
  campaignname,	
  campaign_country_region,	
  campaignid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  conversions,	
  campaign_status,
  dailybudget
)
WITH campaign AS (
  SELECT
    campaign.timeperiod, 
    campaign.campaignname,
    CASE 
      WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
      WHEN campaignname LIKE '%Germany%' THEN 'Germany'
      WHEN campaignname LIKE '%UK%' THEN 'UK'
      WHEN campaignname LIKE '%US%' THEN 'US'
      WHEN campaignname LIKE '%APAC%' THEN 'APAC'
      WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    campaign.campaignid, 
    addistribution,
    campaign.currencycode, 
    campaign.spend AS cost,
    campaign.impressions,
    campaign.clicks,
    CAST(REPLACE(campaign.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64) AS AbsoluteTopImpressionRatePercent, 
    campaign.conversions,
    campaign.campaignstatus AS campaign_status
  FROM `x-marketing.emburse_bing_ads.campaign_performance_report` campaign
  QUALIFY RANK() OVER (
    PARTITION BY campaign.timeperiod, campaign.campaignid
    ORDER BY campaign._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  campaign.*, 
  budget.dailybudget 
FROM campaign
JOIN budget 
  ON campaign.campaignid = budget.campaignid;

---Bing Ad Group Performance
TRUNCATE TABLE `x-marketing.emburse.bing_adgroup_performance`;
INSERT INTO `x-marketing.emburse.bing_adgroup_performance` (
  timeperiod,	
  adgroupid,	
  addistribution,	
  currencycode,	
  cost,	
  impressions,	
  clicks,	
  AbsoluteTopImpressionRatePercent,	
  ctr,	
  avgcpc,	
  conversions,	
  conversionrate,	
  dailybudget
)
WITH adgroups AS (
  SELECT
    adgroups.timeperiod, 
    adgroups.adgroupid, 
    addistribution, 
    campaignid,
    adgroups.currencycode, 
    adgroups.allreturnonadspend AS cost, 
    adgroups.impressions , 
    adgroups.clicks, 
    CAST(REPLACE(adgroups.AbsoluteTopImpressionRatePercent, "%","") AS FLOAT64 ) AS AbsoluteTopImpressionRatePercent , 
    adgroups.ctr, adgroups.averagecpc AS avgcpc,
    adgroups.conversions , 
    adgroups.conversionrate
  FROM `x-marketing.emburse_bing_ads.ad_group_performance_report` adgroups
  QUALIFY RANK() OVER (
    PARTITION BY adgroups.timeperiod, adgroups.adgroupid
    ORDER BY adgroups._sdc_report_datetime DESC) = 1
), 
budget AS (
  SELECT 
    id AS campaignid, 
    dailybudget 
  FROM `x-marketing.emburse_bing_ads.campaigns`
)
SELECT 
  adgroups.* EXCEPT (campaignid), 
  budget.dailybudget
FROM adgroups
JOIN budget 
  ON adgroups.campaignid = budget.campaignid;

---Bing Search Query Performance
TRUNCATE TABLE `x-marketing.emburse.bing_search_query_performance`;
INSERT INTO `x-marketing.emburse.bing_search_query_performance` (
  campaignid,	
  campaignname,	
  campaign_country_region,	
  adgroupid,	
  adgroupname,	
  keyword,	
  search_term,	
  day,	
  cost,	
  impressions,	
  clicks,	
  conversions,	
  campaignstatus,	
  adgroupstatus
)
SELECT
  campaignid,
  campaignname,
  CASE 
    WHEN campaignname LIKE '%US/CA/UK%' THEN 'US/CA/UK'
    WHEN campaignname LIKE '%US/CA%' THEN 'US/CA'
    WHEN campaignname LIKE '%Germany%' THEN 'Germany'
    WHEN campaignname LIKE '%UK%' THEN 'UK'
    WHEN campaignname LIKE '%US%' THEN 'US'
    WHEN campaignname LIKE '%APAC%' THEN 'APAC'
    WHEN campaignname LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
    WHEN campaignname LIKE '%NORAM%' THEN 'NORAM'
    ELSE NULL 
  END AS campaign_country_region,
  adgroupid,
  adgroupname,
  keyword AS keyword,
  -- search_term_match_type AS match_type,
  searchquery AS search_term,
  timeperiod AS day,
  spend AS cost,
  impressions,
  clicks,
  conversions,
  campaignstatus,
  adgroupstatus
FROM `x-marketing.emburse_bing_ads.search_query_performance_report`
QUALIFY RANK() OVER (
  PARTITION BY timeperiod, campaignid, adgroupid, keyword, searchquery
  ORDER BY _sdc_received_at DESC) = 1;

---LinkedIn ads
TRUNCATE TABLE `x-marketing.emburse.linkedin_ads_performance` ;
INSERT INTO `x-marketing.emburse.linkedin_ads_performance` (
  _date,	
  _quater_startdate,	
  _ad_id,	
  _startDate,	
  _endDate,	
  _leads,	
  _spent,	
  _impressions,	
  _clicks,	
  _conversions,	
  _landing_pages_clicks,	
  _video_views,	
  _lead_form_opens,	
  account_id,	
  campaignID,	
  _campaignNames,	
  campaign_country_region,	
  _campaign_status,	
  groupID,	
  _groupName,	
  dailyBudget,	
  cost_type,	
  campaign_objective,	
  _campaign_group_status,	
  ads_per_campaign,	
  dailyBudget_per_ad
)
WITH LI_ads AS (
  SELECT
    EXTRACT(DATE FROM start_at) AS _date,
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id AS _ad_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens
  FROM `x-marketing.emburse_linkedin_ads.ad_analytics_by_creative` 
),

ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `x-marketing.emburse_linkedin_ads.creatives`
),

campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    CASE 
      WHEN name LIKE '%US/CA/UK%' THEN 'US/CA/UK'
      WHEN name LIKE '%US/CA%' THEN 'US/CA'
      WHEN name LIKE '%Germany%' THEN 'Germany'
      WHEN name LIKE '%UK%' THEN 'UK'
      WHEN name LIKE '%US%' THEN 'US'
      WHEN name LIKE '%APAC%' THEN 'APAC'
      WHEN name LIKE '%NZ/AU/SG%' THEN 'NZ/AU/SG'
      WHEN name LIKE '%NORAM%' THEN 'NORAM'
      ELSE NULL 
    END AS campaign_country_region, 
    INITCAP(status) AS status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id,
    INITCAP(REPLACE(objective_type,"_"," ")) AS campaign_objective
  FROM `x-marketing.emburse_linkedin_ads.campaigns`
),

campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    INITCAP(status) AS status
  FROM `x-marketing.emburse_linkedin_ads.campaign_groups`
),

_all AS (
  SELECT
    LI_ads.*,
    campaigns.account_id,
    campaigns.campaignID,
    campaigns._campaignNames,
    campaigns.campaign_country_region,
    campaigns.status AS _campaign_status,
    campaign_group.groupID,
    campaign_group._groupName,
    campaigns.dailyBudget,
    campaigns.cost_type,
    campaigns.campaign_objective,
    campaign_group.status AS _campaign_group_status
  FROM LI_ads
  RIGHT JOIN ads_title
    ON CAST(LI_ads._ad_id AS STRING) = ads_title.cID
  LEFT JOIN campaigns
    ON ads_title.campaign_id = campaigns.campaignID
  LEFT JOIN campaign_group
    ON campaigns.campaign_group_id = campaign_group.groupID
),

total_ads AS (
  SELECT 
    *, 
    COUNT(_ad_id) OVER (PARTITION BY _startDate, _campaignNames) AS ads_per_campaign
  FROM _all
),

daily_budget_per_ad_per_campaign AS (
  SELECT 
    *,
    IFNULL(SAFE_DIVIDE(dailyBudget, ads_per_campaign), 0) AS dailyBudget_per_ad
  FROM total_ads
) 
SELECT 
  * 
FROM daily_budget_per_ad_per_campaign
WHERE _date IS NOT NULL;