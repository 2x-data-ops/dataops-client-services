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
  QUALIFY RANK() OVER (PARTITION BY date, campaign_id ORDER BY report._sdc_received_at DESC) = 1
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
      WHEN ads.type = 'RESPONSIVE_SEARCH_AD' THEN 
      REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(ads.responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
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
  QUALIFY RANK() OVER (PARTITION BY date, ads.campaign_id, ads.ad_group_id, ads.id ORDER BY ads._sdc_received_at DESC) = 1
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
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
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
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
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
  QUALIFY RANK() OVER (PARTITION BY date, campaign_id ORDER BY report._sdc_received_at DESC) = 1
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
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
ORDER BY day, campaign_id;