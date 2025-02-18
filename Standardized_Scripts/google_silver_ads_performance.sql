CREATE OR REPLACE TABLE `x-marketing.jellyvision.google_search_campaign_performance` AS
WITH unique_rows AS (
  SELECT
      campaign_id, 
      campaign_name, 
      date AS day, 
      customer_descriptive_name AS company_name,
      customer_currency_code AS currency,
      campaign_budget_amount_micros/1000000 AS budget, 
      cost_micros/1000000 AS cost, 
      search_rank_lost_absolute_top_impression_share,
      search_impression_share,
      search_rank_lost_top_impression_share,
      impressions, 
      CASE
          WHEN ad_network_type = 'SEARCH'
          THEN impressions
          ELSE NULL
      END search_impressions,
      clicks, 
      absolute_top_impression_percentage * impressions AS abs_top_impr,
      conversions, 
      view_through_conversions AS view_through_conv,
      all_conversions AS all_conversions,
      campaign_advertising_channel_type AS campaign_advertising_channel_type,
      campaign_status AS status,
      
  FROM `x-marketing.jellyvision_google_ads.campaign_performance_report` AS report
  
  WHERE campaign_name NOT LIKE "%NB%"
  
  QUALIFY RANK() OVER (PARTITION BY date, campaign_id ORDER BY report._sdc_received_at DESC) = 1
)

SELECT
    campaign_id, 
    campaign_name, 
    day,
    company_name,
    currency,
    budget,
    campaign_advertising_channel_type,
    status,
    SUM(search_rank_lost_absolute_top_impression_share) AS search_rank_lost_absolute_top_impression_share,
    SUM(search_impression_share) AS search_impression_share,
    SUM(search_rank_lost_top_impression_share) AS search_rank_lost_top_impression_share,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv,
    SUM(all_conversions) AS all_conversions

FROM unique_rows

GROUP BY 1,2,3,4,5,6,7,8;

CREATE OR REPLACE TABLE `jellyvision.google_search_adsvariation_performance` AS
WITH unique_rows AS (
  SELECT 
      g_ads.campaign_id,
      CASE
          WHEN g_ads.campaign_id = 20516592937
          THEN '2X | Display | Retargeting'
          ELSE campaign_name
      END AS campaign_name, 
      g_ads.ad_group_id, 
      g_ads.name AS ad_group_name, 
      customer_descriptive_name AS company_name,
      date AS day, 
      g_ads.id AS ad_id, 
      CASE 
          WHEN g_ads.type = 'RESPONSIVE_SEARCH_AD'
          THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(g_ads.responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
      END AS headlines,
      --responsive_search_ad.headlines AS headlines,
      g_ads.final_urls,
      customer_currency_code AS currency, 
      cost_micros/1000000 AS cost, 
      impressions, 
      CASE
          WHEN ad_network_type = 'SEARCH'
          THEN impressions
          ELSE NULL
      END search_impressions,
      clicks, 
      absolute_top_impression_percentage * impressions AS abs_top_impr, 
      conversions, 
      view_through_conversions AS view_through_conv,
      g_ads.type,
      
  FROM `x-marketing.jellyvision_google_ads.ad_performance_report` ads
  LEFT JOIN `x-marketing.jellyvision_google_ads.ads` g_ads
    ON g_ads.campaign_id = g_ads.campaign_id
  
  WHERE campaign_name NOT LIKE "%NB%"
  
  QUALIFY RANK() OVER (PARTITION BY date, g_ads.campaign_id, g_ads.ad_group_id, g_ads.id ORDER BY ads._sdc_received_at DESC) = 1
)
  
SELECT
  campaign_id,
  campaign_name, 
  ad_group_id, 
  ad_group_name, 
  company_name,
  day,
  ad_id,
  headlines,
  final_urls,
  currency,
  type,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
  
FROM unique_rows
  
GROUP BY 1,2,3,4,5,6,7,8,9,10,11;

CREATE OR REPLACE TABLE `jellyvision.google_search_keyword_performance` AS
WITH unique_rows AS (
  SELECT
    campaign_id, 
    campaign_name, 
    ad_group_id, 
    ad_group_name, 
    ad_group_criterion_keyword.match_type AS match_type, 
    ad_group_criterion_keyword.text AS keyword, 
    /*ad_group_criterion_quality_info.quality_score*/ NULL AS quality_score,
    customer_descriptive_name AS company_name,
    date AS day, 
    customer_currency_code AS currency, 
    cost_micros/1000000 AS cost, 
    impressions, 
    CASE
        WHEN ad_network_type = 'SEARCH'
        THEN impressions
        ELSE NULL
    END search_impressions,
    clicks, 
    absolute_top_impression_percentage * impressions AS abs_top_impr, 
    conversions, 
    view_through_conversions AS view_through_conv,
    search_rank_lost_absolute_top_impression_share,
    search_impression_share,
    search_rank_lost_top_impression_share,
                
  FROM `x-marketing.jellyvision_google_ads.keywords_performance_report` AS keywords
  
  WHERE campaign_name NOT LIKE "%NB%"
  
  QUALIFY RANK() OVER (PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text ORDER BY keywords._sdc_received_at DESC) = 1
)
  
SELECT
  campaign_id, 
  campaign_name, 
  ad_group_id, 
  ad_group_name, 
  match_type, 
  keyword, 
  quality_score,
  company_name,
  day,
  currency,
  SUM(search_rank_lost_absolute_top_impression_share) AS search_rank_lost_absolute_top_impression_share,
  SUM(search_impression_share) AS search_impression_share,
  SUM(search_rank_lost_top_impression_share) AS search_rank_lost_top_impression_share ,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv
  
FROM unique_rows
  
GROUP BY 1,2,3,4,5,6,7,8,9,10;

CREATE OR REPLACE TABLE `jellyvision.google_search_query_performance` AS
WITH unique_rows AS (
  SELECT
    campaign_id, 
    campaign_name, 
    ad_group_id,
    ad_group_name, 
    keyword.info.text AS keyword, 
    search_term_view_search_term AS search_term,
    customer_descriptive_name AS company_name,
    date AS day, 
    customer_currency_code AS currency, 
    cost_micros/1000000 AS cost, 
    impressions, 
    CASE
        WHEN ad_network_type = 'SEARCH'
        THEN impressions
        ELSE NULL
    END search_impressions,
    clicks, 
    absolute_top_impression_percentage * impressions AS abs_top_impr, 
    conversions, 
    view_through_conversions AS view_through_conv, 
  
  FROM `x-marketing.jellyvision_google_ads.search_query_performance_report` query

  WHERE campaign_name NOT LIKE "%NB%"
  
  QUALIFY RANK() OVER (PARTITION BY date, campaign_id, ad_group_id, keyword, search_term_view_search_term ORDER BY _sdc_received_at DESC) = 1
)

SELECT
  campaign_id, 
  campaign_name, 
  ad_group_id, 
  ad_group_name,  
  keyword,
  --match_type, 
  search_term,
  company_name,
  day,
  currency,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv

FROM unique_rows

GROUP BY 1,2,3,4,5,6,7,8,9;