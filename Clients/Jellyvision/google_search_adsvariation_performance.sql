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
  SUM(view_through_conv) AS view_through_conv,
  SAFE_DIVIDE(SUM(clicks), SUM(impressions)) AS ctr,
  SAFE_DIVIDE(SUM(cost), SUM(clicks)) AS avg_cpc,
  SAFE_DIVIDE(SUM(abs_top_impr), SUM(search_impressions)) AS abs_top_impr,
  SAFE_DIVIDE(SUM(conversions), SUM(clicks)) AS conv_rate
  
FROM unique_rows
  
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
