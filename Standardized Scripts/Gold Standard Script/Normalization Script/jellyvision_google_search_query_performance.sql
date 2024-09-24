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

GROUP BY 1,2,3,4,5,6,7,8,9