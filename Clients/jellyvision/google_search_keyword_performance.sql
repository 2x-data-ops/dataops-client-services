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
  
GROUP BY 1,2,3,4,5,6,7,8,9,10
