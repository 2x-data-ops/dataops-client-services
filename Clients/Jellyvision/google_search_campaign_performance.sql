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

GROUP BY 1,2,3,4,5,6,7,8
