CREATE OR REPLACE TABLE `jellyvision.google_display_campaign_performance` AS
WITH unique_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    date AS day,
    customer_currency_code AS currency,
    campaign_budget_amount_micros/1000000 AS budget,
    cost_micros/1000000 AS cost,
    impressions,
    CASE
      WHEN ad_network_type = 'DISPLAY' THEN impressions
      ELSE NULL
    END AS search_impressions,
    clicks,
    absolute_top_impression_percentage * impressions AS abs_top_impr,
    conversions,
    view_through_conversions AS view_through_conv,
    campaign_status,
    
  FROM  `jellyvision_google_ads.campaign_performance_report` report
  WHERE campaign_advertising_channel_type = 'DISPLAY'
    AND campaign_name NOT LIKE "%NB%"
  
    QUALIFY RANK() OVER (PARTITION BY date, campaign_id ORDER BY report._sdc_received_at DESC) = 1
)
  
SELECT
  campaign_id,
  campaign_name,
  day,
  currency,
  budget,
  campaign_status,
  SUM(cost) AS cost,
  SUM(impressions) AS impressions,
  SUM(search_impressions) AS search_impressions,
  SUM(abs_top_impr) AS abs_top_impr_value,
  SUM(clicks) AS clicks,
  SUM(conversions) AS conversions,
  SUM(view_through_conv) AS view_through_conv,

FROM unique_rows

GROUP BY 1,2,3,4,5,6
