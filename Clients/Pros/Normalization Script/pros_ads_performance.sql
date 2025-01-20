TRUNCATE TABLE `x-marketing.pros.linkedin_ads_performance`;
INSERT INTO `x-marketing.pros.linkedin_ads_performance` (
  _campaign_group_name,
  _campaign_group_status,
  _campaign_name,
  _campaign_objective,
  _campaign_status,
  _daily_budget,
  _campaign_start_date,
  _campaign_end_date,
  _ad_id,
  _date,
  _leads,
  _spent,
  _impressions,
  _clicks,
  _conversions,
  _website_visits,
  _video_views,
  _video_play,
  _video_views_25percent,
  _video_views_50percent,
  _video_views_75percent,
  _video_completions,
  _ads_per_campaign,
  _daily_budget_per_ad
)
WITH li_ads AS (
  SELECT 
    creative_id AS _ad_id, 
    start_at AS _date, 
    one_click_leads AS _leads, 
    cost_in_usd AS _spent, 
    impressions AS _impressions, 
    clicks AS _clicks, 
    external_website_conversions AS _conversions,
    landing_page_clicks AS _website_visits,
    video_views AS _video_views,
    video_starts AS _video_play,
    video_first_quartile_completions AS _video_views_25percent,
    video_midpoint_completions AS _video_views_50percent,
    video_third_quartile_completions AS _video_views_75percent,
    video_completions AS _video_completions
  FROM `x-marketing.pros_linkedin_ads.ad_analytics_by_creative`
),
ads_title AS (
  SELECT 
    SPLIT(id, 'Creative:')[ORDINAL(2)] AS creative_id, 
    campaign_id 
  FROM `x-marketing.pros_linkedin_ads.creatives`
),
campaigns AS (
  SELECT 
    id AS _campaign_id, 
    name AS _campaign_name, 
    INITCAP(REGEXP_REPLACE(objective_type, '_', ' ')) AS _campaign_objective,
    INITCAP(status) AS _campaign_status,
    COALESCE(
      daily_budget.amount,
      total_budget.amount / TIMESTAMP_DIFF(run_schedule.end, run_schedule.start, DAY) 
    ) AS _daily_budget, 
    run_schedule.start AS _campaign_start_date,
    run_schedule.end AS _campaign_end_date,
    campaign_group_id 
  FROM `x-marketing.pros_linkedin_ads.campaigns` 
), 
campaign_group AS ( 
  SELECT 
    id AS _campaign_group_id, 
    name AS _campaign_group_name, 
    INITCAP(status) AS _campaign_group_status
  FROM `x-marketing.pros_linkedin_ads.campaign_groups`
),
combine_all AS (
  SELECT
    campaign_group._campaign_group_name, 
    campaign_group._campaign_group_status, 
    campaigns._campaign_name, 
    campaigns._campaign_objective,
    campaigns._campaign_status,
    campaigns._daily_budget, 
    campaigns._campaign_start_date,
    campaigns._campaign_end_date,
    li_ads.*
  FROM li_ads
  RIGHT JOIN ads_title 
    ON CAST(li_ads._ad_id AS STRING) = ads_title.creative_id
  JOIN campaigns 
    ON ads_title.campaign_id = campaigns._campaign_id
  JOIN campaign_group 
    ON campaigns.campaign_group_id = campaign_group._campaign_group_id
),
total_ads_per_campaign AS (
  SELECT
    *,
    COUNT(_ad_id) OVER (
        PARTITION BY _date, _campaign_name
    ) AS _ads_per_campaign
  FROM combine_all
),
daily_budget_per_ad_per_campaign AS (
  SELECT
    *,
    _daily_budget / IF(_ads_per_campaign = 0, 1, _ads_per_campaign) AS _daily_budget_per_ad
  FROM total_ads_per_campaign
)
SELECT 
  final_ads.*
FROM daily_budget_per_ad_per_campaign final_ads