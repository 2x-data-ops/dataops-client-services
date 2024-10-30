-- CREATE OR REPLACE TABLE `x-marketing.carenet_health.linkedin_ads_performance` AS
TRUNCATE TABLE `x-marketing.carenet_health.linkedin_ads_performance`;
INSERT INTO  `x-marketing.carenet_health.linkedin_ads_performance` (
  start_year,
  start_month,
  start_day,
  end_month,
  end_year,
  end_day,
  last_start_day,
  start_week,
  start_quater,
  start_month_num,
  weekday,
  start_month_name,
  start_week_num,
  _date,
  _estdate,
  _quater_startdate,
  creative_id,
  _startDate,
  _endDate,
  _leads,
  _reach,
  _spent,
  _impressions,
  _clicks,
  _conversions,
  _landing_pages_clicks,
  _video_views,
  _lead_form_opens,
  _video_play,
  _video_views_25percent,
  _video_views_50percent,
  _video_views_75percent,
  _video_completions,
  account_id,
  campaignID,
  _campaignNames,
  groupID,
  _groupName,
  dailyBudget,
  cost_type,
  status,
  dailyBudget_per_ad
) 
WITH LI_ads AS (
 SELECT
    date_range.start.year AS start_year, 
    date_range.start.month AS start_month, 
    date_range.start.day AS start_day,
    date_range.end.month AS end_month,
    date_range.end.year AS end_year, 
    date_range.end.day AS end_day,
    LAST_DAY( CAST(start_at AS DATE) ,WEEK(MONDAY)) AS last_start_day,
    TIMESTAMP_TRUNC(start_at, WEEK(MONDAY), 'UTC') AS start_week,
    TIMESTAMP_TRUNC(start_at, QUARTER, 'UTC') AS start_quater,
    TIMESTAMP_TRUNC(start_at, MONTH, 'UTC') AS start_month_num,
    FORMAT_DATETIME('%A', start_at) AS weekday,
    FORMAT_DATE('%B', start_at) AS start_month_name,
    EXTRACT(WEEK FROM start_at) AS start_week_num,
    EXTRACT(DATE FROM start_at) AS _date,
    DATETIME (start_at, "America/New_York") AS _estdate,
    CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
    creative_id,
    start_at AS _startDate,
    end_at AS _endDate,
    one_click_leads AS _leads,
    card_impressions AS _reach,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens,
    video_starts AS _video_play,
    video_first_quartile_completions AS _video_views_25percent,
    video_midpoint_completions AS _video_views_50percent,
    video_third_quartile_completions AS _video_views_75percent,
    video_completions AS _video_completions
  FROM `carenet_health_linkedin.ad_analytics_by_creative` 
  ORDER BY start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM `carenet_health_linkedin.creatives`
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    status, --not used in main query// use status from campaign group
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id
  FROM `carenet_health_linkedin.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM `carenet_health_linkedin.campaign_groups`
),
/* airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
    FROM `x-marketing.carenet_health_linkedinzation_airtable_ads_linkedin` 
), */
_all AS (
  SELECT
  --airtable_ads.*EXCEPT(_adid), 
    LI_ads.*,
    campaigns.account_id,
    campaigns.campaignID,
    campaigns._campaignNames,
    campaign_group.groupID,
    campaign_group._groupName,
    campaigns.dailyBudget,
    campaigns.cost_type,
    campaign_group.status
  FROM LI_ads
  RIGHT JOIN ads_title
    ON CAST(LI_ads.creative_id AS STRING) = ads_title.cID
  LEFT JOIN campaigns
    ON ads_title.campaign_id = campaigns.campaignID
  LEFT JOIN campaign_group
    ON campaigns.campaign_group_id = campaign_group.groupID
  /* LEFT JOIN airtable_ads 
  ON 
  CAST(LI_ads.creative_id AS STRING) = CAST(airtable_ads._adid AS STRING) */
),
daily_budget_per_ad_per_campaign AS (
  SELECT 
    *,
    CASE 
      WHEN COUNT(creative_id) OVER (PARTITION BY _startDate, _campaignNames ) > 0 
      THEN dailyBudget / COUNT(creative_id) OVER (PARTITION BY _startDate, _campaignNames )
      ELSE 0 
    END AS dailyBudget_per_ad
  FROM _all
) 
SELECT * FROM daily_budget_per_ad_per_campaign;