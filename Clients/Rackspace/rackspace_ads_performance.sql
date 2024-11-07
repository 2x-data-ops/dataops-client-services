/*

Scheduler: rackspace_ads_performance
Specs:
  - PARTITION BY DAY(_date)

*/

TRUNCATE TABLE `x-marketing.rackspace.linkedin_ads_performance`;
INSERT INTO `x-marketing.rackspace.linkedin_ads_performance`(
  _date,
  _ad_id,
  _start_date,
  _end_date,
  _impressions,
  _clicks,
  _conversions,
  _spent,
  _leads,
  _landing_pages_clicks,
  _video_views,
  _lead_form_opens,
  _video_play,
  _video_views_25percent,
  _video_views_50percent,
  _video_views_75percent,
  _video_completions,
  _platform,
  _campaign_id,
  _campaign_name,
  _campaign_status,
  _campaign_group_id,
  _campaign_group_name,
  _daily_budget,
  _cost_type,
  _campaign_group_status,
  _account_id,
  _account_name,
  _ads_per_campaign,
  _daily_budget_per_ad,
  _finalutmtrackingurl,
  _bodytextlength,
  _status,
  _sponsoredtext,
  _filetype,
  _websiteurl,
  _finaladfilesfolder,
  _copyproductcompanyname,
  _adcopy,
  _jobtitles,
  _designblurp,
  _logos,
  _launchdatemonth,
  _screenshot,
  _assettitle,
  _industry,
  _airtableid,
  _copyassettype,
  _label,
  _ctacopy,
  _designcolor,
  _designimages,
  _designtemplate,
  _size,
  _ctacopysofthard,
  _copymessaging,
  _sponsoredtextlength,
  _adtitlenaming,
  _ctalength,
  _bodytext,
  _adcopylength,
  _copytone,
  _adtype,
  _adtitle,
  _copystatisticproofpoint,
  _layout,
  _segment,
  _adtitlelength,
  _adgroup,
  _stage,
  _adname
)

WITH linkedin_ads AS (
  SELECT
    EXTRACT(DATE FROM start_at) AS _date,
    creative_id AS _ad_id,
    DATE(start_at) AS _start_date,
    DATE(end_at) AS _end_date,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions,
    cost_in_usd AS _spent,
    one_click_leads AS _leads,
    landing_page_clicks AS _landing_pages_clicks,
    video_views AS _video_views,
    one_click_lead_form_opens AS _lead_form_opens,
    video_starts AS _video_play,
    video_first_quartile_completions AS _video_views_25percent,
    video_midpoint_completions AS _video_views_50percent,
    video_third_quartile_completions AS _video_views_75percent,
    video_completions AS _video_completions
  FROM `rackspace_linkedin.ad_analytics_by_creative`
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS _adid,
    campaign_id AS _campaignid,
    account_id AS _account_id,
    REGEXP_REPLACE(acc.name, r'[^a-zA-Z]', '') AS _account_name
  FROM `rackspace_linkedin.creatives` c
  LEFT JOIN `rackspace_linkedin.accounts` acc 
    ON acc.id = account_id
),
campaigns AS (
  SELECT
    id AS _campaign_id,
    name AS _campaign_name,
    status AS _campaign_status,
    cost_type AS _cost_type,
    daily_budget.amount AS _daily_budget,
    campaign_group_id AS _campaign_group_id
  FROM `rackspace_linkedin.campaigns`
),
campaign_group AS (
  SELECT
    id AS _campaign_group_id, 
    name AS _campaign_group_name, 
    status AS _campaign_group_status
  FROM `rackspace_linkedin.campaign_groups`
),
main_data AS (
  SELECT
    linkedin_ads._date,
    linkedin_ads._ad_id,
    linkedin_ads._start_date,
    linkedin_ads._end_date,
    linkedin_ads._impressions,
    linkedin_ads._clicks,
    linkedin_ads._conversions,
    linkedin_ads._spent,
    linkedin_ads._leads,
    linkedin_ads._landing_pages_clicks,
    linkedin_ads._video_views,
    linkedin_ads._lead_form_opens,
    linkedin_ads._video_play,
    linkedin_ads._video_views_25percent,
    linkedin_ads._video_views_50percent,
    linkedin_ads._video_views_75percent,
    linkedin_ads._video_completions,
    "LinkedIn" AS _platform, 
    campaigns._campaign_id,
    campaigns._campaign_name,
    campaigns._campaign_status,
    campaign_group._campaign_group_id,
    campaign_group._campaign_group_name,
    campaigns._daily_budget,
    campaigns._cost_type,
    campaign_group._campaign_group_status,
    ads_title._account_id,
    ads_title._account_name
  FROM linkedin_ads
  RIGHT JOIN ads_title 
    ON CAST( linkedin_ads._ad_id AS STRING) = ads_title._adid
  LEFT JOIN campaigns 
    ON ads_title._campaignid = campaigns._campaign_id 
  LEFT JOIN campaign_group 
    ON campaigns._campaign_group_id = campaign_group._campaign_group_id
),
airtable_data AS (
  SELECT
    *
  FROM `x-marketing.rackspace_mysql_2.optimization_airtable_ads_linkedin`
  WHERE _adid != ''
),
total_ads AS (
  SELECT 
    *, 
    COUNT(_ad_id) OVER (PARTITION BY _start_date, _campaign_name ) AS _ads_per_campaign
  FROM main_data
),
daily_budget_per_ad_per_campaign AS (
  SELECT 
    *,
    CASE
      WHEN _ads_per_campaign > 0 THEN _daily_budget / _ads_per_campaign
      ELSE 0
    END AS _daily_budget_per_ad
  FROM total_ads
)
SELECT 
  daily_budget_per_ad_per_campaign.*,
  airtable_data.* EXCEPT (_platform, _campaignid, _adid, _campaignname, _sdc_batched_at, _id, _sdc_sequence, _sdc_received_at, _sdc_table_version)
FROM daily_budget_per_ad_per_campaign
LEFT JOIN airtable_data
  ON CAST(daily_budget_per_ad_per_campaign._ad_id AS STRING) = airtable_data._adid