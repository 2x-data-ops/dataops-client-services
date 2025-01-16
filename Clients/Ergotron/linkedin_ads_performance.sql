CREATE OR REPLACE TABLE `x-marketing.ergotron.linkedin_ads_performance` AS
WITH LI_airtable AS (
   SELECT
  _ad_id AS _adid,
  _ad_type,
  _ad_variation,
  _ad_name,
  _ad_name_length,
  _introduction_text,
  _intro_text_length,
  _headline_text,
  _headline_text_length,
  _platform,
  _business_segment,
  _landing_page_url,
  _ad_visual,
  IF(
  _live_date != '', 
  PARSE_TIMESTAMP('%d/%m/%Y', _live_date), 
  NULL ) AS _live_date,
  _completed_date,
  _status,
  _ad_group_length,
  _job_title,
  _industry,
  _size,
  _ad_title,
  _ad_title_naming,
  _ad_title_length,
  _sponsored_text,
  _sponsored_text_length,
  _body_text,
  _body_text_length,
  _text_on_image,
  _text_on_image_length,
  _cta_on_image,
  _cta_on_image_length,
  _cta_copy,
  _attachment_file_type,
  _layout,
  _color,
  _image,
  _blurb,
  _logo,
  _messaging,
  _asset_type,
  _tone,
  _product_company_name,
  _stage,
  _campaign_objective,
  _main_keywords,
  _template,
  _statistic_proof_point
FROM
  `x-marketing.jellyvision_google_sheets.db_ads_optimization`
  WHERE _platform = 'LinkedIn'
)
, LI_ads AS (
    SELECT
        date_range.start.year AS _start_year, 
        date_range.start.month AS _start_month, 
        date_range.start.day AS _start_day,
        date_range.end.month AS _end_month,
        date_range.end.year AS _end_year, 
        date_range.end.day AS _end_day,
        LAST_DAY( CAST(start_at AS DATE) ,WEEK(MONDAY)) AS _last_start_day,
        TIMESTAMP_TRUNC(start_at, WEEK(MONDAY), 'UTC') AS _start_week,
        TIMESTAMP_TRUNC(start_at, QUARTER, 'UTC') AS _start_quater,
        TIMESTAMP_TRUNC(start_at, MONTH, 'UTC') AS _start_month_num,
        FORMAT_DATETIME('%A', start_at) AS _weekday,
        FORMAT_DATE('%B', start_at) AS _start_month_name,
        EXTRACT(WEEK FROM start_at) AS _start_week_num,
        EXTRACT(DATE FROM start_at) AS _date,
        CONCAT('Q',EXTRACT(QUARTER FROM start_at),'-',EXTRACT(YEAR FROM start_at) ) AS _quater_startdate,
        creative_id AS _adid,
        DATE(start_at) AS _startdate,
        DATE(end_at) AS _enddate,
        --approximate_unique_impressions AS _reach,
        impressions AS _impressions,
        clicks AS _clicks,
        external_website_conversions AS _conversions,
        cost_in_usd AS _spent,
        one_click_leads AS _leads,
        landing_page_clicks AS _landing_pages_clicks,
        --video_views AS _video_views,
        one_click_lead_form_opens AS _lead_form_opens,
        video_starts AS _video_play,
        video_first_quartile_completions AS _video_views_25percent,
        video_midpoint_completions AS _video_views_50percent,
        video_third_quartile_completions AS _video_views_75percent,
        video_completions AS _video_completions
    FROM `ergotron_linkedin_ads.ad_analytics_by_creative`
  
    ORDER BY start_at DESC
)
, ads_title AS (
    SELECT
        SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS _adid,
        campaign_id AS _campaignid,
        account_id AS _account_id,
        REGEXP_REPLACE(acc.name, r'[^a-zA-Z]', '') AS _account_name
    FROM `ergotron_linkedin_ads.creatives` c
    LEFT JOIN `ergotron_linkedin_ads.accounts` acc 
        ON acc.id = account_id
)
, campaigns AS (
    SELECT
        id AS _campaignid,
        name AS _campaignname,
        status AS _campaign_status,
        cost_type AS _cost_type,
        daily_budget.amount AS _daily_budget,
        campaign_group_id AS _campaign_group_id,
    FROM `ergotron_linkedin_ads.campaigns`
    
)
, campaign_group AS (
    SELECT
        id AS _campaign_group_id, 
        name AS _campaign_group_name, 
        status AS _campaign_group_status
    FROM `ergotron_linkedin_ads.campaign_groups`
)
, _all AS (
    SELECT
        LI_ads._start_year, 
        LI_ads._start_month, 
        LI_ads._start_day,
        LI_ads._end_month,
        LI_ads._end_year, 
        LI_ads._end_day,
        LI_ads._last_start_day,
        LI_ads._start_week,
        LI_ads._start_quater,
        LI_ads._start_month_num,
        LI_ads._weekday,
        LI_ads._start_month_name,
        LI_ads._start_week_num,
        LI_ads._date,
        LI_ads._quater_startdate,
        LI_ads._adid,
        LI_ads._startdate,
        LI_ads._enddate,
        --LI_ads._reach,
        LI_ads._impressions,
        LI_ads._clicks,
        LI_ads._conversions,
        LI_ads._spent,
        LI_ads._leads,
        LI_ads._landing_pages_clicks,
        --LI_ads._video_views,
        LI_ads._lead_form_opens,
        LI_ads._video_play,
        LI_ads._video_views_25percent,
        LI_ads._video_views_50percent,
        LI_ads._video_views_75percent,
        LI_ads._video_completions,
        LI_airtable._cta_copy, 
        LI_airtable._layout, 
        LI_airtable._size, 
        "LinkedIn" AS _platform, 
        LI_airtable._business_segment,
        LI_airtable._color,
        LI_airtable._image,
        LI_airtable._blurb,
        LI_airtable._logo,
        LI_airtable._messaging,
        LI_airtable._asset_type,
        LI_airtable._tone,
        LI_airtable._product_company_name,
        LI_airtable._statistic_proof_point,
        LI_airtable._ad_visual AS _screenshot,
        LI_airtable._ad_title_naming AS _creativedirections,
        LI_airtable._live_date,
        campaigns._campaignid,
        campaigns._campaignname,
        campaigns._campaign_status,
        campaign_group._campaign_group_id,
        campaign_group._campaign_group_name,
        campaigns._daily_budget,
        campaigns._cost_type,
        campaign_group._campaign_group_status,
        ads_title._account_id,
        ads_title._account_name
    FROM LI_ads
    RIGHT JOIN ads_title 
        ON CAST( LI_ads._adid AS STRING) = ads_title._adid
    LEFT JOIN campaigns 
        ON ads_title._campaignid = campaigns._campaignid 
    LEFT JOIN campaign_group 
        ON campaigns._campaign_group_id = campaign_group._campaign_group_id
    LEFT JOIN LI_airtable 
        ON CAST( LI_ads._adid AS STRING) = CAST(LI_airtable._adid AS STRING)
)
, total_ads AS (
    SELECT 
        *, 
        count(_adid) OVER (PARTITION BY _startDate, _campaignName ) AS ads_per_campaign
    FROM _all
)
, daily_budget_per_ad_per_campaign AS (
    SELECT 
        *,
        CASE WHEN ads_per_campaign > 0 THEN _daily_budget / ads_per_campaign
        ELSE 0 END AS dailyBudget_per_ad
    FROM total_ads
)
    SELECT 
        daily_budget_per_ad_per_campaign.*
    FROM daily_budget_per_ad_per_campaign