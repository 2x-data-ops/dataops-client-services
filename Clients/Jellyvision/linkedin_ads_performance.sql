CREATE OR REPLACE TABLE `x-marketing.jellyvision.linkedin_ads_performance` AS
WITH LI_airtable AS (
    SELECT
        _adid, 
        _adtitle AS _adname, 
        _campaignid,  
        _campaignname, 
        _adgroup,
        _adcopy, 
        _ctacopy, 
        IF(LENGTH(_designtemplate) > 0, _designtemplate, _layout) AS _layout,
        _size, 
        _platform, 
        _segment,
        _designcolor,
        _designimages,
        _designblurp,
        _logos,
        _copymessaging,
        _copyassettype,
        _copytone,
        _copyproductcompanyname,
        _copystatisticproofpoint,
        _ctacopysofthard, 
        _screenshot,
        _creativedirections
    FROM `x-marketing.jellyvision_mysql.jellyvision_optimization_airtable_ads_linkedin`
    
    WHERE LENGTH(_adid) > 2
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
        video_views AS _video_views,
        one_click_lead_form_opens AS _lead_form_opens,
        video_starts AS _video_play,
        video_first_quartile_completions AS _video_views_25percent,
        video_midpoint_completions AS _video_views_50percent,
        video_third_quartile_completions AS _video_views_75percent,
        video_completions AS _video_completions
    FROM `jellyvision_linkedin_ads.ad_analytics_by_creative`
  
    ORDER BY start_at DESC
)
, ads_title AS (
    SELECT
        SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS _adid,
        campaign_id AS _campaignid,
        account_id AS _account_id,
        REGEXP_REPLACE(acc.name, r'[^a-zA-Z]', '') AS _account_name
    FROM `jellyvision_linkedin_ads.creatives` c
        LEFT JOIN `jellyvision_linkedin_ads.accounts` acc ON acc.id = account_id
)
, campaigns AS (
    SELECT
        id AS _campaignid,
        name AS _campaignname,
        status AS _campaign_status,
        cost_type AS _cost_type,
        daily_budget.amount AS _daily_budget,
        campaign_group_id AS _campaign_group_id,
    FROM `jellyvision_linkedin_ads.campaigns`
    
)
, campaign_group AS (
    SELECT
        id AS _campaign_group_id, 
        name AS _campaign_group_name, 
        status AS _campaign_group_status
    FROM `jellyvision_linkedin_ads.campaign_groups`
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
        LI_ads._video_views,
        LI_ads._lead_form_opens,
        LI_ads._video_play,
        LI_ads._video_views_25percent,
        LI_ads._video_views_50percent,
        LI_ads._video_views_75percent,
        LI_ads._video_completions,
        LI_airtable._adcopy, 
        LI_airtable._ctacopy, 
        LI_airtable._layout,
        LI_airtable._size, 
        "LinkedIn" AS _platform, 
        LI_airtable._segment,
        LI_airtable._designcolor,
        LI_airtable._designimages,
        LI_airtable._designblurp,
        LI_airtable._logos,
        LI_airtable._copymessaging,
        LI_airtable._copyassettype,
        LI_airtable._copytone,
        LI_airtable._copyproductcompanyname,
        LI_airtable._copystatisticproofpoint,
        LI_airtable._ctacopysofthard, 
        LI_airtable._screenshot,
        LI_airtable._creativedirections,
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
        RIGHT JOIN ads_title ON CAST( LI_ads._adid AS STRING) = ads_title._adid
        LEFT JOIN campaigns ON ads_title._campaignid = campaigns._campaignid 
        LEFT JOIN campaign_group ON campaigns._campaign_group_id = campaign_group._campaign_group_id
        LEFT JOIN LI_airtable ON CAST( LI_ads._adid AS STRING) = CAST(LI_airtable._adid AS STRING)
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