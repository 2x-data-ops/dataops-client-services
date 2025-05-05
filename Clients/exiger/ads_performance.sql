CREATE OR REPLACE TABLE `x-marketing.exiger.linkedin_ads_performance` AS
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
    FROM`x-marketing.exiger_google_sheets.db_ads_optimization`
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
        video_views AS _video_views,
        one_click_lead_form_opens AS _lead_form_opens,
        video_starts AS _video_play,
        video_first_quartile_completions AS _video_views_25percent,
        video_midpoint_completions AS _video_views_50percent,
        video_third_quartile_completions AS _video_views_75percent,
        video_completions AS _video_completions
    FROM `exiger_linkedin_ads.ad_analytics_by_creative`
  
    ORDER BY start_at DESC
)
, ads_title AS (
    SELECT
        SPLIT(SUBSTR(c.id, STRPOS(c.id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS _adid,
        campaign_id AS _campaignid,
        account_id AS _account_id,
        REGEXP_REPLACE(acc.name, r'[^a-zA-Z]', '') AS _account_name
    FROM `exiger_linkedin_ads.creatives` c
    LEFT JOIN `exiger_linkedin_ads.accounts` acc 
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
    FROM `exiger_linkedin_ads.campaigns`
    
)
, campaign_group AS (
    SELECT
        id AS _campaign_group_id, 
        name AS _campaign_group_name, 
        status AS _campaign_group_status
    FROM `exiger_linkedin_ads.campaign_groups`
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
    FROM daily_budget_per_ad_per_campaign;

----6sense

CREATE OR REPLACE TABLE `x-marketing.exiger.db_6sense_daily_campaign_performance` AS
WITH
  sixsense_airtable AS (
  SELECT
    DISTINCT _campaign_name AS _campaign_name,
    _campaign_id AS _campaign_id,
    _ad_id AS _ad_id,
    _ad_type AS _ad_type,
    _ad_variation AS _ad_variation,
    _ad_name AS _ad_name,
    _ad_name_length AS _ad_name_length_100_char_limit,
    _introduction_text AS _introduction_text,
    _intro_text_length AS _intro_text_length_125_char_limit,
    _headline_text AS _headline_text,
    _headline_text_length AS _headline_text_length_60_char_limit,
    _platform AS _platform,
    _business_segment AS _business_segment,
    _landing_page_url AS _landing_page_url,
    _ad_visual AS _ad_visual,
  IF ( _live_date != '', PARSE_TIMESTAMP('%m/%d/%Y', _live_date), NULL ) AS _live_date,
  IF ( _completed_date != '', PARSE_TIMESTAMP('%m/%d/%Y', _completed_date), NULL ) AS _completed_date,
    _status AS _status,
    _ad_group AS _ad_group,
    _ad_group_id AS _ad_group_id,
    _ad_group_length AS _ad_group_length,
    _job_title AS _job_title,
    _industry AS _industry,
    _size AS _size,
    _ad_title AS _ad_title,
    _ad_title_naming AS _ad_title_naming,
    _ad_title_length AS _ad_title_length,
    _sponsored_text AS _sponsored_text,
    _sponsored_text_length AS _sponsored_text_length,
    _body_text AS _body_text,
    _body_text_length AS _body_text_length_300_char_limit,
    _text_on_image AS _text_on_image,
    _text_on_image_length AS _text_on_image_length_55_char_limit,
    _cta_on_image AS _cta_on_image,
    _cta_on_image_length AS _cta_on_image_length_20_char_limit,
    _utm_source AS _utm_source,
    _utm_medium AS _utm_medium,
    _utm_campaign AS _utm_campaign,
    _utm_content AS _utm_content,
    _utm_term AS _utm_term,
    _utm_id AS _utm_id,
    _final_utm_tracking_url AS _final_utm_tracking_url,
    _final_ad_files_folder AS _final_ad_files_folder
  FROM
    `x-marketing.exiger_google_sheets.db_ads_optimization`
  WHERE
    _platform = '6sense' 
  ),
  sixsense_campaigns AS (
  SELECT
    DISTINCT _6senseid AS _campaignid,
    _name AS _campaignname
  FROM
    `x-marketing.exiger_mysql.db_daily_campaign_performance`
  WHERE
    _datatype = 'Campaign' 
  ),
  sixsense_base AS (
  SELECT
    base._6senseid AS _adid,
    base._name AS _adname,
    SAFE_CAST(base._campaignid AS INT64) AS _campaignid,
    sixsense_campaigns._campaignname,
    SAFE_CAST(base._adgroupid AS INT64) AS _adgroup
  FROM
    `x-marketing.exiger_mysql.db_daily_campaign_performance` AS base
  LEFT JOIN
    sixsense_campaigns
  ON
    base._campaignid = sixsense_campaigns._campaignid
  WHERE
    base._datatype = 'Ad'
  GROUP BY ALL 
  
  ),
  sixsense_performance AS (
  SELECT
    performance._enddate,
    performance._6senseid,
    performance._clicks,
    performance._6senseid AS _adid,
    performance._status,
    performance._adgroupid,
    performance._viewability,
    performance._name,
    CASE
      WHEN performance._date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', performance._date)
      ELSE PARSE_DATE('%F', performance._date) END AS _extractdate,
    performance._accountreached,
    performance._accountvtr,
    performance._ecpm,
    performance._ecpc,
    performance._vtr,
    performance._campaignid,
    performance._datatype,
    performance._viewthroughs,
    performance._spend,
    performance._campaigntype,
    performance._startdate,
    performance._accountctr,
    '' AS _cpc,
    CASE
      WHEN performance._date LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y',performance._date)
      ELSE PARSE_DATE('%F', performance._date) END AS _date,
    performance._budget,
    performance._impressions AS _impressions,
    performance._ctr
  FROM
    `x-marketing.exiger_mysql.db_daily_campaign_performance` AS performance 
  
  )
  
  SELECT
    performance._enddate,
    performance._6senseid,
    performance._clicks,
    performance._adid,
    performance._status,
    performance._adgroupid,
    performance._viewability,
    performance._name,
    performance._extractdate,
    performance._accountreached,
    performance._accountvtr,
    performance._ecpm,
    performance._ecpc,
    performance._vtr,
    performance._campaignid,
    performance._datatype,
    performance._viewthroughs,
    performance._spend,
    performance._campaigntype,
    performance._startdate,
    performance._accountctr,
    performance._cpc,
    performance._date,
    performance._budget,
    performance._impressions,
    performance._ctr,
    sixsense_base._adname,
    sixsense_base._campaignname,
    sixsense_base._adgroup,
    "6Sense" AS _platform,
    sixsense_airtable._campaign_name,
    sixsense_airtable._campaign_id,
    sixsense_airtable._ad_type,
    sixsense_airtable._ad_variation,
    sixsense_airtable._ad_name,
    sixsense_airtable._ad_name_length_100_char_limit,
    sixsense_airtable._introduction_text,
    sixsense_airtable._intro_text_length_125_char_limit,
    sixsense_airtable._headline_text,
    sixsense_airtable._headline_text_length_60_char_limit,
    sixsense_airtable._business_segment,
    sixsense_airtable._landing_page_url,
    sixsense_airtable._ad_visual,
    sixsense_airtable._live_date,
    sixsense_airtable._completed_date,
    sixsense_airtable._ad_group,
    sixsense_airtable._ad_group_id,
    sixsense_airtable._ad_group_length,
    sixsense_airtable._job_title,
    sixsense_airtable._industry,
    sixsense_airtable._size,
    sixsense_airtable._ad_title,
    sixsense_airtable._ad_title_naming,
    sixsense_airtable._ad_title_length,
    sixsense_airtable._sponsored_text,
    sixsense_airtable._sponsored_text_length,
    sixsense_airtable._body_text,
    sixsense_airtable._body_text_length_300_char_limit,
    sixsense_airtable._text_on_image,
    sixsense_airtable._text_on_image_length_55_char_limit,
    sixsense_airtable._cta_on_image,
    sixsense_airtable._cta_on_image_length_20_char_limit,
    sixsense_airtable._utm_source,
    sixsense_airtable._utm_medium,
    sixsense_airtable._utm_campaign,
    sixsense_airtable._utm_content,
    sixsense_airtable._utm_term,
    sixsense_airtable._utm_id,
    sixsense_airtable._final_utm_tracking_url,
    sixsense_airtable._final_ad_files_folder
FROM
  sixsense_performance AS performance
LEFT JOIN
  sixsense_base
ON
  sixsense_base._adid = performance._adid AND sixsense_base._adgroup = SAFE_CAST(performance._adgroupid AS INT64) 
LEFT JOIN
  sixsense_airtable
ON
  sixsense_base._adid = CAST(sixsense_airtable._ad_id AS STRING);
-----6sense

CREATE OR REPLACE TABLE `x-marketing.exiger.6sense_ads_performance` AS
WITH
  sixsense_airtable AS (
  SELECT
    DISTINCT _campaign_name AS _campaign_name,
    _campaign_id AS _campaign_id,
    _ad_id AS _ad_id,
    _ad_type AS _ad_type,
    _ad_variation AS _ad_variation,
    _ad_name AS _ad_name,
    _ad_name_length AS _ad_name_length_100_char_limit,
    _introduction_text AS _introduction_text,
    _intro_text_length AS _intro_text_length_125_char_limit,
    _headline_text AS _headline_text,
    _headline_text_length AS _headline_text_length_60_char_limit,
    _platform AS _platform,
    _business_segment AS _business_segment,
    _landing_page_url AS _landing_page_url,
    _ad_visual AS _ad_visual,
  IF
    ( _live_date != '', PARSE_TIMESTAMP('%m/%d/%Y', _live_date), NULL ) AS _live_date,
  IF
    ( _completed_date != '', PARSE_TIMESTAMP('%m/%d/%Y', _completed_date), NULL ) AS _completed_date,
    _status AS _status,
    _ad_group AS _ad_group,
    _ad_group_id AS _ad_group_id,
    _ad_group_length AS _ad_group_length,
    _job_title AS _job_title,
    _industry AS _industry,
    _size AS _size,
    _ad_title AS _ad_title,
    _ad_title_naming AS _ad_title_naming,
    _ad_title_length AS _ad_title_length,
    _sponsored_text AS _sponsored_text,
    _sponsored_text_length AS _sponsored_text_length,
    _body_text AS _body_text,
    _body_text_length AS _body_text_length_300_char_limit,
    _text_on_image AS _text_on_image,
    _text_on_image_length AS _text_on_image_length_55_char_limit,
    _cta_on_image AS _cta_on_image,
    _cta_on_image_length AS _cta_on_image_length_20_char_limit,
    _utm_source AS _utm_source,
    _utm_medium AS _utm_medium,
    _utm_campaign AS _utm_campaign,
    _utm_content AS _utm_content,
    _utm_term AS _utm_term,
    _utm_id AS _utm_id,
    _final_utm_tracking_url AS _final_utm_tracking_url,
    _final_ad_files_folder AS _final_ad_files_folder
  FROM
    `x-marketing.exiger_google_sheets.db_ads_optimization`
  WHERE
    _platform = '6sense' 
  
  ),
  sixsense_campaigns AS (
  SELECT
    DISTINCT _6senseid AS _campaignid,
    _name AS _campaignname
  FROM
    `x-marketing.exiger.db_6sense_daily_campaign_performance`
  WHERE
    _datatype = 'Campaign' 
  
  ),
  sixsense_base AS (
  SELECT
    base._6senseid AS _adid,
    base._name AS _adname,
    SAFE_CAST(base._campaignid AS INT64) AS _campaignid,
    sixsense_campaigns._campaignname,
    SAFE_CAST(base._adgroupid AS INT64) AS _adgroup,
    _date,
    SAFE_CAST(REPLACE(base._spend, '$', '') AS FLOAT64) AS _spend,
    SAFE_CAST(base._clicks AS INT64) AS _clicks,
    SAFE_CAST(REPLACE(base._impressions, ',', '') AS INT64) AS _impressions,
    base._accountreached AS _reach,
    0 AS _conversions,
    0 AS _video_views,
  FROM
    `x-marketing.exiger.db_6sense_daily_campaign_performance` AS base
  LEFT JOIN
    sixsense_campaigns
  ON
    base._campaignid = sixsense_campaigns._campaignid
  WHERE
    base._datatype = 'Ad' 
  
  )
  
  SELECT
  sixsense_base._adid,
  sixsense_base._adname,
  sixsense_base._campaignid,
  sixsense_base._campaignname,
  sixsense_base._adgroup,
  "6Sense" AS _platform,
  sixsense_base._date,
  sixsense_base._spend,
  sixsense_base._clicks,
  sixsense_base._impressions,
  sixsense_base._reach,
  sixsense_base._conversions,
  sixsense_base._video_views,
  sixsense_airtable._campaign_name,
  sixsense_airtable._campaign_id,
  sixsense_airtable._ad_type,
  sixsense_airtable._ad_variation,
  sixsense_airtable._ad_name,
  sixsense_airtable._ad_name_length_100_char_limit,
  sixsense_airtable._introduction_text,
  sixsense_airtable._intro_text_length_125_char_limit,
  sixsense_airtable._headline_text,
  sixsense_airtable._headline_text_length_60_char_limit,
  sixsense_airtable._business_segment,
  sixsense_airtable._landing_page_url,
  sixsense_airtable._ad_visual,
  sixsense_airtable._live_date,
  sixsense_airtable._completed_date,
  sixsense_airtable._ad_group,
  sixsense_airtable._ad_group_id,
  sixsense_airtable._ad_group_length,
  sixsense_airtable._job_title,
  sixsense_airtable._industry,
  sixsense_airtable._size,
  sixsense_airtable._ad_title,
  sixsense_airtable._ad_title_naming,
  sixsense_airtable._ad_title_length,
  sixsense_airtable._sponsored_text,
  sixsense_airtable._sponsored_text_length,
  sixsense_airtable._body_text,
  sixsense_airtable._body_text_length_300_char_limit,
  sixsense_airtable._text_on_image,
  sixsense_airtable._text_on_image_length_55_char_limit,
  sixsense_airtable._cta_on_image,
  sixsense_airtable._cta_on_image_length_20_char_limit,
  sixsense_airtable._utm_source,
  sixsense_airtable._utm_medium,
  sixsense_airtable._utm_campaign,
  sixsense_airtable._utm_content,
  sixsense_airtable._utm_term,
  sixsense_airtable._utm_id,
  sixsense_airtable._final_utm_tracking_url,
  sixsense_airtable._final_ad_files_folder
FROM
  sixsense_base
LEFT JOIN
  sixsense_airtable
ON
  sixsense_base._adid = CAST(sixsense_airtable._ad_id AS STRING)